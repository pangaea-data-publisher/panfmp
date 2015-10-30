/*
 *   Copyright panFMP Developers Team c/o Uwe Schindler
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package de.pangaea.metadataportal.harvester;

import java.io.StringReader;
import java.util.Arrays;
import java.util.Date;
import java.util.Set;

import javax.xml.transform.stream.StreamSource;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;

import de.pangaea.metadataportal.config.HarvesterConfig;
import de.pangaea.metadataportal.processor.DocumentProcessor;
import de.pangaea.metadataportal.processor.ElasticsearchConnection;
import de.pangaea.metadataportal.utils.HostAndPort;

/**
 * TODO
 * <p>
 * This harvester supports the following additional <b>harvester properties</b>:
 * <ul>
 * <li><code>identifierPrefix</code>: This prefix is added in front of all
 * identifiers from the foreign ES instance (default: "")</li>
 * <li>TODO</li>
 * </ul>
 * 
 * @author Uwe Schindler
 */
public class ElasticsearchHarvester extends SingleFileEntitiesHarvester {
  
  // Class members
  private final String identifierPrefix, xmlField, datestampField;
  private final String[] sourceIndexes, types;
  private final QueryBuilder query;
  private final String queryInfo;
  private final int bulkSize;
  private boolean closeClient = false;
  private Client client = null;
  
  public ElasticsearchHarvester(HarvesterConfig iconfig) {
    super(iconfig);

    bulkSize = Integer.parseInt(iconfig.properties.getProperty("bulkSize", Integer.toString(DocumentProcessor.DEFAULT_BULK_SIZE)));
    identifierPrefix = iconfig.properties.getProperty("identifierPrefix", "");
    datestampField = iconfig.properties.getProperty("datestampField", iconfig.root.fieldnameDatestamp);
    xmlField = iconfig.properties.getProperty("xmlField", iconfig.root.fieldnameXML);
    final String v = iconfig.properties.getProperty("indexes");
    if (v == null || v.isEmpty()) {
      throw new IllegalArgumentException("Missing harvester property 'indexes'.");
    }
    sourceIndexes = v.split("\\s*,\\s*");
    types = iconfig.properties.getProperty("types", iconfig.root.typeName).split("\\s*,\\s*");

    final String qstr = iconfig.properties.getProperty("queryString"),
        jsonQuery = iconfig.properties.getProperty("jsonQuery");
    final boolean hasQstr = (qstr != null && !qstr.isEmpty()),
        hasJsonQuery = (jsonQuery != null && !jsonQuery.isEmpty());
    if (hasQstr && hasJsonQuery) {
      throw new IllegalArgumentException("Cannot give both 'queryString' and 'jsonQuery' harvester property.");
    } else if (hasQstr) {
      queryInfo = "documents matching query [" + qstr + "]";
      query = QueryBuilders.queryStringQuery(qstr);
    } else if (hasJsonQuery) {
      queryInfo = "documents matching JSON query";
      query = QueryBuilders.wrapperQuery(jsonQuery);
    } else {
      assert !hasJsonQuery && !hasQstr;
      queryInfo = "all documents";
      query = QueryBuilders.matchAllQuery();
    }
  }

  @SuppressWarnings("resource")
  @Override
  public void open(ElasticsearchConnection es, String targetIndex) throws Exception {
    super.open(es, targetIndex);

    final String esAddress = iconfig.properties.getProperty("elasticsearchAddress");
    if (esAddress != null && !esAddress.isEmpty()) {
      // TODO: Really use ES settings from config!? => make configurable somehow
      final Settings settings = iconfig.root.esSettings == null ? Settings.Builder.EMPTY_SETTINGS : iconfig.root.esSettings;
      final InetSocketTransportAddress addr = new InetSocketTransportAddress(HostAndPort.parse(esAddress, ElasticsearchConnection.ELASTICSEARCH_DEFAULT_PORT));
      
      log.info("Connecting to external Elasticsearch node " + addr + " for harvesting " + queryInfo + "...");
      if (log.isDebugEnabled()) {
        log.debug("ES connection settings: " + settings.getAsMap());
      }
      this.client = TransportClient.builder().settings(settings).build().addTransportAddress(addr);
      this.closeClient = true;
    } else {
      log.info("Connecting to global Elasticsearch node for harvesting " + queryInfo + "...");
      this.closeClient = false;
      this.client = es.client();
    }
  }
  
  @Override
  public void close(boolean cleanShutdown) throws Exception {
    try {
      if (closeClient && client != null) {
        // separate client, not the shared one!
        client.close();
      }
    } finally {
      client = null;
      closeClient = false;
      super.close(cleanShutdown);
    }
  }
  
  @Override
  public void harvest() throws Exception {
    if (client == null) throw new IllegalStateException("Harvester was not opened!");
    final TimeValue time = TimeValue.timeValueMinutes(10);
    SearchResponse scrollResp = client.prepareSearch(sourceIndexes)
      .setTypes(types)
      .addFields(datestampField, xmlField)
      .setQuery(query)
      .setFetchSource(false)
      .setSize(bulkSize)
      .setSearchType(SearchType.SCAN).setScroll(time)
      .get();
    do {
      for (final SearchHit hit : scrollResp.getHits()) {
        addSearchHit(hit);
      }
      if (scrollResp.getScrollId() == null) break;
      scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(time).get();
    } while (scrollResp.getHits().getHits().length > 0);
  }
  
  private void addSearchHit(SearchHit hit) throws Exception {
    final String identifier = identifierPrefix + hit.getId();
    
    // try to read date stamp
    Date datestamp = null;
    final SearchHitField dateFld = hit.field(datestampField);
    final String datestampStr = (dateFld == null) ? null : dateFld.<String>getValue();
    if (datestampStr != null) {
      try {
        datestamp = XContentBuilder.defaultDatePrinter.parseDateTime(datestampStr).toDate();
      } catch (IllegalArgumentException iae) {
        log.warn("Datestamp of document '" + identifier + "' is invalid: " + iae.getMessage() + " - Deleting datestamp.");
        datestamp = null;
      }
    }

    if (isDocumentOutdated(datestamp)) {
      // read XML
      final SearchHitField xmlFld = hit.field(xmlField);
      final String xml = (xmlFld == null) ? null : xmlFld.<String>getValue();
      if (xml != null) {
        addDocument(identifier, datestamp, new StreamSource(new StringReader(xml), identifier));
      } else {
        log.warn("Document '" + identifier + "' has no XML contents, ignoring.");
      }
    } else {
      // update datestamp only
      addDocument(identifier, datestamp, null);
    }
  }
  
  @Override
  protected void enumerateValidHarvesterPropertyNames(Set<String> props) {
    super.enumerateValidHarvesterPropertyNames(props);
    props.addAll(Arrays.asList("elasticsearchCluster", "datestampField", "xmlField", "indexes",
        "types", "identifierPrefix", "queryString", "jsonQuery"));
  }
  
}