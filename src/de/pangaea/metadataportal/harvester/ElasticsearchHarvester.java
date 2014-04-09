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
import java.text.ParseException;
import java.util.Arrays;
import java.util.Date;
import java.util.Set;

import javax.xml.transform.stream.StreamSource;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import de.pangaea.metadataportal.config.HarvesterConfig;
import de.pangaea.metadataportal.processor.DocumentProcessor;
import de.pangaea.metadataportal.processor.ElasticsearchConnection;
import de.pangaea.metadataportal.utils.LenientDateParser;

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
  private String identifierPrefix = "", xmlField = null, datestampField = null;
  private String[] sourceIndexes = null, types = null;
  private QueryBuilder query = null;
  private ElasticsearchConnection es = null;
  private Client client = null;
  private int bulkSize = DocumentProcessor.DEFAULT_BULK_SIZE;
  
  public ElasticsearchHarvester(HarvesterConfig iconfig) {
    super(iconfig);
  }

  @Override
  public void open(ElasticsearchConnection es) throws Exception {
    super.open(es);
    this.es = es;
    
    // TODO: use our own ES instance, make configureable!!!
    client = es.client();
    
    bulkSize = Integer.parseInt(iconfig.harvesterProperties.getProperty("bulkSize", Integer.toString(DocumentProcessor.DEFAULT_BULK_SIZE)));
    identifierPrefix = iconfig.harvesterProperties.getProperty("identifierPrefix", "");
    datestampField = iconfig.harvesterProperties.getProperty("datestampField", iconfig.parent.fieldnameDatestamp);
    xmlField = iconfig.harvesterProperties.getProperty("xmlField", iconfig.parent.fieldnameXML);
    sourceIndexes = iconfig.harvesterProperties.getProperty("indexes", "").split("\\\\s*,\\s*");
    types = iconfig.harvesterProperties.getProperty("types", iconfig.parent.typeName).split("\\\\s*,\\s*");

    final String info, qstr = iconfig.harvesterProperties.getProperty("queryString"),
        jsonQuery = iconfig.harvesterProperties.getProperty("jsonQuery");
    final boolean hasQstr = (qstr != null && !qstr.isEmpty()),
        hasJsonQuery = (jsonQuery != null && !jsonQuery.isEmpty());
    if (hasQstr && hasJsonQuery) {
      throw new IllegalArgumentException("Cannot give both 'queryString' and 'jsonQuery' harvester property.");
    } else if (hasQstr) {
      info = "documents matching query [" + qstr + "]";
      query = QueryBuilders.queryString(qstr);
    } else if (hasJsonQuery) {
      info = "documents matching JSON query";
      query = QueryBuilders.wrapperQuery(jsonQuery);
    } else {
      assert !hasJsonQuery && !hasQstr;
      info = "all documents";
      query = QueryBuilders.matchAllQuery();
    }
        
    log.info("Connecting to Elasticsearch '" + client + "' for harvesting " + info + "...");
  }
  
  @Override
  public void close(boolean cleanShutdown) throws Exception {
    query = null;
    try {
      if (es == null) {
        // separate client, not the shared one!
        client.close();
      }
    } finally {
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
      scrollResp = client.prepareSearchScroll(scrollResp.getScrollId())
        .setScroll(time)
        .get();
      for (final SearchHit hit : scrollResp.getHits()) {
        addSearchHit(hit);
      }
    } while (scrollResp.getHits().getHits().length > 0);
  }
  
  private void addSearchHit(SearchHit hit) throws Exception {
    final String identifier = identifierPrefix + hit.getId();
    
    // try to read date stamp
    Date datestamp = null;
    final String v = hit.field(datestampField).getValue();
    if (v != null) {
      try {
        datestamp = LenientDateParser.parseDate(v);
      } catch (ParseException e) {
        log.warn("Datestamp of document '" + identifier + "' is invalid: " + e.getMessage() + " - Deleting datestamp.");
        datestamp = null;
      }
    }

    if (isDocumentOutdated(datestamp)) {
      // read XML
      final String xml = hit.field(xmlField).getValue();
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
    props.addAll(Arrays.<String>asList("datestampField", "xmlField", "indexes", "types", "identifierPrefix", "queryString", "jsonQuery"));
  }
  
}