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

import java.util.Set;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;

import de.pangaea.metadataportal.config.Config;
import de.pangaea.metadataportal.config.HarvesterConfig;
import de.pangaea.metadataportal.processor.DocumentProcessor;
import de.pangaea.metadataportal.processor.ElasticsearchConnection;
import de.pangaea.metadataportal.processor.MetadataDocument;

/**
 * Index rebuilder implemented as harvester that reads all documents from an
 * index and pushes them back into the index. Only the XML blobs and control
 * fields are read, all other info is rebuild like in the normal harvester. This
 * helps during restructuring the index fields. This can be done without
 * re-harvesting from the original metadata providers. This class is called from
 * command line or using {@link Harvester#runHarvester} with this class as
 * harvester class parameter.
 * 
 * @author Uwe Schindler
 */
public class Rebuilder extends Harvester {
  
  private static final org.apache.commons.logging.Log staticLog = org.apache.commons.logging.LogFactory
      .getLog(Rebuilder.class);
  
  // main-Methode
  public static void main(String[] args) {
    if (args.length < 1 || args.length > 2) {
      System.err.println("Command line: java " + Rebuilder.class.getName()
          + " config.xml [index-name|*]");
      return;
    }
    
    try {
      Config conf = new Config(args[0]);
      runHarvester(conf, (args.length == 2) ? args[1] : "*", Rebuilder.class);
    } catch (Exception e) {
      staticLog.fatal("Rebuilder general error:", e);
    }
  }
  
  // harvester interface
  private Harvester wrappedHarvester = null;
  private Client client = null;
  private String sourceIndex = null;
  private int bulkSize = DocumentProcessor.DEFAULT_BULK_SIZE;
  
  public Rebuilder(HarvesterConfig iconfig) {
    super(iconfig);
  }

  @Override
  public void open(ElasticsearchConnection es) throws Exception {
    this.sourceIndex = iconfig.properties.getProperty("targetIndex", DocumentProcessor.DEFAULT_INDEX);
    this.bulkSize = Integer.parseInt(iconfig.properties.getProperty("bulkSize", Integer.toString(DocumentProcessor.DEFAULT_BULK_SIZE)));
    log.info("Opening Elasticsearch index '" + sourceIndex + "' for rebuilding all documents of harvester '" + iconfig.id + "'...");

    this.wrappedHarvester = iconfig.harvesterClass.getConstructor(HarvesterConfig.class).newInstance(iconfig);
    
    this.client = es.client();
    super.open(es);
  }
  
  @Override
  public void close(boolean cleanShutdown) throws Exception {
    client = null;
    super.close(cleanShutdown);
  }

  @Override
  public MetadataDocument createMetadataDocumentInstance() {
    return wrappedHarvester.createMetadataDocumentInstance();
  }
  
  @Override
  protected void enumerateValidHarvesterPropertyNames(Set<String> props) {
    props.addAll(wrappedHarvester.getValidHarvesterPropertyNames());
  }
  
  @Override
  public void harvest() throws Exception {
    if (client == null) throw new IllegalStateException("Rebuilder was not opened!");
    final TimeValue time = TimeValue.timeValueMinutes(10);
    SearchResponse scrollResp = client.prepareSearch(sourceIndex)
      .setTypes(iconfig.parent.typeName)
      .addFields(iconfig.parent.fieldnameDatestamp, iconfig.parent.fieldnameXML, iconfig.parent.fieldnameSource)
      .setQuery(QueryBuilders.termQuery(iconfig.parent.fieldnameSource, iconfig.id))
      .setFetchSource(false)
      .setSize(bulkSize)
      .setSearchType(SearchType.SCAN).setScroll(time)
      .get();
    do {
      scrollResp = client.prepareSearchScroll(scrollResp.getScrollId())
        .setScroll(time)
        .get();
      for (final SearchHit hit : scrollResp.getHits()) {
        SearchHitField fld = hit.field(iconfig.parent.fieldnameSource);
        if (fld == null || !iconfig.id.equals(fld.getValue())) {
          log.warn("Document '" + hit.getId() + "' is from an invalid source, the harvester ID does not match! This may be caused by an invalid Elasticsearch mapping.");
          continue;
        }
        MetadataDocument mdoc = createMetadataDocumentInstance();
        mdoc.loadFromElasticSearchHit(hit);
        if (mdoc.getXML() == null) {
          mdoc.setDeleted(true);
          log.warn("Document '" + mdoc.getIdentifier() + "' contains no XML code. It will be deleted!");
        }
        addDocument(mdoc);
      }
    } while (scrollResp.getHits().getHits().length > 0);
  }
  
}