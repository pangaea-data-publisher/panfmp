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

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.Bits;

import de.pangaea.metadataportal.config.Config;
import de.pangaea.metadataportal.config.HarvesterConfig;
import de.pangaea.metadataportal.processor.ElasticSearchConnection;
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
  private IndexReader reader = null;
  
  @Override
  public void open(ElasticSearchConnection es, HarvesterConfig iconfig) throws Exception {
    log.info("Opening index \"" + iconfig.id
        + "\" for harvesting all documents...");
    super.open(es, iconfig);
  }
  
  @Override
  public void close(boolean cleanShutdown) throws Exception {
    if (reader != null) reader.close();
    reader = null;
    super.close(cleanShutdown);
  }
  
  @Override
  protected MetadataDocument createMetadataDocumentInstance() {
    throw new UnsupportedOperationException(
        "The rebuilder uses an internal mechanism to generate metadata documents.");
  }
  
  @Override
  public void harvest() throws Exception {
    if (reader == null) throw new IllegalStateException(
        "Rebuilder was not opened!");
    for (final AtomicReaderContext ctx : reader.leaves()) {
      final AtomicReader r = ctx.reader();
      final Bits liveDocs = r.getLiveDocs();
      for (int i = 0, c = r.maxDoc(); i < c; i++) {
        if (liveDocs.get(i)) {
          MetadataDocument mdoc = null; //TODO: MetadataDocument.createInstanceFromLucene(iconfig, r.document(i));
          if (mdoc.getIdentifier() == null) {
            log.error("Cannot process or delete a document without an identifier! "
                + "It will stay forever in index and pollute search results. "
                + "You should drop index and re-harvest!");
            continue;
          }
          if (mdoc.getXML() == null) {
            mdoc.setDeleted(true);
            log.warn("Document '" + mdoc.getIdentifier()
                + "' contains no XML code. It will be deleted!");
          }
          addDocument(mdoc);
        }
      }
    }
  }
  
}