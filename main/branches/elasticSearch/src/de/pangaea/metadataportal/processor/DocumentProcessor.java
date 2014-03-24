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

package de.pangaea.metadataportal.processor;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndexMissingException;

import de.pangaea.metadataportal.config.HarvesterConfig;

/**
 * Component of <b>panFMP</b> that analyzes and indexes harvested documents in
 * different threads.
 * 
 * @author Uwe Schindler
 */
public final class DocumentProcessor {
  private static final Log log = LogFactory.getLog(DocumentProcessor.class);
  
  protected final HarvesterConfig iconfig;
  protected final Client client;
  protected final String targetIndex;
  
  private Date lastHarvested = null;
  
  private static final MetadataDocument MDOC_EOF = new MetadataDocument(null);
  private final BlockingQueue<MetadataDocument> mdocBuffer;
    
  private final AtomicReference<Exception> failure = new AtomicReference<Exception>(null);
  private final AtomicReference<CommitEvent> commitEvent = new AtomicReference<CommitEvent>(null);
  private Set<String> validIdentifiers = null;
  
  private final AtomicInteger updated = new AtomicInteger(0), deleted = new AtomicInteger(0);
  
  private final int bulkSize;
  private final DocumentErrorAction conversionErrorAction;
  
  private final AtomicInteger runningThreads = new AtomicInteger(0);
  private ThreadGroup threadGroup;
  private Thread[] threadList;
  private boolean threadsStarted = false;
  
  DocumentProcessor(Client client, HarvesterConfig iconfig) {
    this.client = client;
    this.iconfig = iconfig;
    
    targetIndex = iconfig.harvesterProperties.getProperty("targetIndex", "panfmp");
    bulkSize = Integer.parseInt(iconfig.harvesterProperties
        .getProperty("bulkSize", "100"));
    
    final String s = iconfig.harvesterProperties.getProperty("conversionErrorAction", "STOP");
    try {
      conversionErrorAction = DocumentErrorAction.valueOf(s.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid value '"
              + s
              + "' for harvester property 'conversionErrorAction', valid ones are: "
              + Arrays.toString(DocumentErrorAction.values()));
    }
    
    int threadCount = Integer.parseInt(iconfig.harvesterProperties.getProperty(
        "numThreads", "1"));
    if (threadCount < 1) throw new IllegalArgumentException(
        "numThreads harvester-property must be >=1!");
    
    int size = Integer.parseInt(iconfig.harvesterProperties.getProperty(
        "maxQueue", "100"));
    if (size < threadCount) throw new IllegalArgumentException(
        "maxQueue must be >=numThreads!");
    mdocBuffer = new ArrayBlockingQueue<MetadataDocument>(size, true);
    
    // threads
    threadGroup = new ThreadGroup(getClass().getName()
        + "#ThreadGroup");
    threadList = new Thread[threadCount];
    for (int i = 0; i < threadCount; i++) {
      threadList[i] = new Thread(threadGroup, new Runnable() {
        @Override
        public void run() {
          threadRun();
        }
      }, getClass().getName() + "#" + (i + 1));
    }
  }
  
  public boolean isFailed() {
    return (failure.get() != null);
  }
  
  public void registerHarvesterCommitEvent(CommitEvent event) {
    commitEvent.set(event);
  }
  
  public void setValidIdentifiers(Set<String> validIdentifiers) {
    this.validIdentifiers = validIdentifiers;
  }
  
  public boolean isClosed() {
    return (threadGroup == null || threadList == null);
  }
  
  public void close() throws Exception {
    if (isClosed()) throw new IllegalStateException("DocumentProcessor already closed");
    
    if (threadsStarted) {
      try {
        for (int i = 0; i < threadList.length; i++)
          mdocBuffer.put(MDOC_EOF);
        for (Thread t : threadList) {
          if (t.isAlive()) t.join();
        }
      } catch (InterruptedException e) {
        log.error(e);
      }
    }
      
    threadGroup = null;
    threadList = null;
    
    // check for validIdentifiers Set and remove all unknown identifiers from
    // index, if available
    if (validIdentifiers != null) {
      log.info("Removing documents not seen while harvesting (this may take a while)...");
      final QueryBuilder query = QueryBuilders.boolQuery()
          .must(QueryBuilders.termQuery(iconfig.parent.fieldnameSource, iconfig.id))
          .mustNot(QueryBuilders.idsQuery(iconfig.parent.typeName).ids(validIdentifiers.toArray(new String[validIdentifiers.size()])));
      client.prepareDeleteByQuery(targetIndex).setTypes(iconfig.parent.typeName).setQuery(query).execute().actionGet();
    }
    
    // exit here before we write status info to disk!
    Exception f = failure.get();
    if (f != null) throw f;
    
    // save datestamp
    saveLastHarvestedOnDisk();
    
    log.info(deleted + " docs presumably deleted (only if existent) and "
        + updated + " docs (re-)indexed - finished.");
  }
  
  public void addDocument(MetadataDocument mdoc) throws BackgroundFailure, InterruptedException {
    if (isClosed()) throw new IllegalStateException("DocumentProcessor already closed");
    throwFailure();
    startThreads();
    
    mdocBuffer.put(mdoc);
  }
  
  // sets the date of last harvesting (written to disk after closing!!!)
  public void setLastHarvested(Date datestamp) {
    this.lastHarvested = datestamp;
  }
  
  public Date getLastHarvestedFromDisk() {
    Date d = null;
    try {
      final GetResponse resp = client.prepareGet(targetIndex, "panfmp_meta", iconfig.id)
          .setFields("lastHarvested").setFetchSource(false).execute().actionGet();
      final Object v;
      if (resp.isExists() && (v = resp.getField("lastHarvested").getValue()) != null) {
        d = XContentBuilder.defaultDatePrinter
            .parseDateTime(v.toString())
            .toDate();
      }
    } catch (IndexMissingException e) {
      d = null;
    }
    return d;
  }
  
  private void saveLastHarvestedOnDisk() {
    if (lastHarvested != null) {
      client.prepareIndex(targetIndex, "panfmp_meta", iconfig.id)
        .setSource("lastHarvested", lastHarvested)
        .execute().actionGet();
      lastHarvested = null;
    }
  }
  
  void threadRun() {
    final Log log = LogFactory.getLog(Thread.currentThread().getName());
    log.info("Processor thread started.");
    boolean finished = false;
    try {
      final HashSet<String> committedIdentifiers = new HashSet<String>(bulkSize);
      BulkRequestBuilder bulkRequest = client.prepareBulk();
      
      while (failure.get() == null) {
        final MetadataDocument mdoc;
        try {
          mdoc = mdocBuffer.take();
        } catch (InterruptedException ie) {
          continue;
        }
        if (mdoc == MDOC_EOF) break;
        
        if (log.isDebugEnabled()) log.debug("Converting document: "
            + mdoc.toString());
        if (log.isTraceEnabled()) log.trace("XML: " + mdoc.getXML());
        XContentBuilder json = null;
        try {
          json = mdoc.getElasticSearchJSON();
        } catch (InterruptedException ie) {
          throw ie; // no handling here
        } catch (Exception e) {
          // handle exception
          boolean ignore = false;
          switch (conversionErrorAction) {
            case IGNOREDOCUMENT:
              log.error(
                  "Conversion XML to Elasticsearch document failed for '"
                      + mdoc.getIdentifier() + "' (object ignored):", e);
              json = null;
              ignore = true;
              break;
            case DELETEDOCUMENT:
              log.error(
                  "Conversion XML to Elasticsearch document failed for '"
                      + mdoc.getIdentifier() + "' (object marked deleted):", e);
              json = null;
              break;
            default:
              log.fatal("Conversion XML to Lucene document failed for '"
                  + mdoc.getIdentifier() + "' (fatal, stopping conversions).");
              throw e;
          }
          if (ignore) {
            continue; // next entry in buffer
          }
        }

        if (json == null) {
          if (log.isDebugEnabled()) log.debug("Deleting document: " + mdoc.getIdentifier());
          bulkRequest.add(
            client.prepareDelete(targetIndex, iconfig.parent.typeName, mdoc.getIdentifier())
          );
          deleted.incrementAndGet();
        } else {
          if (log.isDebugEnabled()) log.debug("Updating document: " + mdoc.getIdentifier());
          if (log.isTraceEnabled()) log.trace("Data: " + json.string());
          bulkRequest.add(
            client.prepareIndex(targetIndex, iconfig.parent.typeName, mdoc.getIdentifier()).setSource(json)
          );
          updated.incrementAndGet();
        }
        committedIdentifiers.add(mdoc.getIdentifier());
        
        if (bulkRequest.numberOfActions() >= bulkSize) {
          pushBulk(bulkRequest, committedIdentifiers);
          
          // create new bulk
          bulkRequest = client.prepareBulk();
        }
      }

      if (bulkRequest.numberOfActions() > 0) {
        pushBulk(bulkRequest, committedIdentifiers);
      }
      
      finished = true;
    } catch (Exception e) {
      if (!finished) log.warn("Only " + deleted
          + " docs presumably deleted (only if existent) and " + updated
          + " docs (re-)indexed before the following error occurred: " + e);
      // only store the first error in failure variable, other errors are logged
      // only
      if (!failure.compareAndSet(null, e)) log.error(e);
    } finally {
      if (runningThreads.decrementAndGet() == 0) {
        mdocBuffer.clear();
      }
      log.info("Processor thread stopped.");
    }
  }
  
  private void pushBulk(BulkRequestBuilder bulkRequest, Set<String> committedIdentifiers) throws IOException {
    final Log log = LogFactory.getLog(Thread.currentThread().getName());
    
    assert committedIdentifiers.size() <= bulkRequest.numberOfActions();
    
    BulkResponse bulkResponse = bulkRequest.execute().actionGet();
    if (bulkResponse.hasFailures()) {
      throw new IOException("Error while executing bulk request: " + bulkResponse.buildFailureMessage());
    }
    
    log.info(deleted + " docs presumably deleted (if existent) and "
        + updated + " docs (re-)indexed so far.");
    
    // notify Harvester of index commit
    final CommitEvent ce = commitEvent.get();
    if (ce != null) ce.harvesterCommitted(Collections
        .unmodifiableSet(committedIdentifiers));
    committedIdentifiers.clear();    
  }
  
  private void throwFailure() throws BackgroundFailure {
    Exception f = failure.get();
    if (f != null) {
      if (threadGroup != null) threadGroup.interrupt();
      throw new BackgroundFailure(f);
    }
  }
  
  private void startThreads() {
    if (!threadsStarted) try {
      for (Thread t : threadList) {
        runningThreads.incrementAndGet();
        t.start();
      }
    } finally {
      threadsStarted = true;
    }
  }
  
}