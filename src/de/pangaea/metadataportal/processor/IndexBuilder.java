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

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import de.pangaea.metadataportal.config.IndexConfig;
import de.pangaea.metadataportal.harvester.HarvesterCommitEvent;
import de.pangaea.metadataportal.utils.IndexConstants;

/**
 * Component of <b>panFMP</b> that analyzes and indexes harvested documents in
 * different threads.
 * 
 * @author Uwe Schindler
 */
public final class IndexBuilder {
  private static final org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory
      .getLog(IndexBuilder.class);
  
  protected final IndexConfig iconfig;
  protected final Client client;
  
  private Date lastHarvested = null;
  
  private static MetadataDocument MDOC_EOF = new MetadataDocument(null);
  private static IndexerQueueEntry LDOC_EOF = new IndexerQueueEntry(null, null);
  
  private AtomicInteger runningConverters = new AtomicInteger(0);
  private AtomicReference<Exception> failure = new AtomicReference<Exception>(
      null);
  private AtomicReference<HarvesterCommitEvent> commitEvent = new AtomicReference<HarvesterCommitEvent>(
      null);
  private AtomicReference<Set<String>> validIdentifiers = new AtomicReference<Set<String>>(
      null);
  
  private BlockingQueue<MetadataDocument> mdocBuffer;
  private BlockingQueue<IndexerQueueEntry> ldocBuffer;
  
  private final Object indexerLock = new Object();
  
  private int maxBufferedChanges;
  private DocumentErrorAction conversionErrorAction = DocumentErrorAction.STOP;
  
  private Thread indexerThread;
  private ThreadGroup converterThreads;
  private Thread[] converterThreadList;
  private boolean threadsStarted = false;
  
  IndexBuilder(Client client, IndexConfig iconfig) {
    this.client = client;
    this.iconfig = iconfig;
    
    maxBufferedChanges = Integer.parseInt(iconfig.harvesterProperties
        .getProperty("maxBufferedIndexChanges", "100"));
    
    String s = iconfig.harvesterProperties.getProperty("conversionErrorAction");
    if (s != null) try {
      conversionErrorAction = DocumentErrorAction.valueOf(s
          .toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid value '"
              + s
              + "' for harvester property 'conversionErrorAction', valid ones are: "
              + Arrays.toString(DocumentErrorAction.values()));
    }
    
    int threadCount = Integer.parseInt(iconfig.harvesterProperties.getProperty(
        "numConverterThreads", "1"));
    if (threadCount < 1) throw new IllegalArgumentException(
        "numConverterThreads harvester-property must be >=1!");
    
    int size = Integer.parseInt(iconfig.harvesterProperties.getProperty(
        "maxConverterQueue", "250"));
    if (size < threadCount) throw new IllegalArgumentException(
        "maxConverterQueue must be >=numConverterThreads!");
    mdocBuffer = new ArrayBlockingQueue<MetadataDocument>(size, true);
    
    size = Integer.parseInt(iconfig.harvesterProperties.getProperty(
        "maxIndexerQueue", "250"));
    if (size < 1) throw new IllegalArgumentException(
        "maxIndexerQueue must be >=1!");
    ldocBuffer = new ArrayBlockingQueue<IndexerQueueEntry>(size, false);
    
    // converter threads
    converterThreads = new ThreadGroup(getClass().getName()
        + "#Converter#ThreadGroup");
    converterThreadList = new Thread[threadCount];
    for (int i = 0; i < threadCount; i++) {
      converterThreadList[i] = new Thread(converterThreads, new Runnable() {
        public void run() {
          converterThreadRun();
        }
      }, getClass().getName() + "#Converter#" + (i + 1));
    }
    
    // indexer
    indexerThread = new Thread(new Runnable() {
      public void run() {
        indexerThreadRun();
      }
    }, getClass().getName() + "#Indexer");
  }
  
  public boolean isFailed() {
    return (failure.get() != null);
  }
  
  public void registerHarvesterCommitEvent(HarvesterCommitEvent event) {
    commitEvent.set(event);
  }
  
  public void setValidIdentifiers(Set<String> validIdentifiers) {
    this.validIdentifiers.set(validIdentifiers);
  }
  
  public boolean isClosed() {
    return (indexerThread == null || converterThreads == null || converterThreadList == null);
  }
  
  public void close() throws Exception {
    if (isClosed()) throw new IllegalStateException(
        "IndexBuilder already closed");
    
    startThreads(true);
    
    try {
      for (int i = 0; i < converterThreadList.length; i++)
        mdocBuffer.put(MDOC_EOF);
      for (Thread t : converterThreadList) {
        if (t.isAlive()) t.join();
      }
      
      // if ldocBuffer not empty there were already some threads filling the
      // queue
      // => LDOC_EOF is queued by the threads
      // explicitely putting a LDOC_EOF is only needed when converterThreads
      // were never running!
      if (ldocBuffer.size() == 0) ldocBuffer.put(LDOC_EOF);
      if (indexerThread.isAlive()) indexerThread.join();
    } catch (InterruptedException e) {
      log.error(e);
    }
    
    if (lastHarvested != null) {
      /* TODO:
      IndexConstants.FILENAME_LASTHARVESTED, IOContext.DEFAULT);
      out.writeLong(lastHarvested.getTime());
      out.close();
      */
      lastHarvested = null;
    }
    
    converterThreads = null;
    converterThreadList = null;
    indexerThread = null;
    
    Exception f = failure.get();
    if (f != null) throw f;
  }
  
  public void addDocument(MetadataDocument mdoc)
      throws IndexBuilderBackgroundFailure, InterruptedException {
    if (isClosed()) throw new IllegalStateException(
        "IndexBuilder already closed");
    throwFailure();
    startThreads(false);
    
    mdocBuffer.put(mdoc);
  }
  
  // call this between harvest resumptions to wait if buffer 2/3 full, this
  // helps to not block while running HTTP transfers (if buffer is big enough)
  public void checkIndexerBuffer() throws IndexBuilderBackgroundFailure,
      InterruptedException {
    if (isClosed()) throw new IllegalStateException(
        "IndexBuilder already closed");
    throwFailure();
    startThreads(false);
    
    if (ldocBuffer.remainingCapacity() * 2 < ldocBuffer.size()) {
      log.warn("Harvester is too fast for indexer thread, that is blocked. Waiting...");
      synchronized (indexerLock) {
        if (ldocBuffer.size() > 0 && failure.get() == null) indexerLock.wait();
      }
    }
  }
  
  // sets the date of last harvesting (written to disk after closing!!!)
  public void setLastHarvested(Date datestamp) {
    this.lastHarvested = datestamp;
  }
  
  public Date getLastHarvestedFromDisk() {
    Date d = null;
    /*IndexInput in = null;
    try {
      in = iconfig.getIndexDirectory().openInput(
          IndexConstants.FILENAME_LASTHARVESTED, IOContext.DEFAULT);
      d = new Date(in.readLong());
      in.close();
    } catch (IOException e) {
      if (in != null) try {
        in.close();
      } catch (IOException ie) {}
      d = null;
    }*/
    return d;
  }
  
  void converterThreadRun() {
    org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory
        .getLog(Thread.currentThread().getName());
    log.info("Converter thread started.");
    XPathResolverImpl.getInstance().setIndexBuilder(this);
    try {
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
        try {
          final IndexerQueueEntry en = new IndexerQueueEntry(mdoc.getIdentifier(),
              mdoc.getElasticSearchJSON());
          ldocBuffer.put(en);
        } catch (InterruptedException ie) {
          throw ie; // no handling here
        } catch (Exception e) {
          // handle exception
          switch (conversionErrorAction) {
            case IGNOREDOCUMENT:
              log.error(
                  "Conversion XML to Lucene document failed for '"
                      + mdoc.getIdentifier() + "' (object ignored):", e);
              break;
            case DELETEDOCUMENT:
              log.error(
                  "Conversion XML to Lucene document failed for '"
                      + mdoc.getIdentifier() + "' (object marked deleted):", e);
              ldocBuffer.put(new IndexerQueueEntry(mdoc.getIdentifier(), null));
              break;
            default:
              log.fatal("Conversion XML to Lucene document failed for '"
                  + mdoc.getIdentifier() + "' (fatal, stopping conversions).");
              throw e;
          }
        }
      }
    } catch (InterruptedException ie) {
      log.debug(ie);
    } catch (Exception e) {
      // only store the first error in failure variable, other errors are logged
      // only
      if (failure.compareAndSet(null, e)) log.debug(e);
      else log.error(e);
    } finally {
      if (runningConverters.decrementAndGet() == 0) try {
        mdocBuffer.clear();
        ldocBuffer.put(LDOC_EOF);
      } catch (InterruptedException e) {
        log.error(e);
      }
      XPathResolverImpl.getInstance().unsetIndexBuilder();
      log.info("Converter thread stopped.");
    }
  }
  
  void indexerThreadRun() {
    org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory
        .getLog(Thread.currentThread().getName());
    log.info("Indexer thread started.");
    int updated = 0, deleted = 0;
    boolean finished = false;
    try {
      final HashSet<String> committedIdentifiers = new HashSet<String>(maxBufferedChanges);
      BulkRequestBuilder bulkRequest = client.prepareBulk();
      
      while (failure.get() == null) {
        // notify eventually waiting checkIndexerBuffer() calls
        synchronized (indexerLock) {
          if (ldocBuffer.isEmpty()) indexerLock.notifyAll();
        }
        
        // take entry from buffer
        IndexerQueueEntry entry;
        try {
          entry = ldocBuffer.take();
        } catch (InterruptedException ie) {
          continue;
        }
        if (entry == LDOC_EOF) break;
        
        if (entry.builder == null) {
          if (log.isDebugEnabled()) log.debug("Deleting document: "
              + entry.identifier);
          bulkRequest.add(
            client.prepareDelete(iconfig.id, iconfig.parent.typeName, entry.identifier)
          );
          deleted++;
        } else {
          if (log.isDebugEnabled()) log.debug("Updating document: "
              + entry.identifier);
          if (log.isTraceEnabled()) log.trace("Data: " + entry.builder.string());
          bulkRequest.add(
            client.prepareIndex(iconfig.id, iconfig.parent.typeName, entry.identifier).setSource(entry.builder)
          );
          updated++;
        }
        committedIdentifiers.add(entry.identifier);
        
        if (committedIdentifiers.size() >= maxBufferedChanges) {
          assert committedIdentifiers.size() == bulkRequest.numberOfActions();
          BulkResponse bulkResponse = bulkRequest.execute().actionGet();
          if (bulkResponse.hasFailures()) {
            // TODO
            throw new IOException("TODO: Add correct error handling");
          }
          
          log.info(deleted + " docs presumably deleted (if existent) and "
              + updated + " docs (re-)indexed so far.");
          
          // notify Harvester of index commit
          final HarvesterCommitEvent ce = commitEvent.get();
          if (ce != null) ce.harvesterCommitted(Collections
              .unmodifiableSet(committedIdentifiers));
          committedIdentifiers.clear();
          
          // create new bulk
          bulkRequest = client.prepareBulk();
        }
      }
      
      // notify eventually waiting checkIndexerBuffer() calls, as we are
      // finished
      synchronized (indexerLock) {
        indexerLock.notifyAll();
      }
      
      assert committedIdentifiers.size() == bulkRequest.numberOfActions();
      if (bulkRequest.numberOfActions() > 0) {
        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        bulkRequest = null;
        if (bulkResponse.hasFailures()) {
          // TODO
          throw new IOException("TODO: Add correct error handling");
        }
      } else {
        bulkRequest = null;
      }

      // notify Harvester of index commit
      HarvesterCommitEvent ce = commitEvent.get();
      if (ce != null && committedIdentifiers.size() > 0) ce
          .harvesterCommitted(Collections.unmodifiableSet(committedIdentifiers));
      committedIdentifiers.clear();
      
      // check for validIdentifiers Set and remove all unknown identifiers from
      // index, if available
      Set<String> validIdentifiers = this.validIdentifiers.get();
      if (validIdentifiers != null) {
        log.info("Removing documents not seen while harvesting (this may take a while)...");
        final QueryBuilder query = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery(IndexConstants.FIELDNAME_SOURCE, iconfig.id))
            .mustNot(QueryBuilders.idsQuery(iconfig.parent.typeName).ids(validIdentifiers.toArray(new String[validIdentifiers.size()])));
        client.prepareDeleteByQuery(iconfig.id).setTypes(iconfig.parent.typeName).setQuery(query).execute().actionGet();
      }
      
      finished = true;
      log.info(deleted + " docs presumably deleted (only if existent) and "
          + updated + " docs (re-)indexed - finished.");
    } catch (Exception e) {
      if (!finished) log.warn("Only " + deleted
          + " docs presumably deleted (only if existent) and " + updated
          + " docs (re-)indexed before the following error occurred: " + e);
      // only store the first error in failure variable, other errors are logged
      // only
      if (!failure.compareAndSet(null, e)) log.error(e);
    } finally {
      ldocBuffer.clear();
      // notify eventually waiting checkIndexerBuffer() calls, as we are
      // finished
      synchronized (indexerLock) {
        indexerLock.notifyAll();
      }
      log.info("Indexer thread stopped.");
    }
  }
  
  private void throwFailure() throws IndexBuilderBackgroundFailure {
    Exception f = failure.get();
    if (f != null) {
      if (converterThreads != null) converterThreads.interrupt();
      if (indexerThread != null) indexerThread.interrupt();
      throw new IndexBuilderBackgroundFailure(f);
    }
  }
  
  private void startThreads(boolean onlyIndexer) {
    if (!threadsStarted) try {
      if (!onlyIndexer) for (Thread t : converterThreadList) {
        runningConverters.incrementAndGet();
        t.start();
      }
      indexerThread.start();
    } finally {
      threadsStarted = true;
    }
  }
  
  private static final class IndexerQueueEntry {
    
    protected IndexerQueueEntry(String identifier, XContentBuilder builder) {
      this.identifier = identifier;
      this.builder = builder;
    }
    
    protected final String identifier;
    protected final XContentBuilder builder;
    
  }
  
}