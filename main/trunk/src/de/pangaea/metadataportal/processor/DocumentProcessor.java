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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import de.pangaea.metadataportal.config.HarvesterConfig;
import de.pangaea.metadataportal.utils.KeyValuePairs;

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
  protected final String targetIndex, sourceIndex; // differs if rebuilding
  
  public final Map<String,String> harvesterMetadata = new LinkedHashMap<>();
  
  private static final MetadataDocument MDOC_EOF = new MetadataDocument(null);
  private final BlockingQueue<MetadataDocument> mdocBuffer;
    
  private final AtomicReference<Exception> failure = new AtomicReference<>(null);
  private final AtomicReference<CommitEvent> commitEvent = new AtomicReference<>(null);
  private Set<String> validIdentifiers = null;
  
  private final AtomicInteger processed = new AtomicInteger(0);
  
  private final int bulkSize, deleteUnseenBulkSize;
  private final long maxBulkMemory;
  private final DocumentErrorAction conversionErrorAction;
  private final XContentType contentType;
    
  private final AtomicInteger runningThreads = new AtomicInteger(0);
  private ThreadGroup threadGroup;
  private Thread[] threadList;
  
  public static final String HARVESTER_METADATA_TYPE = "panfmp_meta";

  public static final int DEFAULT_BULK_SIZE = 100;
  public static final int DEFAULT_DELETE_UNSEEN_BULK_SIZE = 1000;
  public static final String DEFAULT_CONTENT_TYPE = "cbor";

  DocumentProcessor(Client client, HarvesterConfig iconfig, String targetIndex) throws IOException {
    this.client = client;
    this.iconfig = iconfig;
    this.sourceIndex = iconfig.parent.indexName;
    this.targetIndex = (targetIndex == null) ? this.sourceIndex : targetIndex;
    this.bulkSize = Integer.parseInt(iconfig.properties.getProperty("bulkSize", Integer.toString(DEFAULT_BULK_SIZE)));
    this.deleteUnseenBulkSize = Integer.parseInt(iconfig.properties.getProperty("deleteUnseenBulkSize", Integer.toString(DEFAULT_DELETE_UNSEEN_BULK_SIZE)));
    this.maxBulkMemory = Long.parseLong(iconfig.properties.getProperty("maxBulkMemory", Long.toString(Long.MAX_VALUE)));
    this.contentType = XContentType.fromRestContentType(iconfig.properties.getProperty("sourceContentType", DEFAULT_CONTENT_TYPE));
    
    final String s = iconfig.properties.getProperty("conversionErrorAction", "STOP");
    try {
      this.conversionErrorAction = DocumentErrorAction.valueOf(s.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid value '" + s + "' for harvester property 'conversionErrorAction', valid ones are: "
          + Arrays.toString(DocumentErrorAction.values()));
    }
    
    final int threadCount = Integer.parseInt(iconfig.properties.getProperty("numThreads", "1"));
    if (threadCount < 1) {
      throw new IllegalArgumentException("numThreads harvester-property must be >=1!");
    }
    final int maxQueue = Integer.parseInt(iconfig.properties.getProperty("maxQueue", "100"));
    if (maxQueue < threadCount) {
      throw new IllegalArgumentException("maxQueue must be >=numThreads!");
    }
    this.mdocBuffer = new ArrayBlockingQueue<>(maxQueue, true);
    
    // load metadata
    final GetResponse resp = client.prepareGet(sourceIndex, HARVESTER_METADATA_TYPE, iconfig.id).setFetchSource(true).get();
    if (resp.isExists()) {
      Map<String,Object> map = resp.getSourceAsMap();
      if (map != null) {
        for (final Map.Entry<String,Object> e : map.entrySet()) {
          harvesterMetadata.put(e.getKey(), e.getValue().toString());
        }
      }
    }
    
    // threads
    this.threadGroup = new ThreadGroup(getClass().getName() + "#ThreadGroup");
    this.threadList = new Thread[threadCount];
    for (int i = 0; i < threadCount; i++) {
      this.threadList[i] = new Thread(threadGroup, new Runnable() {
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
    
    while (runningThreads.get() > 0) {
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
    
    // exit here before we write any status info to disk:
    throwFailure();
    
    // delete all unseen documents, if validIdentifiers is given:
    deleteUnseenDocuments();
    
    // save harvester metadata:
    log.info("Saving harvester metadata...");
    @SuppressWarnings({"rawtypes", "unchecked"}) // fix this type stupidness in ES:
    final XContentBuilder builder = XContentFactory.contentBuilder(contentType).map((Map) harvesterMetadata);
    client.prepareIndex(targetIndex, HARVESTER_METADATA_TYPE, iconfig.id)
      .setSource(builder).get();

    log.info(processed + " metadata items processed - finished.");
  }
  
  public void addDocument(MetadataDocument mdoc) throws BackgroundFailure, InterruptedException {
    if (isClosed()) throw new IllegalStateException("DocumentProcessor already closed");
    throwFailure();
    startThreads();
    mdocBuffer.put(mdoc);
  }
  
  /** This does not use bulk indexing, it starts converting and posts the document to Elasticsearch.
   * It can be used without harvester to index documents directly.
   * This does not start a new thread. */
  public void processDocument(MetadataDocument mdoc) throws Exception {
    if (isClosed()) throw new IllegalStateException("DocumentProcessor already closed");
    
    // TODO: Rewrite without 1-doc bulk!
    final BulkRequestBuilder bulkRequest = client.prepareBulk();
    internalProcessDocument(log, bulkRequest, null, mdoc);
    final BulkResponse bulkResponse = bulkRequest.get();
    if (bulkResponse.hasFailures()) {
      throw new ElasticsearchException("Error while executing request: " + bulkResponse.buildFailureMessage());
    }
    
    processed.addAndGet(1);
    
    // notify Harvester of commit
    final CommitEvent ce = commitEvent.get();
    if (ce != null) ce.harvesterCommitted(Collections.singleton(mdoc.getIdentifier()));
 
    log.info("Document update '" + mdoc.getIdentifier() + "' processed and submitted to Elasticsearch index '" + targetIndex + "'.");
  }
  
  /**
   * check for validIdentifiers Set and remove all unknown
   * identifiers from ES.
   */
  private void deleteUnseenDocuments() {
    if (validIdentifiers != null) {
      log.info("Removing metadata items not seen while harvesting...");
      
      final QueryBuilder query = QueryBuilders.constantScoreQuery(FilterBuilders.andFilter(
        FilterBuilders.termFilter(iconfig.root.fieldnameSource, iconfig.id),
        FilterBuilders.notFilter(FilterBuilders.idsFilter(iconfig.root.typeName).ids(validIdentifiers.toArray(new String[validIdentifiers.size()]))).cache(false)
      ).cache(false));
      
      final TimeValue time = TimeValue.timeValueMinutes(10);
      final Set<String> lostItems = new TreeSet<>();
      long count = 0;
      SearchResponse scrollResp = client.prepareSearch(targetIndex)
        .setTypes(iconfig.root.typeName)
        .setQuery(query)
        .setFetchSource(false)
        .setNoFields()
        .setSize(deleteUnseenBulkSize)
        .setSearchType(SearchType.SCAN).setScroll(time)
        .get();
      do {
        final BulkRequestBuilder bulk = client.prepareBulk();
        for (final SearchHit hit : scrollResp.getHits()) {
          bulk.add(client.prepareDelete(targetIndex, iconfig.root.typeName, hit.getId()));
        }
        if (bulk.numberOfActions() > 0) {
          lostItems.clear();
          final BulkResponse bulkResponse = bulk.get();
          if (bulkResponse.hasFailures()) {
            throw new ElasticsearchException("Error while executing bulk request: " + bulkResponse.buildFailureMessage());
          }
          // count items for safety:
          int deletedItems = 0;
          for (final BulkItemResponse item : bulkResponse.getItems()) {
            final DeleteResponse delResp = item.getResponse();
            if (delResp.isFound()) {
              deletedItems++;
            } else {
              lostItems.add(delResp.getId());
            }
          }
          count += deletedItems;
          if (!lostItems.isEmpty()) {
            log.warn("Some metadata items could not be deleted because they disappeared in the meantime: " + lostItems);
          }
          log.info("Deleted " + count + " metadata items until now, working...");
        }
        if (scrollResp.getScrollId() == null) break;
        scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(time).get();
      } while (scrollResp.getHits().getHits().length > 0);

      log.info("Deleted a total number of " + count + " metadata items.");
    }
  }
  
  void threadRun() {
    final Log log = LogFactory.getLog(Thread.currentThread().getName());
    log.info("Processor thread started.");
    boolean finished = false;
    try {
      final HashSet<String> committedIdentifiers = new HashSet<>(bulkSize);
      BulkRequestBuilder bulkRequest = client.prepareBulk();
      long[] totalSize = new long[1];
      
      while (failure.get() == null) {
        final MetadataDocument mdoc;
        try {
          mdoc = mdocBuffer.take();
        } catch (InterruptedException ie) {
          continue;
        }
        if (mdoc == MDOC_EOF) break;
        
        if (internalProcessDocument(log, bulkRequest, totalSize, mdoc)) {
          committedIdentifiers.add(mdoc.getIdentifier());
          if (needToSubmitBulk(bulkRequest, totalSize[0])) {
            pushBulk(log, bulkRequest, committedIdentifiers);
            // create new bulk:
            bulkRequest = client.prepareBulk();
            totalSize[0] = 0L;
          }
        }
      }

      if (bulkRequest.numberOfActions() > 0) {
        pushBulk(log, bulkRequest, committedIdentifiers);
      }
      
      finished = true;
    } catch (Exception e) {
      if (!finished) log.warn("Only " + processed +
          " metadata items processed before the following error occurred: " + e);
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
  
  private boolean needToSubmitBulk(BulkRequestBuilder bulkRequest, long size) {
    if (bulkRequest.numberOfActions() >= bulkSize) {
      return true;
    }
    if (size >= maxBulkMemory) {
      return true;
    }
    return false;
  }
  
  private void pushBulk(Log log, BulkRequestBuilder bulkRequest, final Set<String> committedIdentifiers) {
    final int items = bulkRequest.numberOfActions();
    final BulkResponse bulkResponse = bulkRequest.get();
    if (bulkResponse.hasFailures()) {
      throw new ElasticsearchException("Error while executing bulk request: " + bulkResponse.buildFailureMessage());
    }
    final int totalItems = processed.addAndGet(items);
    
    // notify Harvester of index commit
    final CommitEvent ce = commitEvent.get();
    if (ce != null) ce.harvesterCommitted(Collections.unmodifiableSet(committedIdentifiers));
    committedIdentifiers.clear();

    log.info(totalItems + " metadata items processed so far.");
  }
  
  private boolean internalProcessDocument(Log log, BulkRequestBuilder bulkRequest, long[] totalSize, MetadataDocument mdoc) throws Exception {
    final String identifier = mdoc.getIdentifier();
    if (log.isDebugEnabled()) log.debug("Converting document: " + mdoc.toString());
    if (log.isTraceEnabled()) log.trace("XML: " + mdoc.getXML());
    KeyValuePairs kv = null;
    try {
      kv = mdoc.getKeyValuePairs();
    } catch (Exception e) {
      // handle exception
      switch (conversionErrorAction) {
        case IGNOREDOCUMENT:
          log.error(
              "Conversion XML to Elasticsearch document failed for '"
                  + identifier + "' (object ignored):", e);
          // exit method
          return false;
        case DELETEDOCUMENT:
          log.error(
              "Conversion XML to Elasticsearch document failed for '"
                  + identifier + "' (object marked deleted):", e);
          kv = null;
          break;
        default:
          log.fatal("Conversion XML to Lucene document failed for '"
              + identifier + "' (fatal, stopping conversions).");
          throw e;
      }
    }

    if (kv == null || kv.isEmpty()) {
      if (log.isDebugEnabled()) log.debug("Deleting document: " + identifier);
      bulkRequest.add(
        client.prepareDelete(targetIndex, iconfig.root.typeName, identifier)
      );
    } else {
      final XContentBuilder source = XContentFactory.contentBuilder(contentType);
      kv.serializeToContentBuilder(source);
      if (totalSize != null) {
        totalSize[0] += source.bytes().length();
      }
      if (log.isDebugEnabled()) log.debug("Updating document: " + identifier);
      bulkRequest.add(
        client.prepareIndex(targetIndex, iconfig.root.typeName, identifier).setSource(source)
      );
    }
    
    return true;
  }
  
  private void throwFailure() throws BackgroundFailure {
    Exception f = failure.getAndSet(null);
    if (f != null) {
      if (threadGroup != null) threadGroup.interrupt();
      throw new BackgroundFailure(f);
    }
  }
  
  private void startThreads() {
    if (threadList != null && runningThreads.get() == 0) {
      for (Thread t : threadList) {
        runningThreads.incrementAndGet();
        t.start();
      }
    }
  }
  
}