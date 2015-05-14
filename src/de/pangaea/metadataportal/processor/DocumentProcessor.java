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
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.ByteSizeValue;
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
 * Component of <b>panFMP</b> that analyzes and indexes harvested documents in a thread pool.
 * 
 * @author Uwe Schindler
 */
public final class DocumentProcessor {
  private static final Log log = LogFactory.getLog(DocumentProcessor.class);
  
  private final HarvesterConfig iconfig;
  private final Client client;
  private final String targetIndex, sourceIndex; // differs if rebuilding
  private final int threadCount;
  
  private volatile boolean isClosed = false;
  
  public final Map<String,String> harvesterMetadata = new LinkedHashMap<>();
  
  private static final MetadataDocument MDOC_EOF = new MetadataDocument(null);
  private final BlockingQueue<MetadataDocument> mdocBuffer;
    
  private final AtomicReference<Exception> failure = new AtomicReference<>(null);
  private Set<String> validIdentifiers = null;
  
  final AtomicInteger processed = new AtomicInteger(0);
  
  private final int bulkSize, deleteUnseenBulkSize;
  private final ByteSizeValue maxBulkMemory;
  private final DocumentErrorAction conversionErrorAction;
  private final XContentType contentType;
  
  private final Object poolInitLock = new Object();
  private ExecutorService pool = null;
  
  public static final String HARVESTER_METADATA_TYPE = "panfmp_meta";

  public static final int DEFAULT_BULK_SIZE = 100;
  public static final int DEFAULT_DELETE_UNSEEN_BULK_SIZE = 1000;
  public static final XContentType DEFAULT_CONTENT_TYPE = XContentType.CBOR;

  DocumentProcessor(Client client, HarvesterConfig iconfig, String targetIndex) throws IOException {
    this.client = client;
    this.iconfig = iconfig;
    this.sourceIndex = iconfig.parent.indexName;
    this.targetIndex = (targetIndex == null) ? this.sourceIndex : targetIndex;
    this.bulkSize = Integer.parseInt(iconfig.properties.getProperty("bulkSize", Integer.toString(DEFAULT_BULK_SIZE)));
    this.deleteUnseenBulkSize = Integer.parseInt(iconfig.properties.getProperty("deleteUnseenBulkSize", Integer.toString(DEFAULT_DELETE_UNSEEN_BULK_SIZE)));
    this.maxBulkMemory = ByteSizeValue.parseBytesSizeValue(iconfig.properties.getProperty("maxBulkMemory", Long.toString(Long.MAX_VALUE)));
    
    final String ct = iconfig.properties.getProperty("sourceContentType");
    if (ct != null) {
      this.contentType = XContentType.fromRestContentType(ct);
      if (this.contentType == null) {
        throw new IllegalArgumentException("Illegal content type for _source field (sourceContentType property): " + ct);
      }
    } else {
      this.contentType = DEFAULT_CONTENT_TYPE;
    }
    
    final String s = iconfig.properties.getProperty("conversionErrorAction", "STOP");
    try {
      this.conversionErrorAction = DocumentErrorAction.valueOf(s.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid value '" + s + "' for harvester property 'conversionErrorAction', valid ones are: "
          + Arrays.toString(DocumentErrorAction.values()));
    }
    
    this.threadCount = Integer.parseInt(iconfig.properties.getProperty("numThreads", "1"));
    if (this.threadCount < 1) {
      throw new IllegalArgumentException("numThreads harvester-property must be >=1!");
    }
    final int maxQueue = Integer.parseInt(iconfig.properties.getProperty("maxQueue", "100"));
    if (maxQueue < this.threadCount) {
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
  }
  
  public boolean isFailed() {
    return (failure.get() != null);
  }
  
  public void setValidIdentifiers(Set<String> validIdentifiers) {
    this.validIdentifiers = validIdentifiers;
  }
  
  public boolean isClosed() {
    return isClosed;
  }
  
  public void close() throws Exception {
    if (isClosed()) throw new IllegalStateException("DocumentProcessor already closed");
    isClosed = true;
    
    synchronized(poolInitLock) {
      if (pool != null) {
        if (this.isFailed()) {
          mdocBuffer.clear();
        }
        // enqueue dummy documents to signal end of processing to all threads in pool:
        for (int i = 0; i < threadCount; i++) {
          mdocBuffer.put(MDOC_EOF);
        }
        // shutdown thread pool
        pool.shutdown();
        while (!pool.awaitTermination(5, TimeUnit.SECONDS)) {
          log.warn("Still waiting for document processor threadpool to terminate...");
        }
        pool = null;
      }
      
      // exit here before we write any status info to disk:
      throwFailure();
    }
    
    // delete all unseen documents, if validIdentifiers is given:
    deleteUnseenDocuments();
    
    // save harvester metadata:
    log.info("Saving harvester metadata...");
    final XContentBuilder builder = XContentFactory.contentBuilder(contentType).map(harvesterMetadata);
    client.prepareIndex(targetIndex, HARVESTER_METADATA_TYPE, iconfig.id).setSource(builder).get();

    log.info(processed + " metadata items processed - finished.");
  }
  
  public void addDocument(MetadataDocument mdoc) throws BackgroundFailure, InterruptedException {
    if (isClosed()) throw new IllegalStateException("DocumentProcessor already closed");
    throwFailure();
    startPool();
    mdocBuffer.put(mdoc);
  }
  
  /** This does not use bulk indexing, it starts converting and posts the document to Elasticsearch.
   * It can be used without harvester to index documents directly.
   * This does not start a new thread pool. */
  public void processDocument(MetadataDocument mdoc) throws Exception {
    if (isClosed()) throw new IllegalStateException("DocumentProcessor already closed");
    
    final ActionRequest<?> req = internalProcessDocument(log, mdoc);
    if (req instanceof IndexRequest) {
      client.index((IndexRequest) req).get();
      processed.addAndGet(1);
      log.info(String.format(Locale.ENGLISH, "Document update '%s' processed and submitted to Elasticsearch index '%s'.", mdoc.getIdentifier(), targetIndex));
    } else if (req instanceof DeleteRequest) {
      client.delete((DeleteRequest) req).get();
      processed.addAndGet(1);
      log.info(String.format(Locale.ENGLISH, "Document delete '%s' processed and submitted to Elasticsearch index '%s'.", mdoc.getIdentifier(), targetIndex));
    } else {
      log.warn(String.format(Locale.ENGLISH, "Nothing done for metadata item '%s'.", mdoc.getIdentifier()));
    }
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
  
  void runProcessor(String name) {
    final Log log = LogFactory.getLog(name);
    log.info("Processor started.");
    try {
      final BulkProcessor bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
        @Override
        public void beforeBulk(long executionId, BulkRequest request) {
          if (log.isDebugEnabled()) {
            log.debug(String.format(Locale.ENGLISH, "Sending bulk with %d actions to Elasticsearch...", request.numberOfActions()));
          }
        }
        
        @Override
        public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
          throw new ElasticsearchException("Error executing bulk request.", failure);
        }
        
        @Override
        public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
          if (response.hasFailures()) {
            throw new ElasticsearchException("Error while executing bulk request: " + response.buildFailureMessage());
          }
          final int totalItems = processed.addAndGet(request.numberOfActions());
          log.info(totalItems + " metadata items processed so far.");
        }
      }).setName(name)
        .setConcurrentRequests(1)
        .setBulkActions(bulkSize)
        .setBulkSize(maxBulkMemory)
        .build();
      
      while (!isFailed()) {
        final MetadataDocument mdoc;
        try {
          mdoc = mdocBuffer.take();
        } catch (InterruptedException ie) {
          continue;
        }
        if (mdoc == MDOC_EOF) break;
        
        final ActionRequest<?> req = internalProcessDocument(log, mdoc);
        if (req != null) {
          bulkProcessor.add(req);
        }
      }

      bulkProcessor.flush();
      while (!bulkProcessor.awaitClose(5, TimeUnit.SECONDS)) {
        log.warn("Still waiting for bulk processor to finish...");
      }
    } catch (Exception e) {
      log.warn(String.format("Only %d metadata items processed before the following error occurred: %s", processed, e));
      // only store the first error in failure variable, other errors are only logged
      if (!failure.compareAndSet(null, e)) {
        log.error(e);
      }
    } finally {
      log.info("Processor stopped.");
    }
  }
  
  private ActionRequest<?> internalProcessDocument(Log log, MetadataDocument mdoc) throws Exception {
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
          log.error(String.format(Locale.ENGLISH, "Conversion XML to Elasticsearch document failed for '%s' (object ignored):", identifier), e);
          // exit method
          return null;
        case DELETEDOCUMENT:
          log.error(String.format(Locale.ENGLISH, "Conversion XML to Elasticsearch document failed for '%s' (object marked deleted):", identifier), e);
          kv = null;
          break;
        default:
          log.fatal(String.format(Locale.ENGLISH, "Conversion XML to Elasticsearch document failed for '%s' (fatal, stopping conversions).", identifier));
          throw e;
      }
    }

    if (kv == null || kv.isEmpty()) {
      if (log.isDebugEnabled()) log.debug("Deleting document: " + identifier);
      return client.prepareDelete(targetIndex, iconfig.root.typeName, identifier).request();
    } else {
      final XContentBuilder source = XContentFactory.contentBuilder(contentType);
      kv.serializeToContentBuilder(source);
      if (log.isDebugEnabled()) log.debug("Updating document: " + identifier);
      return client.prepareIndex(targetIndex, iconfig.root.typeName, identifier).setSource(source).request();
    }
  }
  
  private void throwFailure() throws BackgroundFailure {
    final Exception f = failure.get();
    if (f != null) {
      synchronized(poolInitLock) {
        if (pool != null) {
          // don't accept any docs anymore (also interrupt threads)
          pool.shutdownNow();
        }
      }
      throw new BackgroundFailure(f);
    }
  }
  
  private void startPool() {
    synchronized(poolInitLock) {
      if (pool == null) {
        pool = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
          final String name = String.format(Locale.ROOT, "%s#%d", getClass().getName(), i + 1);
          pool.execute(new Runnable() {
            @Override
            public void run() {
              runProcessor(name);
            }
          });
        }
      }
    }
  }
  
}