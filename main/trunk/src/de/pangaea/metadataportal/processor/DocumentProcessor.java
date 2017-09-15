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

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;

import de.pangaea.metadataportal.config.HarvesterConfig;
import de.pangaea.metadataportal.utils.KeyValuePairs;

/**
 * Component of <b>panFMP</b> that analyzes and indexes harvested documents in a thread pool.
 * 
 * @author Uwe Schindler
 */
public final class DocumentProcessor {
  static final Log log = LogFactory.getLog(DocumentProcessor.class);
  
  private final HarvesterConfig iconfig;
  private final Client client;
  private final String targetIndex, sourceIndex; // differs if rebuilding
  private final int threadCount;
  
  private volatile boolean isClosed = false;
  
  public final Map<String,String> harvesterMetadata = new LinkedHashMap<>();
    
  final AtomicReference<Throwable> failure = new AtomicReference<>(null);
  
  final AtomicInteger processed = new AtomicInteger(0);
  
  private final int bulkSize, maxQueue, concurrentBulkRequests;
  private final ByteSizeValue maxBulkMemory;
  private final DocumentErrorAction conversionErrorAction;
  private final XContentType contentType;
  
  private final Object poolInitLock = new Object();
  private ExecutorService pool = null;
  BulkProcessor bulkProcessor = null;
  
  public static final String HARVESTER_METADATA_TYPE = "panfmp_meta";

  public static final int DEFAULT_BULK_SIZE = 100;
  public static final ByteSizeValue DEFAULT_BULK_MEMORY = new ByteSizeValue(5, ByteSizeUnit.MB); // Elasticsearch's default, just copypasted
  public static final int DEFAULT_MAX_QUEUE = 100;
  public static final int DEFAULT_CONCURRENT_BULK_REQUESTS = 1;
  public static final int DEFAULT_NUM_THREADS = 1;
  public static final int DEFAULT_DELETE_UNSEEN_BULK_SIZE = 1000;
  public static final XContentType DEFAULT_CONTENT_TYPE = XContentType.CBOR;

  DocumentProcessor(Client client, HarvesterConfig iconfig, String targetIndex) {
    this.client = client;
    this.iconfig = iconfig;
    this.sourceIndex = iconfig.parent.indexName;
    this.targetIndex = (targetIndex == null) ? this.sourceIndex : targetIndex;
    this.bulkSize = Integer.parseInt(iconfig.properties.getProperty("bulkSize", Integer.toString(DEFAULT_BULK_SIZE)));
    final String sz = iconfig.properties.getProperty("maxBulkMemory");
    this.maxBulkMemory = (sz == null) ? DEFAULT_BULK_MEMORY : ByteSizeValue.parseBytesSizeValue(sz, "panfmp.maxBulkMemory");
    
    final String ct = iconfig.properties.getProperty("sourceContentType");
    if (ct != null) {
      this.contentType = XContentType.fromMediaTypeOrFormat(ct);
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
    
    this.concurrentBulkRequests = Integer.parseInt(iconfig.properties.getProperty("concurrentBulkRequests", Integer.toString(DEFAULT_CONCURRENT_BULK_REQUESTS)));

    this.threadCount = Integer.parseInt(iconfig.properties.getProperty("numThreads", Integer.toString(DEFAULT_NUM_THREADS)));
    if (this.threadCount < 1) {
      throw new IllegalArgumentException("numThreads harvester-property must be >=1!");
    }
    this.maxQueue = Integer.parseInt(iconfig.properties.getProperty("maxQueue", Integer.toString(DEFAULT_MAX_QUEUE)));
    if (this.maxQueue < this.threadCount) {
      throw new IllegalArgumentException("maxQueue must be >=numThreads!");
    }
    
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
  
  public boolean isClosed() {
    return isClosed;
  }
  
  public void close(Set<String> validIdentifiers) throws Exception {
    if (isClosed()) throw new IllegalStateException("DocumentProcessor already closed");
    isClosed = true;
    
    synchronized(poolInitLock) {
      if (pool != null) {
        log.info("Waiting for document processor to finish...");
        // shutdown thread pool
        pool.shutdown();
        while (!pool.awaitTermination(5, TimeUnit.SECONDS)) {
          log.warn("Still waiting for document processor threadpool to finish...");
        }
        pool = null;
        log.info("Document processor to terminated.");

        log.info("Waiting for Elasticsearch bulk processor to finish...");
        // TODO: ES bulk processor does not support while()-based waiting
        // (it closes on first try and waits afterwards)!
        bulkProcessor.awaitClose(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        bulkProcessor = null;
        log.info("Elasticsearch bulk processor terminated.");

        // exit here before we write any status info to disk:
        throwFailure();

        log.info(processed + " metadata items processed - finished.");
      }
    }
    
    // exit here before we write any status info to disk:
    throwFailure();

    // delete all unseen documents, if validIdentifiers is given:
    if (validIdentifiers != null) {
      deleteUnseenDocuments(validIdentifiers);
    }
    
    // save harvester metadata:
    log.info("Saving harvester metadata...");
    final XContentBuilder builder = XContentFactory.contentBuilder(contentType).map(harvesterMetadata);
    client.prepareIndex(targetIndex, HARVESTER_METADATA_TYPE, iconfig.id).setSource(builder).get();
  }
  
  public void addDocument(MetadataDocument mdoc) throws BackgroundFailure {
    if (isClosed()) throw new IllegalStateException("DocumentProcessor already closed");
    throwFailure();
    startPool();
    pool.execute(getRunnable(mdoc));
    throwFailure(); // fail is queue was full and it was executed in this thread
  }
  
  /**
   * Check for validIdentifiers Set and remove all unknown identifiers from ES.
   */
  private void deleteUnseenDocuments(Set<String> validIdentifiers) {
    log.info("Removing metadata items not seen while harvesting...");
    
    final IdsQueryBuilder bld = QueryBuilders.idsQuery(iconfig.root.typeName);
    bld.ids().addAll(validIdentifiers);
    final QueryBuilder query = QueryBuilders.boolQuery()
      .filter(QueryBuilders.termQuery(iconfig.root.fieldnameSource, iconfig.id))
      .mustNot(bld);
    
    final BulkByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
      .filter(query)
      .source(targetIndex)
      .get();

    log.info("Deleted a total number of " + response.getDeleted() + " metadata items.");
  }
  
  private Runnable getRunnable(final MetadataDocument mdoc) {
    return () -> {
      if (failure.get() != null) {
        return; // cancel execution
      }
      try {      
        final DocWriteRequest<?> req = buildDocumentAction(mdoc);
        if (req != null) {
          bulkProcessor.add(req);
        }
      } catch (Throwable e) {
        // only store the first error in failure variable, other errors are only logged
        if (!failure.compareAndSet(null, e)) {
          log.error(e);
        }
      }
    };
  }
  
  /**
   * Processes the given {@link MetadataDocument} and returns
   * the {@link DocWriteRequest} to pass to Elasticsearch
   * (can either be {@link IndexRequest} or {@link DeleteRequest}).
   */
  public DocWriteRequest<?> buildDocumentAction(MetadataDocument mdoc) throws Exception {
    final String identifier = mdoc.getIdentifier();
    if (log.isDebugEnabled()) log.debug("Converting document: " + mdoc.toString());
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
          throw e;
      }
    }

    if (kv == null || kv.isEmpty()) {
      if (log.isDebugEnabled()) log.debug("Deleting document: " + identifier);
      return new DeleteRequest(targetIndex, iconfig.root.typeName, identifier);
    } else {
      final XContentBuilder source = XContentFactory.contentBuilder(contentType);
      kv.serializeToContentBuilder(source);
      if (log.isDebugEnabled()) log.debug("Updating document: " + identifier);
      return new IndexRequest(targetIndex, iconfig.root.typeName, identifier).source(source);
    }
  }
  
  private void throwFailure() throws BackgroundFailure {
    final Throwable f = failure.get();
    if (f != null) {
      throw new BackgroundFailure(f);
    }
  }
  
  private void startPool() {
    synchronized(poolInitLock) {
      if (pool == null) {
        assert bulkProcessor == null;
        bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
          @Override
          public void beforeBulk(long executionId, BulkRequest request) {
            if (log.isDebugEnabled()) {
              log.debug(String.format(Locale.ENGLISH, "Sending bulk with %d actions to Elasticsearch...", request.numberOfActions()));
            }
          }
          
          @Override
          public void afterBulk(long executionId, BulkRequest request, Throwable f) {
            if (f instanceof Exception) {
              // only store the first error in failure variable, other errors are only logged
              if (!failure.compareAndSet(null, (Exception) f)) {
                log.error("Exception happened while doing bulk request.", f);
              }
            } else if (f instanceof Error) {
              throw (Error) f;
            } else {
              throw new AssertionError(f); // should not happen
            }
          }
          
          @Override
          public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
            if (response.hasFailures()) {
              afterBulk(executionId, request, new ElasticsearchException("Error while executing bulk request: " + response.buildFailureMessage()));
              return;
            }
            final int totalItems = processed.addAndGet(request.numberOfActions());
            log.info(totalItems + " metadata items processed so far.");
          }
        }).setConcurrentRequests(concurrentBulkRequests)
          .setBulkActions(bulkSize)
          .setBulkSize(maxBulkMemory)
          .build();

        pool = new ThreadPoolExecutor(threadCount, threadCount,
            0L, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<Runnable>(maxQueue, false),
            new RejectedExecutionHandler() {
              @Override
              public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
                if (e.isShutdown()) {
                  throw new RejectedExecutionException("Executor shutdown.");
                }
                // run in caller's thread:
                r.run();
              }
          });
      }
    }
  }
  
}