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

package de.pangaea.metadataportal.push;

import java.time.Instant;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import javax.xml.transform.Source;

import de.pangaea.metadataportal.config.Config;
import de.pangaea.metadataportal.config.HarvesterConfig;
import de.pangaea.metadataportal.harvester.Harvester;
import de.pangaea.metadataportal.harvester.SingleFileEntitiesHarvester;
import de.pangaea.metadataportal.processor.DocumentErrorAction;
import de.pangaea.metadataportal.processor.ElasticsearchConnection;
import de.pangaea.metadataportal.processor.MetadataDocument;

public final class PushWrapperHarvester extends SingleFileEntitiesHarvester {
  
  private static final ThreadLocal<Consumer<PushWrapperHarvester>> STARTUP_CALLBACK_HOLDER = new ThreadLocal<>();
  private static final ThreadLocal<Consumer<PushWrapperHarvester>> SHUTDOWN_CALLBACK_HOLDER = new ThreadLocal<>();
  
  public static boolean isValidHarvesterId(Config conf, String id) {
    return !isAllIndexes(id) && conf.harvestersAndIndexes.contains(id) && !conf.targetIndexes.containsKey(id);
  }
  
  public static PushWrapperHarvester initializeWrapper(Config conf, String id, Consumer<PushWrapperHarvester> shutdownCallback) {
    if (!isValidHarvesterId(conf, id)) {
      throw new IllegalArgumentException("Cannot find harvester name: " + id);
    }
    final CompletableFuture<PushWrapperHarvester> result = new CompletableFuture<>();
    new Thread(() -> {
      STARTUP_CALLBACK_HOLDER.set(result::complete);
      SHUTDOWN_CALLBACK_HOLDER.set(shutdownCallback);
      try {
        runHarvester(conf, id, PushWrapperHarvester.class);
      } catch (Exception e) {
        result.completeExceptionally(e);
      } finally {
        STARTUP_CALLBACK_HOLDER.remove();
        SHUTDOWN_CALLBACK_HOLDER.remove();
      }
    }, String.format(Locale.ROOT,"PushWrapperHarvester(%s)", id)).start();
    return result.join();
  }
  
  // harvester interface
  private final Harvester wrappedHarvester;
  private final long timeout = TimeUnit.SECONDS.toNanos(60);
  private final AtomicLong lastAccessed = new AtomicLong(System.nanoTime());
  private final CountDownLatch latch = new CountDownLatch(1);
  
  public PushWrapperHarvester(HarvesterConfig iconfig) throws Exception {
    super(iconfig, DocumentErrorAction.STOP);
    this.wrappedHarvester = iconfig.harvesterClass.getConstructor(HarvesterConfig.class).newInstance(iconfig);
  }
  
  @Override
  public void prepareReindex(ElasticsearchConnection es, String targetIndex) throws Exception {
    throw new IllegalStateException();
  }

  @Override
  public void finishReindex(boolean cleanShutdown) throws Exception {
    throw new IllegalStateException();
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
  public void open(ElasticsearchConnection es, String targetIndex) throws Exception {
    super.open(es, targetIndex);
    cancelMissingDocumentDelete();
  }

  @Override
  public void addDocument(String identifier, Instant lastModified, Source xml) throws Exception {
    resetTimer();
    super.addDocument(identifier, lastModified, xml);
  }

  @Override
  public void deleteDocument(String identifier) throws Exception {
    resetTimer();
    super.deleteDocument(identifier);
  }
  
  public void commitAndClose() throws Exception {
    latch.countDown();
  }

  private void resetTimer() {
    lastAccessed.set(System.nanoTime());
  }

  @Override
  public void harvest() throws Exception {
    cancelMissingDocumentDelete(); // be safe
    
    log.info("Waiting for push connections...");
    STARTUP_CALLBACK_HOLDER.get().accept(this);
    while (latch.getCount() > 0L && System.nanoTime() - lastAccessed.get() < timeout) {
      latch.await(1, TimeUnit.SECONDS);
    }
    SHUTDOWN_CALLBACK_HOLDER.get().accept(this);
    log.info("Shutting down push mode...");
  }
  
}