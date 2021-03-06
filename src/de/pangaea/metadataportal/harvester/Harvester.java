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

import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

import javax.xml.transform.TransformerException;

import org.xml.sax.SAXParseException;

import de.pangaea.metadataportal.config.Config;
import de.pangaea.metadataportal.config.HarvesterConfig;
import de.pangaea.metadataportal.config.TargetIndexConfig;
import de.pangaea.metadataportal.processor.ElasticsearchConnection;
import de.pangaea.metadataportal.processor.DocumentProcessor;
import de.pangaea.metadataportal.processor.BackgroundFailure;
import de.pangaea.metadataportal.processor.MetadataDocument;
import de.pangaea.metadataportal.utils.ISODateFormatter;

/**
 * Harvester interface to panFMP. This class is the abstract superclass of all
 * harvesters. It also supplies an entry point for the command line interface.
 * <p>
 * All panFMP harvesters support the following <b>harvester properties</b>:
 * <ul>
 * <li><code>harvestMessageStep</code>: After how many documents should a status
 * message be printed out by the method {@link #addDocument}? (default: 100)</li>
 * <li><code>numThreads</code>: how many threads should process
 * documents (XPath queries and XSL templates)? (default: 1) Raise this value,
 * if the indexer waits to often for more documents and you have more than one
 * processor. The optimal value is one lower than the number of processors. If
 * you have very simple metadata documents (simple XML schmema) and few fields,
 * lower values may be enough. The optimal value could only be found by testing.
 * </li>
 * <li><code>maxQueue</code>: size of queue for threads.
 * (default 100 metadata documents)</li>
 * <li><code>bulkSize</code>: size of bulk requests sent to Elasticsearch. (default
 * 100 metadata documents)</li>
 * <li><code>concurrentBulkRequests</code>: how many bulk requests can be sent in
 * parallel to Elasticsearch. (default 1)</li>
 * <li><code>maxBulkMemory</code>: maximum size of CBOR/JSON source for a bulk request.
 * After a bulk gets larger than this, it will be submitted. Please note, that a bulk
 * might get significantly larger, because the check is done after the document is added.
 * Must be given using a unit like MB for megabytes. (default 5 MB)
 * </li>
 * <li><code>validate</code>: validate harvested documents against schema given
 * in configuration? (default: true, if schema given)</li>
 * <li><code>conversionErrorAction</code>: What to do if a conversion error
 * occurs (e.g. number format error)? Can be <code>STOP</code>,
 * <code>IGNOREDOCUMENT</code>, <code>DELETEDOCUMENT</code> (default is to stop
 * conversion)</li>
 * </ul>
 * 
 * @author Uwe Schindler
 */
public abstract class Harvester {
  
  private static final org.apache.commons.logging.Log staticLog = org.apache.commons.logging.LogFactory.getLog(Harvester.class);
  
  /**
   * External entry point to the harvester interface. Called from the Java
   * command line with two parameters (config file, harvester name)
   */
  public static void main(String[] args) {
    if (args.length < 1 || args.length > 2) {
      System.err.println("Command line: java " + Harvester.class.getName()
          + " config.xml [harvester-name|*]");
      return;
    }
    
    try {
      Config conf = new Config(args[0]);
      runHarvester(conf, (args.length == 2) ? args[1] : null);
    } catch (Exception e) {
      staticLog.fatal("Harvester general error:", e);
    }
  }
  
  /**
   * Harvests one (<code>harvesterId='name'</code>) or more (<code>harvesterId='*'</code>
   * ) sources. The harvester implementation is defined by the given
   * configuration.
   */
  public static boolean runHarvester(Config conf, String harvesterId) {
    return runHarvester(conf, harvesterId, null);
  }
  
  /**
   * Harvests one (<code>harvesterId="name"</code>) or more (
   * <code>harvesterId="*"/"all"/null</code>) sources. The harvester implementation is defined by
   * the given configuration or if <code>harvesterClass</code> is not
   * <code>null</code>, the specified harvester will be used. This is used by
   * {@link Rebuilder}. Public code should use
   * {@link #runHarvester(Config,String)}.
   */
  protected static boolean runHarvester(Config conf, String id, Class<? extends Harvester> harvesterClass) {
    final Set<String> activeIds;
    if (isAllIndexes(id)) {
      activeIds = conf.targetIndexes.keySet();
    } else {
      if (!conf.harvestersAndIndexes.contains(id))
        throw new IllegalArgumentException("There is no harvester or targetIndex defined with id=\"" + id + "\"!");
      activeIds = Collections.singleton(id);
    }
    
    if (Collections.disjoint(activeIds, conf.harvestersAndIndexes)) {
      staticLog.warn("No sources to harvest.");
      return false;
    }
    
    try (final ElasticsearchConnection es = new ElasticsearchConnection(conf)) {
      boolean anyUpdate = false;
      for (TargetIndexConfig ticonf : conf.targetIndexes.values()) {
        if (!activeIds.contains(ticonf.indexName) && Collections.disjoint(activeIds, ticonf.harvesters.keySet()))
          continue; // nothing to do for this index!
        final boolean isRebuilder = (harvesterClass == Rebuilder.class);
        final String targetIndex = es.createIndex(ticonf, isRebuilder);
        boolean globalCleanShutdown = true;
        for (HarvesterConfig harvesterConf : ticonf.harvesters.values()) {
          if (!(activeIds.contains(ticonf.indexName) || activeIds.contains(harvesterConf.id)))
            continue;
          final Class<? extends Harvester> hc = (harvesterClass == null) ? harvesterConf.harvesterClass : harvesterClass;
          staticLog.info("Harvesting documents from \"" + harvesterConf.id + "\" using harvester class \"" + hc.getName() + "\"...");
          Harvester h = null;
          boolean cleanShutdown = false;
          try {
            try {
              h = hc.getConstructor(HarvesterConfig.class).newInstance(harvesterConf);
              h.open(es, targetIndex);
              h.harvest();
              // everything OK => clean shutdown with storing all infos
              cleanShutdown = true;
            } catch (Exception e) {
              logExceptions(e, harvesterConf);
            }
            // cleanup
            if (h != null && !h.isClosed()) try {
              h.close(cleanShutdown);
              staticLog.info("Harvester \"" + harvesterConf.id + "\" closed.");
            } catch (Exception e) {
              staticLog.fatal("Error during harvesting from \"" + harvesterConf.id
                  + "\" occurred:", e);
            }
          } finally {
            globalCleanShutdown &= cleanShutdown;
          }
        }
        es.closeIndex(ticonf, targetIndex, globalCleanShutdown);
        anyUpdate |= globalCleanShutdown;
      }
      return anyUpdate;
    }
  }
  
  private static void logExceptions(Throwable e, HarvesterConfig harvesterConf) {
    if (e instanceof SAXParseException) {
      final SAXParseException saxe = (SAXParseException) e;
      staticLog.fatal("Harvesting documents from \"" + harvesterConf.id
          + "\" failed due to SAX parse error in \""
          + saxe.getSystemId() + "\", line " + saxe.getLineNumber()
          + ", column " + saxe.getColumnNumber() + ":", saxe);
    } else if (e instanceof TransformerException) {
      final TransformerException transfe = (TransformerException) e;
      String loc = transfe.getLocationAsString();
      staticLog.fatal("Harvesting documents from \"" + harvesterConf.id
          + "\" failed due to transformer/parse error"
          + ((loc != null) ? (" at " + loc) : "") + ":", transfe);
    } else if (e instanceof InvocationTargetException && e.getCause() != null) {
      logExceptions(e.getCause(), harvesterConf);
    } else {
      staticLog.fatal("Harvesting documents from \"" + harvesterConf.id + "\" failed!", e);
    } 
  }
  
  protected static boolean isAllIndexes(String id) {
    return id == null || "*".equals(id) || "all".equals(id);
  }
  
  /**
   * Logger instance (shared by all subclasses).
   */
  protected final org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory
      .getLog(this.getClass());
  
  /**
   * Instance of {@link DocumentProcessor} that converts and updates the Elasticsearch instance
   * in other threads.
   */
  protected DocumentProcessor processor = null;
  
  /**
   * Harvester configuration
   */
  protected final HarvesterConfig iconfig;
  
  /**
   * Count of harvested documents. Incremented by {@link #addDocument}.
   */
  protected int harvestCount = 0;
  
  /**
   * Step at which {@link #addDocument} prints log messages. Can be changed by
   * the harvester property <code>harvestMessageStep</code>.
   */
  protected final int harvestMessageStep;
  
  /**
   * Date from which should be harvested (in time reference of the original
   * server)
   */
  protected Instant fromDateReference = null;
  
  /**
   * Default constructor.
   */
  public Harvester(HarvesterConfig iconfig) {
    if (iconfig == null) throw new IllegalArgumentException("Missing harvester configuration");
    this.iconfig = iconfig;
    harvestMessageStep = Integer.parseInt(iconfig.properties
        .getProperty("harvestMessageStep", "100"));
    if (harvestMessageStep <= 0) throw new IllegalArgumentException(
        "Invalid value for harvestMessageStep: " + harvestMessageStep);
  }
  
  /**
   * Opens harvester for harvesting documents described by the
   * given {@link HarvesterConfig}. Opens {@link #processor} for usage in
   * {@link #harvest} method.
   * 
   * @throws Exception
   *           if an exception occurs during opening (various types of
   *           exceptions can be thrown).
   */
  public void open(ElasticsearchConnection es, String targetIndex) throws Exception {
    processor = es.getDocumentProcessor(iconfig, targetIndex);
    Object v = processor.harvesterMetadata.get(HARVESTER_METADATA_FIELD_LAST_HARVESTED);
    fromDateReference = (v != null) ? Instant.parse(v.toString()) : null;
  }
  
  /**
   * Prepares harvester for rebuilding the index by {@link Rebuilder}.
   * By default this method does nothing, but can be overridden by subclasses
   * that need to setup additional things.
   * 
   * @throws Exception
   *           if an exception occurs during opening (various types of
   *           exceptions can be thrown).
   */
  public void prepareReindex(ElasticsearchConnection es, String targetIndex) throws Exception {
  }
  
  /**
   * Does cleanup work after rebuilding the index by {@link Rebuilder}.
   * By default this method does nothing, but can be overridden by subclasses
   * that need to shutdown additional things.
   * 
   * @throws Exception
   *           if an exception occurs during closing (various types of
   *           exceptions can be thrown). Exceptions can be thrown asynchronous
   *           and may not affect the correct document.
   */
  public void finishReindex(boolean cleanShutdown) throws Exception {
  }
  
  /**
   * Checks if harvester is closed.
   */
  public boolean isClosed() {
    return (processor == null);
  }
  
  /**
   * Closes harvester. All resources are freed and the {@link #processor} is
   * closed.
   * 
   * @param cleanShutdown
   *          enables writing of status information to the Elasticsearch instance for the next
   *          harvesting. If an error occurred during harvesting this should not
   *          be done.
   * @throws Exception
   *           if an exception occurs during closing (various types of
   *           exceptions can be thrown). Exceptions can be thrown asynchronous
   *           and may not affect the correct document.
   */
  public void close(boolean cleanShutdown) throws Exception {
    if (processor == null) throw new IllegalStateException("Harvester must be opened before closing");
    
    if (cleanShutdown && harvestingDateReference != null) {
      processor.harvesterMetadata.put(HARVESTER_METADATA_FIELD_LAST_HARVESTED, ISODateFormatter.formatElasticsearch(harvestingDateReference));
    }
    harvestingDateReference = null;
    
    if (!processor.isClosed()) {
      processor.close(cleanShutdown ? validIdentifiers : null);
    }
    processor = null;
    validIdentifiers = null;
    
    if (cleanShutdown) {
      log.info("Harvested " + harvestCount + " objects - finished.");
    } else {
      log.warn("Harvesting stopped unexspected, but " + harvestCount + " objects harvested - finished.");
    }
  }
  
  /**
   * Creates an instance of MetadataDocument and initializes it with the harvester
   * config. This method should be overwritten, if a harvester uses another
   * class.
   */
  public MetadataDocument createMetadataDocumentInstance() {
    return new MetadataDocument(iconfig);
  }
  
  /**
   * Adds a document to the {@link #processor} working in the background.
   * 
   * @throws BackgroundFailure
   *           if an error occurred in background thread. Exceptions can be
   *           thrown asynchronous and may not affect the currect document. The
   *           real exception is thrown again in {@link #close}.
   */
  protected void addDocument(MetadataDocument mdoc) throws Exception {
    if (processor == null) throw new IllegalStateException(
        "Harvester must be opened before using");
    processor.addDocument(mdoc);
    harvestCount++;
    if (harvestCount % harvestMessageStep == 0) log.info("Harvested " + harvestCount + " objects so far.");
  }
  
  /**
   * Queues the given ID for deletion. This delegates to {@link #addDocument},
   * with an empty document.
   * 
   * @throws BackgroundFailure
   *           if an error occurred in background thread. Exceptions can be
   *           thrown asynchronous and may not affect the currect document. The
   *           real exception is thrown again in {@link #close}.
   */
  protected void deleteDocument(String identifier) throws Exception {
    final MetadataDocument mdoc = createMetadataDocumentInstance();
    mdoc.setIdentifier(identifier);
    mdoc.setDeleted(true);
    addDocument(mdoc);
  }
  
  /**
   * Checks, if the supplied Datestamp needs harvesting. This method can be used
   * to find out, if a documents needs harvesting.
   */
  protected boolean isDocumentOutdated(Instant lastModified) {
    return fromDateReference == null || lastModified == null || fromDateReference.isBefore(lastModified);
  }
  
  /**
   * Reference date of this harvesting event (in time reference of the original
   * server). This date is used on the next harvesting in variable
   * {@link #fromDateReference}. As long as this is null, the harvester will not
   * write or update the value in Elasticsearch.
   */
  protected void setHarvestingDateReference(Instant harvestingDateReference) {
    this.harvestingDateReference = harvestingDateReference;
  }
  
  /**
   * Set a set of all "seen" valid identifiers. Must be set, before
   * {@link #close(boolean)} is called, as the information is passed
   * to the processor before finalizing the index.
   */
  protected void setValidIdentifiers(Set<String> validIdentifiers) {
    this.validIdentifiers = validIdentifiers;
  }
  
  /**
   * This method is used by subclasses to enumerate all available harvester
   * properties that are implemented by them. Overwrite this method in your own
   * implementation and append all harvester names to the supplied
   * <code>Set</code>. The public API for client code requesting property names
   * is {@link #getValidHarvesterPropertyNames}.
   * 
   * @see #getValidHarvesterPropertyNames
   */
  protected void enumerateValidHarvesterPropertyNames(Set<String> props) {
    props.addAll(Arrays.asList(
        // own
        "harvestMessageStep",
        // DocumentProcessor
        "bulkSize", "deleteUnseenBulkSize", "numThreads", "maxQueue", "maxBulkMemory", "sourceContentType", "concurrentBulkRequests",
        "conversionErrorAction",
        // XMLConverter
        "validate"));
  }
  
  /**
   * Return the <code>Set</code> of harvester property names that this harvester
   * supports. This method is called on {@link Config} loading to check if all
   * property names in the config file are correct. You cannot override this
   * method in your own implementation, as this method is responsible for
   * returning an unmodifieable <code>Set</code>. For custom harvesters, append
   * your property names in {@link #enumerateValidHarvesterPropertyNames}.
   * 
   * @see #enumerateValidHarvesterPropertyNames
   */
  public final Set<String> getValidHarvesterPropertyNames() {
    TreeSet<String> props = new TreeSet<>();
    enumerateValidHarvesterPropertyNames(props);
    return Collections.unmodifiableSet(props);
  }
  
  /**
   * This method is called by the harvester after {@link #open}'ing it.
   * Overwrite this method in your harvester class. This method should harvest
   * files from somewhere, generate {@link MetadataDocument}s and add them with
   * {@link #addDocument}.
   * 
   * @throws Exception
   *           of any type.
   */
  public abstract void harvest() throws Exception;
  
  // private mebers
  private Instant harvestingDateReference = null;
  private Set<String> validIdentifiers = null;
  
  public static final String HARVESTER_METADATA_FIELD_LAST_HARVESTED = "lastHarvested";
  
}