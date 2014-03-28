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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Set;
import java.util.TreeSet;

import javax.xml.transform.TransformerException;

import org.xml.sax.SAXParseException;

import de.pangaea.metadataportal.config.Config;
import de.pangaea.metadataportal.config.HarvesterConfig;
import de.pangaea.metadataportal.processor.ElasticSearchConnection;
import de.pangaea.metadataportal.processor.CommitEvent;
import de.pangaea.metadataportal.processor.DocumentProcessor;
import de.pangaea.metadataportal.processor.BackgroundFailure;
import de.pangaea.metadataportal.processor.MetadataDocument;

/**
 * Harvester interface to panFMP. This class is the abstract superclass of all
 * harvesters. It also supplies an entry point for the command line interface.
 * <p>
 * All panFMP harvesters support the following <b>harvester properties</b>:
 * <ul>
 * <li><code>harvestMessageStep</code>: After how many documents should a status
 * message be printed out by the method {@link #addDocument}? (default: 100)</li>
 * <li><code>bulkSize</code>: how many documents should be
 * harvested before the changes are written to disk? If
 * {@link CommitEvent}s are used, the changes are also committed (seen
 * by search service) after this number of changes (default: 100)</li>
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
  
  private static final org.apache.commons.logging.Log staticLog = org.apache.commons.logging.LogFactory
      .getLog(Harvester.class);
  
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
      runHarvester(conf, (args.length == 2) ? args[1] : "*");
    } catch (Exception e) {
      staticLog.fatal("Harvester general error:", e);
    }
  }
  
  /**
   * Harvests one (<code>harvesterId='name'</code>) or more (<code>harvesterId='*'</code>
   * ) sources. The harvester implementation is defined by the given
   * configuration.
   */
  public static void runHarvester(Config conf, String harvesterId) {
    runHarvester(conf, harvesterId, null);
  }
  
  /**
   * Harvests one (<code>harvesterId="name"</code>) or more (
   * <code>harvesterId="*"</code>) sources. The harvester implementation is defined by
   * the given configuration or if <code>harvesterClass</code> is not
   * <code>null</code>, the specified harvester will be used. This is used by
   * {@link Rebuilder}. Public code should use
   * {@link #runHarvester(Config,String)}.
   */
  protected static void runHarvester(Config conf, String harvesterId,
      Class<? extends Harvester> harvesterClass) {
    Collection<HarvesterConfig> harvesterList = null;
    if (harvesterId == null || "*".equals(harvesterId) || "all".equals(harvesterId)) {
      harvesterList = conf.harvesteres.values();
    } else {
      HarvesterConfig iconf = conf.harvesteres.get(harvesterId);
      if (iconf == null || !(iconf instanceof HarvesterConfig)) throw new IllegalArgumentException(
          "There is no harvester defined with id=\"" + harvesterId + "\"!");
      harvesterList = Collections.singletonList(iconf);
    }
    
    final ElasticSearchConnection es = new ElasticSearchConnection(conf);
    try {
      for (HarvesterConfig siconf : harvesterList) {
        Class<? extends Harvester> hc = (harvesterClass == null) ? siconf.harvesterClass
            : harvesterClass;
        staticLog.info("Harvesting documents from \"" + siconf.id
            + "\" using harvester class \"" + hc.getName() + "\"...");
        Harvester h = null;
        boolean cleanShutdown = false;
        try {
          h = hc.newInstance();
          h.open(es, siconf);
          h.harvest();
          // everything OK => clean shutdown with storing all infos
          cleanShutdown = true;
        } catch (BackgroundFailure ibf) {
          // do nothing, this exception is only to break out, real exception is
          // thrown on close
        } catch (SAXParseException saxe) {
          staticLog.fatal(
              "Harvesting documents from \"" + siconf.id
                  + "\" failed due to SAX parse error in \""
                  + saxe.getSystemId() + "\", line " + saxe.getLineNumber()
                  + ", column " + saxe.getColumnNumber() + ":", saxe);
        } catch (TransformerException transfe) {
          String loc = transfe.getLocationAsString();
          staticLog.fatal("Harvesting documents from \"" + siconf.id
              + "\" failed due to transformer/parse error"
              + ((loc != null) ? (" at " + loc) : "") + ":", transfe);
        } catch (Exception e) {
          staticLog.fatal("Harvesting documents from \"" + siconf.id
              + "\" failed!", e);
        }
        // cleanup
        if (h != null && !h.isClosed()) try {
          h.close(cleanShutdown);
          staticLog.info("Harvester \"" + siconf.id + "\" closed.");
        } catch (Exception e) {
          staticLog.fatal("Error during harvesting from \"" + siconf.id
              + "\" occurred:", e);
        }
      }
    } finally {
      es.close();
    }
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
  protected HarvesterConfig iconfig = null;
  
  /**
   * Count of harvested documents. Incremented by {@link #addDocument}.
   */
  protected int harvestCount = 0;
  
  /**
   * Step at which {@link #addDocument} prints log messages. Can be changed by
   * the harvester property <code>harvestMessageStep</code>.
   */
  protected int harvestMessageStep = 100;
  
  /**
   * Date from which should be harvested (in time reference of the original
   * server)
   */
  protected Date fromDateReference = null;
  
  /**
   * Default constructor.
   */
  public Harvester() {}
  
  /**
   * Opens harvester for harvesting documents described by the
   * given {@link HarvesterConfig}. Opens {@link #processor} for usage in
   * {@link #harvest} method.
   * 
   * @throws Exception
   *           if an exception occurs during opening (various types of
   *           exceptions can be thrown).
   */
  public void open(ElasticSearchConnection es, HarvesterConfig iconfig) throws Exception {
    if (iconfig == null) throw new IllegalArgumentException(
        "Missing harvester configuration");
    this.iconfig = iconfig;
    harvestMessageStep = Integer.parseInt(iconfig.harvesterProperties
        .getProperty("harvestMessageStep", "100"));
    if (harvestMessageStep <= 0) throw new IllegalArgumentException(
        "Invalid value for harvestMessageStep: " + harvestMessageStep);
    processor = es.getDocumentProcessor(iconfig);
    
    fromDateReference = processor.getLastHarvestedFromDisk();
  }
  
  /**
   * Checks if harvester is closed.
   */
  public boolean isClosed() {
    return (processor == null);
  }
  
  /**
   * Closes harvester. All ressources are freed and the {@link #processor} is
   * closed.
   * 
   * @param cleanShutdown
   *          enables writing of status information to the Elasticsearch instance for the next
   *          harvesting. If an error occured during harvesting this should not
   *          be done.
   * @throws Exception
   *           if an exception occurs during closing (various types of
   *           exceptions can be thrown). Exceptions can be thrown asynchronous
   *           and may not affect the currect document.
   */
  public void close(boolean cleanShutdown) throws Exception {
    if (processor == null) throw new IllegalStateException(
        "Harvester must be opened before closing");
    
    if (cleanShutdown && harvestingDateReference != null) processor
        .setLastHarvested(harvestingDateReference);
    
    if (!processor.isClosed()) processor.close();
    processor = null;
    
    if (cleanShutdown) log.info("Harvested " + harvestCount
        + " objects - finished.");
    else log.warn("Harvesting stopped unexspected, but " + harvestCount
        + " objects harvested - finished.");
  }
  
  /**
   * Creates an instance of MetadataDocument and initializes it with the harvester
   * config. This method should be overwritten, if a harvester uses another
   * class.
   */
  protected MetadataDocument createMetadataDocumentInstance() {
    return new MetadataDocument(iconfig);
  }
  
  /**
   * Adds a document to the {@link #processor} working in the background.
   * 
   * @throws BackgroundFailure
   *           if an error occurred in background thread. Exceptions can be
   *           thrown asynchronous and may not affect the currect document. The
   *           real exception is thrown again in {@link #close}.
   * @throws InterruptedException
   *           if wait operation was interrupted.
   */
  protected void addDocument(MetadataDocument mdoc)
      throws BackgroundFailure, InterruptedException {
    if (processor == null) throw new IllegalStateException(
        "Harvester must be opened before using");
    processor.addDocument(mdoc);
    harvestCount++;
    if (harvestCount % harvestMessageStep == 0) log.info("Harvested "
        + harvestCount + " objects so far.");
  }
  
  /**
   * Checks, if the supplied Datestamp needs harvesting. This method can be used
   * to find out, if a documents needs harvesting.
   * 
   * @see #isDocumentOutdated(long)
   */
  protected final boolean isDocumentOutdated(Date lastModified) {
    return isDocumentOutdated((lastModified == null) ? -1L : lastModified
        .getTime());
  }
  
  /**
   * Checks, if the supplied Datestamp needs harvesting. This method can be used
   * to find out, if a documents needs harvesting.
   * 
   * @see #isDocumentOutdated(Date)
   */
  protected boolean isDocumentOutdated(long lastModified) {
    return (lastModified <= 0L || fromDateReference == null || fromDateReference
        .getTime() < lastModified);
  }
  
  /**
   * Reference date of this harvesting event (in time reference of the original
   * server). This date is used on the next harvesting in variable
   * {@link #fromDateReference}. As long as this is null, the harvester will not
   * write or update the value in Elasticsearch.
   */
  protected void setHarvestingDateReference(Date harvestingDateReference) {
    this.harvestingDateReference = harvestingDateReference;
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
    props.addAll(Arrays.<String> asList(
        // own
        "harvestMessageStep",
        // DocumentProcessor
        "targetIndex",
        "bulkSize", "numThreads", "maxQueue",
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
    TreeSet<String> props = new TreeSet<String>();
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
  private Date harvestingDateReference = null;
  
}