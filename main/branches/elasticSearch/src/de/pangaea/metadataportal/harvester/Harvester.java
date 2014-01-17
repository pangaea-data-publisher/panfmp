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

import java.util.*;
import de.pangaea.metadataportal.config.*;

import javax.xml.transform.TransformerException;
import org.xml.sax.SAXParseException;

/**
 * Harvester interface to panFMP. This class is the abstract superclass of all
 * harvesters. It also supplies an entry point for the command line interface.
 * <p>
 * All panFMP harvesters support the following <b>harvester properties</b>:
 * <ul>
 * <li><code>harvestMessageStep</code>: After how many documents should a status
 * message be printed out by the method {@link #addDocument}? (default: 100)</li>
 * <li><code>maxBufferedIndexChanges</code>: how many documents should be
 * harvested before the index changes are written to disk? If
 * {@link HarvesterCommitEvent}s are used, the changes are also committed (seen
 * by search service) after this number of changes (default: 1000)</li>
 * <li><code>numConverterThreads</code>: how many threads should convert
 * documents (XPath queries and XSL templates)? (default: 1) Raise this value,
 * if the indexer waits to often for more documents and you have more than one
 * processor. The optimal value is one lower than the number of processors. If
 * you have very simple metadata documents (simple XML schmema) and few fields,
 * lower values may be enough. The optimal value could only be found by testing.
 * </li>
 * <li><code>maxConverterQueue</code>: size of queue for converter threads.
 * (default 250 metadata documents)</li>
 * <li><code>maxIndexerQueue</code>: size of queue for indexer thread. (default
 * 250 metadata documents)</li>
 * <li><code>autoOptimize</code>: should the index be optimzed after harvesting
 * is finished? (default: false)</li>
 * <li><code>validate</code>: validate harvested documents against schema given
 * in configuration? (default: true, if schema given)</li>
 * <li><code>compressXML</code>: compress the harvested XML blob when storing in
 * index? (default: true)</li>
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
   * command line with two parameters (config file, index name)
   */
  public static void main(String[] args) {
    if (args.length < 1 || args.length > 2) {
      System.err.println("Command line: java " + Harvester.class.getName()
          + " config.xml [index-name|*]");
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
   * Harvests one (<code>index='indexname'</code> or more <code>index='*'</code>
   * ) indexes. The harvester implementation is defined by the given
   * configuration.
   */
  public static void runHarvester(Config conf, String index) {
    runHarvester(conf, index, null);
  }
  
  /**
   * Harvests one (<code>index="indexname"</code>) or more (
   * <code>index="*"</code>) indexes. The harvester implementation is defined by
   * the given configuration or if <code>harvesterClass</code> is not
   * <code>null</code>, the specified harvester will be used. This is used by
   * {@link Rebuilder}. Public code should use
   * {@link #runHarvester(Config,String)}.
   */
  protected static void runHarvester(Config conf, String index,
      Class<? extends Harvester> harvesterClass) {
    Collection<IndexConfig> indexList = null;
    if (index == null || "*".equals(index)) {
      indexList = conf.indexes.values();
    } else {
      IndexConfig iconf = conf.indexes.get(index);
      if (iconf == null || !(iconf instanceof IndexConfig)) throw new IllegalArgumentException(
          "There is no index defined with id=\"" + index + "\"!");
      indexList = Collections.singletonList(iconf);
    }
    
    for (IndexConfig siconf : indexList) {
      Class<? extends Harvester> hc = (harvesterClass == null) ? siconf.harvesterClass
          : harvesterClass;
      staticLog.info("Harvesting documents into index \"" + siconf.id
          + "\" using harvester \"" + hc.getName() + "\"...");
      Harvester h = null;
      boolean cleanShutdown = false;
      try {
        h = hc.newInstance();
        h.open(siconf);
        h.harvest();
        // everything OK => clean shutdown with storing all infos
        cleanShutdown = true;
      } catch (IndexBuilderBackgroundFailure ibf) {
        // do nothing, this exception is only to break out, real exception is
        // thrown on close
      } catch (SAXParseException saxe) {
        staticLog.fatal(
            "Harvesting documents into index \"" + siconf.id
                + "\" failed due to SAX parse error in \""
                + saxe.getSystemId() + "\", line " + saxe.getLineNumber()
                + ", column " + saxe.getColumnNumber() + ":", saxe);
      } catch (TransformerException transfe) {
        String loc = transfe.getLocationAsString();
        staticLog.fatal("Harvesting documents into index \"" + siconf.id
            + "\" failed due to transformer/parse error"
            + ((loc != null) ? (" at " + loc) : "") + ":", transfe);
      } catch (Exception e) {
        staticLog.fatal("Harvesting documents into index \"" + siconf.id
            + "\" failed!", e);
      }
      // cleanup
      if (h != null && !h.isClosed()) try {
        h.close(cleanShutdown);
        staticLog.info("Harvester for index \"" + siconf.id + "\" closed.");
      } catch (Exception e) {
        staticLog.fatal("Error during harvesting into index \"" + siconf.id
            + "\" occurred:", e);
      }
    }
  }
  
  /**
   * Logger instance (shared by all subclasses).
   */
  protected org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory
      .getLog(this.getClass());
  
  /**
   * Instance of {@link IndexBuilder} that converts and updates the Lucene index
   * in other threads.
   */
  protected IndexBuilder index = null;
  
  /**
   * Index configuration
   */
  protected IndexConfig iconfig = null;
  
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
   * Opens harvester for harvesting documents into the index described by the
   * given {@link SingleIndexConfig}. Opens {@link #index} for usage in
   * {@link #harvest} method.
   * 
   * @throws Exception
   *           if an exception occurs during opening (various types of
   *           exceptions can be thrown).
   */
  public void open(IndexConfig iconfig) throws Exception {
    if (iconfig == null) throw new IllegalArgumentException(
        "Missing index configuration");
    this.iconfig = iconfig;
    harvestMessageStep = Integer.parseInt(iconfig.harvesterProperties
        .getProperty("harvestMessageStep", "100"));
    if (harvestMessageStep <= 0) throw new IllegalArgumentException(
        "Invalid value for harvestMessageStep: " + harvestMessageStep);
    index = new IndexBuilder(false, iconfig);
    
    fromDateReference = index.getLastHarvestedFromDisk();
  }
  
  /**
   * Checks if harvester is closed.
   */
  public boolean isClosed() {
    return (index == null);
  }
  
  /**
   * Closes harvester. All ressources are freed and the {@link #index} is
   * closed.
   * 
   * @param cleanShutdown
   *          enables writing of status information to the index for the next
   *          harvesting. If an error occured during harvesting this should not
   *          be done.
   * @throws Exception
   *           if an exception occurs during closing (various types of
   *           exceptions can be thrown). Exceptions can be thrown asynchronous
   *           and may not affect the currect document.
   */
  public void close(boolean cleanShutdown) throws Exception {
    if (index == null) throw new IllegalStateException(
        "Harvester must be opened before closing");
    
    if (cleanShutdown && harvestingDateReference != null) index
        .setLastHarvested(harvestingDateReference);
    
    if (!index.isClosed()) index.close();
    index = null;
    
    if (cleanShutdown) log.info("Harvested " + harvestCount
        + " objects - finished.");
    else log.warn("Harvesting stopped unexspected, but " + harvestCount
        + " objects harvested - finished.");
  }
  
  /**
   * Creates an instance of MetadataDocument and initializes it with the index
   * config. This method should be overwritten, if a harvester uses another
   * class.
   */
  protected MetadataDocument createMetadataDocumentInstance() {
    return new MetadataDocument(iconfig);
  }
  
  /**
   * Adds a document to the {@link #index} working in the background.
   * 
   * @throws IndexBuilderBackgroundFailure
   *           if an error occurred in background thread. Exceptions can be
   *           thrown asynchronous and may not affect the currect document. The
   *           real exception is thrown again in {@link #close}.
   * @throws InterruptedException
   *           if wait operation was interrupted.
   */
  protected void addDocument(MetadataDocument mdoc)
      throws IndexBuilderBackgroundFailure, InterruptedException {
    if (index == null) throw new IllegalStateException(
        "Harvester must be opened before using");
    index.addDocument(mdoc);
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
   * write or update the value in the index directory.
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
        // IndexBuilder
        "maxBufferedIndexChanges", "numConverterThreads", "maxConverterQueue",
        "maxIndexerQueue", "autoOptimize", "conversionErrorAction",
        // IndexBuilder.XMLConverter
        "validate",
        // MetadataDocument
        "compressXML"));
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