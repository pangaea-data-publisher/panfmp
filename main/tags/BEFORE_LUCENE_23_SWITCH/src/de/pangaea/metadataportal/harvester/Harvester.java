/*
 *   Copyright 2007-2008 panFMP Developers Team c/o Uwe Schindler
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

/**
 * Harvester interface to panFMP. This class is the abstract superclass of all harvesters.
 * It also supplies an entry point for the command line interface.
 * <p>All panFMP harvesters support the following <b>harvester properties</b>:<ul>
 * <li><code>harvestMessageStep</code>: After how many documents should a status message be printed out by the method {@link #addDocument}? (default: 100)</li>
 * <li><code>changesBeforeIndexCommit</code>: how many documents should be harvested before the index changes are commit? (default: 1000)</li>
 * <li><code>numConverterThreads</code>: how many threads should convert documents (XPath queries and XSL templates)? (default: 1)
 * Raise this value, if the indexer waits to often for more documents and you have more than one processor. The optimal value is one lower than the number of processors. If you have very simple
 * metadata documents (simple XML schmema) and few fields, lower values may be enough. The optimal value could only be found by testing.</li>
 * <li><code>maxConverterQueue</code>: size of queue for converter threads. (default 250 metadata documents)</li>
 * <li><code>maxIndexerQueue</code>: size of queue for indexer thread. (default 250 metadata documents)</li>
 * <li><code>autoOptimize</code>: should the index be optimzed after harvesting is finished? (default: false)</li>
 * <li><code>validate</code>: validate harvested documents against schema given in configuration? (default: true, if schema given)</li>
 * <li><code>compressXML</code>: compress the harvested XML blob when storing in index? (default: true)</li>
 * </ul>
 * @author Uwe Schindler
 */
public abstract class Harvester {

	private static org.apache.commons.logging.Log staticLog = org.apache.commons.logging.LogFactory.getLog(Harvester.class);

	/**
	 * External entry point to the harvester interface. Called from the Java command line with two parameters (config file, index name)
	 */
	public static void main(String[] args) {
		if (args.length!=2) {
			System.err.println("Command line: java "+Harvester.class.getName()+" config.xml index-name|*");
			return;
		}

		try {
			Config conf=new Config(args[0],Config.ConfigMode.HARVESTING);
			runHarvester(conf,args[1]);
		} catch (Exception e) {
			staticLog.fatal("Harvester general error:",e);
		}
	}

	/**
	 * Harvests one (<code>index='indexname'</code> or more <code>index='*'</code>) indexes. The harvester
	 * implementation is defined by the given configuration.
	 */
	public static void runHarvester(Config conf, String index) {
		runHarvester(conf,index,null);
	}

	/**
	 * Harvests one (<code>index="indexname"</code>) or more (<code>index="*"</code>) indexes. The harvester
	 * implementation is defined by the given configuration or if
	 * <code>harvesterClass</code> is not <code>null</code>, the specified harvester will be used.
	 * This is used by {@link Rebuilder}.
	 * Public code should use {@link #runHarvester(Config,String)}.
	 */
	protected static void runHarvester(Config conf, String index, Class<? extends Harvester> harvesterClass) {
		Collection<IndexConfig> indexList=null;
		if (index==null || "*".equals(index)) {
			indexList=conf.indexes.values();
		} else {
			IndexConfig iconf=conf.indexes.get(index);
			if (iconf==null || !(iconf instanceof SingleIndexConfig)) throw new IllegalArgumentException("There is no index defined with id=\""+index+"\"!");
			indexList=Collections.singletonList(iconf);
		}

		for (IndexConfig iconf : indexList) if (iconf instanceof SingleIndexConfig) {
			SingleIndexConfig siconf=(SingleIndexConfig)iconf;

			Class<? extends Harvester> hc=(harvesterClass==null) ? siconf.harvesterClass : harvesterClass;
			staticLog.info("Harvesting documents into index \""+siconf.id+"\" using harvester \""+hc.getName()+"\"...");
			Harvester h=null;
			try {
				h=hc.newInstance();
				h.open(siconf);
				h.harvest();
			} catch (IndexBuilderBackgroundFailure ibf) {
				// do nothing, this exception is only to break out, real exception is thrown on close
			} catch (org.xml.sax.SAXParseException saxe) {
				staticLog.fatal("Harvesting documents into index \""+siconf.id+"\" failed due to SAX parse error in \""+saxe.getSystemId()+"\", line "+saxe.getLineNumber()+", column "+saxe.getColumnNumber()+":",saxe);
			} catch (Exception e) {
				if (e.getCause() instanceof org.xml.sax.SAXParseException) {
					org.xml.sax.SAXParseException saxe=(org.xml.sax.SAXParseException)e.getCause();
					staticLog.fatal("Harvesting documents into index \""+siconf.id+"\" failed due to SAX parse error in \""+saxe.getSystemId()+"\", line "+saxe.getLineNumber()+", column "+saxe.getColumnNumber()+":",saxe);
				} else staticLog.fatal("Harvesting documents into index \""+siconf.id+"\" failed!",e);
			}
			// cleanup
			if (h!=null && !h.isClosed()) try {
				h.close();
				staticLog.info("Harvester for index \""+siconf.id+"\" closed.");
			} catch (Exception e) {
				staticLog.fatal("Error during harvesting into index \""+siconf.id+"\" occurred:",e);
			}
		}
	}

	/**
	 * Logger instance (shared by all subclasses).
	 */
	protected org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(this.getClass());

	/**
	 * Instance of {@link IndexBuilder} that converts and updates the Lucene index in other threads.
	 */
	protected IndexBuilder index=null;

	/**
	 * Index configuration
	 */
	protected SingleIndexConfig iconfig=null;

	/**
	 * Instance of {@link XMLConverter} that helps to convert harvested XML documents.
	 * It does only convert SAX events or DOM trees to the final index DOM trees
	 * (by the index specific XSLT) and optionally validates the result.
	 */
	protected XMLConverter xmlConverter=null;

	/**
	 * Count of harvested documents. Incremented by {@link #addDocument}. Can be changed by
	 * the harvester property <code>harvestMessageStep</code>.
	 */
	protected int harvestCount=0;

	/**
	 * Step at which {@link #addDocument} prints log messages.
	 */
	protected int harvestMessageStep=100;

	/**
	 * Date from which should be harvested (in time reference of the original server)
	 */
	protected Date fromDateReference=null;

	/**
	 * Reference date of this harvesting event (in time reference of the original server).
	 * This date is used on the next harvesting in variable {@link #fromDateReference}.
	 * Be sure to set this variable only <b>after</b> the successful harvesting, probably at the end of {@link #harvest}.
	 * As long as this variable is null, the harvester will not write or update the value in the index directory.
	 */
	protected Date thisHarvestDateReference=null;

	/**
	 * Default constructor.
	 */
	public Harvester() {}

	/**
	 * Opens harvester for harvesting documents into the index described by the given {@link SingleIndexConfig}.
	 * Opens {@link #index} and {@link #xmlConverter} for usage in {@link #harvest} method.
	 * @throws Exception if an exception occurs during opening (various types of exceptions can be thrown).
	 */
	public void open(SingleIndexConfig iconfig) throws Exception {
		if (iconfig==null) throw new IllegalArgumentException("Missing index configuration");
		this.iconfig=iconfig;
		harvestMessageStep=Integer.parseInt(iconfig.harvesterProperties.getProperty("harvestMessageStep","100"));
		if (harvestMessageStep<=0) throw new IllegalArgumentException("Invalid value for harvestMessageStep: "+harvestMessageStep);
		index = new IndexBuilder(false,iconfig);
		xmlConverter=new XMLConverter(iconfig);

		fromDateReference=index.getLastHarvestedFromDisk();
	}

	/**
	 * Checks if harvester is closed.
	 */
	public boolean isClosed() {
		return (index==null);
	}

	/**
	 * Closes harvester. All ressources are freed and the {@link #index} is closed.
	 * @throws Exception if an exception occurs during closing (various types of exceptions can be thrown).
	 * Exceptions can be thrown asynchronous and may not affect the currect document.
	 */
	public void close() throws Exception {
		if (index==null) throw new IllegalStateException("Harvester must be opened before using");

		if (thisHarvestDateReference!=null) index.setLastHarvested(thisHarvestDateReference);

		if (!index.isClosed()) index.close();
		index=null;

		log.info("Harvested "+harvestCount+" objects - finished.");
	}

	/**
	 * Adds a document to the {@link #index} working in the background.
	 * @throws IndexBuilderBackgroundFailure if an error occurred in background thread.
	 * Exceptions can be thrown asynchronous and may not affect the currect document.
	 * The real exception is thrown again in {@link #close}.
	 * @throws InterruptedException if wait operation was interrupted.
	 */
	protected void addDocument(MetadataDocument mdoc) throws IndexBuilderBackgroundFailure,InterruptedException {
		if (index==null) throw new IllegalStateException("Harvester must be opened before using");
		mdoc.setIndexConfig(iconfig);
		index.addDocument(mdoc);
		harvestCount++;
		if (harvestCount%harvestMessageStep==0) log.info("Harvested "+harvestCount+" objects so far.");
	}

	/**
	 * Optional method: Assigns a {@link Set} of valid identifiers observed during harvesting.
	 * {@link IndexBuilder} will check all documents in index with this set and delete
	 * all documents that are missing in it.
	 * <B>Only assign this set after harvesting is finished and before closing the index.
	 * A set, that does not contain really all valid identifiers, deletes unwanted ones from the index!</B>
	 */
	protected void setValidIdentifiers(Set<String> validIdentifiers) {
		if (index==null && !index.isClosed()) throw new IllegalStateException("Harvester must be opened before using");
		index.setValidIdentifiers(validIdentifiers);
	}

	/**
	 * This method is used by subclasses to enumerate all available harvester properties that are implemented by them.
	 * Overwrite this method in your own implementation and append all harvester names to the supplied <code>Set</code>.
	 * The public API for client code requesting property names is {@link #getValidHarvesterPropertyNames}.
	 * @see #getValidHarvesterPropertyNames
	 */
	protected void enumerateValidHarvesterPropertyNames(Set<String> props) {
		props.addAll(Arrays.<String>asList(
			// own
			"harvestMessageStep",
			// IndexBuilder
			"changesBeforeIndexCommit",
			"numConverterThreads",
			"maxConverterQueue",
			"maxIndexerQueue",
			"autoOptimize",
			// XMLConverter
			"validate",
			// MetadataDocument
			"compressXML"
		));
	}

	/**
	 * Return the <code>Set</code> of harvester property names that this harvester supports.
	 * This method is called on {@link Config} loading to check if all property names in the config file are correct.
	 * You cannot override this method in your own implementation, as this method is
	 * responsible for returning an unmodifieable <code>Set</code>.
	 * For custom harvesters, append your property names in {@link #enumerateValidHarvesterPropertyNames}.
	 * @see #enumerateValidHarvesterPropertyNames
	 */
	public final Set<String> getValidHarvesterPropertyNames() {
		TreeSet<String> props=new TreeSet<String>();
		enumerateValidHarvesterPropertyNames(props);
		return Collections.unmodifiableSet(props);
	}

	/**
	 * This method is called by the harvester after {@link #open}'ing it. Overwrite this
	 * method in your harvester class.
	 * This method should harvest files from somewhere, generate {@link MetadataDocument}s and add
	 * them with {@link #addDocument}.
	 * @throws Exception of any type.
	 */
	public abstract void harvest() throws Exception;
}