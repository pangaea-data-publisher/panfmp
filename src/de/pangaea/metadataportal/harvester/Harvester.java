/*
 *   Copyright 2007 panFMP Developers Team c/o Uwe Schindler
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
     * Return the list of harvester property names that this harvester supports.
     * This method is called on {@link Config} loading to check if all property names in the config file are correct.
     * Overwrite this method in your own implementation and create a new {@link List} with the <code>List</code> returned by
     * the superclass as basis.
     */
    public List<String> getValidHarvesterPropertyNames() {
        return Arrays.<String>asList(
            // own
            "harvestMessageStep",
            // IndexBuilder
            "changesBeforeIndexCommit",
            "numConverterThreads",
            "maxConverterQueue",
            "maxIndexerQueue",
            "autoOptimize",
            // XMLConverter
            "validate"
        );
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