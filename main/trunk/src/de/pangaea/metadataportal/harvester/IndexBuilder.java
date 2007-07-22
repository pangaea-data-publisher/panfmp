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

import de.pangaea.metadataportal.utils.*;
import de.pangaea.metadataportal.config.*;
import java.util.*;
import java.io.*;
import org.apache.lucene.analysis.standard.*;
import org.apache.lucene.index.*;
import org.apache.lucene.document.*;
import org.apache.lucene.store.*;

public class IndexBuilder {
    private static org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(IndexBuilder.class);

    protected SingleIndexConfig iconfig;
    private FSDirectory dir;
    private Date lastHarvested=null;
    private boolean create;

    private volatile boolean indexerFinished=false,converterFinished=false;
    private volatile int runningConverters=0;
    private volatile TreeMap<String,Document> ldocBuffer=new TreeMap<String,Document>();
    private volatile Stack<MetadataDocument> mdocBuffer=new Stack<MetadataDocument>();
    private volatile Exception failure=null;
    private volatile long maxWaitForIndexer=-1L;
    private volatile HarvesterCommitEvent commitEvent=null;
    private int maxConverterQueue;
    private int minChangesBeforeCommit;
    private int maxChangesBeforeCommit;

    private Thread indexerThread;
    private ThreadGroup converterThreads;
    private Thread[] converterThreadList;
    private Object commitEventLock=new Object();
    private Object converterQueueLock=mdocBuffer,indexerQueueLock=new Object();
    private Object converterQueueFullLock=new Object(),indexerQueueFullLock=new Object();
    private boolean threadsStarted=false;

    public IndexBuilder(boolean create, SingleIndexConfig iconfig) throws IOException {
        if (!IndexReader.indexExists(iconfig.getFullIndexPath())) create=true;
        this.dir=FSDirectory.getDirectory(iconfig.getFullIndexPath());
        this.create=create;
        this.iconfig=iconfig;
        if (create) try {
            dir.deleteFile(IndexConstants.FILENAME_LASTHARVESTED);
        } catch (IOException e) {}

        // converter threads
        converterThreads=new ThreadGroup(getClass().getName()+"#Converter#ThreadGroup");
        int threadCount=Integer.parseInt(iconfig.harvesterProperties.getProperty("numConverterThreads","1"));
        if (threadCount<1) throw new IllegalArgumentException("numConverterThreads harvester-property must be >=1!");
        converterThreadList=new Thread[threadCount];
        for (int i=0; i<threadCount; i++) {
            converterThreadList[i]=new Thread(converterThreads,new Runnable() {
                public void run() { converterThreadRun(); }
            },getClass().getName()+"#Converter#"+(i+1));
        }

        // indexer
        indexerThread=new Thread(new Runnable() {
            public void run() { indexerThreadRun(); }
        },getClass().getName()+"#Indexer");

        // properties
        setMaxConverterQueue(
            Integer.parseInt(iconfig.harvesterProperties.getProperty("maxConverterQueue","250"))
        );
        setChangesBeforeCommit(
            Integer.parseInt(iconfig.harvesterProperties.getProperty("minChangesBeforeIndexCommit","1000")),
            Integer.parseInt(iconfig.harvesterProperties.getProperty("maxChangesBeforeIndexCommit","2000"))
        );
    }

    public boolean isCreatingNew() {
        return create;
    }

    public void setChangesBeforeCommit(int min, int max) {
        if (min<1 || max<1 || max<min) throw new IllegalArgumentException("Invalid values for minChangesBeforeCommit, maxChangesBeforeCommit.");
        synchronized(indexerQueueLock) {
            this.minChangesBeforeCommit=min;
            this.maxChangesBeforeCommit=max;
            if (ldocBuffer.size()>=minChangesBeforeCommit) indexerQueueLock.notifyAll();
        }
    }

    public void setMaxConverterQueue(int max) {
        if (max<1) throw new IllegalArgumentException("Invalid value for maxConverterQueue.");
        synchronized(converterQueueLock) {
            this.maxConverterQueue=max;
            converterQueueLock.notifyAll();
        }
    }

    public void setMaxWaitForIndexer(long maxWaitForIndexer) {
        synchronized(indexerQueueLock) {
            this.maxWaitForIndexer=maxWaitForIndexer;
        }
    }

    public void registerHarvesterCommitEvent(HarvesterCommitEvent event) {
        synchronized(commitEventLock) {
            commitEvent=event;
        }
    }

    public boolean isClosed() {
        return (indexerThread==null || converterThreads==null || converterThreadList==null);
    }

    public void close() throws Exception {
        if (isClosed()) throw new IllegalStateException("IndexBuilder already closed");

        if (failure!=null) {
            throwFailure();
        } else {
            startThreads(true);

            synchronized(converterQueueLock) {
                converterFinished=true;
                converterQueueLock.notifyAll();
            }
            for (Thread t : converterThreadList) {
                if (t.isAlive()) try {
                    t.join();
                } catch (InterruptedException e) {}
            }

            // wait for indexer
            synchronized(indexerQueueLock) {
                indexerFinished=true;
                indexerQueueLock.notifyAll();
            }
            if (indexerThread.isAlive()) try {
                indexerThread.join();
            } catch (InterruptedException e) {}

            throwFailure();
        }

        if (lastHarvested!=null) {
            org.apache.lucene.store.IndexOutput out=dir.createOutput(IndexConstants.FILENAME_LASTHARVESTED);
            out.writeLong(lastHarvested.getTime());
            out.close();
            lastHarvested=null;
        }

        converterThreads=null;
        converterThreadList=null;
        indexerThread=null;
    }

    public void addDocument(MetadataDocument mdoc) throws Exception {
        if (isClosed()) throw new IllegalStateException("IndexBuilder already closed");
        throwFailure();

        startThreads(false);

        if (mdocBuffer.size()>=maxConverterQueue) internalWaitConverter(); // mdocBuffer is synchronized!
        synchronized(converterQueueLock) {
            mdocBuffer.push(mdoc);
            converterQueueLock.notify();
        }
    }

    // call this between harvest resumptions to give the indexer a chance NOW to set this thread to wait not while HTTP transfers
    public void checkIndexerBuffer() throws Exception {
        if (isClosed()) throw new IllegalStateException("IndexBuilder already closed");
        throwFailure();

        startThreads(false);

        boolean doWait;
        synchronized(indexerQueueLock) {
            doWait=(ldocBuffer.size()>=(minChangesBeforeCommit+maxChangesBeforeCommit)/2);
        }
        if (doWait) internalWaitIndexer();
    }

    // sets the date of last harvesting (written to disk after closing!!!)
    public void setLastHarvested(Date datestamp) {
        this.lastHarvested=datestamp;
    }

    public Date getLastHarvestedFromDisk() {
        if (create) return null;
        org.apache.lucene.store.IndexInput in=null;
        Date d=null;
        try {
            in=dir.openInput(IndexConstants.FILENAME_LASTHARVESTED);
            d=new Date(in.readLong());
            in.close();
        } catch (IOException e) {
            if (in!=null) try { in.close(); } catch (IOException ie) {}
            d=null;
        }
        return d;
    }

    private void converterThreadRun() {
        org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(Thread.currentThread().getName());
        log.info("Converter thread started.");
        XPathResolverImpl.getInstance().setIndexBuilder(this);
        try {
            while ((!converterFinished || mdocBuffer.size()>0) && failure==null) {
                MetadataDocument mdoc;
                synchronized(converterQueueFullLock) {
                    // notify eventually waiting threads
                    converterQueueFullLock.notifyAll();
                }
                synchronized(converterQueueLock) {
                    // wait if queue is empty
                    if (!converterFinished && mdocBuffer.size()==0) try {
                        converterQueueLock.wait();
                    } catch (InterruptedException ie) {}
                    // fetch a document
                    if (mdocBuffer.size()==0) continue;
                    mdoc=mdocBuffer.pop();
                }

                if (Thread.interrupted()) break;
                if (log.isDebugEnabled()) log.debug("Handling document: "+mdoc.toString());
                if (log.isTraceEnabled()) log.trace("XML: "+mdoc.getXML());

                Document ldoc=mdoc.getLuceneDocument(iconfig);

                boolean doWait;
                synchronized(indexerQueueLock) {
                    doWait=(ldocBuffer.size()>=maxChangesBeforeCommit);
                }
                if (doWait) internalWaitIndexer();
                synchronized(indexerQueueLock) {
                    ldocBuffer.put(mdoc.identifier,ldoc);
                    if (ldocBuffer.size()>=minChangesBeforeCommit) indexerQueueLock.notify();
                }
            }

            // notify indexer of end
            synchronized(converterQueueLock) {
                // is this really the last running converter?
                if (runningConverters<=1) synchronized(indexerQueueLock) {
                    indexerFinished=true;
                    indexerQueueLock.notifyAll();
                }
            }
        } catch (Exception e) {
            log.debug(e);
            failure=e;
        } finally {
            synchronized(converterQueueLock) {
                runningConverters--;
            }
            XPathResolverImpl.getInstance().unsetIndexBuilder();
            log.info("Converter thread stopped.");
        }

        synchronized(converterQueueFullLock) {
            // notify eventually waiting threads
            converterQueueFullLock.notifyAll();
        }
    }

    // this thread eats from the docs and fills index
    private void indexerThreadRun() {
        org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(Thread.currentThread().getName());
        log.info("Indexer thread started.");
        IndexWriter writer=null;
        try {
            writer = new IndexWriter(dir, true, iconfig.parent.getAnalyzer(), !IndexReader.indexExists(dir));
            //writer.setInfoStream(System.err);
            writer.setMaxFieldLength(Integer.MAX_VALUE);
            writer.setMaxBufferedDocs(minChangesBeforeCommit);
            writer.setMaxBufferedDeleteTerms(minChangesBeforeCommit);

            int docBufferSize=0;
            do {
                TreeMap<String,Document> docs;
                synchronized(indexerQueueFullLock) {
                    // notify eventually waiting threads
                    indexerQueueFullLock.notifyAll();
                }
                synchronized(indexerQueueLock) {
                    // wait if queue is too empty
                    if (!indexerFinished && ldocBuffer.size()<minChangesBeforeCommit) try {
                        indexerQueueLock.wait();
                    } catch (InterruptedException ie) {}
                    // fetch docs and replace by new one
                    docs=ldocBuffer;
                    ldocBuffer=new TreeMap<String,Document>();
                }

                log.info("Updating index...");

                int updated=0, deleted=0;
                for (Map.Entry<String,Document> docEntry : docs.entrySet()) {
                    if (Thread.interrupted()) break;
                    // map contains NULL if only delete doc
                    Term t=new Term(IndexConstants.FIELDNAME_IDENTIFIER,docEntry.getKey());
                    Document ldoc=docEntry.getValue();
                    if (ldoc==null) {
                        writer.deleteDocuments(t);
                        deleted++;
                    } else {
                        writer.updateDocument(t,ldoc);
                        updated++;
                    }
                }

                // get current status of buffer
                synchronized(indexerQueueLock) { docBufferSize=ldocBuffer.size(); }

                synchronized(commitEventLock) {
                    // only flush if commitEvent interface registered
                    if (commitEvent!=null) writer.flush();

                    log.info(deleted+" docs presumably deleted (only if existent) and "+updated+" docs (re-)indexed.");

                    // notify Harvester of index commit
                    if (commitEvent!=null) commitEvent.harvesterCommitted(Collections.unmodifiableMap(docs).keySet().iterator());
                }
            } while ((!indexerFinished || docBufferSize>0) && failure==null);

            writer.flush();

            if (Boolean.parseBoolean(iconfig.harvesterProperties.getProperty("autoOptimize","false"))) {
                log.info("Optimizing index...");
                writer.optimize();
                log.info("Index optimized.");
            }
        } catch (IOException e) {
            log.debug(e);
            failure=e;
        } finally {
            if (writer!=null) try {
                writer.close();
            } catch (IOException ioe) {
                log.warn("Failed to close Lucene IndexWriter, you may need to remove lock files!",ioe);
            }
            writer=null;
            log.info("Indexer thread stopped.");
        }

        synchronized(indexerQueueFullLock) {
            // notify eventually waiting threads
            indexerQueueFullLock.notifyAll();
        }
    }

    private void throwFailure() throws Exception {
        if (failure!=null) {
            if (converterThreads!=null) converterThreads.interrupt();
            if (indexerThread!=null) indexerThread.interrupt();
            Exception f=failure;
            failure=null;
            throw f;
        }
    }

    private void startThreads(boolean onlyIndexer) {
        if (!threadsStarted) try {
            if (!onlyIndexer) for (Thread t : converterThreadList) {
                t.start();
                runningConverters++;
            }
            indexerThread.start();
        } finally {
            threadsStarted=true;
        }
    }

    private void checkThreads() {
        int rc=0;
        synchronized(converterQueueLock) {
            rc=runningConverters;
        }
        if (rc==0 || !indexerThread.isAlive())
            throw new IllegalStateException("IndexBuilder threads died unexspected!");
    }

    private void internalWaitConverter() {
        checkThreads();
        synchronized (converterQueueFullLock) {
            try {
                log.debug("Harvester is too fast for converter thread(s), waiting..."); // only debug, it happens often!
                converterQueueFullLock.wait();
            } catch (InterruptedException ie) {}
        }
    }

    private void internalWaitIndexer() {
        checkThreads();
        synchronized (indexerQueueFullLock) {
            try {
                log.info("Converter is too fast for indexer thread, waiting...");
                if (maxWaitForIndexer>0L) indexerQueueFullLock.wait(maxWaitForIndexer); else indexerQueueFullLock.wait();
            } catch (InterruptedException ie) {}
        }
    }

}