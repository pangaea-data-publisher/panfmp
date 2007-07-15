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
    private Object commitEventLock=new Object(),converterThreadLock=mdocBuffer,indexerThreadLock=new Object();
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
            },getClass().getName()+"#Converter#"+i);
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
        synchronized(indexerThreadLock) {
            this.minChangesBeforeCommit=min;
            this.maxChangesBeforeCommit=max;
            if (ldocBuffer.size()>=minChangesBeforeCommit) indexerThreadLock.notify();
        }
    }

    public void setMaxConverterQueue(int max) {
        if (max<1) throw new IllegalArgumentException("Invalid value for maxConverterQueue.");
        synchronized(converterThreadLock) {
            this.maxConverterQueue=max;
            converterThreadLock.notifyAll();
        }
    }

    public void setMaxWaitForIndexer(long maxWaitForIndexer) {
        synchronized(indexerThreadLock) {
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
            startThreads();

            synchronized(converterThreadLock) {
                converterFinished=true;
                converterThreadLock.notifyAll();
            }
            for (Thread t : converterThreadList) {
                if (t.isAlive()) try {
                    t.join();
                } catch (InterruptedException e) {}
            }

            // wait for indexer
            synchronized(indexerThreadLock) {
                indexerFinished=true;
                indexerThreadLock.notifyAll();
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

        startThreads();

        synchronized(converterThreadLock) {
            if (mdocBuffer.size()>=maxConverterQueue) internalWaitConverter();
            mdocBuffer.push(mdoc);
            converterThreadLock.notify();
        }
    }

    // call this between harvest resumptions to give the indexer a chance NOW to set this thread to wait not while HTTP transfers
    public void checkIndexerBuffer() throws Exception {
        if (isClosed()) throw new IllegalStateException("IndexBuilder already closed");
        throwFailure();

        startThreads();

        synchronized(indexerThreadLock) {
            if (ldocBuffer.size()>=(minChangesBeforeCommit+maxChangesBeforeCommit)/2) internalWaitIndexer();
        }
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
        org.apache.commons.logging.Log converterLog = org.apache.commons.logging.LogFactory.getLog(Thread.currentThread().getName());
        XPathResolverImpl.getInstance().setIndexBuilder(this);
        try {
            while ((!converterFinished || mdocBuffer.size()>0) && failure==null) {
                MetadataDocument mdoc=null;
                synchronized(converterThreadLock) {
                    // notify eventually waiting threads
                    converterThreadLock.notify();
                    // wait if queue is empty
                    if (!converterFinished && mdocBuffer.size()==0) try {
                        converterThreadLock.wait();
                    } catch (InterruptedException ie) {}
                    // fetch docs and replace by new one
                    if (mdocBuffer.size()==0) continue;
                    mdoc=mdocBuffer.pop();
                }

                if (Thread.interrupted()) break;
                if (converterLog.isDebugEnabled()) converterLog.debug("Handling document: "+mdoc.toString());
                if (converterLog.isTraceEnabled()) converterLog.trace("XML: "+mdoc.getXML());

                Document ldoc=mdoc.getLuceneDocument(iconfig);

                synchronized(indexerThreadLock) {
                    if (ldocBuffer.size()>=maxChangesBeforeCommit) internalWaitIndexer();
                    ldocBuffer.put(mdoc.identifier,ldoc);
                    if (ldocBuffer.size()>=minChangesBeforeCommit) indexerThreadLock.notify();
                }
            }

            // notify indexer of end
            synchronized(indexerThreadLock) {
                indexerFinished=true;
                indexerThreadLock.notify();
            }
        } catch (Exception e) {
            converterLog.debug(e);
            failure=e;
        } finally {
            XPathResolverImpl.getInstance().unsetIndexBuilder();
        }

        synchronized(converterThreadLock) {
            // notify eventually waiting threads
            converterThreadLock.notify();
        }
    }

    // this thread eats from the docs and fills index
    private void indexerThreadRun() {
        org.apache.commons.logging.Log indexerLog = org.apache.commons.logging.LogFactory.getLog(Thread.currentThread().getName());
        IndexWriter writer=null;
        try {
            writer = new IndexWriter(dir, true, iconfig.parent.getAnalyzer(), !IndexReader.indexExists(dir));
            //writer.setInfoStream(System.err);
            writer.setMaxFieldLength(Integer.MAX_VALUE);
            writer.setMaxBufferedDocs(minChangesBeforeCommit);
            writer.setMaxBufferedDeleteTerms(minChangesBeforeCommit);

            int docBufferSize=0;
            do {
                TreeMap<String,Document> docs=null;
                synchronized(indexerThreadLock) {
                    // notify eventually waiting threads
                    indexerThreadLock.notify();
                    // wait if queue is too empty
                    if (!indexerFinished && ldocBuffer.size()<minChangesBeforeCommit) try {
                        indexerThreadLock.wait();
                    } catch (InterruptedException ie) {}
                    // fetch docs and replace by new one
                    docs=ldocBuffer;
                    ldocBuffer=new TreeMap<String,Document>();
                }

                indexerLog.info("Updating index...");

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
                synchronized(indexerThreadLock) { docBufferSize=ldocBuffer.size(); }

                synchronized(commitEventLock) {
                    // only flush if commitEvent interface registered
                    if (commitEvent!=null) writer.flush();

                    indexerLog.info(deleted+" docs presumably deleted (only if existent) and "+updated+" docs (re-)indexed.");

                    // notify Harvester of index commit
                    if (commitEvent!=null) commitEvent.harvesterCommitted(Collections.unmodifiableMap(docs).keySet().iterator());
                }
            } while ((!indexerFinished || docBufferSize>0) && failure==null);

            writer.flush();

            if (iconfig.autoOptimize) {
                indexerLog.info("Optimizing index...");
                writer.optimize();
                indexerLog.info("Index optimized.");
            }
        } catch (IOException e) {
            indexerLog.debug(e);
            failure=e;
        } finally {
            if (writer!=null) try {
                writer.close();
            } catch (IOException ioe) {
                indexerLog.warn("Failed to close Lucene IndexWriter, you may need to remove lock files!",ioe);
            }
            writer=null;
        }

        synchronized(indexerThreadLock) {
            // notify eventually waiting threads
            indexerThreadLock.notify();
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

    private void startThreads() {
        if (!threadsStarted) try {
            log.info("Starting "+converterThreadList.length+" converter thread(s)...");
            for (Thread t : converterThreadList) t.start();
            log.info("Starting indexer thread...");
            indexerThread.start();
        } finally {
            threadsStarted=true;
        }
    }

    private void checkThreads() {
        if (converterThreads.activeCount()==0 || !indexerThread.isAlive())
            throw new IllegalStateException("IndexBuilder threads died unexspected!");
    }

    private void internalWaitConverter() {
        checkThreads();
        synchronized (converterThreadLock) {
            try {
                log.debug("Harvester is too fast for converter thread(s), waiting..."); // only debug, it happens often!
                converterThreadLock.wait();
            } catch (InterruptedException ie) {}
        }
    }

    private void internalWaitIndexer() {
        checkThreads();
        synchronized (indexerThreadLock) {
            try {
                log.info("Converter is too fast for indexer thread, waiting...");
                if (maxWaitForIndexer>0L) indexerThreadLock.wait(maxWaitForIndexer); else indexerThreadLock.wait();
            } catch (InterruptedException ie) {}
        }
    }

}