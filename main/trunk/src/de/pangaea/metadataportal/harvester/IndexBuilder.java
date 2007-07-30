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
import java.util.concurrent.*;
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

    private static MetadataDocument  MDOC_EOF = new MetadataDocument();
    private static IndexerQueueEntry LDOC_EOF = new IndexerQueueEntry(null,null);

    private volatile int runningConverters=0;
    private volatile Exception failure=null;

    private BlockingQueue<MetadataDocument> mdocBuffer;
    private BlockingQueue<IndexerQueueEntry> ldocBuffer;

    private HarvesterCommitEvent commitEvent=null;
    private Object commitEventLock=new Object();
    private Object indexerLock=new Object();

    private int changesBeforeCommit;

    private Thread indexerThread;
    private ThreadGroup converterThreads;
    private Thread[] converterThreadList;
    private boolean threadsStarted=false;

    public IndexBuilder(boolean create, SingleIndexConfig iconfig) throws IOException {
        if (!IndexReader.indexExists(iconfig.getFullIndexPath())) create=true;
        this.dir=FSDirectory.getDirectory(iconfig.getFullIndexPath());
        this.create=create;
        this.iconfig=iconfig;
        if (create) try {
            dir.deleteFile(IndexConstants.FILENAME_LASTHARVESTED);
        } catch (IOException e) {}

        changesBeforeCommit=Integer.parseInt(iconfig.harvesterProperties.getProperty("changesBeforeIndexCommit","1000"));

        int threadCount=Integer.parseInt(iconfig.harvesterProperties.getProperty("numConverterThreads","1"));
        if (threadCount<1) throw new IllegalArgumentException("numConverterThreads harvester-property must be >=1!");

        int size=Integer.parseInt(iconfig.harvesterProperties.getProperty("maxConverterQueue","250"));
        if (size<threadCount) throw new IllegalArgumentException("maxConverterQueue must be >=numConverterThreads!");
        mdocBuffer=new ArrayBlockingQueue<MetadataDocument>(size,true);

        size=Integer.parseInt(iconfig.harvesterProperties.getProperty("maxIndexerQueue","250"));
        if (size<1) throw new IllegalArgumentException("maxConverterQueue must be >=1!");
        ldocBuffer=new ArrayBlockingQueue<IndexerQueueEntry>(size,false);

        // converter threads
        converterThreads=new ThreadGroup(getClass().getName()+"#Converter#ThreadGroup");
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
    }

    public boolean isCreatingNew() {
        return create;
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

        throwFailure();
        startThreads(true);

        try {
            for (int i=0; i<converterThreadList.length; i++)
                mdocBuffer.put(MDOC_EOF);
            throwFailure();
            for (Thread t : converterThreadList) {
                if (t.isAlive()) t.join();
            }
            throwFailure();

            // in ldocBuffer not empty there were already some threads filling the queue
            // => LDOC_EOF is queued by the threads
            // explicitely putting a LDOC_EOF is only needed when converterThreads were never running!
            if (ldocBuffer.size()==0) ldocBuffer.put(LDOC_EOF);
            throwFailure();
            if (indexerThread.isAlive()) indexerThread.join();
        } catch (InterruptedException e) {
            log.error(e);
        }

        throwFailure();

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

        mdocBuffer.put(mdoc);
    }

    // call this between harvest resumptions to wait if buffer 2/3 full, this helps to not block while running HTTP transfers (if buffer is big enough)
    public void checkIndexerBuffer() throws Exception {
        if (isClosed()) throw new IllegalStateException("IndexBuilder already closed");
        throwFailure();
        startThreads(false);

        if (ldocBuffer.remainingCapacity()*2<ldocBuffer.size()) {
            log.warn("Harvester is too fast for indexer thread, that is blocked. Waiting...");
            while (ldocBuffer.size()>0) synchronized(indexerLock) {
                indexerLock.wait();
            }
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
        org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(Thread.currentThread().getName());
        log.info("Converter thread started.");
        XPathResolverImpl.getInstance().setIndexBuilder(this);
        try {
            while (failure==null) {
                MetadataDocument mdoc;
                try {
                    mdoc=mdocBuffer.take();
                } catch (InterruptedException ie) {
                    continue;
                }
                if (mdoc==MDOC_EOF) break;

                if (log.isDebugEnabled()) log.debug("Handling document: "+mdoc.toString());
                if (log.isTraceEnabled()) log.trace("XML: "+mdoc.getXML());
                ldocBuffer.put(new IndexerQueueEntry(mdoc.getIdentifier(),mdoc.getLuceneDocument()));
            }
        } catch (InterruptedException ie) {
            log.debug(ie);
        } catch (Exception e) {
            log.debug(e);
            failure=e;
        } finally {
            runningConverters--;
            if (runningConverters==0) try {
                ldocBuffer.put(LDOC_EOF);
                mdocBuffer.clear();
            } catch (InterruptedException e) {
                log.error(e);
            }
            XPathResolverImpl.getInstance().unsetIndexBuilder();
            log.info("Converter thread stopped.");
        }
    }

    private void indexerThreadRun() {
        org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(Thread.currentThread().getName());
        log.info("Indexer thread started.");
        IndexWriter writer=null;
        int updated=0, deleted=0;
        boolean finished=false;
        try {
            writer = new IndexWriter(dir, true, iconfig.parent.getAnalyzer(), !IndexReader.indexExists(dir));
            //writer.setInfoStream(System.err);
            writer.setMaxFieldLength(Integer.MAX_VALUE);
            writer.setMaxBufferedDocs(changesBeforeCommit);
            writer.setMaxBufferedDeleteTerms(changesBeforeCommit);

            HashSet<String> committedIdentifiers=new HashSet<String>(changesBeforeCommit);

            while (failure==null) {
                IndexerQueueEntry entry;
                try {
                    entry=ldocBuffer.take();
                } catch (InterruptedException ie) {
                    continue;
                }
                if (entry==LDOC_EOF) break;

                Term t=new Term(IndexConstants.FIELDNAME_IDENTIFIER,entry.identifier);
                if (entry.ldoc==null) {
                    writer.deleteDocuments(t);
                    deleted++;
                } else {
                    writer.updateDocument(t,entry.ldoc);
                    updated++;
                }
                committedIdentifiers.add(entry.identifier);

                if (committedIdentifiers.size()>=changesBeforeCommit) synchronized(commitEventLock) {
                    // only flush if commitEvent interface registered
                    if (commitEvent!=null) writer.flush();

                    log.info(deleted+" docs presumably deleted (if existent) and "+updated+" docs (re-)indexed so far.");

                    // notify Harvester of index commit
                    if (commitEvent!=null) commitEvent.harvesterCommitted(Collections.unmodifiableSet(committedIdentifiers).iterator());
                    committedIdentifiers.clear();
                }

                // notify eventually waiting checkIndexerBuffer() calls
                synchronized(indexerLock) {
                    indexerLock.notifyAll();
                }
            }

            writer.flush();

            // notify Harvester of index commit
            synchronized(commitEventLock) {
                if (commitEvent!=null) commitEvent.harvesterCommitted(Collections.unmodifiableSet(committedIdentifiers).iterator());
            }

            finished=true;
            log.info(deleted+" docs presumably deleted (only if existent) and "+updated+" docs (re-)indexed - finished.");

            if (Boolean.parseBoolean(iconfig.harvesterProperties.getProperty("autoOptimize","false"))) {
                log.info("Optimizing index...");
                writer.optimize();
                log.info("Index optimized.");
            }
        } catch (IOException e) {
            log.debug(e);
            if (!finished) log.warn("Only "+deleted+" docs presumably deleted (only if existent) and "+updated+" docs (re-)indexed before the following error occurred: "+e);
            failure=e;
        } finally {
            ldocBuffer.clear();
            // notify eventually waiting checkIndexerBuffer() calls
            synchronized(indexerLock) {
                indexerLock.notifyAll();
            }
            // close reader
            if (writer!=null) try {
                writer.close();
            } catch (IOException ioe) {
                log.warn("Failed to close Lucene IndexWriter, you may need to remove lock files!",ioe);
            }
            writer=null;
            log.info("Indexer thread stopped.");
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
                runningConverters++;
                t.start();
            }
            indexerThread.start();
        } finally {
            threadsStarted=true;
        }
    }

    private static final class IndexerQueueEntry {

        protected IndexerQueueEntry(String identifier, Document ldoc) {
            this.identifier=identifier;
            this.ldoc=ldoc;
        }

        protected String identifier;
        protected Document ldoc;

    }

}