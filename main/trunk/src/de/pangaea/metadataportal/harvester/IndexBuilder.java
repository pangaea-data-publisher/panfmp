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

public class IndexBuilder implements Runnable {
    private static org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(IndexBuilder.class);

    private int minChangesBeforeCommit = 1000;
    private int maxChangesBeforeCommit = 2000;

    private boolean create;
    private FSDirectory dir=null;
    private Date lastHarvested=null;
    private SingleIndexConfig iconfig;

    private Thread th;
    private volatile boolean finished=false;
    private volatile TreeMap<String,Document> docBuffer=new TreeMap<String,Document>();
    private volatile IOException failure=null;
    private volatile long maxWaitForIndexer=-1L;
    private Object commitEventLock=new Object();
    private volatile HarvesterCommitEvent commitEvent=null;

    // construtor
    private IndexBuilder() {}

    public IndexBuilder(boolean create, SingleIndexConfig iconfig) throws IOException {
        if (!IndexReader.indexExists(iconfig.getFullIndexPath())) create=true;
        this.dir=FSDirectory.getDirectory(iconfig.getFullIndexPath(),create);
        this.create=create;
        this.iconfig=iconfig;
        if (create) try {
            dir.deleteFile(IndexConstants.FILENAME_LASTHARVESTED);
        } catch (IOException e) {}
        th=new Thread(this);
    }

    public boolean isCreatingNew() {
        return create;
    }

    public synchronized void setChangesBeforeCommit(int min, int max) {
        this.minChangesBeforeCommit=min;
        this.maxChangesBeforeCommit=max;
        if (docBuffer.size()>=minChangesBeforeCommit) this.notify();
    }

    public synchronized void setMaxWaitForIndexer(long maxWaitForIndexer) {
        this.maxWaitForIndexer=maxWaitForIndexer;
    }

    public void registerHarvesterCommitEvent(HarvesterCommitEvent event) {
        synchronized(commitEventLock) {
            commitEvent=event;
        }
    }

    // this thread eats from the docs and fills index
    public void run() {
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
                synchronized(this) {
                    // notify eventually waiting addDocument()-thread
                    this.notify();
                    // wait if queue is too empty
                    if (!finished && docBuffer.size()<minChangesBeforeCommit) try {
                        this.wait();
                    } catch (InterruptedException ie) {}
                    // fetch docs and replace by new one
                    docs=docBuffer;
                    docBuffer=new TreeMap<String,Document>();
                }

                log.info("Updating index...");

                int updated=0, deleted=0;
                for (Map.Entry<String,Document> docEntry : docs.entrySet()) {
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
                synchronized(this) { docBufferSize=docBuffer.size(); }

                synchronized(commitEventLock) {
                    // only flush if commitEvent interface registered
                    if (commitEvent!=null) writer.flush();

                    log.info(deleted+" documents deleted and "+updated+" documents indexed.");

                    // notify Harvester of index commit
                    if (commitEvent!=null) commitEvent.harvesterCommitted(Collections.unmodifiableMap(docs).keySet().iterator());
                }
            } while (!finished || docBufferSize>0);

            writer.flush();

            if (iconfig.autoOptimize) {
                log.info("Optimizing index...");
                writer.optimize();
                log.info("Index optimized.");
            }
        } catch (IOException e) {
            log.debug("IO error in indexer thread",e);
            failure=e;
        } finally {
            if (writer!=null) try {
                writer.close();
            } catch (IOException ioe) {
                log.warn("Failed to close IndexWriter, you may need to remove lock files!",ioe);
            }
            writer=null;
        }

        synchronized(this) {
            // notify eventually waiting addDocument()/checkIndexerBuffer()-threads
            this.notify();
        }
    }

    public boolean isClosed() {
        return (th==null);
    }

    public void close() throws IOException {
        if (th==null) throw new IllegalStateException("IndexBuilder already closed");

        if (failure!=null) throw failure;
        else {
            if (!th.isAlive()) th.start(); // start thread to make sure index is created if nothing was added before
            finished=true;
            synchronized(this) {
                this.notify();
            }
            try {
                th.join();
            } catch (InterruptedException e) {}
        }
        if (failure!=null) throw failure;

        if (lastHarvested!=null) {
            org.apache.lucene.store.IndexOutput out=dir.createOutput(IndexConstants.FILENAME_LASTHARVESTED);
            out.writeLong(lastHarvested.getTime());
            out.close();
            lastHarvested=null;
        }

        th=null;
    }

    private synchronized void internalWaitIndexer() {
        try {
            log.info("Harvester is to fast for indexer thread, waiting...");
            if (maxWaitForIndexer>0L) this.wait(maxWaitForIndexer); else this.wait();
        } catch (InterruptedException ie) {}
    }

    public void addDocument(MetadataDocument mdoc) throws Exception {
        if (th==null) throw new IllegalStateException("IndexBuilder already closed");
        if (failure!=null) throw failure;

        if (!th.isAlive()) th.start();

        if (log.isDebugEnabled()) log.debug("Handling document: "+mdoc.toString());
        if (log.isTraceEnabled()) log.trace("XML: "+mdoc.getXML());

        Document ldoc=mdoc.getLuceneDocument(iconfig);

        synchronized(this) {
            if (docBuffer.size()>=maxChangesBeforeCommit) internalWaitIndexer();
            docBuffer.put(mdoc.identifier,ldoc);
            if (docBuffer.size()>=minChangesBeforeCommit) this.notify();
        }
    }

    // call this between harvest resumptions to give the indexer a chance NOW to set this thread to wait not while HTTP transfers
    public synchronized void checkIndexerBuffer() throws IOException {
        if (th==null) throw new IllegalStateException("IndexBuilder already closed");
        if (failure!=null) throw failure;

        if (!th.isAlive()) th.start();

        if (docBuffer.size()>=(maxChangesBeforeCommit+minChangesBeforeCommit)/2) internalWaitIndexer();
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

}