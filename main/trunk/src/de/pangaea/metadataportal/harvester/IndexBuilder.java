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

    protected void setIndexerProperties(IndexWriter writer) {
        writer.setMaxFieldLength(Integer.MAX_VALUE);
        writer.setMaxBufferedDocs(minChangesBeforeCommit);
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
        try {
            IndexWriter writer=null;
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

                if (!create) {
                    IndexReader reader = IndexReader.open(dir);
                    int count=0;
                    for (String i : docs.keySet()) {
                        count+=reader.deleteDocuments(new Term(IndexConstants.FIELDNAME_IDENTIFIER,i));
                    }
                    reader.close();
                    log.info(count+" old documents deleted from index.");
                }

                // open index only if not already opened
                if (writer==null) {
                    writer = new IndexWriter(dir, iconfig.parent.getAnalyzer(), !IndexReader.indexExists(dir));
                    //writer.setInfoStream(System.err);
                    setIndexerProperties(writer);
                }
                int count=0;
                for (Document ldoc : docs.values()) {
                    // map contains NULL if only delete doc
                    if (ldoc!=null) {
                        writer.addDocument(ldoc);
                        count++;
                    }
                }
                log.info(count+" new documents added to index.");

                // get current status of buffer
                synchronized(this) { docBufferSize=docBuffer.size(); }

                if (iconfig.autoOptimize && finished && docBufferSize==0) {
                    log.info("Optimizing index...");
                    writer.optimize();
                    log.info("Index optimized.");
                }

                // close writer only when updating existing index
                if (!create) {
                    writer.close();
                    writer=null;
                    // notify Harvester of index commit
                    synchronized(commitEventLock) {
                        if (commitEvent!=null) commitEvent.harvesterCommitted(Collections.unmodifiableMap(docs).keySet().iterator());
                    }
                }
            } while (!finished || docBufferSize>0);

            if (writer!=null) {
                writer.close();
                // notify Harvester of index commit
                synchronized(commitEventLock) {
                    if (commitEvent!=null) commitEvent.harvesterCommitted(null);
                }
            }
        } catch (IOException e) {
            log.debug("IO error in indexer thread",e);
            failure=e;
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