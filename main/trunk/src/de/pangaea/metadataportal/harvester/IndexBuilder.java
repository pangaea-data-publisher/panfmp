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

import de.pangaea.metadataportal.utils.*;
import de.pangaea.metadataportal.config.*;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.*;
import java.util.concurrent.atomic.*;
import java.io.IOException;
import org.apache.lucene.index.*;
import org.apache.lucene.document.Document;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

/**
 * Component of <b>panFMP</b> that analyzes and indexes harvested documents in different threads.
 * @author Uwe Schindler
 */
public class IndexBuilder {
	private static org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(IndexBuilder.class);

	protected SingleIndexConfig iconfig;

	private Date lastHarvested=null;
	private boolean create;

	private static MetadataDocument  MDOC_EOF = new MetadataDocument();
	private static IndexerQueueEntry LDOC_EOF = new IndexerQueueEntry(null,null);

	private AtomicInteger runningConverters=new AtomicInteger(0);
	private AtomicReference<Exception> failure=new AtomicReference<Exception>(null);
	private AtomicReference<HarvesterCommitEvent> commitEvent=new AtomicReference<HarvesterCommitEvent>(null);
	private AtomicReference<Set<String>> validIdentifiers=new AtomicReference<Set<String>>(null);

	private BlockingQueue<MetadataDocument> mdocBuffer;
	private BlockingQueue<IndexerQueueEntry> ldocBuffer;

	private final Lock indexerLock=new ReentrantLock();
	private final Condition indexerLockCondition=indexerLock.newCondition(); 
	
	private int changesBeforeCommit;
	private DocumentErrorAction conversionErrorAction=DocumentErrorAction.STOP;

	private Thread indexerThread;
	private ThreadGroup converterThreads;
	private Thread[] converterThreadList;
	private boolean threadsStarted=false;

	public IndexBuilder(boolean create, SingleIndexConfig iconfig) throws IOException {
		if (!iconfig.isIndexAvailable()) create=true;
		this.create=create;
		this.iconfig=iconfig;
		if (create) try {
			iconfig.getIndexDirectory().deleteFile(IndexConstants.FILENAME_LASTHARVESTED);
		} catch (IOException e) {}

		changesBeforeCommit=Integer.parseInt(iconfig.harvesterProperties.getProperty("changesBeforeIndexCommit","1000"));

		String s=iconfig.harvesterProperties.getProperty("conversionErrorAction");
		if (s!=null) try {
			conversionErrorAction=DocumentErrorAction.valueOf(s.toUpperCase());
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException("Invalid value '"+s+"' for harvester property 'conversionErrorAction', valid ones are: "+Arrays.toString(DocumentErrorAction.values()));
		}

		int threadCount=Integer.parseInt(iconfig.harvesterProperties.getProperty("numConverterThreads","1"));
		if (threadCount<1) throw new IllegalArgumentException("numConverterThreads harvester-property must be >=1!");

		int size=Integer.parseInt(iconfig.harvesterProperties.getProperty("maxConverterQueue","250"));
		if (size<threadCount) throw new IllegalArgumentException("maxConverterQueue must be >=numConverterThreads!");
		mdocBuffer=new ArrayBlockingQueue<MetadataDocument>(size,true);

		size=Integer.parseInt(iconfig.harvesterProperties.getProperty("maxIndexerQueue","250"));
		if (size<1) throw new IllegalArgumentException("maxIndexerQueue must be >=1!");
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

	public boolean isFailed() {
		return (failure.get()!=null);
	}

	public void registerHarvesterCommitEvent(HarvesterCommitEvent event) {
		commitEvent.set(event);
	}

	public void setValidIdentifiers(Set<String> validIdentifiers) {
		this.validIdentifiers.set(validIdentifiers);
	}

	public boolean isClosed() {
		return (indexerThread==null || converterThreads==null || converterThreadList==null);
	}

	public void close() throws Exception {
		if (isClosed()) throw new IllegalStateException("IndexBuilder already closed");

		startThreads(true);

		try {
			for (int i=0; i<converterThreadList.length; i++)
				mdocBuffer.put(MDOC_EOF);
			for (Thread t : converterThreadList) {
				if (t.isAlive()) t.join();
			}

			// if ldocBuffer not empty there were already some threads filling the queue
			// => LDOC_EOF is queued by the threads
			// explicitely putting a LDOC_EOF is only needed when converterThreads were never running!
			if (ldocBuffer.size()==0) ldocBuffer.put(LDOC_EOF);
			if (indexerThread.isAlive()) indexerThread.join();
		} catch (InterruptedException e) {
			log.error(e);
		}

		if (lastHarvested!=null) {
			IndexOutput out=iconfig.getIndexDirectory().createOutput(IndexConstants.FILENAME_LASTHARVESTED);
			out.writeLong(lastHarvested.getTime());
			out.close();
			lastHarvested=null;
		}

		converterThreads=null;
		converterThreadList=null;
		indexerThread=null;

		Exception f=failure.get();
		if (f!=null) throw f;
	}

	public void addDocument(MetadataDocument mdoc) throws IndexBuilderBackgroundFailure,InterruptedException {
		if (isClosed()) throw new IllegalStateException("IndexBuilder already closed");
		throwFailure();
		startThreads(false);

		mdocBuffer.put(mdoc);
	}

	// call this between harvest resumptions to wait if buffer 2/3 full, this helps to not block while running HTTP transfers (if buffer is big enough)
	public void checkIndexerBuffer() throws IndexBuilderBackgroundFailure,InterruptedException {
		if (isClosed()) throw new IllegalStateException("IndexBuilder already closed");
		throwFailure();
		startThreads(false);

		if (ldocBuffer.remainingCapacity()*2<ldocBuffer.size()) {
			log.warn("Harvester is too fast for indexer thread, that is blocked. Waiting...");
			// we use >=2, because there seems to be a synchronization bug. TODO: check this!!!
			while (ldocBuffer.size()>=2 && failure.get()==null) {
				indexerLock.lock(); try {
					indexerLockCondition.awaitNanos(10000L);
				} finally {
					indexerLock.unlock();
				}				
			}
		}
	}

	// sets the date of last harvesting (written to disk after closing!!!)
	public void setLastHarvested(Date datestamp) {
		this.lastHarvested=datestamp;
	}

	public Date getLastHarvestedFromDisk() {
		if (create) return null;
		IndexInput in=null;
		Date d=null;
		try {
			in=iconfig.getIndexDirectory().openInput(IndexConstants.FILENAME_LASTHARVESTED);
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
			while (failure.get()==null) {
				MetadataDocument mdoc;
				try {
					mdoc=mdocBuffer.take();
				} catch (InterruptedException ie) {
					continue;
				}
				if (mdoc==MDOC_EOF) break;

				if (log.isDebugEnabled()) log.debug("Converting document: "+mdoc.toString());
				if (log.isTraceEnabled()) log.trace("XML: "+mdoc.getXML());
				try {
					IndexerQueueEntry en=new IndexerQueueEntry(mdoc.getIdentifier(),mdoc.getLuceneDocument());
					ldocBuffer.put(en);
				} catch (InterruptedException ie) {
					throw ie; // no handling here
				} catch (Exception e) {
					// handle exception
					switch (conversionErrorAction) {
						case IGNOREDOCUMENT: 
							log.error("Conversion XML to Lucene document failed for '"+mdoc.getIdentifier()+"' (object ignored):",e);
							break;
						case DELETEDOCUMENT:
							log.error("Conversion XML to Lucene document failed for '"+mdoc.getIdentifier()+"' (object marked deleted):",e);
							ldocBuffer.put(new IndexerQueueEntry(mdoc.getIdentifier(),null));
							break;
						default:
							log.fatal("Conversion XML to Lucene document failed for '"+mdoc.getIdentifier()+"' (fatal, stopping conversions).");
							throw e;
					}
				}
			}
		} catch (InterruptedException ie) {
			log.debug(ie);
		} catch (Exception e) {
			// only store the first error in failure variable, other errors are logged only
			if (failure.compareAndSet(null,e)) log.debug(e); else log.error(e);
		} finally {
			if (runningConverters.decrementAndGet()==0) try {
				mdocBuffer.clear();
				ldocBuffer.put(LDOC_EOF);
			} catch (InterruptedException e) {
				log.error(e);
			}
			XPathResolverImpl.getInstance().unsetIndexBuilder();
			log.info("Converter thread stopped.");
		}
	}

	private void indexerThreadRun() {
		org.apache.commons.logging.Log log=org.apache.commons.logging.LogFactory.getLog(Thread.currentThread().getName());
		log.info("Indexer thread started.");
		IndexWriter writer=null;
		int updated=0, deleted=0;
		boolean finished=false;
		try {
			writer=iconfig.newIndexWriter(create);
			writer.setMaxBufferedDocs(changesBeforeCommit);
			writer.setMaxBufferedDeleteTerms(changesBeforeCommit);

			HashSet<String> committedIdentifiers=new HashSet<String>(changesBeforeCommit);

			while (failure.get()==null) {
				// notify eventually waiting checkIndexerBuffer() calls
				if (indexerLock.tryLock()) try { 
					indexerLockCondition.signalAll();
				} finally {
					indexerLock.unlock();
				}

				IndexerQueueEntry entry;
				try {
					entry=ldocBuffer.take();
				} catch (InterruptedException ie) {
					continue;
				}
				if (entry==LDOC_EOF) break;

				Term t=new Term(IndexConstants.FIELDNAME_IDENTIFIER,entry.identifier);
				if (entry.ldoc==null) {
					if (log.isDebugEnabled()) log.debug("Deleting document: "+entry.identifier);
					writer.deleteDocuments(t);
					deleted++;
				} else {
					if (log.isDebugEnabled()) log.debug("Updating document: "+entry.identifier);
					if (log.isTraceEnabled()) log.debug("Data: "+entry.ldoc.toString());
					writer.updateDocument(t,entry.ldoc);
					updated++;
				}
				committedIdentifiers.add(entry.identifier);

				if (committedIdentifiers.size()>=changesBeforeCommit)  {
					HarvesterCommitEvent ce=commitEvent.get();

					// only flush if commitEvent interface registered
					if (ce!=null) writer.commit();

					log.info(deleted+" docs presumably deleted (if existent) and "+updated+" docs (re-)indexed so far.");

					// notify Harvester of index commit
					if (ce!=null) ce.harvesterCommitted(Collections.unmodifiableSet(committedIdentifiers));
					committedIdentifiers.clear();
				}
			}

			// notify eventually waiting checkIndexerBuffer() calls
			if (indexerLock.tryLock()) try { 
				indexerLockCondition.signalAll();
			} finally {
				indexerLock.unlock();
			}

			// check vor validIdentifiers Set and remove all unknown identifiers from index, if available (but not if new-created index)
			Set<String> validIdentifiers=this.validIdentifiers.get();
			if (validIdentifiers!=null && !create) {
				log.info(deleted+" docs presumably deleted (if existent) and "+updated+" docs (re-)indexed so far.");
				writer.commit();
				writer.close(); writer=null;

				log.info("Removing documents not seen while harvesting (this may take a while)...");
				IndexReader reader=null;
				TermEnum terms=null;
				Term base=new Term(IndexConstants.FIELDNAME_IDENTIFIER,"");
				try {
					reader = iconfig.newIndexReader(false);
					terms=reader.terms(base);
					do {
						Term t=terms.term();
						if (t!=null && base.field()==t.field()) {
							if (!validIdentifiers.contains(t.text())) {
							    int count=reader.deleteDocuments(t);
							    if (count>0) committedIdentifiers.add(t.text());
							    deleted+=count;
							}
						} else {
							break;
						}
					} while (terms.next());
				} finally {
					if (terms!=null) terms.close();
					if (reader!=null) reader.close();
				}
			}

			// notify Harvester of index commit
			HarvesterCommitEvent ce=commitEvent.get();
			if (writer!=null && ce!=null) writer.commit();
			if (ce!=null && committedIdentifiers.size()>0) ce.harvesterCommitted(Collections.unmodifiableSet(committedIdentifiers));
			committedIdentifiers.clear();

			finished=true;
			log.info(deleted+" docs presumably deleted (only if existent) and "+updated+" docs (re-)indexed - finished.");

			if (BooleanParser.parseBoolean(iconfig.harvesterProperties.getProperty("autoOptimize","false"))) {
				if (writer==null) writer=iconfig.newIndexWriter(false);
				log.info("Optimizing index...");
				writer.optimize(true);
				log.info("Index optimized.");
			}
		} catch (IOException e) {
			if (!finished) log.warn("Only "+deleted+" docs presumably deleted (only if existent) and "+updated+" docs (re-)indexed before the following error occurred: "+e);
			// only store the first error in failure variable, other errors are logged only
			if (!failure.compareAndSet(null,e)) log.error(e);
		} finally {
			ldocBuffer.clear();
			// notify eventually waiting checkIndexerBuffer() calls
			indexerLock.lock(); try { 
				indexerLockCondition.signalAll();
			} finally {
				indexerLock.unlock();
			}
			// close reader
			if (writer!=null) try {
				writer.commit();
				writer.close();
			} catch (IOException ioe) {
				log.warn("Failed to close Lucene IndexWriter, you may need to remove lock files!",ioe);
			}
			writer=null;
			log.info("Indexer thread stopped.");
		}
	}

	private void throwFailure() throws IndexBuilderBackgroundFailure {
		Exception f=failure.get();
		if (f!=null) {
			if (converterThreads!=null) converterThreads.interrupt();
			if (indexerThread!=null) indexerThread.interrupt();
			throw new IndexBuilderBackgroundFailure(f);
		}
	}

	private void startThreads(boolean onlyIndexer) {
		if (!threadsStarted) try {
			if (!onlyIndexer) for (Thread t : converterThreadList) {
				runningConverters.incrementAndGet();
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