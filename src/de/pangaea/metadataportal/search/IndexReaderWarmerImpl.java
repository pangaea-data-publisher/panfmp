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

package de.pangaea.metadataportal.search;

import de.pangaea.metadataportal.config.Config;
import de.pangaea.metadataportal.utils.LRUMap;
import de.pangaea.metadataportal.utils.IndexConstants;
import org.apache.lucene.search.*;
import org.apache.lucene.index.IndexWriter.IndexReaderWarmer;
import org.apache.lucene.index.IndexReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class warms IndexReaders after reopens. It uses some default queries and sort options from the config file
 * But SearchService can also add extra {@link Query} instances during operation which are maintained by usage count.
 * @author Uwe Schindler
 */
public final class IndexReaderWarmerImpl extends IndexReaderWarmer {

	private static final org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(IndexReaderWarmerImpl.class);
	
	public static final int DEFAULT_WARM_LIFE_QUERIES = 20;
	
	public IndexReaderWarmerImpl(Config config) {
		this.config=config;
		config.indexReaderWarmers.add(this);
		
		/*
		<cfg:warmLifeQueries>20</cfg:warmLifeQueries>
		*/
		final int size=Integer.parseInt(config.searchProperties.getProperty("warmLifeQueries",Integer.toString(DEFAULT_WARM_LIFE_QUERIES)));
		warmQueries=new LRUMap<Entry,Object>(size);
		// TODO!!!
	}
	
	/** This should only be called after ctor initialization to specify fixed queries to execute */
	public void addFixedQuery(Query q,Sort s) {
		lock.lock();
		try {
			fixedWarmQueries.add(new Entry(q,s));
		} finally {
			lock.unlock();
		}
	}
	
	/** Registers a user-query in the LRU list. If a warmup is currently running it does nothing for performance reasons */
	public void addLifeQuery(Query q,Sort s) {
		// only add the new query, if no warmup is running
		if (lock.tryLock()) try {
			warmQueries.put(new Entry(q,s),PLACEHOLDER);
		} finally {
			lock.unlock();
		}
	}
	
	protected void runQuery(final IndexSearcher searcher, final Entry e) throws IOException {
		final SortField[] sf=(e.sort==null)?null:e.sort.getSort();
		final FieldComparator[] comparators;
		if (sf==null) {
			comparators=new FieldComparator[0];
		} else {
			comparators=new FieldComparator[sf.length];
			for (int i=0;i<sf.length;i++) {
				comparators[i]=sf[i].getComparator(0,i);
			}
		}
		searcher.search(e.query, new Collector() {
			@Override
			public final void setScorer(Scorer scorer) {}
			
			@Override 
			public final void setNextReader(IndexReader reader, int docBase) throws IOException {
				for (FieldComparator comp : comparators) {
					if (comp!=null) comp.setNextReader(reader,docBase);
				}
			}
			
			@Override
			public final void collect(int doc) {}
			
			@Override
			public boolean acceptsDocsOutOfOrder() {
				return true;
			}
		});
	}

	public void warm(final IndexReader reader) throws IOException {
		final IndexSearcher searcher=new IndexSearcher(reader);
		lock.lock();
		try {
			for (Entry e : fixedWarmQueries) {
				if (log.isDebugEnabled()) log.debug("Run fixed warmup: "+e);
				runQuery(searcher,e);
			}
			for (Entry e : warmQueries.keySet()) {
				if (log.isDebugEnabled()) log.debug("Run dynamic warmup: "+e);
				runQuery(searcher,e);
			}
		} finally {
			lock.unlock();
			searcher.close();
		}
	}
	
	private static final class Entry {
		public final Query query;
		public final Sort sort;
		
		public Entry(final Query query, final Sort sort) {
			if (query==null)
				throw new NullPointerException("sort may not be null");
			this.query=query;
			this.sort=sort;
		}
		
		@Override
		public int hashCode() {
			return query.hashCode() * 31 + ((sort == null) ? 0 : sort.hashCode());
		}
		
		@Override
		public boolean equals(Object o) {
			if (!(o instanceof Entry))
				return false;
			final Entry e=(Entry)o;
			return this.query.equals(e.query) &&
			 ((this.sort==null) ? e.sort==null : this.sort.equals(e.sort));
		}
		
		@Override
		public String toString() {
			final StringBuilder sb = new StringBuilder("query='").append(query.toString(IndexConstants.FIELDNAME_CONTENT)).append('\'');
			if (sort!=null) sb.append("; sort=[").append(sort).append(']');
			return sb.toString();
		}
	}
	
	private static final Object PLACEHOLDER = new Object();
	
	private final ReentrantLock lock = new ReentrantLock();
	private final Set<Entry> fixedWarmQueries=new LinkedHashSet<Entry>();
	private final Map<Entry,Object> warmQueries;
	private final Config config;
	
}