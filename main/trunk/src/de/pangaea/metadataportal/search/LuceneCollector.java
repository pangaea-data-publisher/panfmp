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
import org.apache.lucene.search.*;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.SorterTemplate;
import java.io.IOException;

/**
 * Internal implementation of a Lucene {@link Collector} for the collector API of {@link SearchService}.
 * @author Uwe Schindler
 */
public final class LuceneCollector extends Collector {

	private static org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(LuceneCollector.class);

	/**
	 * Creates an instance using the specified buffer size wrapping the {@link SearchResultCollector}.
	 */
	protected LuceneCollector(int bufferSize, SearchResultCollector coll, Config config, FieldSelector fields) {
		if (bufferSize<=0) throw new IllegalArgumentException("Buffer must have a size >0");
		docIds=new int[bufferSize];
		scores=new float[bufferSize];
		this.coll=coll;
		this.config=config;
		this.fields=fields;
	}

	/**
	 * Flushes the internal buffer by calling {@link SearchResultCollector#collect} with
	 * the loaded document instance for each buffer entry.
	 */
	protected void flushBuffer() throws IOException {
		if (log.isDebugEnabled()) log.debug("Flushing buffer containing "+count+" search results...");
		// we do the buffer in index order which is less IO expensive!
		new SorterTemplate() {
			@Override
			protected final void swap(int i,int j) {
				final int tempId=docIds[i];
				docIds[i]=docIds[j];
				docIds[j]=tempId;
				final float tempScore=scores[i];
				scores[i]=scores[j];
				scores[j]=tempScore;
			}
			
			@Override
			protected final int compare(int i,int j) {
				return docIds[i]-docIds[j];
			}
		}.quickSort(0,count-1);
		try {
			for (int i=0; i<count; i++) {
				Document doc = currReader.document(docIds[i], fields);
				if (doc==null) continue;
				if (!coll.collect(new SearchResultItem(config, scores[i], doc ))) throw new StopException();
			}
		} finally {
			count=0;
		}
	}

	public final void setScorer(Scorer scorer) {
		this.currScorer=scorer;
	}
	
	public final void setNextReader(IndexReader reader, int docBase) throws IOException {
		flushBuffer();
		this.currReader=reader;
	}
	
	/**
	 * Called by Lucene to collect search result items.
	 */
	public final void collect(final int doc) throws IOException {
		docIds[count]=doc;
		scores[count]=currScorer.score();
		count++;
		if (count==docIds.length) flushBuffer();
	}
	
	public boolean acceptsDocsOutOfOrder() {
		return true;
	}

	protected int[] docIds; // protected because of speed with inner class
	protected float[] scores; // protected because of speed with inner class
	private int count=0;
	private SearchResultCollector coll;
	private Config config;
	private FieldSelector fields;
	
	private Scorer currScorer=null;
	private IndexReader currReader=null;

	/**
	 * Thrown to stop collecting of results when {@link SearchResultCollector#collect} returns <code>false</code>.
	 */
	protected static final class StopException extends RuntimeException {
	}

}