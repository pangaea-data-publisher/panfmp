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

import de.pangaea.metadataportal.config.*;
import de.pangaea.metadataportal.utils.*;
import java.io.IOException;
import org.apache.lucene.document.*;
import org.apache.lucene.search.ScoreDoc;
import java.util.*;

/**
 * This class holds search result items. It is implemented with the {@link List} interface to make it usable like any other Java list (iterators,...).
 * <p><em>Be warned:</em> Iterating over this list is very slow. When trying to iterate over all search results use a search method of {@link SearchService}
 * that notifies you on each result item using a {@link SearchResultCollector} instance.
 * @author Uwe Schindler
 */
public class SearchResultList extends AbstractList<SearchResultItem> {

	/**
	 * For internal use only!
	 */
	protected SearchResultList(LuceneCache.Session session, FieldSelector fields) {
		super();
		this.session=session;
		this.fields=fields;
	}
	
	/**
	 * Gets search result at the supplied index.
	 * @throws RuntimeException wrapping an {@link IOException}. This is needed because the generic {@link List} interface does not allow us to throw exceptions.
	 * @see #getResult
	 */
	@Override
	public SearchResultItem get(int index) {
		try {
			return getResult(index);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Returns the number of search results.
	 * @throws RuntimeException wrapping an {@link IOException}. This is needed because the generic {@link List} interface does not allow us to throw exceptions.
	 */
	@Override
	public int size() {
		try {
			return getResultCount();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Gets search result at the supplied index. Use this method in not {@link List}-specific code because you can catch the {@link IOException}.
	 */
	public SearchResultItem getResult(int index) throws IOException {
		synchronized (session) {
			session.ensureFetchable(index);
			ScoreDoc sd=session.topDocs.scoreDocs[index];
			return new SearchResultItem(
				session.parent.config,
				sd.score/session.topDocs.getMaxScore(),
				session.searcher.doc(sd.doc,fields)
			);
		}
	}

	/**
	 * Returns the number of search results. Use this method in not {@link List}-specific code because you can catch the {@link IOException}.
	 */
	public int getResultCount() throws IOException {
		synchronized (session) {
			session.ensureFetchable(0);
			return session.topDocs.totalHits;
		}
	}

	/**
	 * Gets the duration of the query in milliseconds.
	 */
	public long getQueryTime() throws IOException {
		synchronized (session) {
			session.ensureFetchable(0);
			return session.queryTime;
		}
	}

	private LuceneCache.Session session;
	private FieldSelector fields;
}