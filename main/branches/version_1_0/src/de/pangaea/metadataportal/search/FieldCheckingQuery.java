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

import org.apache.lucene.search.Query;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexReader;
import java.io.IOException;
import java.util.*;

/**
 * Low-level implementation of a Lucene {@link Query} that is needed by {@link SearchService#newTextQuery} to
 * parse query strings that are specific to one field. When a query string is assigned to a specific
 * field, the user should not be allowed to use prefixes to reference other field names in his query string.
 * <code>SearchService</code> wraps the parsed query string with this &quot;filter&quot;. In the {@link #rewrite} method
 * (when query is expanded to native queries) it checks for terms with foreign field names and throws an exception
 * if one was found.
 * @author Uwe Schindler
 */
public final class FieldCheckingQuery extends Query {

	/**
	 * Constructor that wraps a query with a check for a specific field name.
	 */
	public FieldCheckingQuery(String field, Query query) {
		this.field=field;
		this.query=query;
	}

	/**
	 * Returns the wrapped query.
	 */
	public Query getQuery() {
		return query;
	}

	@Override
	public String toString(String field) {
		return query.toString(field);
	}

	@Override
	public final boolean equals(Object o) {
		if (o instanceof FieldCheckingQuery) {
			FieldCheckingQuery q=(FieldCheckingQuery)o;
			return this.query.equals(q.query) && this.field.equals(q.field);
		} else return false;
	}

	@Override
	public final int hashCode() {
		return query.hashCode()^0x743fb1ae+field.hashCode()^0xd2dd34aa;
	}

	/**
	 * Expands query to native queries by calling the <code>rewrite</code>-method of the wrapped query.
	 * After that it extracts all terms in the query and checks the used field names.
	 * @throws IllegalArgumentException if an invalid field name was detected.
	 */
	@Override
	public Query rewrite(IndexReader reader) throws java.io.IOException {
		Query q = query.rewrite(reader);
		q.extractTerms(new TermCheckerSet(field));
		return q;
	}
	
	/**
	 * Sets the boost for this query clause to <code>b</code>. Delegated to wrapped <code>Query</code>.
	 */
	@Override
	public void setBoost(float b) {
		query.setBoost(b);
	}
	
	/**
	 * Delegated to wrapped <code>Query</code>
	 */
	@Override
	public float getBoost() {
		return query.getBoost();
	}

	// members
	private String field;
	private Query query;

	// this class is a helper for checking a query for invalid (wrong field) Terms. It implements a set but only tests
	// in the add() method for invalid Terms. Nothing more is done, the Set is always empty!
	private final class TermCheckerSet extends AbstractSet<Term> {

		protected TermCheckerSet(String field) {
			this.field=field.intern();
		}

		@Override
		public int size() {
			return 0;
		}

		@Override
		public Iterator<Term> iterator() {
			return Collections.<Term>emptySet().iterator();
		}

		@Override
		public boolean add(Term t) {
			if (t.field()!=field) throw new IllegalArgumentException("A query on a specific field may not reference other fields by prefixing with 'fieldname:'!");
			return false;
		}

		private String field;
	}
}