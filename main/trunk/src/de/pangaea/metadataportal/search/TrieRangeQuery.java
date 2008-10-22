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

package de.pangaea.metadataportal.search;

import de.pangaea.metadataportal.utils.TrieUtils;
import org.apache.lucene.search.*;
import org.apache.lucene.index.*;
import org.apache.lucene.util.OpenBitSet;
import org.apache.lucene.util.ToStringUtils;
import java.io.IOException;
import java.util.Date;

/**
 * Low-level implementation of a Lucene {@link Query} that implements a trie-based range query.
 * This query depends on a specific structure of terms in the index that can only be created
 * by {@link TrieUtils} methods.
 * <p>This is the central implementation of <b>panFMP</b>'s
 * high performance range queries on numeric and date/time fields using &quot;trie memory&quot; structures.
 * See the publication about <b>panFMP</b>:
 * <blockquote><strong>Schindler, U, Diepenbroek, M</strong>, 2008. <em>Generic XML-based Framework for Metadata Portals.</em>
 * Computers &amp; Geosciences 34 (12), 1947-1955.
 * <a href="http://dx.doi.org/10.1016/j.cageo.2008.02.023" target="_blank">doi:10.1016/j.cageo.2008.02.023</a></blockquote>
 * <p><em>A quote from this paper:</em> Because Apache Lucene is a full-text search engine and not a conventional database,
 * it cannot handle numerical ranges (e.g., field value is inside user defined bounds, even dates are numerical values).
 * We have developed an extension to Apache Lucene that stores
 * the numerical values in a special string-encoded format with variable precision
 * (all numerical values like doubles, longs, and timestamps are converted to lexicographic sortable string representations of
 * <code>long long words</code> and stored with precisions from one byte to the full 8 bytes).
 * For a more detailed description, how the values are stored, see {@link TrieUtils}.
 * A range is then divided recursively into multiple intervals for searching:
 * The center of the range is searched only with the lowest possible precision in the trie, the boundaries are matched
 * more exactly. This reduces the number of terms dramatically (in the lowest precision of 1-byte the index only
 * contains a maximum of 256 distinct values). Overall, a range could consist of a theoretical maximum of
 * <code>7*256*2 + 255 = 3825</code> distinct terms (when there is a term for every distinct value of an
 * 8-byte-number in the index and the range covers all of them; a maximum of 255 distinct values is used
 * because it would always be possible to reduce the full 256 values to one term with degraded precision).
 * In practise, we have seen up to 300 terms in most cases (index with 500,000 metadata records
 * and a homogeneous dispersion of values).
 * <p>This dramatically improves the performance of Apache Lucene with range queries, which
 * is no longer dependent on the index size and number of distinct values because there is
 * an upper limit not related to any of these properties.
 * <p>This query type works only with indexes created by <b>panFMP</b>'s index builder / using {@link TrieUtils}.
 * @author Uwe Schindler
 */
public final class TrieRangeQuery extends Query {

	private static org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(TrieRangeQuery.class);

	/** Generic constructor (internal use only): Uses already trie-converted min/max values */
	public TrieRangeQuery(final String field, final String min, final String max) {
		if (min==null && max==null) throw new IllegalArgumentException("The min and max values cannot be both null.");
		this.minUnconverted=min;
		this.maxUnconverted=max;
		this.min=(min==null) ? TrieUtils.TRIE_CODED_NUMERIC_MIN : min;
		this.max=(max==null) ? TrieUtils.TRIE_CODED_NUMERIC_MAX : max;
		this.field=field.intern();
	}

	/**
	 * Generates a trie query using the supplied field with range bounds in numeric form (double).
	 * You can set <code>min</code> or <code>max</code> (but not both) to <code>null</code> to leave one bound open.
	 */
	public TrieRangeQuery(final String field, final Double min, final Double max) {
		if (min==null && max==null) throw new IllegalArgumentException("The min and max double values cannot be both null.");
		this.minUnconverted=min;
		this.maxUnconverted=max;
		this.min=(min==null) ? TrieUtils.TRIE_CODED_NUMERIC_MIN : TrieUtils.doubleToTrieCoded(min.doubleValue());
		this.max=(max==null) ? TrieUtils.TRIE_CODED_NUMERIC_MAX : TrieUtils.doubleToTrieCoded(max.doubleValue());
		this.field=field.intern();
	}

	/**
	 * Generates a trie query using the supplied field with range bounds in date/time form.
	 * You can set <code>min</code> or <code>max</code> (but not both) to <code>null</code> to leave one bound open.
	 */
	public TrieRangeQuery(final String field, final Date min, final Date max) {
		if (min==null && max==null) throw new IllegalArgumentException("The min and max date values cannot be both null.");
		this.minUnconverted=min;
		this.maxUnconverted=max;
		this.min=(min==null) ? TrieUtils.TRIE_CODED_NUMERIC_MIN : TrieUtils.dateToTrieCoded(min);
		this.max=(max==null) ? TrieUtils.TRIE_CODED_NUMERIC_MAX : TrieUtils.dateToTrieCoded(max);
		this.field=field.intern();
	}

	/**
	 * Generates a trie query using the supplied field with range bounds in integer form (long).
	 * You can set <code>min</code> or <code>max</code> (but not both) to <code>null</code> to leave one bound open.
	 * This data type is currently not used by <b>panFMP</b>.
	 */
	public TrieRangeQuery(final String field, final Long min, final Long max) {
		if (min==null && max==null) throw new IllegalArgumentException("The min and max long values cannot be both null.");
		this.minUnconverted=min;
		this.maxUnconverted=max;
		this.min=(min==null) ? TrieUtils.TRIE_CODED_NUMERIC_MIN : TrieUtils.longToTrieCoded(min.longValue());
		this.max=(max==null) ? TrieUtils.TRIE_CODED_NUMERIC_MAX : TrieUtils.longToTrieCoded(max.longValue());
		this.field=field.intern();
	}

	@Override
	public String toString(final String field) {
		final StringBuilder sb=new StringBuilder();
		if (!this.field.equals(field)) sb.append(this.field).append(':');
		sb.append('[');
		sb.append(minUnconverted);
		sb.append(" TO ");
		sb.append(maxUnconverted);
		sb.append(']');
		return sb.append(ToStringUtils.boost(getBoost())).toString();
	}

	@Override
	public final boolean equals(final Object o) {
		if (o instanceof TrieRangeQuery) {
			TrieRangeQuery q=(TrieRangeQuery)o;
			return (field==q.field && min.equals(q.min) && max.equals(q.max) && getBoost()==q.getBoost());
		} else return false;
	}

	@Override
	public final int hashCode() {
		return field.hashCode()+(min.hashCode()^0x14fa55fb)+(max.hashCode()^0x733fa5fe)+Float.floatToIntBits(getBoost());
	}

	/**
	 * Rewrites the query to native Lucene {@link Query}'s. This implementation uses a {@link ConstantScoreQuery} with
	 * a {@link TrieRangeFilter} as implementation of the trie algorithm.
	 */
	@Override
	public Query rewrite(final IndexReader reader) throws java.io.IOException {
		final ConstantScoreQuery q = new ConstantScoreQuery(new TrieRangeFilter());
		q.setBoost(getBoost());
		return q.rewrite(reader);
	}

	protected String field,min,max;
	private Object minUnconverted,maxUnconverted;

	/**
	 * Internal implementation of a trie-based range query using a {@link Filter}.
	 * This filter depends on a specific structure of terms in the index that can only be created
	 * by {@link TrieUtils} methods.
	 */
	protected final class TrieRangeFilter extends Filter {

		// code borrowed from original RangeFilter and simplified (and returns number of terms)
		private int setBits(final IndexReader reader, final TermDocs termDocs, final OpenBitSet bits, String lowerTerm, String upperTerm) throws IOException {
			int count=0,len=lowerTerm.length();
			// add padding before loose/inprecise values to group them
			if (len<16) {
				len++; // length is longer by 1 char because of padding
				lowerTerm=new StringBuilder(len).append((char)(TrieUtils.TRIE_CODED_PADDING_START+(len/2))).append(lowerTerm).toString();
				upperTerm=new StringBuilder(len).append((char)(TrieUtils.TRIE_CODED_PADDING_START+(len/2))).append(upperTerm).toString();
			}
			final TermEnum enumerator = reader.terms(new Term(field, lowerTerm));
			try {
				do {
					final Term term = enumerator.term();
					if (term!=null && term.field()==field) {
						// break out when upperTerm reached or length of term is different
						String t=term.text();
						if (len!=t.length() || t.compareTo(upperTerm)>0) break;
						// we have a good term, find the docs
						count++;
						termDocs.seek(enumerator);
						while (termDocs.next()) bits.set(termDocs.doc());
					} else break;
				} while (enumerator.next());
			} finally {
				enumerator.close();
			}
			return count;
		}

		// splits range recursively (and returns number of terms)
		private int splitRange(final IndexReader reader, final TermDocs termDocs, final OpenBitSet bits, final String min, final boolean lowerBoundOpen, final String max, final boolean upperBoundOpen) throws IOException {
			int count=0;
			final int length=min.length();
			final String minShort=lowerBoundOpen ? min.substring(0,length-2) : TrieUtils.incrementTrieCoded(min.substring(0,length-2));
			final String maxShort=upperBoundOpen ? max.substring(0,length-2) : TrieUtils.decrementTrieCoded(max.substring(0,length-2));

			if (length==2 || minShort.compareTo(maxShort)>=0) {
				count+=setBits(reader,termDocs,bits,min,max);
			} else {
				if (!lowerBoundOpen) count+=setBits(reader,termDocs,bits,min,minShort+TrieUtils.TRIE_CODED_SYMBOL_MIN+TrieUtils.TRIE_CODED_SYMBOL_MIN);
				count+=splitRange(reader,termDocs,bits,minShort,lowerBoundOpen,maxShort,upperBoundOpen);
				if (!upperBoundOpen) count+=setBits(reader,termDocs,bits,maxShort+TrieUtils.TRIE_CODED_SYMBOL_MAX+TrieUtils.TRIE_CODED_SYMBOL_MAX, max);
			}
			return count;
		}

		/**
		 * Returns a DocIdSet that provides the documents which should be permitted or prohibited in search results.
		 */
		@Override
		public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
			final OpenBitSet bits = new OpenBitSet(reader.maxDoc());
			final TermDocs termDocs=reader.termDocs();
			try {
				final int count=splitRange(reader,termDocs,bits,min,TrieUtils.TRIE_CODED_NUMERIC_MIN.equals(min),max,TrieUtils.TRIE_CODED_NUMERIC_MAX.equals(max));
				if (log.isDebugEnabled()) log.debug("Found "+count+" distinct terms in filtered range for field '"+field+"'.");
			} finally {
				termDocs.close();
			}
			return bits;
		}
	}


}