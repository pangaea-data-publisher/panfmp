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

import de.pangaea.metadataportal.utils.LuceneConversions;
import org.apache.lucene.search.*;
import org.apache.lucene.index.*;
import java.io.IOException;
import java.util.BitSet;
import java.util.Date;

/**
 * Low-level implementation of a Lucene {@link Query} that implements a trie-based range query.
 * This query depends on a specific structure of terms in the index that can only be created
 * by {@link LuceneConversions} methods.
 * <p>This is the central implementation of <b>panFMP</b>'s
 * high performance range queries on numeric and date/time fields using &quot;trie memory&quot; structures
 * (see the publication about <b>panFMP</b>:
 * <em>Schindler, U, Diepenbroek, M, 2007. Generic Framework for Metadata Portals. Computers &amp; Geosciences, submitted.</em>).
 * This query type works only with indexes created by <b>panFMP</b>'s index builder.
 * @author Uwe Schindler
 */
public class TrieRangeQuery extends Query {

	private static org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(TrieRangeQuery.class);

	/** Generic constructor (internal use only): Uses already trie-converted min/max values */
	public TrieRangeQuery(String field, String min, String max) {
		if (min==null && max==null) throw new IllegalArgumentException("The min and max values cannot be both null.");
		this.minUnconverted=min;
		this.maxUnconverted=max;
		this.min=(min==null) ? LuceneConversions.LUCENE_NUMERIC_MIN : min;
		this.max=(max==null) ? LuceneConversions.LUCENE_NUMERIC_MAX : max;
		this.field=field.intern();
	}

	/**
	 * Generates a trie query using the supplied field with range bounds in numeric form (double).
	 * You can set <code>min</code> or <code>max</code> (but not both) to <code>null</code> to leave one bound open.
	 */
	public TrieRangeQuery(String field, Double min, Double max) {
		if (min==null && max==null) throw new IllegalArgumentException("The min and max double values cannot be both null.");
		this.minUnconverted=min;
		this.maxUnconverted=max;
		this.min=(min==null) ? LuceneConversions.LUCENE_NUMERIC_MIN : LuceneConversions.doubleToLucene(min.doubleValue());
		this.max=(max==null) ? LuceneConversions.LUCENE_NUMERIC_MAX : LuceneConversions.doubleToLucene(max.doubleValue());
		this.field=field.intern();
	}

	/**
	 * Generates a trie query using the supplied field with range bounds in date/time form.
	 * You can set <code>min</code> or <code>max</code> (but not both) to <code>null</code> to leave one bound open.
	 */
	public TrieRangeQuery(String field, Date min, Date max) {
		if (min==null && max==null) throw new IllegalArgumentException("The min and max date values cannot be both null.");
		this.minUnconverted=min;
		this.maxUnconverted=max;
		this.min=(min==null) ? LuceneConversions.LUCENE_NUMERIC_MIN : LuceneConversions.dateToLucene(min);
		this.max=(max==null) ? LuceneConversions.LUCENE_NUMERIC_MAX : LuceneConversions.dateToLucene(max);
		this.field=field.intern();
	}

	/**
	 * Generates a trie query using the supplied field with range bounds in integer form (long).
	 * You can set <code>min</code> or <code>max</code> (but not both) to <code>null</code> to leave one bound open.
	 * This data type is currently not used by <b>panFMP</b>.
	 */
	public TrieRangeQuery(String field, Long min, Long max) {
		if (min==null && max==null) throw new IllegalArgumentException("The min and max long values cannot be both null.");
		this.minUnconverted=min;
		this.maxUnconverted=max;
		this.min=(min==null) ? LuceneConversions.LUCENE_NUMERIC_MIN : LuceneConversions.longToLucene(min.longValue());
		this.max=(max==null) ? LuceneConversions.LUCENE_NUMERIC_MAX : LuceneConversions.longToLucene(max.longValue());
		this.field=field.intern();
	}

	@Override
	public String toString(String field) {
		StringBuilder sb=new StringBuilder();
		if (!this.field.equals(field)) sb.append(this.field).append(':');
		sb.append('[');
		sb.append(minUnconverted);
		sb.append(" TO ");
		sb.append(maxUnconverted);
		sb.append(']');
		return sb.toString();
	}

	@Override
	public final boolean equals(Object o) {
		if (o instanceof TrieRangeQuery) {
			TrieRangeQuery q=(TrieRangeQuery)o;
			return (field==q.field && min.equals(q.min) && max.equals(q.max));
		} else return false;
	}

	@Override
	public final int hashCode() {
		return field.hashCode()+(min.hashCode()^0x14fa55fb)+(max.hashCode()^0x733fa5fe);
	}

	/**
	 * Rewrites the query to native Lucene {@link Query}'s. This implementation uses a {@link ConstantScoreQuery} with
	 * a {@link TrieRangeFilter} as implementation of the trie algorithm.
	 */
	@Override
	public Query rewrite(IndexReader reader) throws java.io.IOException {
		ConstantScoreQuery q = new ConstantScoreQuery(new TrieRangeFilter());
		q.setBoost(getBoost());
		return q.rewrite(reader);
	}

	protected String field,min,max;
	private Object minUnconverted,maxUnconverted;

	/**
	 * Internal implementation of a trie-based range query using a {@link Filter}.
	 * This filter depends on a specific structure of terms in the index that can only be created
	 * by {@link LuceneConversions} methods.
	 * <p>This is the base of the internal implementation of <b>panFMP</b>'s
	 * high performance range queries on numeric and date/time fields using &quot;trie memory&quot; structures
	 * (see the publication about <b>panFMP</b>:
	 * <em>Schindler, U, Diepenbroek, M, 2007. Generic Framework for Metadata Portals. Computers &amp; Geosciences, submitted.</em>).
	 */
	protected final class TrieRangeFilter extends Filter {

		// code borrowed from original RangeFilter and simplified (and returns number of terms)
		private int setBits(IndexReader reader, TermDocs termDocs, BitSet bits, String lowerTerm, String upperTerm) throws IOException {
			int count=0,len=lowerTerm.length();
			// add padding before loose/inprecise values to group them
			if (len<16) {
				len++; // length is longer by 1 char because of padding
				lowerTerm=new StringBuilder(len).append((char)(LuceneConversions.LUCENE_PADDING_START+(len/2))).append(lowerTerm).toString();
				upperTerm=new StringBuilder(len).append((char)(LuceneConversions.LUCENE_PADDING_START+(len/2))).append(upperTerm).toString();
			}
			TermEnum enumerator = reader.terms(new Term(field, lowerTerm));
			try {
				do {
					Term term = enumerator.term();
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
		private int splitRange(IndexReader reader, TermDocs termDocs, BitSet bits, String min, boolean lowerBoundOpen, String max, boolean upperBoundOpen) throws IOException {
			int length=min.length(),count=0;
			String minShort=lowerBoundOpen ? min.substring(0,length-2) : LuceneConversions.incrementLucene(min.substring(0,length-2));
			String maxShort=upperBoundOpen ? max.substring(0,length-2) : LuceneConversions.decrementLucene(max.substring(0,length-2));

			if (length==2 || minShort.compareTo(maxShort)>=0) {
				count+=setBits(reader,termDocs,bits,min,max);
			} else {
				if (!lowerBoundOpen) count+=setBits(reader,termDocs,bits,min,minShort+LuceneConversions.LUCENE_SYMBOL_MIN+LuceneConversions.LUCENE_SYMBOL_MIN);
				count+=splitRange(reader,termDocs,bits,minShort,lowerBoundOpen,maxShort,upperBoundOpen);
				if (!upperBoundOpen) count+=setBits(reader,termDocs,bits,maxShort+LuceneConversions.LUCENE_SYMBOL_MAX+LuceneConversions.LUCENE_SYMBOL_MAX, max);
			}
			return count;
		}

		/**
		 * Returns a BitSet with true for documents which should be
		 * permitted in search results, and false for those that should
		 * not.
		 */
		@Override
		public BitSet bits(IndexReader reader) throws IOException {
			BitSet bits = new BitSet(reader.maxDoc());
			TermDocs termDocs=reader.termDocs();
			try {
				int count=splitRange(reader,termDocs,bits,min,LuceneConversions.LUCENE_NUMERIC_MIN.equals(min),max,LuceneConversions.LUCENE_NUMERIC_MAX.equals(max));
				if (log.isDebugEnabled()) log.debug("Found "+count+" distinct terms in filtered range for field '"+field+"'.");
			} finally {
				termDocs.close();
			}
			return bits;
		}
	}


}