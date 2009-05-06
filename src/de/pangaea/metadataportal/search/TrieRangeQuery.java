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

import java.util.Date;

import de.pangaea.metadataportal.utils.ISODateFormatter;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.util.ToStringUtils;
import org.apache.lucene.search.trie.*;

/**
* A Lucene {@link Query} that implements a trie-based range query.
* This query depends on a specific structure of terms in the index that can only be created
* by {@link TrieUtils} methods.
* <p>This class wraps a {@link LongTrieRangeFilter} from Lucene Contrib.
* @see LongTrieRangeFilter
*/
public final class TrieRangeQuery extends ConstantScoreQuery {

	/**
	* A trie query using the supplied field with range bounds in integer form (long).
	* You can set <code>min</code> or <code>max</code> (but not both) to <code>null</code> to leave one bound open.
	* With <code>minInclusive</code> and <code>maxInclusive</code> can be choosen, if the corresponding
	* bound should be included or excluded from the range.
	*/
	public TrieRangeQuery(final String field, final int precisionStep, final Long min, final Long max,
		final boolean minInclusive, final boolean maxInclusive
	) {
		super(new LongTrieRangeFilter(field,precisionStep,min,max,minInclusive,maxInclusive));
		this.field=field;
		this.minInclusive=minInclusive;
		this.maxInclusive=maxInclusive;
		this.minUnconverted=min;
		this.maxUnconverted=max;
	}

	/**
	* A trie query using the supplied field with range bounds in numeric form (double).
	* You can set <code>min</code> or <code>max</code> (but not both) to <code>null</code> to leave one bound open.
	* With <code>minInclusive</code> and <code>maxInclusive</code> can be choosen, if the corresponding
	* bound should be included or excluded from the range.
	*/
	public TrieRangeQuery(final String field, final int precisionStep, final Double min, final Double max,
		final boolean minInclusive, final boolean maxInclusive
	) {
		this(field,precisionStep,
			(min==null) ? null : Long.valueOf(TrieUtils.doubleToSortableLong(min.doubleValue())),
			(max==null) ? null : Long.valueOf(TrieUtils.doubleToSortableLong(max.doubleValue())),
			minInclusive,maxInclusive
		);
		this.minUnconverted=min;
		this.maxUnconverted=max;
	}

	/**
	* A trie query using the supplied field with range bounds in date/time form.
	* You can set <code>min</code> or <code>max</code> (but not both) to <code>null</code> to leave one bound open.
	* With <code>minInclusive</code> and <code>maxInclusive</code> can be choosen, if the corresponding
	* bound should be included or excluded from the range.
	*/
	public TrieRangeQuery(final String field, final int precisionStep, final Date min, final Date max,
		final boolean minInclusive, final boolean maxInclusive
	) {
		this(field,precisionStep,
			(min==null) ? null : Long.valueOf(min.getTime()),
			(max==null) ? null : Long.valueOf(max.getTime()),
			minInclusive,maxInclusive
		);
		this.minUnconverted=min;
		this.maxUnconverted=max;
	}

	@Override
	public String toString(final String field) {
		// return a more convenient representation of this query than ConstantScoreQuery does:
		final StringBuilder sb=new StringBuilder();
		if (!this.field.equals(field)) sb.append(this.field).append(':');
		String min="*";
		if (minUnconverted instanceof Date) min=ISODateFormatter.formatLong((Date)minUnconverted);
		else if (minUnconverted!=null) min=minUnconverted.toString();
		String max="*";
		if (maxUnconverted instanceof Date) max=ISODateFormatter.formatLong((Date)maxUnconverted);
		else if (maxUnconverted!=null) max=maxUnconverted.toString();
		return sb.append(minInclusive ? '[' : '{')
			.append(min).append(" TO ").append(max)
			.append(maxInclusive ? ']' : '}').append(ToStringUtils.boost(getBoost())).toString();
	}
	
	@Override
	public final boolean equals(Object o) {
		if (this==o) return true;
		if (o instanceof TrieRangeQuery) {
			final TrieRangeQuery q=(TrieRangeQuery)o;
			// this compares only the extra members
			return (
				super.equals(q) &&
				(q.minUnconverted == null ? minUnconverted == null : q.minUnconverted.equals(minUnconverted)) &&
				(q.maxUnconverted == null ? maxUnconverted == null : q.maxUnconverted.equals(maxUnconverted))
			);
		} else return false;
	}

	@Override
	public final int hashCode() {
		return super.hashCode()^0x743fb1ae; // a little bit different
	}

	// for nicer display, the original data types
	private Object minUnconverted,maxUnconverted;
	// these members are duplicates of the filters members
	private final boolean minInclusive,maxInclusive;
	private final String field;

}
