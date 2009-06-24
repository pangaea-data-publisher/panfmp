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
import org.apache.lucene.search.NumericRangeFilter;

/**
* A Lucene {@link Query} that implements a trie-based range query.
* This query depends on a specific structure of terms in the index that can only be created
* by {@link TrieUtils} methods.
* <p>This class wraps a {@link NumericRangeFilter} from Lucene.
* @see NumericRangeFilter
*/
public final class DateRangeQuery extends ConstantScoreQuery {

	/**
	* A trie query using the supplied field with range bounds in date/time form.
	* You can set <code>min</code> or <code>max</code> (but not both) to <code>null</code> to leave one bound open.
	* With <code>minInclusive</code> and <code>maxInclusive</code> can be choosen, if the corresponding
	* bound should be included or excluded from the range.
	*/
	public DateRangeQuery(final String field, final int precisionStep, final Date min, final Date max,
		final boolean minInclusive, final boolean maxInclusive
	) {
		super(NumericRangeFilter.newLongRange(field,precisionStep,
			(min==null) ? null : Long.valueOf(min.getTime()),
			(max==null) ? null : Long.valueOf(max.getTime()),
			minInclusive,maxInclusive
		));
	}

	@Override
	public String toString(final String field) {
		// return a more convenient representation of this query than ConstantScoreQuery does:
		final StringBuilder sb=new StringBuilder();
		final NumericRangeFilter filter = (NumericRangeFilter)this.filter;
		if (!filter.getField().equals(field)) sb.append(filter.getField()).append(':');
		String min="*";
		if (filter.getMin()!=null) min=ISODateFormatter.formatLong(filter.getMin().longValue());
		String max="*";
		if (filter.getMax()!=null) max=ISODateFormatter.formatLong(filter.getMax().longValue());
		return sb.append(filter.includesMin() ? '[' : '{')
			.append(min).append(" TO ").append(max)
			.append(filter.includesMax() ? ']' : '}').append(ToStringUtils.boost(getBoost())).toString();
	}

}
