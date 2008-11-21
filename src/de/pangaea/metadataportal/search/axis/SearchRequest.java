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

package de.pangaea.metadataportal.search.axis;

import de.pangaea.metadataportal.search.*;
import org.apache.lucene.search.*;
import java.util.*;

public class SearchRequest implements java.io.Serializable {

	public void setIndex(String v) { indexName=v; }
	public void setStoredQueryUUID(String v) { storedQueryUUID=v; }
	public void setQueries(SearchRequestQuery[] v) { queries=v; }
	public void setRanges(SearchRequestRange[] v) { ranges=v; }
	public void setSortField(String v) { sortFieldName=v; }
	public void setSortReverse(Boolean v) { sortReverse=v; }

	protected Query getLuceneQuery(SearchService service) throws Exception {
		if (storedQueryUUID!=null) {
			if (queries!=null || ranges!=null)
				throw new IllegalArgumentException("If you use a hash of a stored query, you may not give other constraints!");
			Query q=service.readStoredQuery(UUID.fromString(storedQueryUUID));
			if (q==null)
				throw new IllegalArgumentException("The stored query is no longer available!");
			return q;
		} else {
			BooleanQuery q=service.newBooleanQuery();
			boolean ok=false;

			// queries
			HashMap<String,BooleanQuery> anyOfMap=new HashMap<String,BooleanQuery>();
			if (queries!=null) for (SearchRequestQuery fq : queries) {
				if (fq.query==null || fq.query.equals("")) continue;
				// check if anyOf
				BooleanQuery dest;
				BooleanClause.Occur destType;
				if (fq.anyof && fq.fieldName==null)
					throw new IllegalArgumentException("anyOf cannot be used with the default field!");
				if (fq.anyof) {
					dest=anyOfMap.get(fq.fieldName);
					if (dest==null) {
						q.add(dest=service.newBooleanQuery(), BooleanClause.Occur.MUST);
						anyOfMap.put(fq.fieldName,dest);
					}
					destType=BooleanClause.Occur.SHOULD;
				} else {
					dest=q;
					destType=BooleanClause.Occur.MUST;
				}
				// add field to query (in anyOf subclause or as main query part)
				if (fq.fieldName==null) {
					dest.add(service.newDefaultFieldQuery(fq.query), destType);
				} else {
					dest.add(service.newTextQuery(fq.fieldName,fq.query), destType);
				}
				ok=true;
			}
			anyOfMap=null; // free it now

			// ranges
			if (ranges!=null) for (SearchRequestRange r : ranges) {
				Query rq=null;
				if (r.min instanceof String || r.max instanceof String) {
					// guess type by checking both possibilities
					Exception ex=null;
					try {
						rq=service.newNumericRangeQuery(r.fieldName,(String)r.min,(String)r.max);
					} catch (RuntimeException e) {
						if (e instanceof NumberFormatException) ex=e;
						try {
							rq=service.newDateRangeQuery(r.fieldName,(String)r.min,(String)r.max);
							ex=null;
						} catch (IllegalFieldConfigException e2) {
							throw new IllegalFieldConfigException("Field '"+r.fieldName+"' has wrong type!");
						} catch (NumberFormatException e2) {
							ex=e2;
						} catch (java.text.ParseException e3) {
							ex=e3;
						}
					}
					if (ex!=null) throw ex;
				} else if (r.min instanceof Number || r.max instanceof Number) {
					rq=service.newNumericRangeQuery(r.fieldName,(Number)r.min,(Number)r.max);
				} else if (r.min instanceof Date || r.max instanceof Date) {
					rq=service.newDateRangeQuery(r.fieldName,(Date)r.min,(Date)r.max);
				} else if (r.min instanceof Calendar || r.max instanceof Calendar) {
					rq=service.newDateRangeQuery(r.fieldName,(Calendar)r.min,(Calendar)r.max);
				} else throw new IllegalArgumentException("Invalid values in SearchRequestRange");
				q.add(rq,BooleanClause.Occur.MUST);
				ok=true;
			}

			if (!ok) throw new IllegalArgumentException("The search request does not contain any constraints!");
			return q;
		}
	}

	// members
	protected String indexName=null;
	protected SearchRequestRange ranges[]=null;
	protected SearchRequestQuery queries[]=null;
	protected String sortFieldName=null;
	protected Boolean sortReverse=null;
	protected String storedQueryUUID=null;

}