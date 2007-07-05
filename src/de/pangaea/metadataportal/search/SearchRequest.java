/*
 *   Copyright 2007 panFMP Developers Team c/o Uwe Schindler
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
import java.util.Arrays;

public class SearchRequest implements java.io.Serializable {

    public void setIndex(String v) { indexName=v; }

    public void setQueries(SearchRequestQuery[] v) { queries=v; }
    public void setRanges(SearchRequestRange[] v) { ranges=v; }
    public void setSortField(String v) {
        if (v!=null) {
            sortFieldName=v.intern();
        } else {
            sortFieldName=null; sortReverse=true;
        }
    }
    public void setSortReverse(Boolean v) { sortReverse=v; }

    // cast the min max values to correct types
    protected void normalize(Config config) {
        // checks
        if (indexName==null) throw new IllegalArgumentException("indexName may not be null!");
        if (!config.indices.containsKey(indexName)) throw new IllegalArgumentException("indexName='"+indexName+"' is not a valid index!");
        // normalize
        if (ranges!=null) for (SearchRequestRange r : ranges) r.normalize(config);
        if (queries!=null) for (SearchRequestQuery fq : queries) fq.normalize(config);
        if ((sortFieldName!=null) ^ (sortReverse!=null))
            throw new IllegalArgumentException("When defining a 'sortField', you must also define 'sortReverse' (and vice versa)!");
        if (sortFieldName!=null) {
            Config.Config_Field f=config.fields.get(sortFieldName);
            if (f==null) throw new IllegalArgumentException("Field name '"+sortFieldName+"' is unknown!");
            if (!f.luceneindexed) throw new IllegalArgumentException("Field '"+sortFieldName+"' is not searchable!");
            if (f.datatype==Config.DataType.TOKENIZEDTEXT) throw new IllegalArgumentException("Field '"+sortFieldName+"' is tokenized which prevents sorting!");
        }
    }

    // hashing & compare & string representation
    public final boolean equals(Object o) {
        if (o!=null && o instanceof SearchRequest) {
            SearchRequest s=(SearchRequest)o;
            boolean ok=true;
            ok&=(sortFieldName==s.sortFieldName);
            ok&=(sortReverse==s.sortReverse || (s.sortReverse!=null && s.sortReverse.equals(sortReverse)));
            ok&=(indexName==s.indexName || (s.indexName!=null && s.indexName.equals(indexName)));
            ok&=Arrays.equals(s.queries,queries);
            ok&=Arrays.equals(s.ranges,ranges);
            return ok;
        } else return false;
    }

    public final int hashCode() {
        int hashCode=0;
        if (indexName!=null) hashCode^=indexName.hashCode();
        if (queries!=null) hashCode^=Arrays.hashCode(queries)^0x1ab3456f;
        if (ranges!=null) hashCode^=Arrays.hashCode(ranges)^0x4567abcd;
        if (sortFieldName!=null) hashCode^=sortFieldName.hashCode()^0x984af32a;
        if (sortReverse!=null) hashCode^=sortReverse.hashCode()^0x1aabb245;
        return hashCode;
    }

    public String toString() {
        StringBuilder sb=new StringBuilder("index='"+indexName+"'; ");
        if (queries!=null) sb.append("queries="+Arrays.toString(queries)+"; ");
        if (ranges!=null) sb.append("ranges="+Arrays.toString(ranges)+"; ");
        if (sortFieldName!=null && sortReverse!=null) {
            sb.append("sortField="+sortFieldName+"("+(sortReverse.booleanValue()?"reverse":"forward")+")");
        } else {
            sb.append("relevanceSorted");
        }
        return sb.toString();
    }

    // members
    protected String indexName=null;
    protected SearchRequestRange ranges[]=null;
    protected SearchRequestQuery queries[]=null;
    protected String sortFieldName=null; // interned
    protected Boolean sortReverse=null;

}