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

package de.pangaea.metadataportal.search.axis;

import java.util.Arrays;

public class SearchRequest implements java.io.Serializable {

    public void setIndex(String v) { indexName=v; }

    public void setQueries(SearchRequestQuery[] v) { queries=v; }
    public void setRanges(SearchRequestRange[] v) { ranges=v; }
    public void setSortField(String v) { sortFieldName=v; }
    public void setSortReverse(Boolean v) { sortReverse=v; }

    // members
    protected String indexName=null;
    protected SearchRequestRange ranges[]=null;
    protected SearchRequestQuery queries[]=null;
    protected String sortFieldName=null;
    protected Boolean sortReverse=null;

}