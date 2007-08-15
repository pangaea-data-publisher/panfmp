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

import de.pangaea.metadataportal.search.*;
import org.apache.lucene.search.*;
import java.util.*;

public class SearchServiceImpl {

    private static org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(SearchServiceImpl.class);

    public SearchServiceImpl(String cfgFile) {
        this.cfgFile=cfgFile;
    }

    // Search
    public SearchResponse search(SearchRequest req, int offset, int count) throws Exception {
        SearchService service=new SearchService(cfgFile,req.indexName);

        boolean returnXML=Boolean.parseBoolean(service.getConfig().searchProperties.getProperty("returnXML","true"));
        boolean returnStoredFields=Boolean.parseBoolean(service.getConfig().searchProperties.getProperty("returnStoredFields","true"));

        BooleanQuery q=service.newBooleanQuery();
        boolean ok=false;

        // queries
        HashMap<String,BooleanQuery> anyOfMap=new HashMap<String,BooleanQuery>();
        if (req.queries!=null) for (SearchRequestQuery fq : req.queries) {
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
        if (req.ranges!=null) for (SearchRequestRange r : req.ranges) {
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

        Sort sort=null;
        if (req.sortFieldName!=null && req.sortReverse!=null) sort=service.newSort(service.newFieldBasedSort(req.sortFieldName,req.sortReverse));

        SearchResultList res=service.search(q, sort, returnStoredFields || returnXML, returnStoredFields ? null : Collections.<String>emptySet() );
        SearchResponse resp=new SearchResponse(res, offset, count, returnXML, returnStoredFields);
        return resp;
    }

    public SearchResponseItem getDocument(String indexName, String identifier) throws Exception {
        SearchService service=new SearchService(cfgFile,indexName);
        return new SearchResponseItem(service.getDocument(identifier),true,true);
    }

    public String[] suggest(String indexName, SearchRequestQuery query, int count) throws Exception {
        SearchService service=new SearchService(cfgFile,indexName);
        List<String> sugg=service.suggest(query.fieldName,query.query,count);
        if (sugg==null) return null;
        else return sugg.toArray(new String[sugg.size()]);
    }

    public String[] listTerms(String indexName, String fieldName, int count) throws Exception {
        return listTerms(indexName,fieldName,"",count);
    }

    public String[] listTerms(String indexName, String fieldName, String prefix, int count) throws Exception {
        SearchService service=new SearchService(cfgFile,indexName);
        List<String> list=service.listTerms(fieldName,prefix,count);
        if (list==null) return null;
        else return list.toArray(new String[list.size()]);
    }

    private String cfgFile;

    /* Config options:
        <returnXML>true</returnXML>
        <returnStoredFields>true</returnStoredFields>
    */
}