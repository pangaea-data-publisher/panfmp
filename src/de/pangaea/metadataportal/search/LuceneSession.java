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

import java.util.*;

import de.pangaea.metadataportal.utils.*;
import de.pangaea.metadataportal.config.*;

import org.apache.lucene.index.*;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.*;
import org.apache.lucene.queryParser.*;

public class LuceneSession {

    private static org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(LuceneSession.class);

    public LuceneSession(LuceneCache owner, SearchRequest req) throws Exception {
        this.req=req;
        this.owner=owner;

        // analyze query
        queryTime=new java.util.Date().getTime();

        BooleanQuery q=new BooleanQuery();
        boolean ok=false;

        Analyzer analyzer=owner.config.getAnalyzer();
        IndexConfig index=owner.config.indices.get(req.indexName);
        if (index==null) throw new IllegalArgumentException("Index '"+req.indexName+"' does not exist!");
        Searcher searcher=index.newSearcher();

        // queries
        HashMap<String,BooleanQuery> anyOfMap=new HashMap<String,BooleanQuery>();
        if (req.queries!=null) for (SearchRequestQuery fq : req.queries) {
            if (fq.query.equals("")) continue;
            // check if anyOf
            BooleanQuery dest;
            BooleanClause.Occur destType;
            if (fq.anyof) {
                dest=anyOfMap.get(fq.fieldName);
                if (dest==null) {
                    q.add(dest=new BooleanQuery(), BooleanClause.Occur.MUST);
                    anyOfMap.put(fq.fieldName,dest);
                }
                destType=BooleanClause.Occur.SHOULD;
            } else {
                dest=q;
                destType=BooleanClause.Occur.MUST;
            }
            // add field to query (in anyOf subclause or as main query part)
            if (fq.isStringField) {
                dest.add(new TermQuery(new Term(fq.fieldName,fq.query)), destType);
            } else {
                Query lfq=searcher.rewrite(parseQuery(analyzer,fq.fieldName,fq.query));
                if (fq.fieldName!=IndexConstants.FIELDNAME_CONTENT)
                    lfq.extractTerms(new TermCheckerSet(fq.fieldName));
                dest.add(lfq, destType);
            }
            ok=true;
        }
        anyOfMap=null; // free it now

        // ranges
        if (req.ranges!=null) for (SearchRequestRange r : req.ranges) {
            // min/max is for sure a Date or Number due to normalize(config)!!!
            String minStr=null,maxStr=null;
            if (r.min!=null) {
                if (r.min instanceof Number) minStr=LuceneConversions.doubleToLucene(((Number)r.min).doubleValue());
                else if (r.min instanceof java.util.Date) minStr=LuceneConversions.dateToLucene((java.util.Date)r.min);
                else throw new IllegalArgumentException("Invalid datatype for range query!"); // should not occur
            }
            if (r.max!=null) {
                if (r.max instanceof Number) maxStr=LuceneConversions.doubleToLucene(((Number)r.max).doubleValue());
                else if (r.max instanceof java.util.Date) maxStr=LuceneConversions.dateToLucene((java.util.Date)r.max);
                else throw new IllegalArgumentException("Invalid datatype for range query!"); // should not occur
            }
            q.add(new AdvRangeQuery(r.fieldName,minStr,maxStr),BooleanClause.Occur.MUST);
            ok=true;
        }

        if (!ok) throw new IllegalArgumentException("The search request does not contain any constraints!");

        if (log.isDebugEnabled()) log.debug("Query to send to Lucene: "+q);

        if (req.sortFieldName!=null && req.sortReverse!=null) {
            // TODO: optimized, less memory expensive sorting (SortComparator!!!)
            hits=searcher.search(q,new Sort(new SortField[]{new SortField(req.sortFieldName,SortField.STRING,req.sortReverse.booleanValue()),SortField.FIELD_SCORE,SortField.FIELD_DOC}));
        } else {
            hits=searcher.search(q);
        }

        queryTime=new java.util.Date().getTime()-queryTime;
    }

    public static Query parseQuery(Analyzer ana, String field, String query) throws ParseException {
        QueryParser p=new QueryParser(field, ana);
        p.setDefaultOperator(QueryParser.AND_OPERATOR);
        Query q=p.parse(query);
        return q;
    }

    protected synchronized void logAccess() {
        lastAccess=new java.util.Date().getTime();
    }

    protected SearchRequest req=null;
    protected Hits hits=null;
    protected long lastAccess=new java.util.Date().getTime();
    protected long queryTime=0L;
    private LuceneCache owner=null;

    // this class is a helper for checking a query for invalid (wrong field) Terms. It implements a set but only tests
    // in the add() method for invalid Terms. Nothing more is done, the Set is always empty!
    private final class TermCheckerSet<E> extends AbstractSet<E> {

        public TermCheckerSet(String field) {
            this.field=field.intern();
        }

        public int size() { return 0; }
        public Iterator<E> iterator() { return Collections.EMPTY_SET.iterator(); }

        public boolean add(E t) {
            if (((Term)t).field()!=field) throw new IllegalArgumentException("A query on a specific field may not reference other fields by prefixing with 'fieldname:'!");
            return false;
        }

        private String field;
    }

}