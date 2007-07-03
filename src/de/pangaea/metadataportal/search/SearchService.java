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

import de.pangaea.metadataportal.utils.*;
import de.pangaea.metadataportal.config.*;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.*;
import org.apache.lucene.index.*;
import java.util.*;

public class SearchService {
    private static org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(SearchService.class);

    // constructor for use with applications using this class directly
    public SearchService(String cfgFile) throws Exception {
        cache=LuceneCache.getInstance(cfgFile);
        log.info("Search service initialized with '"+cfgFile+"' as index configuration file.");
    }

    // helpers

    protected SearchResponseItem extractDocument(Document doc, boolean returnXML, boolean returnStoredFields) {
        SearchResponseItem item=new SearchResponseItem();

        if (returnXML)
            item.xml=doc.get(IndexConstants.FIELDNAME_XML);

        item.identifier=doc.get(IndexConstants.FIELDNAME_IDENTIFIER);

        if (returnStoredFields) {
            for (Config.Config_Field f : cache.config.fields.values()) if (f.lucenestorage) {
                String[] data=doc.getValues(f.name);
                if (data!=null) {
                    java.util.ArrayList<Object> vals=new java.util.ArrayList<Object>();
                    for (String val : data) try {
                        switch(f.datatype) {
                            case TOKENIZEDTEXT:
                            case STRING:
                                vals.add(val); break;
                            case NUMBER:
                                vals.add(new Double(LuceneConversions.luceneToDouble(val))); break;
                            case DATETIME:
                                vals.add(LuceneConversions.luceneToDate(val)); break;
                        }
                    } catch (NumberFormatException ex) {
                        // ignore the field if conversion exception
                        log.warn(ex);
                    }
                    if (vals.size()>0) item.fields.put(f.name,vals.toArray());
                }
            }
        }
        return item;
    }

    // Search
    public SearchResponse search(SearchRequest searchRequest, int offset, int count) throws Exception {
        cache.cleanupCache();
        searchRequest.normalize(cache.config);
        if (log.isDebugEnabled()) log.debug("searchRequest: "+searchRequest);

        boolean returnXML=Boolean.parseBoolean(cache.config.searchProperties.getProperty("returnXML","true"));
        boolean returnStoredFields=Boolean.parseBoolean(cache.config.searchProperties.getProperty("returnStoredFields","true"));

        LuceneSession sess=cache.getLuceneResult(searchRequest);
        SearchResponse resp=new SearchResponse();
        synchronized(sess) {
            resp.totalCount=sess.hits.length();
            resp.queryTime=sess.queryTime;
            resp.offset=offset;
            if (resp.totalCount-offset<count) count=resp.totalCount-offset;
            if (count<0) count=0;
            resp.results=new SearchResponseItem[count];
            for (int i=0; i<count; i++) {
                Document doc=sess.hits.doc(offset+i);
                SearchResponseItem item=resp.results[i]=extractDocument(doc,returnXML,returnStoredFields);
                item.score=sess.hits.score(offset+i);
            }
        }
        return resp;
    }

    public SearchResponseItem getDocument(String indexName, String identifier) throws Exception {
        cache.cleanupCache();

        // init Index
        IndexConfig index=cache.config.indices.get(indexName);
        if (index==null) throw new IllegalArgumentException("Index '"+indexName+"' does not exist!");
        IndexReader reader = index.getIndexReader();

        TermDocs td=reader.termDocs(new Term(IndexConstants.FIELDNAME_IDENTIFIER,identifier));
        try {
            if (td.next()) {
                Document doc=reader.document(td.doc());
                if (td.next()) log.warn("There are multiple documents for identifier '"+identifier+"' in index '"+index+"'. Only first one returned!");
                SearchResponseItem item=extractDocument(doc,true,true);
                item.score=1.0f;
                return item;
            } else return null;
        } finally {
            td.close();
        }
    }

    public String[] suggest(String indexName, SearchRequestQuery query, int count) throws Exception {
        try {
            cache.cleanupCache();

            // init Index
            IndexConfig index=cache.config.indices.get(indexName);
            if (index==null) throw new IllegalArgumentException("Index '"+indexName+"' does not exist!");

            query.normalize(cache.config);
            Query q=LuceneSession.parseQuery(cache.config.getAnalyzer(),query.fieldName,query.query);
            return Suggest.suggest(index.getIndexReader(),q,query.query,count);
        } catch (org.apache.lucene.queryParser.ParseException e) {
            return null; // ignore exceptions for suggest, because invalid query strings are "normal"
        }
    }

    public String[] listTerms(String indexName, String fieldName, int count) throws Exception {
        cache.cleanupCache();

        // init Index
        IndexConfig index=cache.config.indices.get(indexName);
        if (index==null) throw new IllegalArgumentException("Index '"+indexName+"' does not exist!");
        IndexReader reader=index.getIndexReader();

        // check field
        fieldName=fieldName.intern();
        Config.Config_Field f=cache.config.fields.get(fieldName);
        if (f==null) throw new IllegalArgumentException("Field name '"+fieldName+"' is unknown!");
        if (!f.luceneindexed) throw new IllegalArgumentException("Field '"+fieldName+"' is not searchable!");
        if (f.datatype!=Config.DataType.TOKENIZEDTEXT && f.datatype!=Config.DataType.STRING)
            throw new IllegalArgumentException("Field '"+fieldName+"' is not of type string or tokenizedText!");

        // scan
        TermEnum terms=reader.terms(new Term(fieldName,new String()));
        try {
            ArrayList<String> termList=new ArrayList<String>();
            do {
                Term t=terms.term();
                if (t!=null && fieldName==t.field()) termList.add(t.text());
                else break;
            } while (termList.size()<count && terms.next());
            return termList.toArray(new String[termList.size()]);
        } finally {
            terms.close();
        }
    }

    private LuceneCache cache=null;

    /* Config options:
        <returnXML>true</returnXML>
        <returnStoredFields>true</returnStoredFields>
    */
}