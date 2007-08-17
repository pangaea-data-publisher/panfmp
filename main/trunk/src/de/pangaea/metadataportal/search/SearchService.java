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

import de.pangaea.metadataportal.config.*;
import de.pangaea.metadataportal.utils.IndexConstants;
import de.pangaea.metadataportal.utils.LenientDateParser;
import org.apache.lucene.search.*;
import org.apache.lucene.queryParser.*;
import org.apache.lucene.index.*;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;
import java.io.IOException;
import java.util.*;

/**
 * This class is the main entry point to panFMP's search engine.
 * <h3>To start a query with panFMP do the following:</h3>
 * <pre>
 * import de.pangaea.metadataportal.search.*;
 * import org.apache.lucene.search.*;
 * ...
 *
 * // create a search service
 * SearchService service=new SearchService("config.xml", "indexname");
 * // build a query
 * BooleanQuery bq=service.newBooleanQuery();
 * bq.add(service.newDefaultFieldQuery("a search query for the simple search"), BooleanClause.Occur.MUST);
 * bq.add(service.newNumericRangeQuery("longitude", -20.0, 10.0), BooleanClause.Occur.MUST);
 * bq.add(service.newNumericRangeQuery("latitude", null, 30.5), BooleanClause.Occur.MUST);
 * </pre>
 * <h3>You have two possibilities to start the search:</h3>
 * <ul>
 * <li><p>Retrieve sorted results as a listing (works good for web pages that display search results like Google with paging):</p><pre>
 * // create a Sort, if you want standard sorting by relevance use sort=null
 * Sort sort=service.newSort(service.newFieldBasedSort("longitude", false));
 * // start search
 * SearchResultList list=service.search(bq,sort);
 * // print search results (start is item to start with, count is number of results)
 * int start=0,count=10;
 * List&lt;SearchResultItem&gt; page=list.subList(
 *   Math.min(start, list.size()),
 *   Math.min(start+count, list.size())
 * );
 * for (SearchResultItem item : page) {
 *   System.out.println(item.getFields());
 * }
 * </pre>
 * <p>It is good to know that {@link SearchResultList} implements the {@link List} interface. This makes it possible to
 * use the standard Java Collection API to access search results as you can see in the example.</p>
 * </li>
 * <li><p>Retrieve a large number of results in unsorted order through a {@link SearchResultCollector}. This is recommended for creating
 * large files with thousands of results or processing map data because iterating over a {@link SearchResultList} is very slow
 * and is expensive in memory consumption!!!</p><pre>
 * service.search(new SearchResultCollector() {
 *   public boolean collect(SearchResultItem item) {
 *     System.out.println(item.getFields());
 *     return true; // return false to stop collecting results
 *   }
 * }, bq);
 * </pre></li>
 * </ul>
 * Both methods use the standard forms of {@link #search(Query,Sort)} that return the whole XML document and all stored fields.
 * This is like a <code>select * from table</code> in SQL.
 * This is not recommended especially when collecting a large number of results! It is better to fetch only fields
 * needed for processing (like a SQL <code>select column1,column2 from table</code>). This can be done by
 * special {@link #search(Query,Sort,boolean,Collection)} methods accepting lists of fields.
 * @author Uwe Schindler
 */
public class SearchService {
    private static org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(SearchService.class);

    /** Main constructor that initializes a <code>SearchService</code>. The underlying {@link LuceneCache} is a singleton per config file,
     * so you can create more than one instance of this class without additional memory consumption.
     * @param cfgFile file name and path of configuration file
     */
    public SearchService(String cfgFile, String indexId) throws Exception {
        cache=LuceneCache.getInstance(cfgFile);
        index=cache.config.indexes.get(indexId);
        if (index==null) throw new IllegalArgumentException("Index '"+indexId+"' does not exist!");
    }

    /**
     * Constructs a {@link BooleanQuery}. Use this query type to combine different query types from the factory methods
     * (native Lucene {@link Query} are useable, too). The current version is equivalent to
     * <pre>
     * {@link BooleanQuery} bq=new BooleanQuery();
     * </pre>
     * but this should be avoided to make further extensions to this class possible.
     */
    public BooleanQuery newBooleanQuery() {
        return new BooleanQuery();
    }

    /**
     * Constructs a {@link Query} for querying a {@link de.pangaea.metadataportal.config.FieldConfig.DataType#TOKENIZEDTEXT} or
     * {@link de.pangaea.metadataportal.config.FieldConfig.DataType#STRING} field.
     * String fields are <b>not</b> parsed by the query parser. They will be matched exact.
     * Tokenized text fields are parsed by {@link #parseQuery} and expand to different query types combined by a {@link BooleanQuery}.
     * @throws IllegalFieldConfigException if the configuration of <code>fieldName</code> does not match this query type or it is unknown
     */
    public Query newTextQuery(String fieldName, String query) throws ParseException {
        FieldConfig f=cache.config.fields.get(fieldName);
        if (f==null) throw new IllegalFieldConfigException("Field name '"+fieldName+"' is unknown!");
        if (!f.luceneindexed) throw new IllegalFieldConfigException("Field '"+fieldName+"' is not searchable!");
        if (query==null) throw new NullPointerException("A query string must be given for field '"+fieldName+"'!");

        switch (f.datatype) {
            case STRING:
                return new TermQuery(new Term(fieldName,query));
            case TOKENIZEDTEXT:
                return new FieldCheckingQuery(fieldName,parseQuery(fieldName,query));
            default:
                throw new IllegalFieldConfigException("Field '"+fieldName+"' is not of data type STRING or TOKENIZEDTEXT!");
        }
    }

    /**
     * Constructs a {@link Query} for querying the default field.
     * The query is parsed by {@link #parseQuery} and expands to different query types combined by a {@link BooleanQuery}.
     */
    public Query newDefaultFieldQuery(String query) throws ParseException {
        if (query==null) throw new NullPointerException("A query string must be given!");
        return parseQuery(IndexConstants.FIELDNAME_CONTENT,query);
    }

    /**
     * Constructs a {@link Query} for querying a {@link de.pangaea.metadataportal.config.FieldConfig.DataType#DATETIME} field.
     * @param min Minimum value as Date or <code>null</code> if lower bound open
     * @param max Maximum value as Date or <code>null</code> if upper bound open
     * @throws IllegalFieldConfigException if the configuration of <code>fieldName</code> does not match this query type or it is unknown
     */
    public Query newDateRangeQuery(String fieldName, Date min, Date max) {
        FieldConfig f=cache.config.fields.get(fieldName);
        if (f==null) throw new IllegalFieldConfigException("Field name '"+fieldName+"' is unknown!");
        if (!f.luceneindexed) throw new IllegalFieldConfigException("Field '"+fieldName+"' is not searchable!");
        if (f.datatype!=FieldConfig.DataType.DATETIME) throw new NullPointerException("The data type of field '"+fieldName+"' must be DATETIME!");
        if (min==null && max==null) throw new NullPointerException("A min or max value must be given for field '"+fieldName+"'!");
        return new TrieRangeQuery(fieldName,min,max);
    }

    /**
     * Constructs a {@link Query} for querying a {@link de.pangaea.metadataportal.config.FieldConfig.DataType#DATETIME} field.
     * @param min Minimum value as Calendar or <code>null</code> if lower bound open; the Calendar is internally converted to a {@link Date}
     * @param max Maximum value as Calendar or <code>null</code> if upper bound open; the Calendar is internally converted to a {@link Date}
     * @throws IllegalFieldConfigException if the configuration of <code>fieldName</code> does not match this query type or it is unknown
     */
    public Query newDateRangeQuery(String fieldName, Calendar min, Calendar max) {
        return newDateRangeQuery(fieldName, (min!=null)?min.getTime():null, (max!=null)?max.getTime():null);
    }

    /**
     * Constructs a {@link Query} for querying a {@link de.pangaea.metadataportal.config.FieldConfig.DataType#DATETIME} field.
     * @param min Minimum value as String or <code>null</code> if lower bound open; the String is parsed by {@link LenientDateParser}
     * @param max Maximum value as String or <code>null</code> if upper bound open; the String is parsed by {@link LenientDateParser}
     * @throws IllegalFieldConfigException if the configuration of <code>fieldName</code> does not match this query type or it is unknown
     * @throws java.text.ParseException if one of the boundaries is not a parseable date string
     */
    public Query newDateRangeQuery(String fieldName, String min, String max) throws java.text.ParseException {
        return newDateRangeQuery(fieldName, (min!=null)?LenientDateParser.parseDate(min):null, (max!=null)?LenientDateParser.parseDate(max):null);
    }

    /**
     * Constructs a {@link Query} for querying a {@link de.pangaea.metadataportal.config.FieldConfig.DataType#NUMBER} field.
     * @param min Minimum value as Double or <code>null</code> if lower bound open
     * @param max Maximum value as Double or <code>null</code> if upper bound open
     * @throws IllegalFieldConfigException if the configuration of <code>fieldName</code> does not match this query type or it is unknown
     */
    public Query newNumericRangeQuery(String fieldName, Double min, Double max) {
        FieldConfig f=cache.config.fields.get(fieldName);
        if (f==null) throw new IllegalFieldConfigException("Field name '"+fieldName+"' is unknown!");
        if (!f.luceneindexed) throw new IllegalFieldConfigException("Field '"+fieldName+"' is not searchable!");
        if (f.datatype!=FieldConfig.DataType.NUMBER) throw new NullPointerException("The data type of field '"+fieldName+"' must be NUMBER!");
        if (min==null && max==null) throw new NullPointerException("A min or max value must be given for field '"+fieldName+"'!");
        return new TrieRangeQuery(fieldName,min,max);
    }

    /**
     * Constructs a {@link Query} for querying a {@link de.pangaea.metadataportal.config.FieldConfig.DataType#NUMBER} field.
     * @param min Minimum value as Number or <code>null</code> if lower bound open; the Number is internally converted to a {@link Double},
     * but it can be any numeric Java type
     * @param max Maximum value as Number or <code>null</code> if upper bound open; the Number is internally converted to a {@link Double},
     * but it can be any numeric Java type
     * @throws IllegalFieldConfigException if the configuration of <code>fieldName</code> does not match this query type or it is unknown
     */
    public Query newNumericRangeQuery(String fieldName, Number min, Number max) {
        return newNumericRangeQuery(fieldName, (min!=null)?Double.valueOf(min.doubleValue()):null, (max!=null)?Double.valueOf(max.doubleValue()):null);
    }

    /**
     * Constructs a {@link Query} for querying a {@link de.pangaea.metadataportal.config.FieldConfig.DataType#NUMBER} field.
     * @param min Minimum value as String or <code>null</code> if lower bound open; the String is parsed according to Java standard
     * @param max Maximum value as String or <code>null</code> if upper bound open; the String is parsed according to Java standard
     * @throws IllegalFieldConfigException if the configuration of <code>fieldName</code> does not match this query type or it is unknown
     * @throws NumberFormatException if one of the boundaries is not a parseable numeric string
     */
    public Query newNumericRangeQuery(String fieldName, String min, String max) {
        return newNumericRangeQuery(fieldName, (min!=null)?Double.valueOf(min):null, (max!=null)?Double.valueOf(max):null);
    }

    /**
     * Constructs a {@link MatchAllDocsQuery}. Use this to generate a query that matches all documents. It is useful under
     * two circumstances:
     * <ul>
     * <li>Return all documents in index e.g. for generating a geographic map containing all documents with the collector search methods.</li>
     * <li>Constructing a query that should return all documents excluding some with special constraints. This can be achieved by constructing
     * a {@link BooleanQuery} containing this <code>MatchAllDocsQuery</code> with {@link org.apache.lucene.search.BooleanClause.Occur#MUST}
     * and the excluding query with {@link org.apache.lucene.search.BooleanClause.Occur#MUST_NOT}</li>
     * </ul>
     * <p>The current version is equivalent to
     * <pre>
     * Query q=new {@link MatchAllDocsQuery}();
     * </pre>
     * but this should be avoided to make further extensions to this class possible (e.g. in future indexes may contain documents
     * marked as &quot;deleted&quot; that should not be returned).
     */
    public Query newMatchAllDocsQuery() {
        return new MatchAllDocsQuery();
    }

    /**
     * Constructs a {@link SortField} instance to sort the results of a query based on a field.
     * @throws IllegalFieldConfigException if the configuration of <code>fieldName</code> is not valid for sorting or it is unknown:
     * The field must be indexed, but not be tokenized, and does not need to be stored (unless you happen to want it back with the rest of your document data).
     * <p><em>Please note:</em> You cannot sort fields that are <b>only stored</b>!
     */
    public SortField newFieldBasedSort(String fieldName, boolean reverse) {
        // TODO: create a SortComparator or something like that to help sorting less memory expensive! (needs further investigation)
        FieldConfig f=cache.config.fields.get(fieldName);
        if (f==null) throw new IllegalFieldConfigException("Field name '"+fieldName+"' is unknown!");
        if (!f.luceneindexed) throw new IllegalFieldConfigException("Field '"+fieldName+"' is not searchable!");
        if (f.datatype==FieldConfig.DataType.TOKENIZEDTEXT) throw new NullPointerException("A field used for sorting may not be tokenized!");
        return new SortField(fieldName,SortField.STRING,reverse);
    }

    /**
     * Constructs a {@link Sort} instance to sort the results of a query based on different fields (like a SELECT ... ORDER BY clause in SQL).
     * This implementation constructs <code>Sort</code> in a way that a search result is sorted by relevance
     * if all other criterias are the same for two search results.
     * @param sortFields a VARARG parameter consisting of a number of previously generated SortField (using {@link #newFieldBasedSort})
     */
    public Sort newSort(SortField... sortFields) {
        SortField[] f=new SortField[sortFields.length+1];
        System.arraycopy(sortFields,0,f,0,sortFields.length);
        f[sortFields.length]=SortField.FIELD_SCORE;
        return new Sort(f);
    }

    /**
     * Returns a list of query strings that can be displayed as a &quot;suggest&quot; drop-down box in search interfaces.
     * @param fieldName contains the field name for a field-specific input field. If you want suggestion for the default field use {@link #suggest(String,int)}
     * @param query is the query string the user have typed in. It will be parsed and the last term found in it is expanded
     * @param count limits the number of results
     * @return a list of query strings correlated to the parameter <code>query</code>
     * @throws IllegalFieldConfigException if the configuration of <code>fieldName</code> does not match data type TOKENIZEDTEXT or it is unknown
     */
    public List<String> suggest(String fieldName, String query, int count) throws IOException {
        cache.cleanupCache();

        if (count==0) return Collections.<String>emptyList();

        if (fieldName!=null) {
            FieldConfig f=cache.config.fields.get(fieldName);
            if (f==null) throw new IllegalFieldConfigException("Field name '"+fieldName+"' is unknown!");
            if (!f.luceneindexed) throw new IllegalFieldConfigException("Field '"+fieldName+"' is not searchable!");
            if (f.datatype!=FieldConfig.DataType.TOKENIZEDTEXT) throw new IllegalFieldConfigException("Field '"+fieldName+"' is not of data type TOKENIZEDTEXT!");
        } else fieldName=IndexConstants.FIELDNAME_CONTENT;
        if (query==null) throw new NullPointerException("A query string must be given!");
        try {
            Query q=parseQuery(fieldName, query);
            Term base=findLastTerm(q);
            if (base!=null) {
                // get strings before and after this term
                int pos=query.toLowerCase().lastIndexOf(base.text().toLowerCase()); // case insensitive
                if (pos<0) {
                    log.error("Term '"+base+"' not found in query '"+query+"' (should never happen)!");
                    Collections.<String>emptyList(); // should never happen
                }
                String before=query.substring(0,pos), after=query.substring(pos+base.text().length());

                // scan
                List<String> list=new ArrayList<String>((count>100)?50:count/2);
                TermEnum terms=index.getIndexReader().terms(base);
                try {
                    int c=0;
                    do {
                        Term t=terms.term();
                        if (t!=null && base.field()==t.field() && t.text().startsWith(base.text()) && terms.docFreq()>0) {
                            list.add(before+t.text()+after);
                        } else break;
                    } while (list.size()<count && terms.next());
                    return Collections.unmodifiableList(list);
                } finally {
                    terms.close();
                }
            }
        } catch (ParseException e) {
             // ignore ParseExceptions for suggest, because invalid query strings are "normal"
        }
        return Collections.<String>emptyList();
    }

    /**
     * Returns a list of query strings that can be displayed as a &quot;suggest&quot; drop-down box in search interfaces.
     * @param query is the query string the user have typed in. It will be parsed and the last term found in it is expanded
     * @param count limits the number of results
     * @return a list of query strings correlated to the parameter <code>query</code>
     */
    public List<String> suggest(String query, int count) throws IOException {
        return suggest(null,query,count);
    }

    /**
     * Returns a list of terms for fields of type {@link de.pangaea.metadataportal.config.FieldConfig.DataType#STRING}.
     * @param fieldName contains the field name for which terms should be listed.
     * @param prefix limits the returned list to terms starting with <code>prefix</code>. Set to <code>&quot;&quot;</code> for a full list.
     * @param count limits the number of results
     * @throws IllegalFieldConfigException if the configuration of <code>fieldName</code> does not match data type STRING or it is unknown
     */
    public List<String> listTerms(String fieldName, String prefix, int count) throws IOException {
        cache.cleanupCache();

        if (count==0) return Collections.<String>emptyList();

        // check field
        FieldConfig f=cache.config.fields.get(fieldName);
        if (f==null) throw new IllegalFieldConfigException("Field name '"+fieldName+"' is unknown!");
        if (!f.luceneindexed) throw new IllegalFieldConfigException("Field '"+fieldName+"' is not searchable!");
        if (f.datatype!=FieldConfig.DataType.STRING)
            throw new IllegalFieldConfigException("Field '"+fieldName+"' is not of type STRING!");

        // scan
        IndexReader reader=index.getIndexReader();
        fieldName=fieldName.intern();
        TermEnum terms=reader.terms(new Term(fieldName,prefix));
        try {
            ArrayList<String> termList=new ArrayList<String>((count>100)?50:count/2);
            do {
                Term t=terms.term();
                if (t!=null && fieldName==t.field() && t.text().startsWith(prefix) && terms.docFreq()>0) termList.add(t.text());
                else break;
            } while (termList.size()<count && terms.next());
            return Collections.unmodifiableList(termList);
        } finally {
            terms.close();
        }
    }

    /**
     * Returns a list of terms for fields of type {@link de.pangaea.metadataportal.config.FieldConfig.DataType#STRING}.
     * @param fieldName contains the field name for which terms should be listed.
     * @param count limits the number of results
     * @throws IllegalFieldConfigException if the configuration of <code>fieldName</code> does not match data type STRING or it is unknown
     */
    public List<String> listTerms(String fieldName, int count) throws IOException {
        return listTerms(fieldName,"",count);
    }

    /**
     * Executes search and returns search results.
     * @param query the previously constructed query
     * @param sort if you want to sort search results supply a {@link Sort} instance that describes the search (use {@link #newSort} for that).
     * Supply <code>null</code> for default sorting (by relevance backwards).
     * @param loadXml return the XML blob of search results.
     * @param fieldsToLoad a collection of field names that should be made available. <code>null</code> to return all fields <b>and</b> XML.
     */
    public SearchResultList search(Query query, Sort sort, boolean loadXml, Collection<String> fieldsToLoad) throws IOException {
        cache.cleanupCache();

        LuceneCache.Session sess=cache.getSession(index,query,sort);
        return sess.getSearchResultList(loadXml,fieldsToLoad);
    }

    /**
     * Executes search and returns search results. This version uses VARARGs to list field names to return.
     * @see #search(Query,Sort,boolean,Collection)
     */
    public SearchResultList search(Query query, Sort sort, boolean loadXml, String... fieldName) throws IOException {
        return search(query,sort,loadXml,Arrays.asList(fieldName));
    }

    /**
     * Executes search and returns search results. All fields are returned.
     * @see #search(Query,Sort,boolean,Collection)
     */
    public SearchResultList search(Query query, Sort sort) throws IOException {
        return search(query,sort,true,(Collection<String>)null);
    }

    /**
     * Executes search and returns search results with default sorting by relevance.
     * @see #search(Query,Sort,boolean,Collection)
     */
    public SearchResultList search(Query query, boolean loadXml, Collection<String> fieldsToLoad) throws IOException {
        return search(query,(Sort)null,loadXml,fieldsToLoad);
    }

    /**
     * Executes search and returns search results with default sorting by relevance. This version uses VARARGs to list field names to return.
     * @see #search(Query,Sort,boolean,String...)
     */
    public SearchResultList search(Query query, boolean loadXml, String... fieldName) throws IOException {
        return search(query,(Sort)null,loadXml,fieldName);
    }

    /**
     * Executes search and returns search results with default sorting by relevance. All fields are returned.
     * @see #search(Query,Sort)
     */
    public SearchResultList search(Query query) throws IOException {
        return search(query,(Sort)null);
    }

    /**
     * Executes search and feeds search results to the supplied {@link SearchResultCollector}.
     * <p><em>Note:</em> Scores of returned documents are <em>raw scores</em> and <b>not</b> normalized to <code>0.0&lt;score&lt;=1.0</code>.
     * @param collector a class implementing interface {@link SearchResultCollector}
     * @param query the previously constructed query
     * @param loadXml return the XML blob of search results
     * @param fieldsToLoad a collection of field names that should be made available. <code>null</code> to return all fields <b>and</b> XML.
     */
    public void search(SearchResultCollector collector, Query query, boolean loadXml, Collection<String> fieldsToLoad) throws IOException {
        cache.cleanupCache();

        log.info("Collecting results for query={"+query.toString(IndexConstants.FIELDNAME_CONTENT)+"}");

        Searcher searcher=index.newSearcher();
        LuceneHitCollector coll=new LuceneHitCollector(
            collectorBufferSize,collector,
            cache.config,searcher,
            cache.getFieldSelector(loadXml,fieldsToLoad)
        );
        try {
            searcher.search(query,coll);
            coll.flushBuffer();
        } catch (LuceneHitCollector.StopException se) {
            // we are finished
        } catch (RuntimeException e) {
            Throwable t=e.getCause();
            if (t instanceof IOException) throw (IOException)t; else throw e;
        }
    }

    /**
     * Executes search and feeds search results to the supplied {@link SearchResultCollector}.
     * This version uses VARARGs to list field names to return.
     * @see #search(SearchResultCollector,Query,boolean,Collection)
     */
    public void search(SearchResultCollector collector, Query query, boolean loadXml, String... fieldName) throws IOException {
        search(collector,query,loadXml,Arrays.asList(fieldName));
    }

    /**
     * Executes search and feeds search results to the supplied {@link SearchResultCollector}.
     * All fields are returned.
     * @see #search(SearchResultCollector,Query,boolean,Collection)
     */
    public void search(SearchResultCollector collector, Query query) throws IOException {
        search(collector,query,true,(Collection<String>)null);
    }

    /**
     * Sets the buffer size of search methods using a {@link SearchResultCollector}.
     * The buffer is filled with document ids and scores during search and when full, all notifications to the
     * collector are done in a bulk operation which fetches the document fields from index. Fetching document fields
     * on every found document id degrades performance by a order of magnitude because of heavy I/O.
     * <p>Default size is <code>32768</code> which is suitable for most use cases. If you exspect
     * large counts of documents to be processed and you have a lot of memory increase this value.
     * The buffer is not needed to buffer the whole documents, it only contains ids and scores, so large values are ok.
     * @see #search(SearchResultCollector,Query,boolean,Collection)
     */
    public void setCollectorBufferSize(int bufferSize) {
        if (bufferSize<1) throw new IllegalArgumentException("Buffer must >1");
        this.collectorBufferSize=bufferSize;
    }

    /**
     * Reads one document from index using its identifier. The score of the returned {@link SearchResultItem} will be set to <code>1.0</code>.
     */
    public SearchResultItem getDocument(String identifier, boolean loadXml, Collection<String> fieldsToLoad) throws IOException {
        cache.cleanupCache();

        IndexReader reader=index.getIndexReader();

        TermDocs td=reader.termDocs(new Term(IndexConstants.FIELDNAME_IDENTIFIER,identifier));
        try {
            if (td.next()) {
                Document doc=reader.document(td.doc(), cache.getFieldSelector(loadXml,fieldsToLoad));
                if (td.next()) log.warn("There are multiple documents for identifier '"+identifier+"' in index '"+index.id+"'. Only first one returned!");
                return new SearchResultItem(cache.config, 1.0f, doc);
            } else return null;
        } finally {
            td.close();
        }
    }

    /**
     * Reads one document from index using its identifier. This version uses VARARGs to list field names to return.
     * @see #getDocument(String,boolean,Collection)
     */
    public SearchResultItem getDocument(String identifier, boolean loadXml, String... fieldName) throws IOException {
        return getDocument(identifier,loadXml,Arrays.asList(fieldName));
    }

    /**
     * Reads one document from index using its identifier. Returns all stored fields and XML.
     * @see #getDocument(String,boolean,Collection)
     */
    public SearchResultItem getDocument(String identifier) throws IOException {
        return getDocument(identifier,true,(Collection<String>)null);
    }

    /**
     * Return the underlying configuration
     */
    public Config getConfig() {
        return cache.config;
    }

    /**
     * Return the underlying index configuration
     */
    public IndexConfig getIndexConfig() {
        return index;
    }

    /**
     * Stores a query for later use in the cache. The query can be retrieved again using the returned string, which is an opaque
     * hash code.
     * <p>This function can be used to store a query a user generated in your web interface for later use,
     * e.g. if you generate a query by the web service interface of panFMP and want to use it later in a Java Servlet for generating a
     * geographical map of document locations using the Collector API. In this case you store the query using the web service and supply the hash code
     * as a parameter to the servlet.
     * @param query the query to store.
     * @return a hash code identifying the query.
     * @see #readStoredQuery
     */
    public String storeQuery(Query query) {
        return cache.storeQuery(query);
    }

    /**
     * Reads a query identified by a hash code from the cache.
     * @param hash the hash code returned by {@link #storeQuery}.
     * @return the stored query or <code>null</code> if the hash code does not specify a query. This method may return <code>null</code>,
     * even if a query identified by this hash existed in the past, when the query store is full and older (LRU) queries are removed by
     * previous {@link #storeQuery} calls.
     * @see #storeQuery
     */
    public Query readStoredQuery(String hash) {
        return cache.readStoredQuery(hash);
    }

    /**
     * Override in a subclass to use another query parser. This version uses the Lucene one with {@link QueryParser#AND_OPERATOR} as default operator.
     * @param fieldName the expanded field name of the field that is used as default when creating queries (when no prefix-notation is used)
     * @param query the query string entered by the user
     * @see QueryParser
     */
    protected Query parseQuery(String fieldName, String query) throws ParseException {
        QueryParser p=new QueryParser(fieldName, cache.config.getAnalyzer());
        p.setDefaultOperator(QueryParser.AND_OPERATOR);
        Query q=p.parse(query);
        return q;
    }

    // internal method for suggest algorithm
    private Term findLastTerm(Query q) {
        Term last=null;
        if (q instanceof BooleanQuery) {
            BooleanClause[] clauses=((BooleanQuery)q).getClauses();
            if (clauses.length>0) last=findLastTerm(clauses[clauses.length-1].getQuery());
        } else if (q instanceof FieldCheckingQuery) {
            last=findLastTerm(((FieldCheckingQuery)q).getQuery());
        } else if (q instanceof TermQuery) {
            last=((TermQuery)q).getTerm();
        } else if (q instanceof PhraseQuery) {
            Term[] terms=((PhraseQuery)q).getTerms();
            if (terms.length>0) last=terms[terms.length-1];
            else last=null;
        } else last=null; // disable autocomplete if last term is not supported type
        return last;
    }

    protected LuceneCache cache=null;
    protected IndexConfig index=null;

    protected int collectorBufferSize=32768;
}