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
import de.pangaea.metadataportal.utils.BooleanParser;
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

		boolean returnXML=BooleanParser.parseBoolean(service.getConfig().searchProperties.getProperty("returnXML","true"));
		boolean returnStoredFields=BooleanParser.parseBoolean(service.getConfig().searchProperties.getProperty("returnStoredFields","true"));

		Query q=req.getLuceneQuery(service);
		Sort sort=null;
		if (req.sortFieldName!=null && req.sortReverse!=null) sort=service.newSort(service.newFieldBasedSort(req.sortFieldName,req.sortReverse));

		SearchResultList res=service.search(q, sort, returnXML, returnStoredFields ? null : Collections.<String>emptySet() );
		SearchResponse resp=new SearchResponse(res, offset, count, returnXML, returnStoredFields);
		return resp;
	}

	public SearchResponse defaultMoreLikeThis(String indexName, String identifier, int offset, int count) throws Exception {
		SearchService service=new SearchService(cfgFile,indexName);

		boolean returnXML=BooleanParser.parseBoolean(service.getConfig().searchProperties.getProperty("returnXML","true"));
		boolean returnStoredFields=BooleanParser.parseBoolean(service.getConfig().searchProperties.getProperty("returnStoredFields","true"));

		SearchResultList res=service.search(service.newDefaultMoreLikeThisQuery(identifier), returnXML, returnStoredFields ? null : Collections.<String>emptySet() );
		SearchResponse resp=new SearchResponse(res, offset, count, returnXML, returnStoredFields);
		return resp;
	}

	public SearchResponse fieldedMoreLikeThis(String indexName, String identifier, String fieldName, int offset, int count) throws Exception {
		SearchService service=new SearchService(cfgFile,indexName);

		boolean returnXML=BooleanParser.parseBoolean(service.getConfig().searchProperties.getProperty("returnXML","true"));
		boolean returnStoredFields=BooleanParser.parseBoolean(service.getConfig().searchProperties.getProperty("returnStoredFields","true"));

		SearchResultList res=service.search(service.newFieldedMoreLikeThisQuery(identifier,fieldName), returnXML, returnStoredFields ? null : Collections.<String>emptySet() );
		SearchResponse resp=new SearchResponse(res, offset, count, returnXML, returnStoredFields);
		return resp;
	}

	public SearchResponseItem getDocument(String indexName, String identifier) throws Exception {
		SearchService service=new SearchService(cfgFile,indexName);
		SearchResultItem i=service.getDocument(identifier);
		return (i==null) ? null : new SearchResponseItem(i,true,true);
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

	public String storeQuery(SearchRequest req) throws Exception {
		SearchService service=new SearchService(cfgFile,req.indexName);
		return service.storeQuery(req.getLuceneQuery(service));
	}

	private String cfgFile;

	/* Config options:
		<returnXML>true</returnXML>
		<returnStoredFields>true</returnStoredFields>
	*/
}