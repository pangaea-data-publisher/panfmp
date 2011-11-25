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

package de.pangaea.metadataportal.harvester;

import java.io.*;
import java.util.*;
import java.lang.reflect.Constructor;
import javax.xml.transform.stream.StreamSource;
import java.util.zip.DataFormatException;

import de.pangaea.metadataportal.utils.*;
import de.pangaea.metadataportal.config.*;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.CompressionTools;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SetBasedFieldSelector;
import org.apache.lucene.search.*;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.Version;
import org.apache.lucene.queryParser.QueryParser;

/**
 * This harvester supports replication XML contents from an foreign <b>panFMP</b> installation.
 * It is possible to replicate indexes with a different XML schema (by applying a transformation on
 * the harvested XML content) or replicate only sub-sets of other indexes, based on a query string.
 * <p>This harvester supports the following additional <b>harvester properties</b>:<ul>
 * <li><code>indexDir</code>: file system directory with foreign index</li>
 * <li><code>query</code>: query that matches all documents to harvest (default: all documents)</li>
 * <li><code>analyzerClass</code>: class name of {@link Analyzer} to use for the above query string (default is the one from panFMP's global config;
 * stop words are always taken from the global config, for simplicity you should not use any of them in your query string)</li>
 * <li><code>queryParserClass</code>: class name of {@link QueryParser} to use for the above query string (default: "org.apache.lucene.queryParser.QueryParser")</li>
 * <li><code>defaultQueryParserOperator</code>: default operator when parsing above query string (AND/OR) (default: "AND")</li>
 * <li><code>identifierPrefix</code>: This prefix is added in front of all identifiers from the foreign index (default: "")</li>
 * <li><code>indexVersionCompatibility</code>: The {@link Version} constant passed to the analyzer and query parser of the foreign
 * index (default is the one from panFMP's global config)</li>
 * </ul>
 * @author Uwe Schindler
 */
public class ExternalIndexHarvester extends SingleFileEntitiesHarvester {

	// Class members
	private String identifierPrefix="";
	private IndexReader reader=null;
	private Directory indexDir=null;
	private Query query=null;
	
	private static final SetBasedFieldSelector FIELD_SELECTOR=new SetBasedFieldSelector(
		new HashSet<String>(Arrays.asList(IndexConstants.FIELDNAME_IDENTIFIER,IndexConstants.FIELDNAME_DATESTAMP)),
		Collections.singleton(IndexConstants.FIELDNAME_XML)
	);

	@Override
	public void open(SingleIndexConfig iconfig) throws Exception {
		super.open(iconfig);

		identifierPrefix=iconfig.harvesterProperties.getProperty("identifierPrefix","");

		String s=iconfig.harvesterProperties.getProperty("indexDir");
		if (s==null) throw new IllegalArgumentException("Missing index directory path (property \"indexDir\")");
		File dir=new File(iconfig.parent.makePathAbsolute(s,false));
		
		String info,qstr=iconfig.harvesterProperties.getProperty("query");
		if (qstr==null || qstr.length()==0) {
			info="all documents";
			query=new MatchAllDocsQuery();
		} else {
			info="documents matching query ["+qstr+"]";
			// save original analyzer class
			final Class<? extends Analyzer> savedAnalyzerClass=iconfig.parent.getAnalyzer().getClass();
			final Version savedIndexVersionCompatibility=iconfig.parent.indexVersionCompatibility;
			try {
				// load query parser (code borrowed from SearchService)
				final Class<?> c=Class.forName(iconfig.harvesterProperties.getProperty("queryParserClass",QueryParser.class.getName()));
				Class<? extends QueryParser> queryParserClass=c.asSubclass(QueryParser.class);
				Constructor<? extends QueryParser> queryParserConstructor=queryParserClass.getConstructor(Version.class,String.class,Analyzer.class);
				// default operator for query parser
				final String operator=iconfig.harvesterProperties.getProperty("defaultQueryParserOperator","AND").toUpperCase(Locale.ENGLISH);
				final QueryParser.Operator defaultQueryParserOperator;
				if ("AND".equals(operator)) defaultQueryParserOperator=QueryParser.AND_OPERATOR;
				else if ("OR".equals(operator)) defaultQueryParserOperator=QueryParser.OR_OPERATOR;
				else throw new IllegalArgumentException("Search property 'defaultQueryParserOperator' is not 'AND'/'OR'");
				// analyzer
				String anaCls=iconfig.harvesterProperties.getProperty("analyzerClass");
				if (anaCls!=null) iconfig.parent.setAnalyzerClass(Class.forName(anaCls).asSubclass(Analyzer.class));
				// version
				String v=iconfig.harvesterProperties.getProperty("indexVersionCompatibility",iconfig.parent.indexVersionCompatibility.toString()).toUpperCase(Locale.ENGLISH);
				try {
					iconfig.parent.indexVersionCompatibility=Version.valueOf(v);
				} catch (IllegalArgumentException iae) {
					throw new IllegalArgumentException("Invalid value '"+v+"' for property 'indexVersionCompatibility', valid ones are: "+
						Arrays.toString(Version.values()));
				}
				// create QP
				QueryParser qp=queryParserConstructor.newInstance(iconfig.parent.indexVersionCompatibility, IndexConstants.FIELDNAME_CONTENT, iconfig.parent.getAnalyzer());
				qp.setDefaultOperator(defaultQueryParserOperator);
				query=qp.parse(qstr);
			} finally {
				iconfig.parent.indexVersionCompatibility=savedIndexVersionCompatibility;
				iconfig.parent.setAnalyzerClass(savedAnalyzerClass);
			}
		}

		log.info("Opening index in directory '"+dir+"' for harvesting "+info+"...");
		indexDir=iconfig.parent.indexDirImplementation.getDirectory(dir);
		reader=IndexReader.open(indexDir,true);
	}

	@Override
	public void close(boolean cleanShutdown) throws Exception {
		if (reader!=null) reader.close();
		reader=null;
		if (indexDir!=null) indexDir.close();
		indexDir=null;
		query=null;
		super.close(cleanShutdown);
	}

	@Override
	public void harvest() throws Exception {
		if (reader==null) throw new IllegalStateException("Harvester was not opened!");
		try {
			new IndexSearcher(reader).search(query, new Collector() {
				private IndexReader currReader=null;
			
				@Override
				public void setScorer(final Scorer scorer) {
				}
				
				@Override
				public void setNextReader(final IndexReader reader, final int docBase) throws IOException {
					this.currReader=reader;
				}
				
				@Override
				public void collect(final int doc) throws IOException {
					Document ldoc=currReader.document(doc,FIELD_SELECTOR);
					try {
						addLuceneDocument(ldoc);
					} catch (IOException ioe) {
						throw ioe;
					} catch (Exception e) {
						throw new RuntimeException("###collectException###",e);
					}
				}
				
				@Override
				public boolean acceptsDocsOutOfOrder() {
					// should be ordered for performance
					return false;
				}
			});
		} catch (RuntimeException e) {
			// throw cause, if the exception is wrapped
			if ("###collectException###".equals(e.getMessage()))
				throw (Exception)e.getCause();
			// rethrow original
			throw e;
		}
	}
  
	@Override
	protected void enumerateValidHarvesterPropertyNames(Set<String> props) {
		super.enumerateValidHarvesterPropertyNames(props);
		props.addAll(Arrays.<String>asList(
			"indexDir",
			"query",
			"queryParserClass",
			"defaultQueryParserOperator",
			"analyzerClass",
			"indexVersionCompatibility",
			"identifierPrefix"
		));
	}

	private void addLuceneDocument(Document ldoc) throws Exception {
		// read identifier
		String identifier=ldoc.get(IndexConstants.FIELDNAME_IDENTIFIER);
		if (identifier==null) {
			log.warn("Document without identifier, ignoring.");
			return;
		}
		// normalize name with prefix
		identifier=identifierPrefix+identifier;
		// try to read date stamp
		long datestamp=-1L;
		// try to read date stamp
		try {
			final Fieldable fld=ldoc.getFieldable(IndexConstants.FIELDNAME_DATESTAMP);
			if (fld instanceof NumericField) {
				datestamp=((NumericField)fld).getNumericValue().longValue();
			} else if (fld!=null) {
				datestamp=NumericUtils.prefixCodedToLong(fld.stringValue());
			}
		} catch (NumberFormatException ne) {
			log.warn("Datestamp of document '"+identifier+"' is invalid: "+ne.getMessage()+" - Ignoring datestamp.");
			datestamp=-1L;
		}
		// read XML
		if (isDocumentOutdated(datestamp)) {
			try {
				final Fieldable fld=ldoc.getFieldable(IndexConstants.FIELDNAME_XML);
				String xml=null;
        if (fld!=null) xml=fld.isBinary() ? CompressionTools.decompressString(fld.getBinaryValue()) : fld.stringValue();
				if (xml!=null) {
					addDocument(identifier,datestamp,new StreamSource(new StringReader(xml),identifier));
				} else {
					log.warn("Document '"+identifier+"' has no XML contents, ignoring.");
				}
			} catch (DataFormatException de) {
				log.warn("Document '"+identifier+"' has invalid compressed XML contents, ignoring.");
			}
		} else {
			addDocument(identifier,datestamp,null);
		}
	}

}