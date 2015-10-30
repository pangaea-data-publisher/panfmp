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

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Constructor;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Locale;
import java.util.Set;
import java.util.zip.DataFormatException;

import javax.xml.transform.stream.StreamSource;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.CompressionTools;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DocumentStoredFieldVisitor;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.Version;

import de.pangaea.metadataportal.config.HarvesterConfig;
import de.pangaea.metadataportal.processor.ElasticsearchConnection;

/**
 * This harvester supports replication XML contents from a legacy
 * <b>panFMP</b> 1.x installation. It is possible to replicate indexes with a
 * different XML schema (by applying a transformation on the harvested XML
 * content) or replicate only sub-sets of other indexes, based on a query
 * string.
 * <p>
 * This harvester supports the following additional <b>harvester properties</b>:
 * <ul>
 * <li><code>indexDir</code>: file system directory with the old panFMP v1 index</li>
 * <li><code>query</code>: query that matches all documents to harvest (default: all documents)</li>
 * <li><code>analyzerClass</code>: class name of {@link Analyzer} to use for the above query string (default: "org.apache.lucene.analysis.standard.StandardAnalyzer")</li>
 * <li><code>queryParserClass</code>: class name of {@link QueryParser} to use for the above query string (default: "org.apache.lucene.queryparser.classic.QueryParser")</li>
 * <li><code>defaultQueryParserOperator</code>: default operator when parsing above query string (AND/OR) (default: "AND")</li>
 * <li><code>identifierPrefix</code>: This prefix is added in front of all identifiers from the foreign index (default: "")</li>
 * <li><code>luceneMatchVersion</code>: The {@link Version} constant passed to the analyzer and query parser of the foreign index (default is {@link Version#LUCENE_CURRENT})</li>
 * </ul>
 * 
 * @author Uwe Schindler
 */
public class PanFMP1IndexHarvester extends SingleFileEntitiesHarvester {
  
  private final String identifierPrefix;
  private final Query query;
  private final String queryInfo;
  
  private DirectoryReader reader = null;
  private Directory indexDir = null;
  
  // legacy constants from panFMP 1.x
  private static final String FIELD_PREFIX = "internal-";
  public static final String FIELDNAME_CONTENT = "textcontent"; // default field for query parser
  public static final String FIELDNAME_IDENTIFIER = (FIELD_PREFIX + "identifier").intern();
  public static final String FIELDNAME_DATESTAMP = (FIELD_PREFIX + "datestamp").intern();
  public static final String FIELDNAME_XML = (FIELD_PREFIX + "xml").intern();
  
  public PanFMP1IndexHarvester(HarvesterConfig iconfig) throws Exception {
    super(iconfig);
    identifierPrefix = iconfig.properties.getProperty("identifierPrefix", "");
    
    String qstr = iconfig.properties.getProperty("query");
    if (qstr == null || qstr.length() == 0) {
      queryInfo = "all documents";
      query = new MatchAllDocsQuery();
    } else {
      queryInfo = "documents matching query [" + qstr + "]";

      @SuppressWarnings("deprecation")
      final Version luceneMatchVersion = Version.parseLeniently(iconfig.properties.getProperty("luceneMatchVersion",
          Version.LUCENE_CURRENT.toString()));
      
      // analyzer
      final String anaCls = iconfig.properties.getProperty("analyzerClass", StandardAnalyzer.class.getName());
      final Class<? extends Analyzer> anaClass = Class.forName(anaCls).asSubclass(Analyzer.class);
      Analyzer ana;
      try {
        ana = anaClass.getConstructor(Version.class).newInstance(luceneMatchVersion);
      } catch (NoSuchMethodException nsme1) {
        try {
          ana = anaClass.getConstructor().newInstance();
        } catch (NoSuchMethodException nsme2) {
          throw new IllegalArgumentException(anaClass.getName() + " does not have a public matchVersion or no-arg constructor");
        }
      }
      
      // load query parser
      final Class<?> c = Class.forName(iconfig.properties
          .getProperty("queryParserClass", QueryParser.class.getName()));
      Class<? extends QueryParser> queryParserClass = c
          .asSubclass(QueryParser.class);
      Constructor<? extends QueryParser> queryParserConstructor = queryParserClass
          .getConstructor(Version.class, String.class, Analyzer.class);
      
      // default operator for query parser
      final String operator = iconfig.properties.getProperty(
          "defaultQueryParserOperator", "AND").toUpperCase(Locale.ROOT);
      
      final QueryParser.Operator defaultQueryParserOperator;
      if ("AND".equals(operator)) {
        defaultQueryParserOperator = QueryParser.AND_OPERATOR;
      } else if ("OR".equals(operator)) {
        defaultQueryParserOperator = QueryParser.OR_OPERATOR;
      } else {
        throw new IllegalArgumentException("Search property 'defaultQueryParserOperator' is not 'AND'/'OR'");
      }
      
      // create QP
      final QueryParser qp = queryParserConstructor.newInstance(luceneMatchVersion, FIELDNAME_CONTENT, ana);
      qp.setDefaultOperator(defaultQueryParserOperator);
      query = qp.parse(qstr);
    }
  }

  @Override
  public void open(ElasticsearchConnection es, String targetIndex) throws Exception {
    super.open(es, targetIndex);
    
    String d = iconfig.properties.getProperty("indexDir");
    if (d == null) {
      throw new IllegalArgumentException("Missing index directory path (property \"indexDir\")");
    }
    final Path dir = iconfig.root.makePathAbsolute(d);
    
    log.info("Opening index in directory '" + dir + "' for harvesting " + queryInfo + "...");
    indexDir = FSDirectory.open(dir.toFile()); // TODO: change this in Lucene 5!
    reader = DirectoryReader.open(indexDir);
  }
  
  @Override
  public void close(boolean cleanShutdown) throws Exception {
    IOUtils.closeWhileHandlingException(reader, indexDir);
    reader = null;
    indexDir = null;
    super.close(cleanShutdown);
  }
  
  @Override
  public void harvest() throws Exception {
    if (reader == null) throw new IllegalStateException(
        "Harvester was not opened!");
    try {
      new IndexSearcher(reader).search(query, new Collector() {
        private AtomicReader currReader = null;
        
        @Override
        public void setScorer(final Scorer scorer) {}
        
        @Override
        public void setNextReader(final AtomicReaderContext ctx)
            throws IOException {
          this.currReader = ctx.reader();
        }
        
        @Override
        public void collect(final int doc) throws IOException {
          DocumentStoredFieldVisitor vis = new DocumentStoredFieldVisitor(FIELDNAME_IDENTIFIER,
              FIELDNAME_DATESTAMP, FIELDNAME_XML);
          currReader.document(doc, vis);
          Document ldoc = vis.getDocument();
          try {
            addLuceneDocument(ldoc);
          } catch (IOException ioe) {
            throw ioe;
          } catch (Exception e) {
            throw new RuntimeException("###collectException###", e);
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
      if ("###collectException###".equals(e.getMessage())) throw (Exception) e
          .getCause();
      // rethrow original
      throw e;
    }
  }
  
  @Override
  protected void enumerateValidHarvesterPropertyNames(Set<String> props) {
    super.enumerateValidHarvesterPropertyNames(props);
    props.addAll(Arrays.asList("indexDir", "query", "luceneMatchVersion",
        "analyzerClass", "queryParserClass", "defaultQueryParserOperator", "identifierPrefix"));
  }
  
  void addLuceneDocument(Document ldoc) throws Exception {
    // read identifier
    String identifier = ldoc.get(FIELDNAME_IDENTIFIER);
    if (identifier == null) {
      log.warn("Document without identifier, ignoring.");
      return;
    }
    // normalize name with prefix
    identifier = identifierPrefix + identifier;
    // try to read date stamp
    long datestamp = -1L;
    // try to read date stamp
    try {
      final IndexableField fld = ldoc
          .getField(FIELDNAME_DATESTAMP);
      datestamp = fld.numericValue().longValue();
    } catch (NullPointerException npe) {
      log.warn("Datestamp of document '" + identifier
          + "' is invalid - Ignoring datestamp.");
      datestamp = -1L;
    } catch (NumberFormatException ne) {
      log.warn("Datestamp of document '" + identifier + "' is invalid: "
          + ne.getMessage() + " - Ignoring datestamp.");
      datestamp = -1L;
    }
    // read XML
    if (isDocumentOutdated(datestamp)) {
      try {
        final IndexableField fld = ldoc.getField(FIELDNAME_XML);
        BytesRef bytes = fld.binaryValue();
        String xml = (bytes != null) ? CompressionTools.decompressString(bytes)
            : fld.stringValue();
        if (xml != null) {
          addDocument(identifier, datestamp, new StreamSource(new StringReader(
              xml), identifier));
        } else {
          log.warn("Document '" + identifier
              + "' has no XML contents, ignoring.");
        }
      } catch (DataFormatException de) {
        log.warn("Document '" + identifier
            + "' has invalid compressed XML contents, ignoring.");
      }
    } else {
      addDocument(identifier, datestamp, null);
    }
  }
  
}