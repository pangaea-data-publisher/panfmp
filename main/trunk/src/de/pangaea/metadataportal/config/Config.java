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

package de.pangaea.metadataportal.config;

import java.util.*;
import java.io.*;
import java.net.URL;
import java.net.MalformedURLException;
import de.pangaea.metadataportal.utils.*;
import org.apache.commons.digester.*;
import java.lang.reflect.Constructor;
import org.xml.sax.*;
import org.xml.sax.helpers.AttributesImpl;
import javax.xml.XMLConstants;
import javax.xml.namespace.QName;
import javax.xml.transform.*;
import javax.xml.transform.sax.*;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.*;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.store.*;

/**
 * Main panFMP configuration class. It loads the configuration from a XML file.
 * @author Uwe Schindler
 */
public class Config {

	protected static org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(Config.class);

	// in configMode!=HARVESTER we leave out Schemas and XSLT to load config faster!
	public Config(String file, ConfigMode configMode) throws Exception {
		this.file=file;
		this.configMode=configMode;

		String version=de.pangaea.metadataportal.Package.getFullPackageDescription();
		if (version!=null) log.info(version);
		de.pangaea.metadataportal.Package.checkMinimumRequirements();

		setAnalyzerClass(StandardAnalyzer.class);
		try {
			final Class[] DIGSTRING_PARAMS=new Class<?>[]{ExtendedDigester.class,String.class};

			dig=new ExtendedDigester();
			dig.setNamespaceAware(true);
			dig.setValidating(false);
			dig.setXIncludeAware(true);
			dig.setRulesWithInvalidElementCheck( new ExtendedBaseRules() );
			dig.setRuleNamespaceURI("urn:java:"+getClass().getName());

			dig.addDoNothing("config");

			// *** METADATA definition ***
			dig.addDoNothing("config/metadata");

			// variables
			dig.addDoNothing("config/metadata/variables");

			dig.addObjectCreate("config/metadata/variables/variable", VariableConfig.class);
			dig.addSetNext("config/metadata/variables/variable", "addVariable");
			dig.addCallMethod("config/metadata/variables/variable","setName", 2, DIGSTRING_PARAMS);
			dig.addObjectParam("config/metadata/variables/variable", 0, dig);
			dig.addCallParam("config/metadata/variables/variable", 1, "name");
			if (configMode==ConfigMode.HARVESTING) {
				dig.addCallMethod("config/metadata/variables/variable","setXPath", 2, DIGSTRING_PARAMS);
				dig.addObjectParam("config/metadata/variables/variable", 0, dig);
				dig.addCallParam("config/metadata/variables/variable", 1);
			}

			dig.addObjectCreate("config/metadata/variables/variable-template", VariableConfig.class);
			dig.addSetNext("config/metadata/variables/variable-template", "addVariable");
			dig.addCallMethod("config/metadata/variables/variable-template","setName", 2, DIGSTRING_PARAMS);
			dig.addObjectParam("config/metadata/variables/variable-template", 0, dig);
			dig.addCallParam("config/metadata/variables/variable-template", 1, "name");
			dig.addRule("config/metadata/variables/variable-template", (configMode==ConfigMode.HARVESTING) ? new TemplateSaxRule() : SaxRule.emptyRule());

			// filters
			dig.addCallMethod("config/metadata/filters", "setFilterDefault", 1);
			dig.addCallParam("config/metadata/filters", 0, "default");

			dig.addObjectCreate("config/metadata/filters/*", FilterConfig.class);
			dig.addSetNext("config/metadata/filters/*", "addFilter");
			if (configMode==ConfigMode.HARVESTING) {
				dig.addCallMethod("config/metadata/filters/*","setXPath", 2, DIGSTRING_PARAMS);
				dig.addObjectParam("config/metadata/filters/*", 0, dig);
				dig.addCallParam("config/metadata/filters/*", 1);
			}

			// fields
			dig.addDoNothing("config/metadata/fields");

			dig.addObjectCreate("config/metadata/fields/field", FieldConfig.class);
			dig.addSetNext("config/metadata/fields/field", "addField");
			String[] propAttr,propMapping;
			SetPropertiesRule r=new SetPropertiesRule(
				propAttr   =new String[]{"lucenestorage", "luceneindexed", "lucenetermvectors", "datatype"},
				propMapping=new String[]{"storage",       "indexed",       "termVectors",       "dataType"}
			);
			r.setIgnoreMissingProperty(false);
			dig.addRule("config/metadata/fields/field",r);
			if (configMode==ConfigMode.HARVESTING) {
				dig.addCallMethod("config/metadata/fields/field","setXPath", 2, DIGSTRING_PARAMS);
				dig.addObjectParam("config/metadata/fields/field", 0, dig);
				dig.addCallParam("config/metadata/fields/field", 1);
			}

			dig.addObjectCreate("config/metadata/fields/field-template", FieldConfig.class);
			dig.addSetNext("config/metadata/fields/field-template", "addField");
			r=new SetPropertiesRule(propAttr,propMapping);
			r.setIgnoreMissingProperty(false);
			dig.addRule("config/metadata/fields/field-template",r);
			dig.addRule("config/metadata/fields/field-template", (configMode==ConfigMode.HARVESTING) ? new TemplateSaxRule() : SaxRule.emptyRule());

			// default field
			dig.addCallMethod("config/metadata/fields/default", "setDefaultField", 2);
			dig.addCallParam("config/metadata/fields/default", 0, "lucenetermvectors");
			dig.addCallParam("config/metadata/fields/default", 1);

			// document boost
			dig.addCallMethod("config/metadata/documentBoost", "setDocumentBoost", 0);

			// transform
			/*dig.addRule("config/metadata/transformBeforeXPath", (configMode==ConfigMode.HARVESTING) ? new IndexConfigTransformerSaxRule() : SaxRule.emptyRule());*/

			// schema
			dig.addDoNothing("config/metadata/schema");
			dig.addCallMethod("config/metadata/schema/url", "setSchema", 2);
			dig.addCallParam("config/metadata/schema/url", 0, "namespace");
			dig.addCallParam("config/metadata/schema/url", 1);
			dig.addCallMethod("config/metadata/schema/haltOnError", "setHaltOnSchemaError", 0);
			dig.addCallMethod("config/metadata/schema/augmentation", "setAugmentation", 0);

			// *** Trie parameters ***/
			dig.addCallMethod("config/triePrecisionStep", "setTriePrecisionStep", 0, new Class<?>[]{int.class});
			
			// *** Directory Impl ***/
			dig.addCallMethod("config/indexDirImplementation", "setIndexDirImplementation", 0);
			
			// *** ANALYZER ***
			dig.addDoNothing("config/analyzer");
			dig.addCallMethod("config/analyzer/class", "setAnalyzer", 0);
			dig.addCallMethod("config/analyzer/importEnglishStopWords", "importEnglishStopWords", 0);
			dig.addCallMethod("config/analyzer/addStopWords", "addStopWords", 0);

			// *** INDEX CONFIG ***
			dig.addDoNothing("config/indexes");

			// SingleIndex
			dig.addFactoryCreate("config/indexes/index", new AbstractObjectCreationFactory() {
				@Override
				public Object createObject(Attributes attributes) {
					return new SingleIndexConfig(Config.this);
				}
			});
			dig.addSetNext("config/indexes/index", "addIndex");
			dig.addCallMethod("config/indexes/index","setId", 1);
			dig.addCallParam("config/indexes/index", 0, "id");

			dig.addCallMethod("config/indexes/index/displayName","setDisplayName",0);
			dig.addCallMethod("config/indexes/index/indexDir","setIndexDir",0);
			dig.addCallMethod("config/indexes/index/harvesterClass","setHarvesterClass",0);

			dig.addRule("config/indexes/index/transform", (configMode==ConfigMode.HARVESTING) ? new IndexConfigTransformerSaxRule() : SaxRule.emptyRule());

			dig.addDoNothing("config/indexes/index/harvesterProperties");
			dig.addCallMethod("config/indexes/index/harvesterProperties/*","addHarvesterProperty",0);

			// VirtualIndex
			dig.addFactoryCreate("config/indexes/virtualIndex", new AbstractObjectCreationFactory() {
				@Override
				public Object createObject(Attributes attributes) {
					return new VirtualIndexConfig(Config.this);
				}
			});
			dig.addSetNext("config/indexes/virtualIndex", "addIndex");
			dig.addCallMethod("config/indexes/virtualIndex","setId", 1);
			dig.addCallParam("config/indexes/virtualIndex", 0, "id");

			dig.addCallMethod("config/indexes/virtualIndex/displayName","setDisplayName",0);
			dig.addCallMethod("config/indexes/virtualIndex/index","addIndex",1);
			dig.addCallParam("config/indexes/virtualIndex/index", 0, "ref");

			// *** SEARCH PROPERTIES ***
			dig.addDoNothing("config/search");
			dig.addCallMethod("config/search/*","addSearchProperty",0);

			// *** GLOBAL HARVESTER PROPERTIES ***
			dig.addDoNothing("config/globalHarvesterProperties");
			dig.addCallMethod("config/globalHarvesterProperties/*","addGlobalHarvesterProperty",0);

			// parse config
			try {
				dig.push(this);
				dig.parse(new File(file));
			} catch (org.xml.sax.SAXException saxe) {
				// throw the real Exception not the digester one
				if (saxe.getException()!=null) throw saxe.getException();
				else throw saxe;
			}
		} finally {
			dig=null;
		}

		// *** After loading do final checks ***
		// consistency in indexes:
		for (IndexConfig iconf : indexes.values()) iconf.check();

		// init boolean query constraints and properties
		final String mcc=searchProperties.getProperty("maxClauseCount",Integer.toString(DEFAULT_MAX_CLAUSE_COUNT));
		BooleanQuery.setMaxClauseCount("inf".equalsIgnoreCase(mcc) ? Integer.MAX_VALUE : Integer.parseInt(mcc));
		BooleanQuery.setAllowDocsOutOfOrder(true);
		
		// cleanup
		templatesCache.clear();
	}

	/** makes the given local filesystem path absolute and resolve it relative to config directory **/
	public final String makePathAbsolute(String file) throws java.io.IOException {
		return makePathAbsolute(file,false);
	}

	/** makes the given local filesystem path or URL absolute and resolve it relative to config directory (if local) **/
	public String makePathAbsolute(String file, boolean allowURL) throws java.io.IOException {
		try {
			if (allowURL) {
				return new URL(file).toString();
			} else  {
				new URL(file);
				throw new IllegalArgumentException("You can only use local file system pathes instead of '"+file+"'.");
			}
		} catch (MalformedURLException me) {
			File f=new File(file);
			if (f.isAbsolute()) return f.getCanonicalPath();
			else return new File(new File(this.file).getAbsoluteFile().getParentFile(),file).getCanonicalPath();
		}
	}

	public void addField(FieldConfig f) {
		if (f.name==null) throw new IllegalArgumentException("A field name is mandatory");
		if (fields.containsKey(f.name)) throw new IllegalArgumentException("A field with name '"+f.name+"' already exists!");
		if (configMode==ConfigMode.HARVESTING) {
			if (f.xPathExpr==null && f.xslt==null) throw new IllegalArgumentException("A XPath or template itsself may not be empty");
			if (f.xPathExpr!=null && f.xslt!=null) throw new IllegalArgumentException("It may not both XPath and template be defined");
			if (f.datatype==FieldConfig.DataType.XHTML && f.xslt==null) throw new IllegalArgumentException("XHTML fields may only be declared as a XSLT template (using <field-template/>)");
		}
		if (f.storage==Field.Store.NO && !f.indexed) throw new IllegalArgumentException("A field must be at least indexed and/or stored");
		if (f.termVectors!=Field.TermVector.NO && (!f.indexed || f.datatype!=FieldConfig.DataType.TOKENIZEDTEXT))
			throw new IllegalArgumentException("A field with term vectors enabled must be at least indexed and tokenized");
		if (f.defaultValue!=null && f.datatype!=FieldConfig.DataType.NUMBER && f.datatype!=FieldConfig.DataType.DATETIME)
			throw new IllegalArgumentException("A default value can only be given for NUMBER or DATETIME fields");
		if ((f.datatype==FieldConfig.DataType.XML || f.datatype==FieldConfig.DataType.XHTML) && (f.indexed || f.storage==Field.Store.NO))
			throw new IllegalArgumentException("Fields with datatype XML or XHTML must be stored, but not indexed");
		fields.put(f.name,f);
	}

	public void addVariable(VariableConfig f) {
		if (configMode!=ConfigMode.HARVESTING) return;
		if (filters.size()>0 || fields.size()>0) throw new IllegalStateException("Variables must be declared before all fields and filters!");
		if (f.name==null) throw new IllegalArgumentException("A variable name is mandatory");
		if (de.pangaea.metadataportal.harvester.XPathResolverImpl.INDEX_BUILDER_NAMESPACE.equals(f.name.getNamespaceURI()))
			throw new IllegalArgumentException("A XPath variable name may not be in the namespace for internal variables ('"+de.pangaea.metadataportal.harvester.XPathResolverImpl.INDEX_BUILDER_NAMESPACE+"')");
		if (f.xPathExpr==null && f.xslt==null) throw new IllegalArgumentException("A XPath or template itsself may not be empty");
		if (f.xPathExpr!=null && f.xslt!=null) throw new IllegalArgumentException("It may not both XPath and template be defined");
		xPathVariables.add(f);
	}

	public void addFilter(FilterConfig f) {
		if (configMode!=ConfigMode.HARVESTING) return;
		if (f.xPathExpr==null) throw new IllegalArgumentException("A filter needs an XPath expression");
		if (f.xslt!=null) throw new IllegalArgumentException("A filter may not contain a template");
		f.type=FilterConfig.FilterType.valueOf(dig.getCurrentElementName().toUpperCase());
		filters.add(f);
	}

	public void addIndex(IndexConfig i) {
		if (indexes.containsKey(i.id))
			throw new IllegalArgumentException("There is already an index with id=\""+i.id+"\" added to configuration!");
		indexes.put(i.id,i);
	}

	@PublicForDigesterUse
	@Deprecated
	public void setFilterDefault(String v) {
		if (v==null) return; // no change
		// a bit of hack, we use an empty filter to find out type :)
		FilterConfig f=new FilterConfig();
		f.setType(v);
		filterDefault=f.type;
	}

	@PublicForDigesterUse
	@Deprecated
	public void setDefaultField(String termVectors, String xpath) throws Exception {
		// Term Vectors
		if (termVectors!=null) {
			FieldConfig f=new FieldConfig();
			f.setTermVectors(termVectors);
			defaultFieldTermVectors=f.termVectors;
		}
		// XPath
		if (xpath==null) {
			defaultField=null;
			return;
		}
		xpath=xpath.trim();
		if (".".equals(xpath) || "/".equals(xpath) || "/*".equals(xpath)) {
			defaultField=null; // all fields from SAX parser
		} else {
			defaultField=new ExpressionConfig();
			defaultField.setXPath(dig,xpath);
		}
	}

	@PublicForDigesterUse
	@Deprecated
	public void setDocumentBoost(String v) throws Exception {
		if (v==null) {
			documentBoost=null;
			return;
		}
		v=v.trim();
		documentBoost=new ExpressionConfig();
		documentBoost.setXPath(dig,v);
	}

	@PublicForDigesterUse
	@Deprecated
	public void addStopWords(String stopWords) {
		for (String w : stopWords.split("[\\,\\;\\s]+")) {
			w=w.trim().toLowerCase();
			if (!"".equals(w)) luceneStopWords.add(w);
		}
	}

	@PublicForDigesterUse
	@Deprecated
	public void importEnglishStopWords(String dummy) {
		luceneStopWords.addAll(Arrays.asList(StopAnalyzer.ENGLISH_STOP_WORDS));
	}

	@PublicForDigesterUse
	@Deprecated
	public void setAnalyzer(String v) throws Exception {
		Class<?> c=Class.forName(v.trim());
		setAnalyzerClass(c.asSubclass(Analyzer.class));
	}

	public void setAnalyzerClass(Class<? extends Analyzer> c) throws Exception {
		analyzerClass=c;
		try {
			analyzerConstructor=analyzerClass.getConstructor(String[].class);
		} catch (NoSuchMethodException nsm) {
			analyzerConstructor=null;
			log.warn("The given analyzer class is not capable of assigning stop words - <stopWords> discarded!");
		}
	}

	@PublicForDigesterUse
	@Deprecated
	public void setTriePrecisionStep(int v) throws Exception {
		if (v<1 || v>64) throw new IllegalArgumentException("Invalid trie precision step [1..64].");
		triePrecisionStep=v;
	}

	@PublicForDigesterUse
	@Deprecated
	public void setIndexDirImplementation(String v) throws Exception {
		try {
			indexDirImplementation=IndexDirImplementation.valueOf(v.toUpperCase());
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException("Invalid value '"+v+"' for <cfg:indexDirImplementation>, valid ones are: "+
				Arrays.toString(IndexDirImplementation.values()));
		}
	}

	@PublicForDigesterUse
	@Deprecated
	public void addSearchProperty(String value) {
		searchProperties.setProperty(dig.getCurrentElementName(),value);
	}

	@PublicForDigesterUse
	@Deprecated
	public void addGlobalHarvesterProperty(String value) {
		globalHarvesterProperties.setProperty(dig.getCurrentElementName(),value);
	}

	public void setSchema(String namespace, String url) throws Exception {
		if (configMode!=ConfigMode.HARVESTING) return; // no schema support when search engine
		if (schema!=null) throw new SAXException("Schema URL already defined!");
		url=makePathAbsolute(url.trim(),true);
		
		if (namespace!=null) namespace=namespace.trim();
		if (namespace==null || "".equals(namespace)) namespace=XMLConstants.W3C_XML_SCHEMA_NS_URI;
		log.info("Loading XML schema in format '"+namespace+"' from URL '"+url+"'...");
		try {
			SchemaFactory fact=SchemaFactory.newInstance(namespace);
			schema=fact.newSchema(new StreamSource(url));
		} catch (IllegalArgumentException iae) {
			throw new IllegalArgumentException("Your XML installation does not support schemas in format '"+namespace+"'!");
		}
	}

	@PublicForDigesterUse
	@Deprecated
	public void setHaltOnSchemaError(String v) {
		haltOnSchemaError=BooleanParser.parseBoolean(v.trim());
	}

	@PublicForDigesterUse
	@Deprecated
	public void setAugmentation(String v) {
		validateWithAugmentation=BooleanParser.parseBoolean(v.trim());
	}

	// get configuration infos

	public Analyzer getAnalyzer() {
		try {
			if (analyzerConstructor!=null) {
				if (log.isDebugEnabled()) log.debug("Using stop words: "+luceneStopWords);

				String[] sw=new String[luceneStopWords.size()];
				sw=luceneStopWords.toArray(sw);
				return analyzerConstructor.newInstance(new Object[]{sw});
			} else
				return analyzerClass.newInstance();
		} catch (Exception e) {
			throw new RuntimeException("Error instantiating analyzer (this should never happen)!",e);
		}
	}
	
	private Templates loadTemplate(String file) throws Exception {
		file=makePathAbsolute(file,true);
		Templates templ=templatesCache.get(file);
		if (templ==null) {
			log.info("Loading XSL transformation from '"+file+"'...");
			templatesCache.put(file,templ=StaticFactories.transFactory.newTemplates(new StreamSource(file)));
		}
		return templ;
	}

	// members "the configuration"
	public final Map<String,IndexConfig> indexes=new LinkedHashMap<String,IndexConfig>();

	public final Map<String,FieldConfig> fields=new LinkedHashMap<String,FieldConfig>();
	public ExpressionConfig defaultField=null;
	public Field.TermVector defaultFieldTermVectors=Field.TermVector.NO;

	// filters
	public FilterConfig.FilterType filterDefault=FilterConfig.FilterType.ACCEPT;
	public final List<FilterConfig> filters=new ArrayList<FilterConfig>();

	// variables
	public final List<VariableConfig> xPathVariables=new ArrayList<VariableConfig>();

	// schema etc
	public Schema schema=null;
	public boolean haltOnSchemaError=false,validateWithAugmentation=true;

	// document boost
	public ExpressionConfig documentBoost=null;

	// Trie implementation
	public int triePrecisionStep=8;
	
	// Implementation of the Lucene index directory
	public IndexDirImplementation indexDirImplementation=IndexDirImplementation.getFromSystemProperty();

	/*public Templates xsltBeforeXPath=null;*/

	// Template cache
	private final Map<String,Templates> templatesCache=new WeakHashMap<String,Templates>();
	
	public final Properties searchProperties=new Properties();
	public final Properties globalHarvesterProperties=new Properties();

	public final Set<String> luceneStopWords=new HashSet<String>();
	protected Class<? extends Analyzer> analyzerClass=null;
	protected Constructor<? extends Analyzer> analyzerConstructor=null;
	
	public String file;
	private ConfigMode configMode;

	protected ExtendedDigester dig=null;

	public static final int DEFAULT_MAX_CLAUSE_COUNT = 131072;

	public static enum ConfigMode { HARVESTING,SEARCH };

	public static enum IndexDirImplementation {
		STANDARD, MMAP, NIO;
		
		public final Directory getDirectory(final File dir) throws IOException {
			switch(this) {
				case STANDARD: return new FSDirectory(dir,null);
				case MMAP: return new MMapDirectory(dir,null);
				case NIO: return new NIOFSDirectory(dir,null);
			}
			throw new Error(); // should never happen
		}
		
		public static final IndexDirImplementation getFromSystemProperty() {
			final String clazz=System.getProperty("org.apache.lucene.FSDirectory.class");
			if (clazz==null || FSDirectory.class.getName().equals(clazz)) return STANDARD;
			if (MMapDirectory.class.getName().equals(clazz)) return MMAP;
			if (NIOFSDirectory.class.getName().equals(clazz)) return NIO;
			throw new IllegalArgumentException("Invalid directory class specified in deprecated system property "+
				"'org.apache.lucene.FSDirectory.class'. Please use the new panFMP config entry <cfg:indexDirectoryType> "+
				"for specifying STANDARD, MMAP, or NIO!");
		}
	};

	// internal classes
	private abstract class TransformerSaxRule extends SaxRule {

		private TemplatesHandler th=null;

		@Override
		public void begin(String namespace, String name, Attributes attributes) throws Exception {
			if (getContentHandler()==null) {
				th=StaticFactories.transFactory.newTemplatesHandler();
				th.setSystemId(file);
				setContentHandler(th);
			}
			super.begin(namespace,name,attributes);
		}

		protected abstract void setResult(Templates t);

		@Override
		public void end(String namespace, String name) throws Exception {
			super.end(namespace,name);
			if (th!=null) setResult(th.getTemplates());
			setContentHandler(th=null);
		}

	}

	private final class IndexConfigTransformerSaxRule extends TransformerSaxRule {

		@Override
		public void begin(String namespace, String name, Attributes attributes) throws Exception {
			final String file=attributes.getValue(XMLConstants.NULL_NS_URI,"src");
			if (file!=null) {
				setResult(loadTemplate(file));
				setContentHandler(new org.xml.sax.helpers.DefaultHandler() {
					@Override
					public void startElement(String namespaceURI, String localName, String qName, Attributes atts) throws SAXException {
						throw new SAXException("No element content allowed here. You can either include an XSL template directly into the config file or use the 'src' attribute!");
					}
					@Override
					public void characters(char[] ch, int start, int length) throws SAXException {
						for (int i=0; i<length; i++) {
							if (Character.isWhitespace(ch[start+i])) continue;
							throw new SAXException("No element content allowed here. You can either include an XSL template directly into the config file or use the 'src' attribute!");
						}
					}
				});
			}
			super.begin(namespace,name,attributes);
		}

		@Override
		protected void setResult(Templates t) {
			Object o=digester.peek();
			if (o instanceof SingleIndexConfig) ((SingleIndexConfig)o).xslt=t;
			/*else if (o instanceof Config) ((Config)o).xsltBeforeXPath=t; // the config itsself*/
			else throw new RuntimeException("An XSLT tree is not allowed here!");
		}

	}

	private final class TemplateSaxRule extends TransformerSaxRule {

		@Override
		protected void initDocument() throws SAXException {
			destContentHandler.startPrefixMapping(XSL_PREFIX,XSL_NAMESPACE);

			AttributesImpl atts=new AttributesImpl();

			// generate prefixes to exclude (all currently defined; if they appear, they will be explicitely defined by processor)
			StringBuilder excludePrefixes=new StringBuilder("#default ").append(XSL_PREFIX);
			for (String prefix : ((ExtendedDigester)digester).getCurrentAssignedPrefixes()) {
				if (!XMLConstants.DEFAULT_NS_PREFIX.equals(prefix)) excludePrefixes.append(' ').append(prefix);
			}
			atts.addAttribute(XMLConstants.NULL_NS_URI,"exclude-result-prefixes","exclude-result-prefixes",CNAME,excludePrefixes.toString());

			// root tag
			atts.addAttribute(XMLConstants.NULL_NS_URI,"version","version",CNAME,"1.0");
			destContentHandler.startElement(XSL_NAMESPACE,"stylesheet",XSL_PREFIX+":stylesheet",atts);
			atts.clear();

			// register variables as params for template
			HashSet<QName> vars=new HashSet<QName>(de.pangaea.metadataportal.harvester.XPathResolverImpl.BASE_VARIABLES);
			for (VariableConfig v : xPathVariables) vars.add(v.name);
			for (QName name : vars) {
				// it is not clear why xalan does not allow a variable with no namespace declared by a prefix that points to the empty namespace
				boolean nullNS=XMLConstants.NULL_NS_URI.equals(name.getNamespaceURI());
				if (nullNS) {
					atts.addAttribute(XMLConstants.NULL_NS_URI,"name","name",CNAME,name.getLocalPart());
				} else {
					destContentHandler.startPrefixMapping("var",name.getNamespaceURI());
					atts.addAttribute(XMLConstants.NULL_NS_URI,"name","name",CNAME,"var:"+name.getLocalPart());
				}
				destContentHandler.startElement(XSL_NAMESPACE,"param",XSL_PREFIX+":param",atts);
				atts.clear();
				destContentHandler.endElement(XSL_NAMESPACE,"param",XSL_PREFIX+":param");
				if (!nullNS) destContentHandler.endPrefixMapping("var");
			}

			// start a template
			atts.addAttribute(XMLConstants.NULL_NS_URI,"match","match",CNAME,"/");
			destContentHandler.startElement(XSL_NAMESPACE,"template",XSL_PREFIX+":template",atts);
			//atts.clear();
		}

		@Override
		protected void finishDocument() throws SAXException {
			destContentHandler.endElement(XSL_NAMESPACE,"template",XSL_PREFIX+":template");
			destContentHandler.endElement(XSL_NAMESPACE,"stylesheet",XSL_PREFIX+":stylesheet");
			destContentHandler.endPrefixMapping(XSL_PREFIX);
		}

		@Override
		protected void setResult(Templates t) {
			Object o=digester.peek();
			if (o instanceof ExpressionConfig) ((ExpressionConfig)o).setTemplate(t);
			else throw new RuntimeException("An XSLT template is not allowed here!");
		}

		private static final String XSL_NAMESPACE="http://www.w3.org/1999/XSL/Transform";
		private static final String XSL_PREFIX="int-tmpl-xsl";
		private static final String CNAME="CNAME";

	}

}