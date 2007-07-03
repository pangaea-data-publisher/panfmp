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

package de.pangaea.metadataportal.config;

import java.util.*;
import java.io.*;
import java.net.*;
import de.pangaea.metadataportal.utils.*;
import org.apache.commons.digester.*;
import org.xml.sax.*;
import javax.xml.XMLConstants;
import javax.xml.namespace.QName;
import javax.xml.transform.*;
import javax.xml.transform.sax.*;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.*;
import javax.xml.xpath.*;

public class Config {

    protected static org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(Config.class);

    // in configMode!=HARVESTER we leave out Schemas and XSLT to load config faster!
    public Config(String file, ConfigMode configMode) throws Exception {
        this.file=file;
        this.configMode=configMode;
        setAnalyzerClass(org.apache.lucene.analysis.standard.StandardAnalyzer.class);
        org.apache.lucene.search.BooleanQuery.setMaxClauseCount(DEFAULT_MAX_CLAUSE_COUNT);
        org.apache.lucene.search.BooleanQuery.setAllowDocsOutOfOrder(true);
        try {
            final Class[] X_PATH_PARAMS=new Class[]{ExtendedDigester.class,String.class};

            dig=new ExtendedDigester(StaticFactories.xinclSaxFactory.newSAXParser());
            dig.setLogger(log.isDebugEnabled()?log:new org.apache.commons.logging.impl.NoOpLog());
            dig.setNamespaceAware(true);
            dig.setValidating(false);
            dig.setRulesWithInvalidElementCheck( new ExtendedBaseRules() );
            dig.setRuleNamespaceURI("urn:java:"+getClass().getName());

            dig.addDoNothing("config");

            // *** METADATA definition ***
            dig.addDoNothing("config/metadata");

            // variables
            dig.addDoNothing("config/metadata/variables");
            dig.addObjectCreate("config/metadata/variables/variable", Config_XPathVariable.class);
            dig.addSetNext("config/metadata/variables/variable", "addVariable");
            dig.addCallMethod("config/metadata/variables/variable","setXPath", 2, X_PATH_PARAMS);
            dig.addObjectParam("config/metadata/variables/variable", 0, dig);
            dig.addCallParam("config/metadata/variables/variable", 1);
            dig.addCallMethod("config/metadata/variables/variable","setName", 2, X_PATH_PARAMS);
            dig.addObjectParam("config/metadata/variables/variable", 0, dig);
            dig.addCallParam("config/metadata/variables/variable", 1, "name");

            // filters
            dig.addCallMethod("config/metadata/filters", "setFilterDefault", 1);
            dig.addCallParam("config/metadata/filters", 0, "default");

            dig.addObjectCreate("config/metadata/filters/*", Config_XPathFilter.class);
            dig.addSetNext("config/metadata/filters/*", "addFilter");
            dig.addCallMethod("config/metadata/filters/*","setXPath", 2, X_PATH_PARAMS);
            dig.addObjectParam("config/metadata/filters/*", 0, dig);
            dig.addCallParam("config/metadata/filters/*", 1);

            // fields
            dig.addDoNothing("config/metadata/fields");
            dig.addObjectCreate("config/metadata/fields/field", Config_Field.class);
            dig.addSetNext("config/metadata/fields/field", "addField");
            SetPropertiesRule r=new SetPropertiesRule();
            r.setIgnoreMissingProperty(false);
            dig.addRule("config/metadata/fields/field",r);
            dig.addCallMethod("config/metadata/fields/field","setXPath", 2, X_PATH_PARAMS);
            dig.addObjectParam("config/metadata/fields/field", 0, dig);
            dig.addCallParam("config/metadata/fields/field", 1);

            dig.addCallMethod("config/metadata/fields/default", "setDefaultField", 0);

            // transform
            /*dig.addRule("config/metadata/transformBeforeXPath", (configMode==ConfigMode.HARVESTING) ? new TransformerSaxRule(this,file) : SaxRule.emptyRule());*/

            // schema
            dig.addDoNothing("config/metadata/schema");
            dig.addCallMethod("config/metadata/schema/url", "setSchema", 2);
            dig.addCallParam("config/metadata/schema/url", 0, "namespace");
            dig.addCallParam("config/metadata/schema/url", 1);
            dig.addCallMethod("config/metadata/schema/haltOnError", "setHaltOnSchemaError", 0);

            // *** ANALYZER ***
            dig.addDoNothing("config/analyzer");
            dig.addCallMethod("config/analyzer/class", "setAnalyzer", 0);
            dig.addCallMethod("config/analyzer/importEnglishStopWords", "importEnglishStopWords", 0);
            dig.addCallMethod("config/analyzer/addStopWords", "addStopWords", 0);

            // *** INDEX CONFIG ***
            dig.addDoNothing("config/indices");

            // SingleIndex
            dig.addObjectCreate("config/indices/index", SingleIndexConfig.class);
            dig.addSetNext("config/indices/index", "addIndex");
            dig.addCallMethod("config/indices/index","setId", 1);
            dig.addCallParam("config/indices/index", 0, "id");

            dig.addCallMethod("config/indices/index/displayName","setDisplayName",0);
            dig.addCallMethod("config/indices/index/indexDir","setIndexDir",0);
            dig.addCallMethod("config/indices/index/harvesterClass","setHarvesterClass",0);
            dig.addCallMethod("config/indices/index/autoOptimize","setAutoOptimize",0);
            dig.addCallMethod("config/indices/index/validate","setValidate",0);

            dig.addRule("config/indices/index/transform", (configMode==ConfigMode.HARVESTING) ? new TransformerSaxRule(this,file) : SaxRule.emptyRule());

            dig.addDoNothing("config/indices/index/harvesterProperties");
            dig.addCallMethod("config/indices/index/harvesterProperties/*","addHarvesterProperty",2, X_PATH_PARAMS);
            dig.addObjectParam("config/indices/index/harvesterProperties/*", 0, dig);
            dig.addCallParam("config/indices/index/harvesterProperties/*", 1);

            // VirtualIndex
            dig.addObjectCreate("config/indices/virtualIndex", VirtualIndexConfig.class);
            dig.addSetNext("config/indices/virtualIndex", "addIndex");
            dig.addCallMethod("config/indices/virtualIndex","setId", 1);
            dig.addCallParam("config/indices/virtualIndex", 0, "id");

            dig.addCallMethod("config/indices/virtualIndex/displayName","setDisplayName",0);
            dig.addCallMethod("config/indices/virtualIndex/threaded","setThreaded",0);
            dig.addCallMethod("config/indices/virtualIndex/index","addIndex",1);
            dig.addCallParam("config/indices/virtualIndex/index", 0, "ref");

            // *** SEARCH PROPERTIES ***
            dig.addDoNothing("config/search");
            dig.addCallMethod("config/search/*","addSearchProperty",2, X_PATH_PARAMS);
            dig.addObjectParam("config/search/*", 0, dig);
            dig.addCallParam("config/search/*", 1);

            // parse config
            dig.push(this);
            dig.parse(file);
        } finally {
            dig=null;
        }
    }

    public String makePathAbsolute(String file) throws java.io.IOException {
        File f=new File(file);
        if (f.isAbsolute()) return f.getCanonicalPath();
        else return new File(new File(this.file).getAbsoluteFile().getParentFile(),file).getCanonicalPath();
    }

    public void addField(Config_Field f) {
        if (f.name==null) throw new IllegalArgumentException("A field name is mandatory");
        if (fields.containsKey(f.name)) throw new IllegalArgumentException("A field with name '"+f.name+"' already exists!");
        if (f.xPathExpr==null) throw new IllegalArgumentException("A XPath itsself may not be empty");
        if (!f.lucenestorage && !f.luceneindexed) throw new IllegalArgumentException("A field must be at least indexed and/or stored");
        if (f.defaultValue!=null && f.datatype!=DataType.NUMBER && f.datatype!=DataType.DATETIME)
            throw new IllegalArgumentException("A default value can only be given for number or dateTime fields");
        fields.put(f.name,f);
    }

    public void addVariable(Config_XPathVariable f) {
        if (f.name==null) throw new IllegalArgumentException("A XPath variable name is mandatory");
        if (de.pangaea.metadataportal.harvester.XPathResolverImpl.INDEX_BUILDER_NAMESPACE.equals(f.name.getNamespaceURI()))
            throw new IllegalArgumentException("A XPath variable name may not be in the namespace for internal variables ('"+de.pangaea.metadataportal.harvester.XPathResolverImpl.INDEX_BUILDER_NAMESPACE+"')");
        if (f.xPathExpr==null) throw new IllegalArgumentException("A XPath itsself may not be empty");
        xPathVariables.add(f);
    }

    public void addFilter(Config_XPathFilter f) {
        if (f.xPathExpr==null) throw new IllegalArgumentException("A XPath itsself may not be empty");
        String type=dig.getCurrentElementName();
        f.setType(type);
        filters.add(f);
    }

    public void addIndex(IndexConfig i) {
        if (indices.containsKey(i.id))
            throw new IllegalArgumentException("There is already an index with id=\""+i.id+"\" added to configuration!");
        i.parent=this;
        i.check();
        indices.put(i.id,i);
    }

    public void setFilterDefault(String v) {
        if (v==null) return; // no change
        // a bit of hack, we use an empty filter to find out type :)
        Config_XPathFilter f=new Config_XPathFilter();
        f.setType(v);
        filterDefault=f.type;
    }

    public void setDefaultField(String v) throws Exception {
        v=v.trim();
        if (".".equals(v) || "/".equals(v) || "/*".equals(v)) {
            defaultField=null; // all fields from SAX parser
        } else {
            defaultField=new Config_Field();
            defaultField.setXPath(dig,v);
            // dummy settings not yet used for default field, to change this, change logic in IndexBuilder.addDocument() !!!
            defaultField.lucenestorage=false;
            defaultField.luceneindexed=true;
            defaultField.datatype=DataType.TOKENIZEDTEXT;
            defaultField.name=IndexConstants.FIELDNAME_CONTENT;
        }
    }

    public void addStopWords(String stopWords) {
        for (String w : stopWords.split("[\\,\\;\\s]+")) {
            w=w.trim().toLowerCase();
            if (!"".equals(w)) luceneStopWords.add(w);
        }
    }

    public void importEnglishStopWords(String dummy) {
        luceneStopWords.addAll(Arrays.asList(org.apache.lucene.analysis.StopAnalyzer.ENGLISH_STOP_WORDS));
    }

    public void setAnalyzer(String v) throws Exception {
        Class<?> c=Class.forName(v.trim());
        setAnalyzerClass(c.asSubclass(org.apache.lucene.analysis.Analyzer.class));
    }

    public void setAnalyzerClass(Class<? extends org.apache.lucene.analysis.Analyzer> c) throws Exception {
        analyzerClass=c;
        try {
            analyzerConstructor=analyzerClass.getConstructor(String[].class);
        } catch (NoSuchMethodException nsm) {
            analyzerConstructor=null;
            log.warn("The given analyzer class is not capable of assigning stop words - <stopWords> discarded!");
        }
    }

    public void addSearchProperty(ExtendedDigester dig, String value) {
        String name=dig.getCurrentElementName();
        if ("maxClauseCount".equals(name)) {
            if ("inf".equals(value))
                org.apache.lucene.search.BooleanQuery.setMaxClauseCount(Integer.MAX_VALUE);
            else
                org.apache.lucene.search.BooleanQuery.setMaxClauseCount(Integer.parseInt(value));
        } else {
        // default
            searchProperties.setProperty(name,value);
        }
    }

    public void setSchema(String namespace, String url) throws Exception {
        if (configMode==ConfigMode.SEARCH) return; // no schema support when search engine
        if (schema!=null) throw new SAXException("Schema URL already defined!");
        url=url.trim();
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

    public void setHaltOnSchemaError(String v) {
        haltOnSchemaError=Boolean.parseBoolean(v.trim());
    }

    // get configuration infos

    public org.apache.lucene.analysis.Analyzer getAnalyzer() {
        try {
            if (analyzerConstructor!=null) {
                if (log.isDebugEnabled()) log.debug("Using stop words: "+luceneStopWords);

                String[] sw=new String[luceneStopWords.size()];
                sw=luceneStopWords.toArray(sw);
                return (org.apache.lucene.analysis.Analyzer)analyzerConstructor.newInstance(new Object[]{sw});
            } else
                return (org.apache.lucene.analysis.Analyzer)analyzerClass.newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Error instantiating analyzer (this should never happen)!",e);
        }
    }

    // members "the configuration"
    public Map<String,IndexConfig> indices=new HashMap<String,IndexConfig>();

    public Map<String,Config_Field> fields=new HashMap<String,Config_Field>();
    public Config_Field defaultField=null;

    // filters
    public FilterType filterDefault=FilterType.ACCEPT;
    public List<Config_XPathFilter> filters=new ArrayList<Config_XPathFilter>();

    // variables
    public List<Config_XPathVariable> xPathVariables=new ArrayList<Config_XPathVariable>();

    // schema etc
    public Schema schema=null;
    public boolean haltOnSchemaError=false;

    /*public Templates xsltBeforeXPath=null;*/

    public Properties searchProperties=new Properties();

    public Set<String> luceneStopWords=new HashSet<String>();
    protected Class<? extends org.apache.lucene.analysis.Analyzer> analyzerClass=null;
    protected java.lang.reflect.Constructor<? extends org.apache.lucene.analysis.Analyzer> analyzerConstructor=null;

    public String file;
    private ConfigMode configMode;

    private ExtendedDigester dig=null;

    public static final int DEFAULT_MAX_CLAUSE_COUNT = 131072;

    // internal classes

    public static enum ConfigMode { HARVESTING,SEARCH };
    public static enum FilterType { ACCEPT,DENY };
    public static enum DataType { TOKENIZEDTEXT,STRING,NUMBER,DATETIME };

    public static class Config_XPathExpression extends Object {

        public void setXPath(ExtendedDigester dig, String xpath) throws XPathExpressionException {
            if ("".equals(xpath)) return; // Exception throws the Config.addField() method
            XPath x=StaticFactories.xpathFactory.newXPath();
            x.setXPathFunctionResolver(de.pangaea.metadataportal.harvester.XPathResolverImpl.getInstance());
            x.setXPathVariableResolver(de.pangaea.metadataportal.harvester.XPathResolverImpl.getInstance());
            // current namespace context with strict=true (display errors when namespace declaration is missing [non-standard!])
            // and with possibly declared default namespace is redefined/deleted to "" (according to XSLT specification,
            // where this is also mandatory).
            x.setNamespaceContext(dig.getCurrentNamespaceContext(true,true));
            xPathExpr=x.compile(xpath);
            cachedXPath=xpath;
        }

        public String toString() {
            return cachedXPath;
        }

        public XPathExpression xPathExpr=null;
        private String cachedXPath=null;
    }

    public static final class Config_Field extends Config_XPathExpression {

        public void setName(String v) {
            name=v;
        }

        public void setDatatype(String v) {
            try {
                datatype=Enum.valueOf(DataType.class,v.toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid value '"+v+"' for attribute datatype!");
            }
        }

        public void setLucenestorage(String v) { lucenestorage=Boolean.parseBoolean(v); }
        public void setLuceneindexed(String v) { luceneindexed=Boolean.parseBoolean(v); }
        public void setDefault(String v) { defaultValue=v; }

        public String toString() {
            return name;
        }

        // members "the configuration"
        public String name=null;
        public String defaultValue=null;
        public DataType datatype=DataType.TOKENIZEDTEXT;
        public boolean lucenestorage=true;
        public boolean luceneindexed=true;
    }

    public static final class Config_XPathVariable extends Config_XPathExpression {

        public void setName(ExtendedDigester dig, String nameStr) {
            if ("".equals(nameStr)) return; // Exception throws the Config.addVariable() method
            // current namespace context with strict=true (display errors when namespace declaration is missing [non-standard!])
            // and with possibly declared default namespace is redefined/deleted to "" (according to XSLT specification,
            // where this is also mandatory).
            this.name=QNameParser.parseLexicalQName(nameStr,dig.getCurrentNamespaceContext(true,true));
        }

        public String toString() {
            return new StringBuilder(name.toString()).append('(').append(super.toString()).append(')').toString();
        }

        // members "the configuration"
        public QName name=null;
    }

    public static final class Config_XPathFilter extends Config_XPathExpression {

        public void setType(String v) {
            try {
                type=Enum.valueOf(FilterType.class,v.toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid filter type: '"+v+"'");
            }
        }

        public String toString() {
            return new StringBuilder(type.toString()).append('(').append(super.toString()).append(')').toString();
        }

        // members "the configuration"
        public FilterType type=null;
    }

    private static final class TransformerSaxRule extends SaxRule {

        private Config owner;
        private TemplatesHandler th=null;
        private String file;

        public TransformerSaxRule(Config owner, String file) {
            super();
            this.owner=owner;
            this.file=file;
        }

        public void begin(java.lang.String namespace, java.lang.String name, Attributes attributes) throws Exception {
            th=StaticFactories.transFactory.newTemplatesHandler();
            th.setSystemId(file);
            setContentHandler(th);
            super.begin(namespace,name,attributes);
        }

        public void end(java.lang.String namespace, java.lang.String name) throws Exception {
            super.end(namespace,name);
            Object o=owner.dig.peek();
            if (o instanceof SingleIndexConfig) ((SingleIndexConfig)o).xslt=th.getTemplates(); // a index config
            /*else if (o instanceof Config) ((Config)o).xsltBeforeXPath=th.getTemplates(); // the config itsself*/
            else throw new RuntimeException("A XSLT tree is not allowed here!");
            th=null;
        }
    }
}