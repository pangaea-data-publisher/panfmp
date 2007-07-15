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
import de.pangaea.metadataportal.utils.*;
import org.apache.commons.digester.*;
import org.xml.sax.*;
import org.xml.sax.helpers.AttributesImpl;
import javax.xml.XMLConstants;
import javax.xml.namespace.QName;
import javax.xml.transform.*;
import javax.xml.transform.sax.*;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.*;

public class Config {

    protected static org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(Config.class);

    // in configMode!=HARVESTER we leave out Schemas and XSLT to load config faster!
    public Config(String file, ConfigMode configMode) throws Exception {
        this.file=file;
        this.configMode=configMode;

        de.pangaea.metadataportal.Package.checkMinimumRequirements();

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

            dig.addObjectCreate("config/metadata/variables/variable", VariableConfig.class);
            dig.addSetNext("config/metadata/variables/variable", "addVariable");
            dig.addCallMethod("config/metadata/variables/variable","setName", 2, X_PATH_PARAMS);
            dig.addObjectParam("config/metadata/variables/variable", 0, dig);
            dig.addCallParam("config/metadata/variables/variable", 1, "name");
            if (configMode==ConfigMode.HARVESTING) {
                dig.addCallMethod("config/metadata/variables/variable","setXPath", 2, X_PATH_PARAMS);
                dig.addObjectParam("config/metadata/variables/variable", 0, dig);
                dig.addCallParam("config/metadata/variables/variable", 1);
            }

            dig.addObjectCreate("config/metadata/variables/variable-template", VariableConfig.class);
            dig.addSetNext("config/metadata/variables/variable-template", "addVariable");
            dig.addCallMethod("config/metadata/variables/variable-template","setName", 2, X_PATH_PARAMS);
            dig.addObjectParam("config/metadata/variables/variable-template", 0, dig);
            dig.addCallParam("config/metadata/variables/variable-template", 1, "name");
            dig.addRule("config/metadata/variables/variable-template", (configMode==ConfigMode.HARVESTING) ? new TemplateSaxRule(this,file) : SaxRule.emptyRule());

            // filters
            dig.addCallMethod("config/metadata/filters", "setFilterDefault", 1);
            dig.addCallParam("config/metadata/filters", 0, "default");

            dig.addObjectCreate("config/metadata/filters/*", FilterConfig.class);
            dig.addSetNext("config/metadata/filters/*", "addFilter");
            if (configMode==ConfigMode.HARVESTING) {
                dig.addCallMethod("config/metadata/filters/*","setXPath", 2, X_PATH_PARAMS);
                dig.addObjectParam("config/metadata/filters/*", 0, dig);
                dig.addCallParam("config/metadata/filters/*", 1);
            }

            // fields
            dig.addDoNothing("config/metadata/fields");

            dig.addObjectCreate("config/metadata/fields/field", FieldConfig.class);
            dig.addSetNext("config/metadata/fields/field", "addField");
            SetPropertiesRule r=new SetPropertiesRule();
            r.setIgnoreMissingProperty(false);
            dig.addRule("config/metadata/fields/field",r);
            if (configMode==ConfigMode.HARVESTING) {
                dig.addCallMethod("config/metadata/fields/field","setXPath", 2, X_PATH_PARAMS);
                dig.addObjectParam("config/metadata/fields/field", 0, dig);
                dig.addCallParam("config/metadata/fields/field", 1);
            }

            dig.addObjectCreate("config/metadata/fields/field-template", FieldConfig.class);
            dig.addSetNext("config/metadata/fields/field-template", "addField");
            r=new SetPropertiesRule();
            r.setIgnoreMissingProperty(false);
            dig.addRule("config/metadata/fields/field-template",r);
            dig.addRule("config/metadata/fields/field-template", (configMode==ConfigMode.HARVESTING) ? new TemplateSaxRule(this,file) : SaxRule.emptyRule());

            dig.addCallMethod("config/metadata/fields/default", "setDefaultField", 0);

            // document boost
            dig.addCallMethod("config/metadata/documentBoost", "setDocumentBoost", 0);

            // transform
            /*dig.addRule("config/metadata/transformBeforeXPath", (configMode==ConfigMode.HARVESTING) ? new IndexConfigTransformerSaxRule(this,file) : SaxRule.emptyRule());*/

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

            dig.addRule("config/indices/index/transform", (configMode==ConfigMode.HARVESTING) ? new IndexConfigTransformerSaxRule(this,file) : SaxRule.emptyRule());

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

            // *** GLOBAL HARVESTER PROPERTIES ***
            dig.addDoNothing("config/globalHarvesterProperties");
            dig.addCallMethod("config/globalHarvesterProperties/*","addGlobalHarvesterProperty",2, X_PATH_PARAMS);
            dig.addObjectParam("config/globalHarvesterProperties/*", 0, dig);
            dig.addCallParam("config/globalHarvesterProperties/*", 1);

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

    public void addField(FieldConfig f) {
        if (f.name==null) throw new IllegalArgumentException("A field name is mandatory");
        if (fields.containsKey(f.name)) throw new IllegalArgumentException("A field with name '"+f.name+"' already exists!");
        if (configMode==ConfigMode.HARVESTING) {
            if (f.xPathExpr==null && f.xslt==null) throw new IllegalArgumentException("A XPath or template itsself may not be empty");
            if (f.xPathExpr!=null && f.xslt!=null) throw new IllegalArgumentException("It may not both XPath and template be defined");
        }
        if (!f.lucenestorage && !f.luceneindexed) throw new IllegalArgumentException("A field must be at least indexed and/or stored");
        if (f.defaultValue!=null && f.datatype!=FieldConfig.DataType.NUMBER && f.datatype!=FieldConfig.DataType.DATETIME)
            throw new IllegalArgumentException("A default value can only be given for number or dateTime fields");
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
        String type=dig.getCurrentElementName();
        f.setType(type);
        filters.add(f);
    }

    public void addIndex(IndexConfig i) {
        if (indices.containsKey(i.id))
            throw new IllegalArgumentException("There is already an index with id=\""+i.id+"\" added to configuration!");
        i.parent=this;
        if (i instanceof SingleIndexConfig) ((SingleIndexConfig)i).harvesterProperties.setParentProperties(globalHarvesterProperties);
        i.check();
        indices.put(i.id,i);
    }

    public void setFilterDefault(String v) {
        if (v==null) return; // no change
        // a bit of hack, we use an empty filter to find out type :)
        FilterConfig f=new FilterConfig();
        f.setType(v);
        filterDefault=f.type;
    }

    public void setDefaultField(String v) throws Exception {
        if (v==null) {
            defaultField=null;
            return;
        }
        v=v.trim();
        if (".".equals(v) || "/".equals(v) || "/*".equals(v)) {
            defaultField=null; // all fields from SAX parser
        } else {
            defaultField=new AnyExpressionConfig();
            defaultField.setXPath(dig,v);
        }
    }

    public void setDocumentBoost(String v) throws Exception {
        if (v==null) {
            documentBoost=null;
            return;
        }
        v=v.trim();
        documentBoost=new AnyExpressionConfig();
        documentBoost.setXPath(dig,v);
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

    public void addGlobalHarvesterProperty(ExtendedDigester dig, String value) {
        String name=dig.getCurrentElementName();
        globalHarvesterProperties.setProperty(name,value);
    }

    public void setSchema(String namespace, String url) throws Exception {
        if (configMode!=ConfigMode.HARVESTING) return; // no schema support when search engine
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

    public Map<String,FieldConfig> fields=new HashMap<String,FieldConfig>();
    public AnyExpressionConfig defaultField=null;

    // filters
    public FilterConfig.FilterType filterDefault=FilterConfig.FilterType.ACCEPT;
    public List<FilterConfig> filters=new ArrayList<FilterConfig>();

    // variables
    public List<VariableConfig> xPathVariables=new ArrayList<VariableConfig>();

    // schema etc
    public Schema schema=null;
    public boolean haltOnSchemaError=false;

    // document boost
    public AnyExpressionConfig documentBoost=null;

    /*public Templates xsltBeforeXPath=null;*/

    public Properties searchProperties=new Properties();
    public Properties globalHarvesterProperties=new Properties();

    public Set<String> luceneStopWords=new HashSet<String>();
    protected Class<? extends org.apache.lucene.analysis.Analyzer> analyzerClass=null;
    protected java.lang.reflect.Constructor<? extends org.apache.lucene.analysis.Analyzer> analyzerConstructor=null;

    public String file;
    private ConfigMode configMode;

    private ExtendedDigester dig=null;

    public static final int DEFAULT_MAX_CLAUSE_COUNT = 131072;

    public static enum ConfigMode { HARVESTING,SEARCH };

    // internal classes
    private abstract static class TransformerSaxRule extends SaxRule {

        protected Config owner;
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

        protected abstract void setResult(Templates t);

        public void end(java.lang.String namespace, java.lang.String name) throws Exception {
            super.end(namespace,name);
            setResult(th.getTemplates());
            th=null;
        }

    }

    private static final class IndexConfigTransformerSaxRule extends TransformerSaxRule {

        public IndexConfigTransformerSaxRule(Config owner, String file) {
            super(owner,file);
        }

        protected void setResult(Templates t) {
            Object o=owner.dig.peek();
            if (o instanceof SingleIndexConfig) ((SingleIndexConfig)o).xslt=t;
            /*else if (o instanceof Config) ((Config)o).xsltBeforeXPath=t; // the config itsself*/
            else throw new RuntimeException("A XSLT tree is not allowed here!");
        }

    }

    private static final class TemplateSaxRule extends TransformerSaxRule {

        public TemplateSaxRule(Config owner, String file) {
            super(owner,file);
        }

        protected void initDocument() throws SAXException {
            AttributesImpl atts=new AttributesImpl();

            atts.addAttribute(XMLConstants.NULL_NS_URI,"version","",CNAME,"1.0");
            destContentHandler.startElement(XSL_NAMESPACE,"stylesheet","",atts);
            atts.clear();

            // register variables as params for template
            HashSet<QName> vars=new HashSet<QName>(de.pangaea.metadataportal.harvester.XPathResolverImpl.BASE_VARIABLES);
            for (VariableConfig v : owner.xPathVariables) vars.add(v.name);
            for (QName name : vars) {
                // it is not clear why xalan does not allow a variable with no namespace declared by a prefix that points to the empty namespace
                boolean nullNS=XMLConstants.NULL_NS_URI.equals(name.getNamespaceURI());
                if (nullNS) {
                    atts.addAttribute(XMLConstants.NULL_NS_URI,"name","",CNAME,name.getLocalPart());
                } else {
                    destContentHandler.startPrefixMapping("var",name.getNamespaceURI());
                    atts.addAttribute(XMLConstants.NULL_NS_URI,"name","",CNAME,"var:"+name.getLocalPart());
                }
                destContentHandler.startElement(XSL_NAMESPACE,"param","",atts);
                atts.clear();
                destContentHandler.endElement(XSL_NAMESPACE,"param","");
                if (!nullNS) destContentHandler.endPrefixMapping("var");
            }

            // start a template
            atts.addAttribute(XMLConstants.NULL_NS_URI,"match","",CNAME,"/");
            destContentHandler.startElement(XSL_NAMESPACE,"template","",atts);
            //atts.clear();
        }

        protected void finishDocument() throws SAXException {
            destContentHandler.endElement(XSL_NAMESPACE,"template","");
            destContentHandler.endElement(XSL_NAMESPACE,"stylesheet","");
        }

        protected void setResult(Templates t) {
            Object o=owner.dig.peek();
            if (o instanceof AnyExpressionConfig) ((AnyExpressionConfig)o).setTemplate(t);
            else throw new RuntimeException("A XSLT template is not allowed here!");
        }

        private static final String XSL_NAMESPACE="http://www.w3.org/1999/XSL/Transform";
        private static final String CNAME="CNAME";

    }

}