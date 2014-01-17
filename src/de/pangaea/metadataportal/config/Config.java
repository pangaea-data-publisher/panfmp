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

import java.io.File;
import java.net.CookieHandler;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.WeakHashMap;

import javax.xml.XMLConstants;
import javax.xml.namespace.QName;
import javax.xml.transform.Templates;
import javax.xml.transform.sax.TemplatesHandler;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.apache.commons.digester.AbstractObjectCreationFactory;
import org.apache.commons.digester.ExtendedBaseRules;
import org.apache.commons.digester.SetPropertiesRule;
import org.apache.lucene.index.IndexWriter.IndexReaderWarmer;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

import de.pangaea.metadataportal.utils.BooleanParser;
import de.pangaea.metadataportal.utils.ExtendedDigester;
import de.pangaea.metadataportal.utils.PublicForDigesterUse;
import de.pangaea.metadataportal.utils.SaxRule;
import de.pangaea.metadataportal.utils.SimpleCookieHandler;
import de.pangaea.metadataportal.utils.StaticFactories;

/**
 * Main panFMP configuration class. It loads the configuration from a XML file.
 * 
 * @author Uwe Schindler
 */
public class Config {
  
  private static final org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory
      .getLog(Config.class);
  
  // in configMode!=HARVESTER we leave out Schemas and XSLT to load config
  // faster!
  public Config(String file) throws Exception {
    this.file = file;
    
    String version = de.pangaea.metadataportal.Package
        .getFullPackageDescription();
    if (version != null) log.info(version);
    de.pangaea.metadataportal.Package.checkMinimumRequirements();
    
    final CookieHandler defCookieH = CookieHandler.getDefault();
    if (defCookieH != null && defCookieH != SimpleCookieHandler.INSTANCE) {
      log.warn("There is a CookieHandler already registered with the JVM, panFMP's customized HTTP cookie handling will be not available during harvesting.");
    } else {
      CookieHandler.setDefault(SimpleCookieHandler.INSTANCE);
    }
    
    try {
      final Class<?>[] DIGSTRING_PARAMS = new Class<?>[] {ExtendedDigester.class,
          String.class};
      
      dig = new ExtendedDigester();
      dig.setNamespaceAware(true);
      dig.setValidating(false);
      dig.setXIncludeAware(true);
      dig.setRulesWithInvalidElementCheck(new ExtendedBaseRules());
      dig.setRuleNamespaceURI("urn:java:" + getClass().getName());
      
      dig.addDoNothing("config");
      
      // *** METADATA definition ***
      dig.addDoNothing("config/metadata");
      
      // variables
      dig.addDoNothing("config/metadata/variables");
      
      dig.addObjectCreate("config/metadata/variables/variable",
          VariableConfig.class);
      dig.addSetNext("config/metadata/variables/variable", "addVariable");
      dig.addCallMethod("config/metadata/variables/variable", "setName", 2,
          DIGSTRING_PARAMS);
      dig.addObjectParam("config/metadata/variables/variable", 0, dig);
      dig.addCallParam("config/metadata/variables/variable", 1, "name");
      dig.addCallMethod("config/metadata/variables/variable", "setXPath", 2,
          DIGSTRING_PARAMS);
      dig.addObjectParam("config/metadata/variables/variable", 0, dig);
      dig.addCallParam("config/metadata/variables/variable", 1);
      
      dig.addObjectCreate("config/metadata/variables/variable-template",
          VariableConfig.class);
      dig.addSetNext("config/metadata/variables/variable-template",
          "addVariable");
      dig.addCallMethod("config/metadata/variables/variable-template",
          "setName", 2, DIGSTRING_PARAMS);
      dig.addObjectParam("config/metadata/variables/variable-template", 0, dig);
      dig.addCallParam("config/metadata/variables/variable-template", 1, "name");
      dig.addRule("config/metadata/variables/variable-template", new TemplateSaxRule());
      
      // filters
      dig.addCallMethod("config/metadata/filters", "setFilterDefault", 1);
      dig.addCallParam("config/metadata/filters", 0, "default");
      
      dig.addObjectCreate("config/metadata/filters/*", FilterConfig.class);
      dig.addSetNext("config/metadata/filters/*", "addFilter");
      dig.addCallMethod("config/metadata/filters/*", "setXPath", 2,
          DIGSTRING_PARAMS);
      dig.addObjectParam("config/metadata/filters/*", 0, dig);
      dig.addCallParam("config/metadata/filters/*", 1);
      
      // fields
      dig.addDoNothing("config/metadata/fields");
      
      dig.addObjectCreate("config/metadata/fields/field", FieldConfig.class);
      dig.addSetNext("config/metadata/fields/field", "addField");
      String[] propAttr = new String[] { "datatype"}, propMapping = new String[] {"dataType"};
      SetPropertiesRule r = new SetPropertiesRule(propAttr, propMapping);
      r.setIgnoreMissingProperty(false);
      dig.addRule("config/metadata/fields/field", r);
      dig.addCallMethod("config/metadata/fields/field", "setXPath", 2,
          DIGSTRING_PARAMS);
      dig.addObjectParam("config/metadata/fields/field", 0, dig);
      dig.addCallParam("config/metadata/fields/field", 1);
      
      dig.addObjectCreate("config/metadata/fields/field-template",
          FieldConfig.class);
      dig.addSetNext("config/metadata/fields/field-template", "addField");
      r = new SetPropertiesRule(propAttr, propMapping);
      r.setIgnoreMissingProperty(false);
      dig.addRule("config/metadata/fields/field-template", r);
      dig.addRule("config/metadata/fields/field-template", new TemplateSaxRule());
      
      // default field
      dig.addCallMethod("config/metadata/fields/default", "setDefaultField", 0);
      
      // transform
      /*
       * dig.addRule("config/metadata/transformBeforeXPath",
       * (configMode==ConfigMode.HARVESTING) ? new
       * IndexConfigTransformerSaxRule() : SaxRule.emptyRule());
       */
      
      // schema
      dig.addDoNothing("config/metadata/schema");
      dig.addCallMethod("config/metadata/schema/url", "setSchema", 2);
      dig.addCallParam("config/metadata/schema/url", 0, "namespace");
      dig.addCallParam("config/metadata/schema/url", 1);
      dig.addCallMethod("config/metadata/schema/haltOnError",
          "setHaltOnSchemaError", 0);
      dig.addCallMethod("config/metadata/schema/augmentation",
          "setAugmentation", 0);
      
      // *** INDEX CONFIG ***
      dig.addDoNothing("config/indexes");
      
      // SingleIndex
      dig.addFactoryCreate("config/indexes/index",
          new AbstractObjectCreationFactory() {
            @Override
            public Object createObject(Attributes attributes) {
              return new IndexConfig(Config.this);
            }
          });
      dig.addSetNext("config/indexes/index", "addIndex");
      dig.addCallMethod("config/indexes/index", "setId", 1);
      dig.addCallParam("config/indexes/index", 0, "id");
      
      dig.addCallMethod("config/indexes/index/displayName", "setDisplayName", 0);
      dig.addCallMethod("config/indexes/index/indexDir", "setIndexDir", 0);
      dig.addCallMethod("config/indexes/index/harvesterClass",
          "setHarvesterClass", 0);
      
      dig.addRule(
          "config/indexes/index/transform",
          new IndexConfigTransformerSaxRule());
      
      dig.addDoNothing("config/indexes/index/harvesterProperties");
      dig.addCallMethod("config/indexes/index/harvesterProperties/*",
          "addHarvesterProperty", 0);
            
      // *** GLOBAL HARVESTER PROPERTIES ***
      dig.addDoNothing("config/globalHarvesterProperties");
      dig.addCallMethod("config/globalHarvesterProperties/*",
          "addGlobalHarvesterProperty", 0);
      
      // parse config
      try {
        dig.push(this);
        dig.parse(new File(file));
      } catch (SAXException saxe) {
        // throw the real Exception not the digester one
        if (saxe.getException() != null) throw saxe.getException();
        else throw saxe;
      }
    } finally {
      dig = null;
    }
    
    // *** After loading do final checks ***
    // consistency in indexes:
    for (IndexConfig iconf : indexes.values())
      iconf.check();
    
    // cleanup
    templatesCache.clear();
  }
  
  /**
   * makes the given local filesystem path absolute and resolve it relative to
   * config directory
   **/
  public final String makePathAbsolute(String file) throws java.io.IOException {
    return makePathAbsolute(file, false);
  }
  
  /**
   * makes the given local filesystem path or URL absolute and resolve it
   * relative to config directory (if local)
   **/
  public String makePathAbsolute(String file, boolean allowURL)
      throws java.io.IOException {
    try {
      if (allowURL) {
        return new URL(file).toString();
      } else {
        new URL(file);
        throw new IllegalArgumentException(
            "You can only use local file system pathes instead of '" + file
                + "'.");
      }
    } catch (MalformedURLException me) {
      File f = new File(file);
      if (f.isAbsolute()) return f.getCanonicalPath();
      else return new File(new File(this.file).getAbsoluteFile()
          .getParentFile(), file).getCanonicalPath();
    }
  }
  
  public void addField(FieldConfig f) {
    if (f.name == null) throw new IllegalArgumentException(
        "A field name is mandatory");
    if (fields.containsKey(f.name)) throw new IllegalArgumentException(
        "A field with name '" + f.name + "' already exists!");
    if (f.xPathExpr == null && f.xslt == null) throw new IllegalArgumentException(
        "A XPath or template itsself may not be empty");
    if (f.xPathExpr != null && f.xslt != null) throw new IllegalArgumentException(
        "It may not both XPath and template be defined");
    if (f.datatype == FieldConfig.DataType.XHTML && f.xslt == null) throw new IllegalArgumentException(
        "XHTML fields may only be declared as a XSLT template (using <field-template/>)");
    if (f.defaultValue != null && f.datatype != FieldConfig.DataType.NUMBER
        && f.datatype != FieldConfig.DataType.DATETIME) throw new IllegalArgumentException(
        "A default value can only be given for NUMBER or DATETIME fields");
    fields.put(f.name, f);
  }
  
  public void addVariable(VariableConfig f) {
    if (filters.size() > 0 || fields.size() > 0) throw new IllegalStateException(
        "Variables must be declared before all fields and filters!");
    if (f.name == null) throw new IllegalArgumentException(
        "A variable name is mandatory");
    if (de.pangaea.metadataportal.harvester.XPathResolverImpl.INDEX_BUILDER_NAMESPACE
        .equals(f.name.getNamespaceURI())) throw new IllegalArgumentException(
        "A XPath variable name may not be in the namespace for internal variables ('"
            + de.pangaea.metadataportal.harvester.XPathResolverImpl.INDEX_BUILDER_NAMESPACE
            + "')");
    if (f.xPathExpr == null && f.xslt == null) throw new IllegalArgumentException(
        "A XPath or template itsself may not be empty");
    if (f.xPathExpr != null && f.xslt != null) throw new IllegalArgumentException(
        "It may not both XPath and template be defined");
    xPathVariables.add(f);
  }
  
  public void addFilter(FilterConfig f) {
    if (f.xPathExpr == null) throw new IllegalArgumentException(
        "A filter needs an XPath expression");
    if (f.xslt != null) throw new IllegalArgumentException(
        "A filter may not contain a template");
    f.type = FilterConfig.FilterType.valueOf(dig.getCurrentElementName()
        .toUpperCase(Locale.ROOT));
    filters.add(f);
  }
  
  public void addIndex(IndexConfig i) {
    if (indexes.containsKey(i.id)) throw new IllegalArgumentException(
        "There is already an index with id=\"" + i.id
            + "\" added to configuration!");
    indexes.put(i.id, i);
  }
  
  @PublicForDigesterUse
  @Deprecated
  public void setFilterDefault(String v) {
    if (v == null) return; // no change
    // a bit of hack, we use an empty filter to find out type :)
    FilterConfig f = new FilterConfig();
    f.setType(v);
    filterDefault = f.type;
  }
  
  @PublicForDigesterUse
  @Deprecated
  public void setDefaultField(String xpath)
      throws Exception {
    // XPath
    if (xpath == null) {
      defaultField = null;
      return;
    }
    xpath = xpath.trim();
    if (".".equals(xpath) || "/".equals(xpath) || "/*".equals(xpath)) {
      defaultField = null; // all fields from SAX parser
    } else {
      defaultField = new ExpressionConfig();
      defaultField.setXPath(dig, xpath);
    }
  }
  
  @PublicForDigesterUse
  @Deprecated
  public void addGlobalHarvesterProperty(String value) {
    globalHarvesterProperties.setProperty(dig.getCurrentElementName(), value);
  }
  
  public void setSchema(String namespace, String url) throws Exception {
    if (schema != null) throw new SAXException("Schema URL already defined!");
    url = makePathAbsolute(url.trim(), true);
    
    if (namespace != null) namespace = namespace.trim();
    if (namespace == null || "".equals(namespace)) namespace = XMLConstants.W3C_XML_SCHEMA_NS_URI;
    log.info("Loading XML schema in format '" + namespace + "' from URL '"
        + url + "'...");
    try {
      SchemaFactory fact = SchemaFactory.newInstance(namespace);
      schema = fact.newSchema(new StreamSource(url));
    } catch (IllegalArgumentException iae) {
      throw new IllegalArgumentException(
          "Your XML installation does not support schemas in format '"
              + namespace + "'!");
    }
  }
  
  @PublicForDigesterUse
  @Deprecated
  public void setHaltOnSchemaError(String v) {
    haltOnSchemaError = BooleanParser.parseBoolean(v.trim());
  }
  
  @PublicForDigesterUse
  @Deprecated
  public void setAugmentation(String v) {
    validateWithAugmentation = BooleanParser.parseBoolean(v.trim());
  }
  
  // get configuration infos
  
  Templates loadTemplate(String file) throws Exception {
    file = makePathAbsolute(file, true);
    Templates templ = templatesCache.get(file);
    if (templ == null) {
      log.info("Loading XSL transformation from '" + file + "'...");
      templatesCache.put(
          file,
          templ = StaticFactories.transFactory.newTemplates(new StreamSource(
              file)));
    }
    return templ;
  }
  
  // members "the configuration"
  public final Map<String,IndexConfig> indexes = new LinkedHashMap<String,IndexConfig>();
  
  public final Map<String,FieldConfig> fields = new LinkedHashMap<String,FieldConfig>();
  public ExpressionConfig defaultField = null;
  
  // filters
  public FilterConfig.FilterType filterDefault = FilterConfig.FilterType.ACCEPT;
  public final List<FilterConfig> filters = new ArrayList<FilterConfig>();
  
  // variables
  public final List<VariableConfig> xPathVariables = new ArrayList<VariableConfig>();
  
  // schema etc
  public Schema schema = null;
  public boolean haltOnSchemaError = false, validateWithAugmentation = true;
  
  /* public Templates xsltBeforeXPath=null; */
  
  // Template cache
  private final Map<String,Templates> templatesCache = new WeakHashMap<String,Templates>();
  
  public final Properties globalHarvesterProperties = new Properties();
    
  public String file;
  
  public final Set<IndexReaderWarmer> indexReaderWarmers = new LinkedHashSet<IndexReaderWarmer>();
  
  protected ExtendedDigester dig = null;
  
  public static final int DEFAULT_MAX_CLAUSE_COUNT = 131072;
  
  // internal classes
  private abstract class TransformerSaxRule extends SaxRule {
    
    private TemplatesHandler th = null;
    
    @Override
    public void begin(String namespace, String name, Attributes attributes)
        throws Exception {
      if (getContentHandler() == null) {
        th = StaticFactories.transFactory.newTemplatesHandler();
        th.setSystemId(file);
        setContentHandler(th);
      }
      super.begin(namespace, name, attributes);
    }
    
    protected abstract void setResult(Templates t);
    
    @Override
    public void end(String namespace, String name) throws Exception {
      super.end(namespace, name);
      if (th != null) setResult(th.getTemplates());
      setContentHandler(th = null);
    }
    
  }
  
  final class IndexConfigTransformerSaxRule extends TransformerSaxRule {
    
    @Override
    public void begin(String namespace, String name, Attributes attributes)
        throws Exception {
      final Object o = digester.peek();
      if (!(o instanceof IndexConfig)) throw new RuntimeException(
          "An XSLT tree is not allowed here!");
      final IndexConfig iconf = (IndexConfig) o;
      
      final String file = attributes.getValue(XMLConstants.NULL_NS_URI, "src");
      if (file != null) {
        setResult(loadTemplate(file));
        setContentHandler(new org.xml.sax.helpers.DefaultHandler() {
          @Override
          public void startElement(String namespaceURI, String localName,
              String qName, Attributes atts) throws SAXException {
            throw new SAXException(
                "No element content allowed here. You can either include an XSL template directly into the config file or use the 'src' attribute!");
          }
          
          @Override
          public void characters(char[] ch, int start, int length)
              throws SAXException {
            for (int i = 0; i < length; i++) {
              if (Character.isWhitespace(ch[start + i])) continue;
              throw new SAXException(
                  "No element content allowed here. You can either include an XSL template directly into the config file or use the 'src' attribute!");
            }
          }
        });
      }
      // collect all params to additionally pass to XSL and store in Map
      iconf.xsltParams = new HashMap<QName,Object>();
      for (int i = 0, c = attributes.getLength(); i < c; i++) {
        QName qname = new QName(attributes.getURI(i),
            attributes.getLocalName(i));
        // filter src attribute
        if (new QName(XMLConstants.NULL_NS_URI, "src").equals(qname)) continue;
        iconf.xsltParams.put(qname, attributes.getValue(i));
      }
      super.begin(namespace, name, attributes);
    }
    
    @Override
    protected void setResult(Templates t) {
      final Object o = digester.peek();
      if (!(o instanceof IndexConfig)) throw new RuntimeException(
          "An XSLT tree is not allowed here!");
      final IndexConfig iconf = (IndexConfig) o;
      
      iconf.xslt = t;
    }
    
  }
  
  final class TemplateSaxRule extends TransformerSaxRule {
    
    @Override
    protected void initDocument() throws SAXException {
      destContentHandler.startPrefixMapping(XSL_PREFIX, XSL_NAMESPACE);
      
      AttributesImpl atts = new AttributesImpl();
      
      // generate prefixes to exclude (all currently defined; if they appear,
      // they will be explicitely defined by processor)
      StringBuilder excludePrefixes = new StringBuilder("#default ")
          .append(XSL_PREFIX);
      for (String prefix : ((ExtendedDigester) digester)
          .getCurrentAssignedPrefixes()) {
        if (!XMLConstants.DEFAULT_NS_PREFIX.equals(prefix)) excludePrefixes
            .append(' ').append(prefix);
      }
      atts.addAttribute(XMLConstants.NULL_NS_URI, "exclude-result-prefixes",
          "exclude-result-prefixes", CNAME, excludePrefixes.toString());
      
      // root tag
      atts.addAttribute(XMLConstants.NULL_NS_URI, "version", "version", CNAME,
          "1.0");
      destContentHandler.startElement(XSL_NAMESPACE, "stylesheet", XSL_PREFIX
          + ":stylesheet", atts);
      atts.clear();
      
      // register variables as params for template
      HashSet<QName> vars = new HashSet<QName>(
          de.pangaea.metadataportal.harvester.XPathResolverImpl.BASE_VARIABLES);
      for (VariableConfig v : xPathVariables)
        vars.add(v.name);
      for (QName name : vars) {
        // it is not clear why xalan does not allow a variable with no namespace
        // declared by a prefix that points to the empty namespace
        boolean nullNS = XMLConstants.NULL_NS_URI
            .equals(name.getNamespaceURI());
        if (nullNS) {
          atts.addAttribute(XMLConstants.NULL_NS_URI, "name", "name", CNAME,
              name.getLocalPart());
        } else {
          destContentHandler.startPrefixMapping("var", name.getNamespaceURI());
          atts.addAttribute(XMLConstants.NULL_NS_URI, "name", "name", CNAME,
              "var:" + name.getLocalPart());
        }
        destContentHandler.startElement(XSL_NAMESPACE, "param", XSL_PREFIX
            + ":param", atts);
        atts.clear();
        destContentHandler.endElement(XSL_NAMESPACE, "param", XSL_PREFIX
            + ":param");
        if (!nullNS) destContentHandler.endPrefixMapping("var");
      }
      
      // start a template
      atts.addAttribute(XMLConstants.NULL_NS_URI, "match", "match", CNAME, "/");
      destContentHandler.startElement(XSL_NAMESPACE, "template", XSL_PREFIX
          + ":template", atts);
      // atts.clear();
    }
    
    @Override
    protected void finishDocument() throws SAXException {
      destContentHandler.endElement(XSL_NAMESPACE, "template", XSL_PREFIX
          + ":template");
      destContentHandler.endElement(XSL_NAMESPACE, "stylesheet", XSL_PREFIX
          + ":stylesheet");
      destContentHandler.endPrefixMapping(XSL_PREFIX);
    }
    
    @Override
    protected void setResult(Templates t) {
      Object o = digester.peek();
      if (o instanceof ExpressionConfig) ((ExpressionConfig) o).setTemplate(t);
      else throw new RuntimeException("An XSLT template is not allowed here!");
    }
    
    private static final String XSL_NAMESPACE = "http://www.w3.org/1999/XSL/Transform";
    private static final String XSL_PREFIX = "int-tmpl-xsl";
    private static final String CNAME = "CNAME";
    
  }
  
}