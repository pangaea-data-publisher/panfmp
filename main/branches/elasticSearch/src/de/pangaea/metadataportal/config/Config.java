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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.CookieHandler;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import javax.xml.XMLConstants;
import javax.xml.transform.Templates;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.apache.commons.digester.AbstractObjectCreationFactory;
import org.apache.commons.digester.ExtendedBaseRules;
import org.apache.commons.digester.SetPropertiesRule;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

import de.pangaea.metadataportal.Package;
import de.pangaea.metadataportal.utils.BooleanParser;
import de.pangaea.metadataportal.utils.ExtendedDigester;
import de.pangaea.metadataportal.utils.HostAndPort;
import de.pangaea.metadataportal.utils.PublicForDigesterUse;
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
  
  public Config(String file) throws Exception {
    this.file = file;
    
    log.info(Package.getFullPackageDescription());
    
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
      dig.addRule("config/metadata/variables/variable-template", new TemplateSaxRule(this));
      
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
      
      // special purpose fields
      for (final String type : Arrays.asList(
          "xml-field", "source-field", "datestamp-field", "mdoc-impl-field"
      )) {
        dig.addCallMethod("config/metadata/fields/" + type, "setSpecialField", 2);
        dig.addObjectParam("config/metadata/fields/" + type, 0, type);
        dig.addCallParam("config/metadata/fields/" + type, 1, "name");
      }
      
      // XPath / template fields
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
      dig.addRule("config/metadata/fields/field-template", new TemplateSaxRule(this));
      
      // transform
      /*
       * dig.addRule("config/metadata/transformBeforeXPath", HarvesterConfigTransformerSaxRule());
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
      
      // typeName
      dig.addDoNothing("config/metadata/elasticsearchMapping");
      dig.addCallMethod("config/metadata/elasticsearchMapping/typeName", "setTypeName", 0);
      dig.addCallMethod("config/metadata/elasticsearchMapping/file", "setEsMappingFile", 0);
      
      // *** HARVESTER CONFIG ***
      dig.addDoNothing("config/sources");
      
      // *** GLOBAL HARVESTER PROPERTIES ***
      dig.addDoNothing("config/sources/globalProperties");
      dig.addCallMethod("config/sources/globalProperties/*",
          "addGlobalHarvesterProperty", 0);
      
      // *** CONFIG FOR EACH HARVESTER ***
      dig.addFactoryCreate("config/sources/harvester",
          new AbstractObjectCreationFactory() {
            @Override
            public Object createObject(Attributes attributes) {
              return new HarvesterConfig(Config.this);
            }
          });
      dig.addSetNext("config/sources/harvester", "addHarvester");
      dig.addCallMethod("config/sources/harvester", "setId", 1);
      dig.addCallParam("config/sources/harvester", 0, "id");
      
      dig.addCallMethod("config/sources/harvester/class",
          "setHarvesterClass", 0);
      
      dig.addRule(
          "config/sources/harvester/transform",
          new HarvesterConfigTransformerSaxRule(this));
      
      dig.addDoNothing("config/sources/harvester/properties");
      dig.addCallMethod("config/sources/harvester/properties/*",
          "addHarvesterProperty", 0);
            
      // *** ELASTICSEARCH TransportClient settings ***
      dig.addDoNothing("config/elasticsearchCluster");
      dig.addCallMethod("config/elasticsearchCluster/address", "addEsAddress", 0);
      dig.addFactoryCreate("config/elasticsearchCluster/settings",
          new AbstractObjectCreationFactory() {
            @Override
            public Object createObject(Attributes attributes) {
              return ImmutableSettings.settingsBuilder();
            }
          });
      dig.addSetNext("config/elasticsearchCluster/settings", "setEsSettings");
      dig.addCallMethod("config/elasticsearchCluster/settings/*", "put", 2);
      dig.addCallParamPath("config/elasticsearchCluster/settings/*", 0);
      dig.addCallParam("config/elasticsearchCluster/settings/*", 1);
      
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
    // consistency in harvesters:
    for (HarvesterConfig iconf : harvesteres.values())
      iconf.check();
    
    // cleanup
    templatesCache.clear();
  }
  
  /**
   * makes the given local filesystem path absolute and resolve it relative to
   * config directory
   **/
  public final String makePathAbsolute(String file) throws IOException {
    return makePathAbsolute(file, false);
  }
  
  /**
   * makes the given local filesystem path or URL absolute and resolve it
   * relative to config directory (if local)
   **/
  public String makePathAbsolute(String file, boolean allowURL) throws IOException {
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
    if (de.pangaea.metadataportal.processor.XPathResolverImpl.DOCUMENT_PROCESSOR_NAMESPACE
        .equals(f.name.getNamespaceURI())) throw new IllegalArgumentException(
        "A XPath variable name may not be in the namespace for internal variables ('"
            + de.pangaea.metadataportal.processor.XPathResolverImpl.DOCUMENT_PROCESSOR_NAMESPACE
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
  
  public void addHarvester(HarvesterConfig i) {
    if (harvesteres.containsKey(i.id)) throw new IllegalArgumentException(
        "There is already a harvester with id=\"" + i.id
            + "\" added to configuration!");
    harvesteres.put(i.id, i);
  }
  
  @PublicForDigesterUse
  @Deprecated
  public void setSpecialField(String type, String name) {
    switch (type) {
      case "xml-field":
        fieldnameXML = name;
        break;
      case "source-field":
        fieldnameSource = name;
        break;
      case "datestamp-field":
        fieldnameDatestamp = name;
      case "mdoc-impl-field":
        fieldnameMdocImpl = name;
        break;
      default:
        throw new IllegalArgumentException("Invalid special field type: " + type);
    }
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
  public void setTypeName(String v) {
    typeName = v;
  }
  
  @PublicForDigesterUse
  @Deprecated
  public void setEsMappingFile(String v) throws IOException {
    if (esMappingFile != null)
      throw new IllegalArgumentException("Duplicate Elasticsearch mapping file");
    esMappingFile = new File(makePathAbsolute(v, true));
    try (
        final InputStream in = new FileInputStream(esMappingFile);
        final Reader reader = new InputStreamReader(in, StandardCharsets.UTF_8)
    ) {
      final StringBuilder sb = new StringBuilder();
      final char[] buf = new char[8192];
      for(;;) {
        final int read = reader.read(buf);
        if (read <= 0) break;
        sb.append(buf, 0, read);
      }
      esMapping = sb.toString();
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
  
  @PublicForDigesterUse
  @Deprecated
  public void addEsAddress(String v) {
    esTransports.add(new InetSocketTransportAddress(HostAndPort.parse(v.trim(), 9300)));
  }
  
  @PublicForDigesterUse
  @Deprecated
  public void setEsSettings(Settings.Builder bld) {
    if (esSettings != null)
      throw new IllegalArgumentException("Duplicate elasticsearchCluster/settings element");
    // strip the XML matcher path:
    esSettings = bld.build().getByPrefix(dig.getMatch() + "/");
  }
  
  // get configuration infos
  
  Templates loadTemplate(String file) throws Exception {
    file = makePathAbsolute(file, true);
    Templates templ = templatesCache.get(file);
    if (templ == null) {
      log.info("Loading XSL transformation from '" + file + "'...");
      templatesCache.put(
          file,
          templ = StaticFactories.transFactory.newTemplates(new StreamSource(file))
      );
    }
    return templ;
  }
  
  // harvesters
  public final Map<String,HarvesterConfig> harvesteres = new LinkedHashMap<String,HarvesterConfig>();
  
  // metadata mapping name
  public String typeName = "doc";
  public File esMappingFile = null;
  public String esMapping = null;
  
  // special fields
  public String fieldnameXML = "xml";  
  public String fieldnameSource = "internal-source";
  public String fieldnameDatestamp = "internal-datestamp";
  public String fieldnameMdocImpl = "internal-mdoc-impl";


  // fields
  public final Map<String,FieldConfig> fields = new LinkedHashMap<String,FieldConfig>();
  
  // filters
  public FilterConfig.FilterType filterDefault = FilterConfig.FilterType.ACCEPT;
  public final List<FilterConfig> filters = new ArrayList<FilterConfig>();
  
  // variables
  public final List<VariableConfig> xPathVariables = new ArrayList<VariableConfig>();
  
  // schema etc
  public Schema schema = null;
  public boolean haltOnSchemaError = false, validateWithAugmentation = true;
  
  // TransportClient settings
  public final List<InetSocketTransportAddress> esTransports = new ArrayList<InetSocketTransportAddress>();
  public Settings esSettings = null;
  
  /* public Templates xsltBeforeXPath=null; */
  
  // Template cache
  private final Map<String,Templates> templatesCache = new HashMap<String,Templates>();
  
  public final Properties globalHarvesterProperties = new Properties();
    
  public String file;
  
  protected ExtendedDigester dig = null;
  
  public static final int DEFAULT_MAX_CLAUSE_COUNT = 131072;
  
}