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

package de.pangaea.metadataportal.processor;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.Date;
import java.util.Map;
import java.util.Set;

import javax.xml.namespace.QName;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.sax.TransformerHandler;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Validator;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.w3c.dom.Document;
import org.w3c.dom.DocumentFragment;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.ContentHandler;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import de.pangaea.metadataportal.config.ExpressionConfig;
import de.pangaea.metadataportal.config.FieldConfig;
import de.pangaea.metadataportal.config.FilterConfig;
import de.pangaea.metadataportal.config.HarvesterConfig;
import de.pangaea.metadataportal.config.VariableConfig;
import de.pangaea.metadataportal.utils.BooleanParser;
import de.pangaea.metadataportal.utils.ISODateFormatter;
import de.pangaea.metadataportal.utils.KeyValuePairs;
import de.pangaea.metadataportal.utils.LenientDateParser;
import de.pangaea.metadataportal.utils.LoggingErrorListener;
import de.pangaea.metadataportal.utils.StaticFactories;
import de.pangaea.metadataportal.utils.XMLToKeyValuePairs;

/**
 * This class holds all information harvested and provides methods for
 * {@link DocumentProcessor} to create a {@link XContentBuilder} instance from it.
 * 
 * @author Uwe Schindler
 */
public class MetadataDocument {
  static final org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory
      .getLog(MetadataDocument.class);
  
  /**
   * Constructor, that creates an empty instance for the supplied index
   * configuration.
   */
  public MetadataDocument(HarvesterConfig iconfig) {
    this.iconfig = iconfig;
  }
  
  /**
   * "Harvests" a Elasticsearch {@link SearchHit} from index for re-parsing.
   * Extracts XML blob, identifier and datestamp from <code>Document</code>.
   * Stored fields are not restored. They are regenerated by re-executing all
   * XPath and Templates. {@link HarvesterConfig} is used for index specific
   * conversions.
   */
  public void loadFromElasticSearchHit(SearchHit hit) throws Exception {
    deleted = false;
    datestamp = null;
    // read identifier
    identifier = hit.getId();
    // try to read date stamp
    final SearchHitField dateFld = hit.field(iconfig.root.fieldnameDatestamp);
    final String datestampStr = (dateFld == null) ? null : dateFld.<String>getValue();
    if (datestampStr != null) {
      try {
        datestamp = XContentBuilder.defaultDatePrinter.parseDateTime(datestampStr).toDate();
      } catch (IllegalArgumentException iae) {
        log.warn("Datestamp of document '" + identifier + "' is invalid: " + iae.getMessage() + " - Deleting datestamp.");
        datestamp = null;
      }
    }
    // read XML
    final SearchHitField xmlFld = hit.field(iconfig.root.fieldnameXML);
    final String xml = (xmlFld == null) ? null : xmlFld.<String>getValue();
    if (xml == null) {
      setFinalDOM(null);
    } else {
      Document dom = StaticFactories.dombuilder.newDocument();
      StreamSource s = new StreamSource(new StringReader(xml), identifier);
      DOMResult r = new DOMResult(dom, identifier);
      Transformer trans = StaticFactories.transFactory.newTransformer();
      trans.setErrorListener(new LoggingErrorListener(log));
      trans.transform(s, r);
      setFinalDOM(dom);
    }
  }
  
  /**
   * Returns XML contents as String (a cache is used).
   */
  public String getXML() throws Exception {
    if (deleted || dom == null) return null;
    if (xmlCache != null) return xmlCache;
    
    // convert DOM
    StringWriter xmlWriter = new StringWriter();
    Transformer trans = StaticFactories.transFactory.newTransformer();
    trans.setErrorListener(new LoggingErrorListener(log));
    trans.setOutputProperty(OutputKeys.INDENT, "no");
    trans.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
    trans.setOutputProperty(OutputKeys.ENCODING, StandardCharsets.UTF_16.name());
    DOMSource in = new DOMSource(dom, identifier);
    StreamResult out = new StreamResult(xmlWriter);
    trans.transform(in, out);
    xmlWriter.close();
    return xmlCache = xmlWriter.toString();
  }
  
  /**
   * Sets XML final (transformed) xml contents as DOM tree. Invalidates cache.
   */
  public void setFinalDOM(Document dom) {
    this.dom = dom;
    xmlCache = null;
  }
  
  /**
   * Returns XML contents as DOM tree.
   */
  public Document getFinalDOM() {
    return dom;
  }
  
  /**
   * Returns a converter instance that does transformation and validation
   * according to index config.
   */
  public synchronized XMLConverter getConverter() {
    if (converter == null) converter = new XMLConverter();
    return converter;
  }
  
  /**
   * Marks a harvested document as deleted. A deleted document is not indexed
   * and will be explicitely deleted from index. A deleted document should not
   * contain XML data, if there is XML data it will be ignored.
   */
  public void setDeleted(boolean deleted) {
    this.deleted = deleted;
  }
  
  /**
   * Returns deletion status.
   * 
   * @see #setDeleted
   */
  public boolean isDeleted() {
    return deleted;
  }
  
  /**
   * Set the datestamp (last modification time of document file).
   */
  public void setDatestamp(Date datestamp) {
    this.datestamp = datestamp;
  }
  
  /**
   * @see #setDatestamp
   */
  public Date getDatestamp() {
    return datestamp;
  }
  
  /**
   * Set the document identifier.
   */
  public void setIdentifier(String identifier) {
    this.identifier = identifier;
  }
  
  /**
   * @see #setIdentifier
   */
  public String getIdentifier() {
    return identifier;
  }
  
  @Override
  public String toString() {
    return "identifier="
        + identifier
        + " deleted="
        + deleted
        + " datestamp="
        + ((datestamp != null) ? ISODateFormatter.formatLong(datestamp)
            : (String) null);
  }
  
  /**
   * Converts this instance to a Elasticsearch JSON node
   * 
   * @return {@link XContentBuilder} or <code>null</code>, if doc was deleted.
   * @throws Exception
   *           if an exception occurs during transformation (various types of
   *           exceptions can be thrown).
   * @throws IllegalStateException
   *           if index configuration is unknown
   */
  public KeyValuePairs getKeyValuePairs() throws Exception {
    final KeyValuePairs kv = createEmptyKeyValuePairs();
    if (!deleted) {
      assert kv != null;
      if (dom == null) throw new NullPointerException(
          "The DOM-Tree of document may not be 'null'!");
      processXPathVariables();
      try {
        boolean filterAccepted = processFilters();
        if (!filterAccepted) {
          log.debug("Document filtered: " + identifier);
          return null;
        }
        addFields(kv);
        finalizeKeyValuePairs(kv);
      } finally {
        XPathResolverImpl.getInstance().unsetVariables();
      }
    }
    return kv;
  }
  
  /**
   * Helper method that generates an empty {@link XContentBuilder} instance. The
   * standard fields are set to the doc properties (identifier, datestamp)
   * 
   * @return {@link XContentBuilder} or <code>null</code>, if doc was deleted.
   * @throws Exception
   *           if an exception occurs during transformation (various types of
   *           exceptions can be thrown).
   * @throws IllegalStateException
   *           if identifier is empty.
   */
  protected KeyValuePairs createEmptyKeyValuePairs() throws Exception {
    // make a new, empty document
    if (deleted) {
      return null; // to delete
    } else {
      KeyValuePairs kv = new KeyValuePairs();
      kv.add(iconfig.root.fieldnameSource, iconfig.id);
      if (datestamp != null) {
        kv.add(iconfig.root.fieldnameDatestamp, datestamp);
      }
      return kv;
    }
  }
  
  /**
   * Helper method that finalizes the JSON document
   */
  protected void finalizeKeyValuePairs(KeyValuePairs kv) throws Exception {
    kv.add(iconfig.root.fieldnameXML, this.getXML());
  }
  
  /**
   * Helper method that adds all fields to the given {@link XContentBuilder}
   * instance. This method executes all XPath/Templates and converts the
   * results.
   * 
   * @throws Exception
   *           if an exception occurs during transformation (various types of
   *           exceptions can be thrown).
   */
  protected void addFields(KeyValuePairs kv) throws Exception {
    for (FieldConfig f : iconfig.root.fields.values()) {
      if (f.datatype == FieldConfig.DataType.XHTML) {
        addField(kv, f, evaluateTemplateAsXHTML(f));
      } else {
        boolean needDefault = (f.datatype == FieldConfig.DataType.NUMBER || f.datatype == FieldConfig.DataType.INTEGER || f.datatype == FieldConfig.DataType.DATETIME);
        Object value = null;
        if (f.xPathExpr != null) {
          try {
            // First: try to get XPath result as Nodelist if that fails (because
            // result is #STRING): fallback
            // TODO: Looking for a better system to detect return type of XPath
            // :-( [slowdown by this?]
            value = f.xPathExpr.evaluate(dom, XPathConstants.NODESET);
          } catch (XPathExpressionException ex) {
            // Fallback: if return type of XPath is a #STRING (for example from
            // a substring() routine)
            value = f.xPathExpr.evaluate(dom, XPathConstants.STRING);
          }
        } else if (f.xslt != null) {
          value = evaluateTemplate(f);
        } else {
          throw new NullPointerException("Both XPath and template are NULL for field " + f.name);
        }
        
        // interpret result
        if (value instanceof NodeList) {
          final NodeList nodes = (NodeList) value;
          final int c = nodes.getLength();
          for (int i = 0; i < c; i++) {
            final Node nod = nodes.item(i);
            switch (f.datatype) {
              case XML:
                if (nod.getNodeType() != Node.ELEMENT_NODE)
                  continue;
                final DOMSource in = new DOMSource(nodes.item(i));
                final StringWriter xmlWriter = new StringWriter();
                final StreamResult out = new StreamResult(xmlWriter);
                final Transformer trans = StaticFactories.transFactory.newTransformer();
                trans.setErrorListener(new LoggingErrorListener(log));
                trans.setOutputProperty(OutputKeys.INDENT, "no");
                trans.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
                trans.setOutputProperty(OutputKeys.ENCODING, StandardCharsets.UTF_16.name());
                trans.transform(in, out);
                xmlWriter.close();
                addField(kv, f, xmlWriter.toString());
                break;
              case JSON:
                if (nod.getNodeType() != Node.ELEMENT_NODE)
                  continue;
                // we need to do this, otherwise may get adjacent text nodes (e.g. for XSL docfrags):
                nod.normalize();
                final Object o = new XMLToKeyValuePairs(true).convertChilds(nod);
                if (o != null) {
                  if (log.isTraceEnabled()) log.trace("AddField: " + f.name + '=' + o);
                  kv.add(f.name, o);
                }
                break;
              default:
                final StringBuilder sb = new StringBuilder();
                walkNodeTexts(sb, nod, true);
                final String val = sb.toString().trim();
                if (!val.isEmpty()) {
                  addField(kv, f, val);
                  needDefault = false;
                }
            }
          }
        } else if (value instanceof String) {
          switch (f.datatype) {
            case XML:
              throw new UnsupportedOperationException(
                  "Fields with datatype XML may only return NODESETs on evaluation!");
            case JSON:
              throw new UnsupportedOperationException(
                  "Fields with datatype JSON may only return NODESETs on evaluation!");
            default:
              String s = (String) value;
              s = s.trim();
              if (!s.isEmpty()) {
                addField(kv, f, s);
                needDefault = false;
              }
          }
        } else {
          throw new UnsupportedOperationException("Invalid Java data type of expression result: " + value.getClass().getName());
        }
        
        if (needDefault && f.defaultValue != null) addField(kv, f,
            f.defaultValue);
      }
    }
  }
    
  /**
   * Helper method that evaluates all filters. This method executes the XPath
   * and converts the results to a boolean. The results of all filters are
   * combined according to the ACCEPT/DENY type.
   * 
   * @throws Exception
   *           if an exception occurs during transformation (various types of
   *           exceptions can be thrown).
   */
  protected boolean processFilters() throws Exception {
    boolean accept = (iconfig.root.filterDefault == FilterConfig.FilterType.ACCEPT);
    for (FilterConfig f : iconfig.root.filters) {
      if (f.xPathExpr == null) throw new NullPointerException(
          "Filters need to contain a XPath expression, which is NULL!");
      Boolean b = (Boolean) f.xPathExpr.evaluate(dom, XPathConstants.BOOLEAN);
      if (b == null) throw new XPathExpressionException(
          "The filter XPath did not return a valid BOOLEAN value!");
      if (b && log.isTraceEnabled()) log.trace("FilterMatch: " + f);
      switch (f.type) {
        case ACCEPT:
          if (b) accept = true;
          break;
        case DENY:
          if (b) accept = false;
          break;
        default:
          throw new AssertionError("Invalid filter type (should never happen!)");
      }
    }
    return accept;
  }
  
  /**
   * Helper method to register all standard variables for the XPath/Templates
   * evaluation. Overwrite this method to register any special variables
   * dependent on the <code>MetadataDocument</code> implementation. The
   * variables must be registered in the supplied {@link Map}.
   */
  protected void addSystemVariables(Map<QName,Object> vars) {
    if (identifier == null || iconfig == null || iconfig.id == null) throw new NullPointerException();
    vars.put(XPathResolverImpl.VARIABLE_HARVESTER_ID, iconfig.id);
    vars.put(XPathResolverImpl.VARIABLE_DOC_IDENTIFIER, identifier);
    vars.put(XPathResolverImpl.VARIABLE_DOC_DATESTAMP, (datestamp == null) ? ""
        : ISODateFormatter.formatLong(datestamp));
  }
  
  /**
   * Helper method to process all user supplied variables for the
   * XPath/Templates evaluation. The variables are stored in thread local
   * storage.
   * 
   * @throws Exception
   *           if an exception occurs during transformation (various types of
   *           exceptions can be thrown).
   */
  protected final void processXPathVariables() throws Exception {
    // put map of variables in thread local storage
    boolean needCleanup = true;
    Map<QName,Object> data = XPathResolverImpl.getInstance().initVariables();
    try {
      addSystemVariables(data);
      
      // variables in config
      for (VariableConfig f : iconfig.root.xPathVariables) {
        Object value = null;
        if (f.xPathExpr != null) {
          try {
            // First: try to get XPath result as Nodelist if that fails (because
            // result is #STRING): fallback
            // TODO: Looking for a better system to detect return type of XPath
            // :-( [slowdown by this?]
            value = f.xPathExpr.evaluate(dom, XPathConstants.NODESET);
          } catch (XPathExpressionException ex) {
            // Fallback: if return type of XPath is a #STRING (for example from
            // a substring() routine)
            value = f.xPathExpr.evaluate(dom, XPathConstants.STRING);
          }
        } else if (f.xslt != null) {
          value = evaluateTemplate(f);
        } else throw new NullPointerException(
            "Both XPath and template are NULL for variable " + f.name);
        
        if (value != null) {
          if (log.isTraceEnabled()) log.trace("Variable: " + f.name + "="
              + value);
          data.put(f.name, value);
        }
      }
      
      needCleanup = false;
    } finally {
      // we need to cleanup on any Exception to keep config in consistent state
      if (needCleanup) XPathResolverImpl.getInstance().unsetVariables();
    }
  }
  
  /**
   * Helper method to evaluate a template. This method is called by variables
   * and fields, when a template is used instead of a XPath.
   * <P>
   * For internal use only!
   */
  protected NodeList evaluateTemplate(ExpressionConfig expr)
      throws TransformerException {
    Transformer trans = expr.xslt.newTransformer();
    trans.setErrorListener(new LoggingErrorListener(log));
    
    // set variables in transformer
    Map<QName,Object> vars = XPathResolverImpl.getInstance()
        .getCurrentVariableMap();
    for (Map.Entry<QName,Object> entry : vars.entrySet()) {
      trans.setParameter(entry.getKey().toString(), entry.getValue());
    }
    
    // transform
    DocumentFragment df = dom.createDocumentFragment();
    trans.transform(new DOMSource(dom, identifier), new DOMResult(df));
    return df.getChildNodes();
  }
  
  /**
   * Helper method to evaluate a template and return result as XHTML. This
   * method is called by fields with datatype XHTML.
   * <P>
   * For internal use only!
   */
  protected String evaluateTemplateAsXHTML(FieldConfig expr)
      throws TransformerException, java.io.IOException {
    if (expr.datatype != FieldConfig.DataType.XHTML) throw new IllegalArgumentException(
        "Datatype must be XHTML for evaluateTemplateAsXHTML()");
    Transformer trans = expr.xslt.newTransformer();
    trans.setErrorListener(new LoggingErrorListener(log));
    trans.setOutputProperty(OutputKeys.METHOD, "xml");
    trans.setOutputProperty(OutputKeys.DOCTYPE_PUBLIC,
        "-//W3C//DTD XHTML 1.0 Transitional//EN");
    trans.setOutputProperty(OutputKeys.INDENT, "no");
    trans.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
    trans.setOutputProperty(OutputKeys.ENCODING, StandardCharsets.UTF_16.name());
    
    // set variables in transformer
    Map<QName,Object> vars = XPathResolverImpl.getInstance()
        .getCurrentVariableMap();
    for (Map.Entry<QName,Object> entry : vars.entrySet()) {
      trans.setParameter(entry.getKey().toString(), entry.getValue());
    }
    
    StringWriter xmlWriter = new StringWriter();
    StreamResult out = new StreamResult(xmlWriter);
    trans.transform(new DOMSource(dom, identifier), out);
    xmlWriter.close();
    return xmlWriter.toString();
  }
  
  /**
   * Helper method to walk through a DOM tree node (n) and collect strings.
   * <P>
   * For internal use only!
   */
  protected void walkNodeTexts(StringBuilder sb, Node n, boolean topLevel) {
    if (n == null) return;
    switch (n.getNodeType()) {
      case Node.ELEMENT_NODE:
      case Node.DOCUMENT_NODE:
      case Node.DOCUMENT_FRAGMENT_NODE:
        for (Node nod = n.getFirstChild(); nod != null; nod = nod.getNextSibling()) {
          walkNodeTexts(sb, nod, false);
          sb.append('\n');
        }
        break;
      case Node.ATTRIBUTE_NODE:
        // This is special: Attributes are normally not converted to String,
        // only if the XPath goes directly to the attribute
        // If this is the case the Attribute is topLevel in the recursion!
        if (!topLevel) break;
        // fall-through
      case Node.TEXT_NODE:
      case Node.CDATA_SECTION_NODE:
        sb.append(n.getNodeValue());
        break;
    }
  }
  
  /**
   * Helper method to add a field in the correct format to given {@link XContentBuilder}.
   * The format is defined by the {@link FieldConfig}. The
   * value is given as string.
   * <P>
   * For internal use only!
   * 
   * @throws Exception
   *           if an exception occurs during transformation (various types of
   *           exceptions can be thrown).
   */
  protected void addField(KeyValuePairs kv, FieldConfig f, String val)
      throws Exception {
    if (log.isTraceEnabled()) log.trace("AddField: " + f.name + '=' + val);
    switch (f.datatype) {
      case DATETIME:
        kv.add(f.name, LenientDateParser.parseDate(val));
        break;
      case NUMBER:
        kv.add(f.name, Double.parseDouble(val));
        break;
      case INTEGER:
        kv.add(f.name, Long.parseLong(val));
        break;
      case STRING:
      case XHTML: // we must support this, the parameter is already formatted html as string!
      case XML: // we must support this, the parameter is already formatted xml as string!
        kv.add(f.name, val);
        break;
      default:
        throw new AssertionError("Invalid field datatype for addField(): " + f.datatype);
    }
  }
  
  /**
   * @see #setDeleted
   */
  protected boolean deleted = false;
  
  /**
   * @see #setDatestamp
   */
  protected Date datestamp = null;
  
  /**
   * @see #setIdentifier
   */
  protected String identifier = null;
  
  /**
   * The harvester configuration.
   */
  protected final HarvesterConfig iconfig;
  
  private Document dom = null;
  private String xmlCache = null;
  private XMLConverter converter = null;
  
  /**
   * This class handles the transformation from any source to the "official"
   * metadata format and can even validate it
   * 
   * @author Uwe Schindler
   */
  public class XMLConverter {
    
    private final boolean validate;
    
    XMLConverter() {
      String v = iconfig.properties.getProperty("validate");
      if (iconfig.root.schema == null) {
        if (v != null) throw new IllegalStateException(
            "The <validate> harvester property is only allowed if a XML schema is set in the metadata properties!");
        validate = false; // no validation if no schema available
      } else {
        if (v == null) validate = true; // validate by default
        else validate = BooleanParser.parseBoolean(v);
      }
    }
    
    private DOMResult validate(final DOMSource ds, final boolean wasTransformed)
        throws SAXException, IOException {
      if (!validate) {
        return DOMSource2Result(ds);
      } else {
        if (log.isDebugEnabled()) log.debug("Validating '" + ds.getSystemId()
            + "'...");
        Validator val = iconfig.root.schema.newValidator();
        val.setErrorHandler(new ErrorHandler() {
          @Override
          public void warning(SAXParseException e) throws SAXException {
            log.warn("Validation warning in "
                + (wasTransformed ? "XSL transformed " : "") + "document '"
                + ds.getSystemId() + "': " + e.getMessage());
          }
          
          @Override
          public void error(SAXParseException e) throws SAXException {
            String msg = "Validation error in "
                + (wasTransformed ? "XSL transformed " : "") + "document '"
                + ds.getSystemId() + "': " + e.getMessage();
            if (iconfig.root.haltOnSchemaError) throw new SAXException(msg);
            log.error(msg);
          }
          
          @Override
          public void fatalError(SAXParseException e) throws SAXException {
            throw new SAXException("Fatal validation error in "
                + (wasTransformed ? "XSL transformed " : "") + "document '"
                + ds.getSystemId() + "': " + e.getMessage());
          }
        });
        DOMResult dr = (iconfig.root.validateWithAugmentation) ? emptyDOMResult(ds
            .getSystemId()) : null;
        val.validate(ds, dr);
        return (dr == null) ? DOMSource2Result(ds) : dr;
      }
    }
    
    private void setTransformerProperties(final Transformer trans)
        throws TransformerException {
      trans.setErrorListener(new LoggingErrorListener(log));
      // create a Map view on the transformer properties
      final Map<QName,Object> paramMap = new AbstractMap<QName,Object>() {
        @Override
        public Set<Map.Entry<QName,Object>> entrySet() {
          throw new UnsupportedOperationException(); // should never be called
        }
        
        @Override
        public Object put(QName key, Object value) {
          trans.setParameter(key.toString(), value);
          return null; // dummy
        }
      };
      // set variables
      addSystemVariables(paramMap);
      // set additional variables from <cfg:transform/> attributes
      paramMap.putAll(iconfig.xsltParams);
    }
    
    private final DOMSource DOMResult2Source(DOMResult dr) {
      return new DOMSource(dr.getNode(), dr.getSystemId());
    }
    
    private final DOMResult DOMSource2Result(DOMSource ds) {
      return new DOMResult(ds.getNode(), ds.getSystemId());
    }
    
    private final DOMResult emptyDOMResult(String systemId) {
      return new DOMResult(StaticFactories.dombuilder.newDocument(), systemId);
    }
    
    // Transforms a Source to a DOM w/wo transformation
    public void transform(Source s) throws TransformerException, SAXException,
        IOException {
      DOMResult dr;
      if (iconfig.xslt == null && s instanceof DOMSource) {
        dr = DOMSource2Result((DOMSource) s);
        dr.setSystemId(identifier);
      } else {
        if (log.isDebugEnabled()) log.debug("XSL-Transforming '"
            + s.getSystemId() + "' to '" + identifier + "'...");
        Transformer trans = (iconfig.xslt == null) ? StaticFactories.transFactory
            .newTransformer() : iconfig.xslt.newTransformer();
        setTransformerProperties(trans);
        dr = emptyDOMResult(identifier);
        trans.transform(s, dr);
      }
      Document dom = (Document) (validate(
          DOMResult2Source(dr), iconfig.xslt != null).getNode());
      dom.normalize();
      setFinalDOM(dom);
    }
    
    // ContentHandler part (gets events and converts it to DOM w/wo
    // transformation)
    private DOMResult dr = null;
    
    public ContentHandler getTransformContentHandler()
        throws TransformerException {
      if (dr != null) throw new IllegalStateException(
          "XMLConverter is currently convertig a SAX document, you cannot get a new ContentHandler!");
      
      if (iconfig.xslt != null && log.isDebugEnabled()) log
          .debug("XSL-Transforming '" + identifier + "'...");
      
      TransformerHandler handler = (iconfig.xslt == null) ? StaticFactories.transFactory
          .newTransformerHandler() : StaticFactories.transFactory
          .newTransformerHandler(iconfig.xslt);
      setTransformerProperties(handler.getTransformer());
      dr = emptyDOMResult(identifier);
      handler.setResult(dr);
      return handler;
    }
    
    public void finishTransformation() throws TransformerException,
        SAXException, IOException {
      if (dr == null) throw new IllegalStateException(
          "XMLConverter is not convertig a SAX document, you cannot get a result DOM tree!");
      
      Document dom = (Document) (validate(
          DOMResult2Source(dr), iconfig.xslt != null).getNode());
      dom.normalize();
      dr = null;
      setFinalDOM(dom);
    }
    
  }
}