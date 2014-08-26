package de.pangaea.metadataportal.config;

import java.util.HashSet;

import javax.xml.XMLConstants;
import javax.xml.namespace.QName;
import javax.xml.transform.Templates;

import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

import de.pangaea.metadataportal.utils.ExtendedDigester;

final class TemplateSaxRule extends TransformerSaxRule {
  
  /**
   * @param config
   */
  TemplateSaxRule(Config config) {
    super(config);
  }

  @Override
  protected void initDocument() throws SAXException {
    if (!hasBody) return;
    
    destContentHandler.startPrefixMapping(XSL_PREFIX, XSL_NAMESPACE);
    
    AttributesImpl atts = new AttributesImpl();
    
    // generate prefixes to exclude (all currently defined; if they appear,
    // they will be explicitely defined by processor)
    StringBuilder excludePrefixes = new StringBuilder("#default ").append(XSL_PREFIX);
    for (String prefix : ((ExtendedDigester) digester).getCurrentAssignedPrefixes()) {
      if (!XMLConstants.DEFAULT_NS_PREFIX.equals(prefix)) {
        excludePrefixes.append(' ').append(prefix);
      }
    }
    atts.addAttribute(XMLConstants.NULL_NS_URI, "exclude-result-prefixes",
        "exclude-result-prefixes", CNAME, excludePrefixes.toString());
    
    // root tag
    atts.addAttribute(XMLConstants.NULL_NS_URI, "version", "version", CNAME, "1.0");
    destContentHandler.startElement(XSL_NAMESPACE, "stylesheet", XSL_PREFIX + ":stylesheet", atts);
    atts.clear();
    
    // register variables as params for template
    HashSet<QName> vars = new HashSet<>(
        de.pangaea.metadataportal.processor.XPathResolverImpl.BASE_VARIABLES);
    for (VariableConfig v : this.config.xPathVariables)
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
      destContentHandler.startElement(XSL_NAMESPACE, "param", XSL_PREFIX + ":param", atts);
      atts.clear();
      destContentHandler.endElement(XSL_NAMESPACE, "param", XSL_PREFIX + ":param");
      if (!nullNS) destContentHandler.endPrefixMapping("var");
    }
    
    // start a template
    atts.addAttribute(XMLConstants.NULL_NS_URI, "match", "match", CNAME, "/");
    destContentHandler.startElement(XSL_NAMESPACE, "template", XSL_PREFIX + ":template", atts);
    atts.clear();
  }
  
  @Override
  protected void finishDocument() throws SAXException {
    if (!hasBody) return;
    
    destContentHandler.endElement(XSL_NAMESPACE, "template", XSL_PREFIX + ":template");
    destContentHandler.endElement(XSL_NAMESPACE, "stylesheet", XSL_PREFIX + ":stylesheet");
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