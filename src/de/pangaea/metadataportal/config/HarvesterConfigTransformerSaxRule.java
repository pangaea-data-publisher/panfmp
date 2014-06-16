package de.pangaea.metadataportal.config;

import java.util.HashMap;

import javax.xml.XMLConstants;
import javax.xml.namespace.QName;
import javax.xml.transform.Templates;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

final class HarvesterConfigTransformerSaxRule extends TransformerSaxRule {
  
  /**
   * @param config
   */
  HarvesterConfigTransformerSaxRule(Config config) {
    super(config);
  }

  @Override
  public void begin(String namespace, String name, Attributes attributes)
      throws Exception {
    final Object o = digester.peek();
    if (!(o instanceof HarvesterConfig)) throw new RuntimeException(
        "An XSLT tree is not allowed here!");
    final HarvesterConfig iconf = (HarvesterConfig) o;
    
    final String file = attributes.getValue(XMLConstants.NULL_NS_URI, "src");
    if (file != null) {
      setResult(this.config.loadTemplate(file));
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
    iconf.xsltParams = new HashMap<>();
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
    if (!(o instanceof HarvesterConfig)) throw new RuntimeException(
        "An XSLT tree is not allowed here!");
    final HarvesterConfig iconf = (HarvesterConfig) o;
    
    iconf.xslt = t;
  }
  
}