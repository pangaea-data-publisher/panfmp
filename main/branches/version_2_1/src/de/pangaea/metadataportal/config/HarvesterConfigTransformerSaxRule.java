package de.pangaea.metadataportal.config;

import javax.xml.XMLConstants;
import javax.xml.namespace.QName;
import javax.xml.transform.Templates;

import org.xml.sax.Attributes;

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
    
    // collect all params to additionally pass to XSL and store in Map
    for (int i = 0, c = attributes.getLength(); i < c; i++) {
      QName qname = new QName(attributes.getURI(i), attributes.getLocalName(i));
      // filter src attribute
      if (new QName(XMLConstants.NULL_NS_URI, "src").equals(qname)) continue;
      iconf.xsltParams.put(qname, attributes.getValue(i));
    }
    super.begin(namespace, name, attributes);
  }
  
  @Override
  protected void setResult(Templates t) {
    final Object o = digester.peek();
    if (!(o instanceof HarvesterConfig)) throw new RuntimeException("An XSLT tree is not allowed here!");
    final HarvesterConfig iconf = (HarvesterConfig) o;
    
    iconf.xslt = t;
  }
  
}