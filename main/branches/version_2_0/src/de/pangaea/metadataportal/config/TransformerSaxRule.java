package de.pangaea.metadataportal.config;

import javax.xml.XMLConstants;
import javax.xml.transform.Templates;
import javax.xml.transform.sax.TemplatesHandler;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

import de.pangaea.metadataportal.utils.SaxRule;
import de.pangaea.metadataportal.utils.StaticFactories;

abstract class TransformerSaxRule extends SaxRule {

  protected final Config config;
  protected boolean hasBody = false;

  /**
   * @param config
   */
  TransformerSaxRule(Config config) {
    this.config = config;
  }

  private TemplatesHandler th = null;
  
  @Override
  public void begin(String namespace, String name, Attributes attributes) throws Exception {
    if (getContentHandler() == null) {
      final String file = attributes.getValue(XMLConstants.NULL_NS_URI, "src");
      if (file != null) {
        setResult(this.config.loadTemplate(file));
        th = null;
        hasBody = false;
        setContentHandler(new org.xml.sax.helpers.DefaultHandler() {
          @Override
          public void startElement(String namespaceURI, String localName, String qName, Attributes atts) throws SAXException {
            throw new SAXException(
                "No element content allowed here. You can either include an XSL template directly into the config file or use the 'src' attribute!");
          }
          
          @Override
          public void characters(char[] ch, int start, int length) throws SAXException {
            for (int i = 0; i < length; i++) {
              if (Character.isWhitespace(ch[start + i])) continue;
              throw new SAXException(
                  "No element content allowed here. You can either include an XSL template directly into the config file or use the 'src' attribute!");
            }
          }
        });
      } else {
        th = StaticFactories.transFactory.newTemplatesHandler();
        th.setSystemId(this.config.file.toUri().toASCIIString());
        hasBody = true;
        setContentHandler(th);
      }
    } else {
      throw new SAXException("Invalid state of SAX parser, content handler already set: " + getContentHandler());
    }
    super.begin(namespace, name, attributes);
  }
  
  protected abstract void setResult(Templates t);
  
  @Override
  public void end(String namespace, String name) throws Exception {
    super.end(namespace, name);
    if (th != null) setResult(th.getTemplates());
    setContentHandler(th = null);
    hasBody = false;
  }
  
}