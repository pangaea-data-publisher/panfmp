package de.pangaea.metadataportal.config;

import javax.xml.transform.Templates;
import javax.xml.transform.sax.TemplatesHandler;

import org.xml.sax.Attributes;

import de.pangaea.metadataportal.utils.SaxRule;
import de.pangaea.metadataportal.utils.StaticFactories;

abstract class TransformerSaxRule extends SaxRule {

  protected final Config config;

  /**
   * @param config
   */
  TransformerSaxRule(Config config) {
    this.config = config;
  }

  private TemplatesHandler th = null;
  
  @Override
  public void begin(String namespace, String name, Attributes attributes)
      throws Exception {
    if (getContentHandler() == null) {
      th = StaticFactories.transFactory.newTemplatesHandler();
      th.setSystemId(this.config.file);
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