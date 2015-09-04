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

package de.pangaea.metadataportal.utils;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.xpath.XPathFactory;

import org.apache.xalan.xsltc.trax.TransformerFactoryImpl;
import org.apache.xerces.jaxp.DocumentBuilderFactoryImpl;
import org.apache.xerces.jaxp.SAXParserFactoryImpl;
import org.apache.xpath.jaxp.XPathFactoryImpl;

/**
 * Some pre-allocated XML factories.
 * 
 * @author Uwe Schindler
 */
public final class StaticFactories {
  
  private StaticFactories() {} // no instance
  
  public static final XPathFactory xpathFactory;
  public static final SAXTransformerFactory transFactory;
  public static final SAXParserFactory saxFactory;
  public static final DocumentBuilderFactory dbf;
  public static final DocumentBuilder dombuilder;
  static {
    xpathFactory = new XPathFactoryImpl();
    
    saxFactory = new SAXParserFactoryImpl();
    saxFactory.setNamespaceAware(true);
    saxFactory.setValidating(false);
    
    transFactory = new TransformerFactoryImpl();
    transFactory.setErrorListener(new LoggingErrorListener(transFactory
        .getClass()));
    
    dbf = new DocumentBuilderFactoryImpl();
    dbf.setNamespaceAware(true);
    dbf.setCoalescing(true);
    dbf.setExpandEntityReferences(true);
    dbf.setIgnoringComments(true);
    dbf.setIgnoringElementContentWhitespace(true);
    try {
      dombuilder = dbf.newDocumentBuilder();
    } catch (ParserConfigurationException e) {
      throw new Error("Failed to initialize DOM document builder", e);
    }
  }
  
}