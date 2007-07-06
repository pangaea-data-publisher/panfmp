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

package de.pangaea.metadataportal.utils;

import javax.xml.transform.sax.*;
import javax.xml.parsers.*;
import javax.xml.xpath.*;

public final class StaticFactories {

    private static org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(StaticFactories.class);

    private StaticFactories() {} // no instance

    public static final XPathFactory xpathFactory;
    public static final SAXTransformerFactory transFactory;
    public static final SAXParserFactory xinclSaxFactory;
    public static final DocumentBuilderFactory dbf;
    public static final DocumentBuilder dombuilder;
    static {
        try {
            xpathFactory=XPathFactory.newInstance();

            xinclSaxFactory=SAXParserFactory.newInstance();
            xinclSaxFactory.setNamespaceAware(true);
            xinclSaxFactory.setValidating(false);
            try {
                xinclSaxFactory.setXIncludeAware(true);
            } catch (UnsupportedOperationException e) {
                log.warn("Your XML parser is not XInclude aware! Please update it to a Java 1.5 compatible one for full functionality!");
            }

            transFactory=(SAXTransformerFactory)SAXTransformerFactory.newInstance();
            transFactory.setErrorListener(new LoggingErrorListener(transFactory.getClass()));

            dbf=DocumentBuilderFactory.newInstance();
            dbf.setNamespaceAware(true);
            dbf.setCoalescing(true);
            dbf.setExpandEntityReferences(true);
            dbf.setIgnoringComments(true);
            dbf.setIgnoringElementContentWhitespace(true);
            dombuilder = dbf.newDocumentBuilder();
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize XML components",e);
        }
    }

}