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

import java.util.*;
import org.apache.commons.collections.ArrayStack;
import de.pangaea.metadataportal.utils.*;
import org.xml.sax.helpers.XMLFilterImpl;
import org.xml.sax.*;

/***
This class is used as a rule for included documents inside a Digester-Input
Whenever this element occurs in digester, begin/end will be called, that then puts all further SAX events to the specified ContentHandler
***/

public class SaxRule extends org.apache.commons.digester.Rule {

    protected ContentHandler destContentHandler=null;
    protected Set<String> excludeNamespaces=Collections.<String>emptySet();
    private org.xml.sax.ContentHandler lastContentHandler=null;

    public static SaxRule emptyRule() {
        SaxRule sr=new SaxRule();
        sr.setContentHandler(new org.xml.sax.helpers.DefaultHandler());
        return sr;
    }

    public void setDigester(org.apache.commons.digester.Digester digester) {
        if (digester instanceof ExtendedDigester) super.setDigester(digester);
        else throw new IllegalArgumentException("You can only use this rule in a "+ExtendedDigester.class.getName()+" instance!");
    }

    public void setContentHandler(ContentHandler ch) {
        this.destContentHandler=ch;
    }

    public void setExcludeNamespaces(Set<String> excludeNamespaces) {
        this.excludeNamespaces=excludeNamespaces;
    }

    // add some tags when document started
    protected void initDocument() throws SAXException {
    }

    // close tags added in initDocument()
    protected void finishDocument() throws SAXException {
    }

    // Digester rule part
    public void begin(java.lang.String namespace, java.lang.String name, Attributes attributes) throws Exception {
        if (destContentHandler==null) throw new IllegalStateException("You must set a target ContentHandler instance before processing this rule!");

        // initialize target ContentHandler
        SaxFilter filter=new SaxFilter(this,destContentHandler);
        filter.setDocumentLocator(digester.getDocumentLocator());
        lastContentHandler=digester.getCustomContentHandler();
        digester.setCustomContentHandler(filter);

        destContentHandler.startDocument();

        // register namespace prefixes
        for (Map.Entry<String,ArrayStack> e : ((ExtendedDigester)digester).getCurrentPrefixMappings().entrySet()) {
            String ns=(String)e.getValue().peek();
            if (!excludeNamespaces.contains(ns)) destContentHandler.startPrefixMapping(e.getKey(),ns);
        }

        initDocument();
    }

    private void release() throws SAXException {
        finishDocument();

        // un-register namespace prefixes
        for (Map.Entry<String,ArrayStack> e : ((ExtendedDigester)digester).getCurrentPrefixMappings().entrySet()) {
            if (!excludeNamespaces.contains((String)e.getValue().peek())) destContentHandler.endPrefixMapping(e.getKey());
        }

        // end document and restore ContentHandler
        destContentHandler.endDocument();
        digester.setCustomContentHandler(lastContentHandler);
        lastContentHandler=null;
    }

    // the XMLFilter
    protected static final class SaxFilter extends XMLFilterImpl {

        private int elementCounter=0;
        private SaxRule owner=null;

        private SaxFilter(SaxRule owner, ContentHandler ch) {
            super();
            this.owner=owner;
            setContentHandler(ch);
            if (ch instanceof EntityResolver) setEntityResolver((EntityResolver)ch);
            if (ch instanceof DTDHandler) setDTDHandler((DTDHandler)ch);
            if (ch instanceof ErrorHandler) setErrorHandler((ErrorHandler)ch);
        }

        /* SAX part */
        public void endElement(String namespaceURI, String localName, String qName) throws SAXException {
            if (elementCounter==0) {
                owner.release();
                owner.digester.endElement(namespaceURI,localName,qName); // this should implicitly call owner.end(namespace,name);
            } else {
                super.endElement(namespaceURI,localName,qName);
                elementCounter--;
            }
        }

        public void startElement(String namespaceURI, String localName, String qName, Attributes atts) throws SAXException {
            elementCounter++;
            super.startElement(namespaceURI,localName,qName,atts);
        }

        public void startDocument() throws SAXException {
            throw new SAXException("Cannot start a new XML document in middle of another document!");
        }

        public void endDocument() throws SAXException {
            throw new SAXException("Cannot end current XML document here!");
        }

    }

}