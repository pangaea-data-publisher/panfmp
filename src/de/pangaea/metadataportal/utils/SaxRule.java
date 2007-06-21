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

    protected SaxFilter filter=null;
    protected Set<String> excludeNamespaces=Collections.EMPTY_SET;
    private org.xml.sax.ContentHandler lastContentHandler=null;

    public SaxRule() {
        super();
    }

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
        filter=new SaxFilter(this,ch);
    }

    public void setExcludeNamespaces(Set<String> excludeNamespaces) {
        this.excludeNamespaces=excludeNamespaces;
    }

    protected void release() throws SAXException {
        // un-register namespace prefixes
        for (Map.Entry<String,ArrayStack> e : ((ExtendedDigester)digester).getCurrentPrefixMappings().entrySet()) {
            if (!excludeNamespaces.contains((String)e.getValue().peek())) filter.endPrefixMapping(e.getKey());
        }

        // end document and restore ContentHandler
        filter.endDocument();
        digester.setCustomContentHandler(lastContentHandler);
        lastContentHandler=null;
    }

    // Digester rule part
    public void begin(java.lang.String namespace, java.lang.String name, Attributes attributes) throws Exception {
        if (filter==null) throw new IllegalStateException("You must set a target ContentHandler instance before processing this rule!");

        // initialize target ContentHandler
        filter.setDocumentLocator(digester.getDocumentLocator());
        lastContentHandler=digester.getCustomContentHandler();
        digester.setCustomContentHandler(filter);
        filter.startDocument();

        // register namespace prefixes
        for (Map.Entry<String,ArrayStack> e : ((ExtendedDigester)digester).getCurrentPrefixMappings().entrySet()) {
            String ns=(String)e.getValue().peek();
            if (!excludeNamespaces.contains(ns)) filter.startPrefixMapping(e.getKey(),ns);
        }
    }

    // the XMLFilter
    private static final class SaxFilter extends XMLFilterImpl {

        private int elementCounter=0;
        private SaxRule owner=null;
        private org.apache.commons.logging.Log log=null;

        private SaxFilter(SaxRule owner, ContentHandler ch) {
            super();
            this.owner=owner;
            if (owner.digester!=null) log=owner.digester.getLogger();
            if (log!=null && !log.isTraceEnabled()) log=null;
            setContentHandler(ch);
            if (ch instanceof EntityResolver) setEntityResolver((EntityResolver)ch);
            if (ch instanceof DTDHandler) setDTDHandler((DTDHandler)ch);
            if (ch instanceof ErrorHandler) setErrorHandler((ErrorHandler)ch);
        }

        /* SAX part */
        public void endElement(String namespaceURI, String localName, String qName) throws SAXException {
            if (elementCounter==0) {
                // root element for this Rule was empty
                // ==> stop all further processing, restore original ContentHandler in owner's digester and call recall endElement() to correctly process this event
                owner.release();
                owner.digester.endElement(namespaceURI,localName,qName); // this should implicitly call owner.end(namespace,name);
            } else {
                super.endElement(namespaceURI,localName,qName);
                elementCounter--;
                // all elements were closed ==> restore original ContentHandler in owner's digester
                if (elementCounter==0) owner.release();
            }
        }

        public void startElement(String namespaceURI, String localName, String qName, Attributes atts) throws SAXException {
            elementCounter++;
            super.startElement(namespaceURI,localName,qName,atts);
        }

        public void characters(char[] ch, int start, int length) throws SAXException {
            if (elementCounter>0) super.characters(ch,start,length);
        }

        public void startPrefixMapping(String prefix, String uri) throws SAXException {
            if (log!=null) log.trace("startPrefixMapping(SaxRule/SaxFilter): "+prefix+"="+uri);
            super.startPrefixMapping(prefix,uri);
        }

        public void endPrefixMapping(String prefix) throws SAXException {
            super.endPrefixMapping(prefix);
            if (log!=null) log.trace("endPrefixMapping(SaxRule/SaxFilter): "+prefix);
        }

    }

}