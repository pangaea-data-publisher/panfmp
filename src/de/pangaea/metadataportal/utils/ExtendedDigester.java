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

import javax.xml.XMLConstants;
import java.util.*;
import org.apache.commons.collections.ArrayStack;
import org.apache.commons.digester.*;
import org.xml.sax.*;

public class ExtendedDigester extends Digester {

    protected HashMap<String,ArrayStack> currentNamespaceMap=new HashMap<String,ArrayStack>();
    protected ContentHandler custContentHandler=null;

    public ExtendedDigester() { super(); }
    public ExtendedDigester(javax.xml.parsers.SAXParser parser) { super(parser); }

    public void setCustomContentHandler(ContentHandler c) {
        custContentHandler=c;
    }

    public ContentHandler getCustomContentHandler() {
        return custContentHandler;
    }

    public void setErrorHandler(ErrorHandler err) {
        if (err!=null) throw new IllegalArgumentException("You cannot set any ErrorHandler with "+getClass().getName());
    }

    public ErrorHandler getErrorHandler() {
        return null;
    }

    public void addDoNothing(String pattern) {
        addRule(pattern,new DoNothingRule());
    }

    public void setRulesWithInvalidElementCheck(Rules rules) {
        WithDefaultsRulesWrapper r=new WithDefaultsRulesWrapper(rules);
        r.addDefault(new InvalidElementRule());
        super.setRules(r);
    }

    // *** START: ContentHandler (to fix bugs in original digester that prevent some events from working)

    public void startPrefixMapping(String prefix, String uri) throws SAXException {
        ArrayStack stack=null;
        if (currentNamespaceMap.containsKey(prefix)) stack=currentNamespaceMap.get(prefix);
        else {
            stack=new ArrayStack();
            currentNamespaceMap.put(prefix,stack);
        }
        stack.push(uri);

        if (custContentHandler!=null)
            custContentHandler.startPrefixMapping(prefix,uri);
        else
            super.startPrefixMapping(prefix,uri);
    }

    public void endPrefixMapping(String prefix) throws SAXException {
        if (custContentHandler!=null)
            custContentHandler.endPrefixMapping(prefix);
        else
            super.endPrefixMapping(prefix);

        ArrayStack stack=currentNamespaceMap.get(prefix);
        stack.remove();
        if (stack.empty()) currentNamespaceMap.remove(prefix);
    }

    public void startElement(String uri,String localName,String qName,Attributes atts) throws SAXException {
        if (custContentHandler!=null)
            custContentHandler.startElement(uri,localName,qName,atts);
        else
            super.startElement(uri,localName,qName,atts);
    }

    public void endElement(String uri,String localName,String qName) throws SAXException {
        if (custContentHandler!=null)
            custContentHandler.endElement(uri,localName,qName);
        else
            super.endElement(uri,localName,qName);
    }

    public void characters(char[] ch,int start,int length) throws SAXException {
        if (custContentHandler!=null)
            custContentHandler.characters(ch,start,length);
        else
            super.characters(ch,start,length);
    }

    public void ignorableWhitespace(char[] ch,int start,int length) throws SAXException {
        if (custContentHandler!=null)
            custContentHandler.ignorableWhitespace(ch,start,length);
        else
            super.ignorableWhitespace(ch,start,length);
    }

    public void processingInstruction(String target,String data) throws SAXException {
        if (custContentHandler!=null)
            custContentHandler.processingInstruction(target,data);
        else
            super.processingInstruction(target,data);
    }

    public void skippedEntity(String name) throws SAXException {
        if (custContentHandler!=null)
            custContentHandler.skippedEntity(name);
        else
            super.skippedEntity(name);
    }

    // *** END: ContentHandler

    // *** START: ErrorHandler (with exceptions)

    public void warning(SAXParseException ex) throws SAXException {
        log.warn(ex);
    }

    public void error(SAXParseException ex) throws SAXException {
        // stop processing on errors
        throw ex;
    }

    public void fatalError(SAXParseException ex) throws SAXException {
        // stop processing on fatal errors
        throw ex;
    }

    // *** END: ErrorHandler

    // *** START: Namespace handling

    // this is a better implementation than in the original digester version 1.8

    public Map<String,ArrayStack> getCurrentPrefixMappings() {
        return Collections.unmodifiableMap(currentNamespaceMap);
    }

    public Map<String,String> getCurrentNamespaceMap() {
        HashMap<String,String> n=new HashMap<String,String>(currentNamespaceMap.size());
        for (Map.Entry<String,ArrayStack> i : currentNamespaceMap.entrySet()) {
            ArrayStack stack=i.getValue();
            n.put(i.getKey(), (String)stack.peek());
        }
        return n;
    }

    public javax.xml.namespace.NamespaceContext getCurrentNamespaceContext(boolean strict, boolean reDefineDefaultPrefix) {
        // create Map
        final boolean isStrict=strict;
        final Map<String,String> prefixToNS=getCurrentNamespaceMap();
        prefixToNS.put(XMLConstants.XML_NS_PREFIX,XMLConstants.XML_NS_URI);
        prefixToNS.put(XMLConstants.XMLNS_ATTRIBUTE,XMLConstants.XMLNS_ATTRIBUTE_NS_URI);
        if (reDefineDefaultPrefix || !prefixToNS.containsKey(XMLConstants.DEFAULT_NS_PREFIX))
            prefixToNS.put(XMLConstants.DEFAULT_NS_PREFIX,XMLConstants.NULL_NS_URI);
        // invert Map
        final Map<String,List<String>> nsToPrefix=new HashMap<String,List<String>>();
        for (Map.Entry<String,String> e : prefixToNS.entrySet()) {
            List<String> dest=nsToPrefix.get(e.getValue());
            if (dest==null) nsToPrefix.put(e.getValue(),dest=new ArrayList<String>());
            dest.add(e.getKey());
        }
        //logging
        if (log!=null && log.isDebugEnabled()) {
            log.debug("Forward NamespaceContext map: "+prefixToNS);
            log.debug("Reverse NamespaceContext map: "+nsToPrefix);
        }
        // create return class
        return new javax.xml.namespace.NamespaceContext() {
            public String getNamespaceURI(String prefix) {
                if (prefix==null)
                    throw new IllegalArgumentException("Namespace prefix cannot be null");
                String uri=prefixToNS.get(prefix);
                if (isStrict && uri==null) throw new IllegalArgumentException("Undeclared namespace prefix: "+prefix);
                return (uri==null) ? XMLConstants.NULL_NS_URI : uri;
            }

            public String getPrefix(String namespaceURI) {
                Iterator i=getPrefixes(namespaceURI);
                return i.hasNext() ? (String)i.next() : null;
            }

            public Iterator getPrefixes(String namespaceURI) {
                if (namespaceURI==null)
                    throw new IllegalArgumentException("Namespace URI cannot be null");
                List<String> plist=nsToPrefix.get(namespaceURI);
                if (plist==null) return Collections.EMPTY_LIST.iterator();
                else return Collections.unmodifiableList(plist).iterator();
            }
        };
    }

    public static class DoNothingRule extends Rule {
        // empty, this only makes the class non-abstract
    }

    private static final class InvalidElementRule extends Rule {

        public void begin(java.lang.String namespace, java.lang.String name, org.xml.sax.Attributes attributes) throws java.lang.Exception {
            throw new SAXException("Unknown element at XML path: '"+digester.getMatch()+"'; tagname: '{"+namespace+"}"+name+"'");
        }

    }
}