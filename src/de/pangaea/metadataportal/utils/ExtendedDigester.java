/*
 *   Copyright 2007-2008 panFMP Developers Team c/o Uwe Schindler
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
import org.apache.commons.digester.*;
import org.xml.sax.*;
import javax.xml.namespace.NamespaceContext;

/**
 * Extension of the Commons Digester Class, that works around some limitations/bugs. It is
 * especially important for {@link SaxRule}, as it supports a stack/list of namespace-prefix assignments,
 * and contains a integrated error handler. It also gives the possibility to not allow
 * invalid element names.
 * @author Uwe Schindler
 */
public class ExtendedDigester extends Digester {

	protected final HashMap<String,LinkedList<String>> currentNamespaceMap=new HashMap<String,LinkedList<String>>();
	protected ContentHandler custContentHandler=null;

	public ExtendedDigester() { super(); }
	public ExtendedDigester(javax.xml.parsers.SAXParser parser) { super(parser); }

	/** Sets a custom {@link ContentHandler}, that receives all SAX events until disabled (<code>null</code>). */
	@Override
	public void setCustomContentHandler(ContentHandler c) {
		custContentHandler=c;
	}

	/** Gets the custom event handler. */
	@Override
	public ContentHandler getCustomContentHandler() {
		return custContentHandler;
	}

	/** Not suppoted, always throws {@link IllegalArgumentException} if not <code>null</code>. */
	@Override
	public void setErrorHandler(ErrorHandler err) {
		if (err!=null) throw new IllegalArgumentException("You cannot set any ErrorHandler with "+getClass().getName());
	}

	/** Not supported, always returns <code>null</code> */
	@Override
	public ErrorHandler getErrorHandler() {
		return null;
	}

	/** Adds a dummy rule for element paths, that are allowed, but not parsed. */
	public void addDoNothing(String pattern) {
		addRule(pattern,new DoNothingRule());
	}

	/** Adds a default Rule for not allowing invalid (not registered) event paths. The given Rules object is
	 * wrapped and set using <code>setRules(Rules rules)</code>. */
	public void setRulesWithInvalidElementCheck(Rules rules) {
		WithDefaultsRulesWrapper r=new WithDefaultsRulesWrapper(rules);
		r.addDefault(new InvalidElementRule());
		super.setRules(r);
	}
	
	@Override
	public void clear() {
		currentNamespaceMap.clear();
		super.clear();
	}

	// *** START: ContentHandler (to fix bugs in original digester that prevent some events from working)

	@Override
	public void startPrefixMapping(String prefix, String uri) throws SAXException {
		LinkedList<String> stack=currentNamespaceMap.get(prefix);
		if (stack==null) currentNamespaceMap.put(prefix,stack=new LinkedList<String>());
		stack.addFirst(uri);

		if (custContentHandler!=null)
			custContentHandler.startPrefixMapping(prefix,uri);
		else
			super.startPrefixMapping(prefix,uri);
	}

	@Override
	public void endPrefixMapping(String prefix) throws SAXException {
		if (custContentHandler!=null)
			custContentHandler.endPrefixMapping(prefix);
		else
			super.endPrefixMapping(prefix);

		final LinkedList<String> stack=currentNamespaceMap.get(prefix);
		stack.removeFirst();
		if (stack.isEmpty()) currentNamespaceMap.remove(prefix);
	}

	@Override
	public void startElement(String uri,String localName,String qName,Attributes atts) throws SAXException {
		if (custContentHandler!=null)
			custContentHandler.startElement(uri,localName,qName,atts);
		else
			super.startElement(uri,localName,qName,atts);
	}

	@Override
	public void endElement(String uri,String localName,String qName) throws SAXException {
		if (custContentHandler!=null)
			custContentHandler.endElement(uri,localName,qName);
		else
			super.endElement(uri,localName,qName);
	}

	@Override
	public void characters(char[] ch,int start,int length) throws SAXException {
		if (custContentHandler!=null)
			custContentHandler.characters(ch,start,length);
		else
			super.characters(ch,start,length);
	}

	@Override
	public void ignorableWhitespace(char[] ch,int start,int length) throws SAXException {
		if (custContentHandler!=null)
			custContentHandler.ignorableWhitespace(ch,start,length);
		else
			super.ignorableWhitespace(ch,start,length);
	}

	@Override
	public void processingInstruction(String target,String data) throws SAXException {
		if (custContentHandler!=null)
			custContentHandler.processingInstruction(target,data);
		else
			super.processingInstruction(target,data);
	}

	@Override
	public void skippedEntity(String name) throws SAXException {
		if (custContentHandler!=null)
			custContentHandler.skippedEntity(name);
		else
			super.skippedEntity(name);
	}
	
	@Override
	public void startDocument() throws SAXException {
		currentNamespaceMap.clear();
		super.startDocument();
	}

	// *** END: ContentHandler

	// *** START: ErrorHandler (with exceptions)

	/** Logs the SAX exception as warning (with location). */
	@Override
	public void warning(SAXParseException ex) throws SAXException {
		log.warn("SAX parse warning in \""+ex.getSystemId()+"\", line "+ex.getLineNumber()+", column "+ex.getColumnNumber()+": "+ex.getMessage());
	}

	/** Just throws <code>ex</code>. */
	@Override
	public void error(SAXParseException ex) throws SAXException {
		// stop processing on errors
		throw ex;
	}

	/** Just throws <code>ex</code>. */
	@Override
	public void fatalError(SAXParseException ex) throws SAXException {
		// stop processing on fatal errors
		throw ex;
	}

	// *** END: ErrorHandler

	// *** START: Namespace handling

	// this is a better implementation than in the original digester version 1.8

	/** Replays all current prefix mappings for another <code>ContentHandler</code> (start mapping).
	 * @param handler the handler which {@link ContentHandler#startPrefixMapping} is called.
	 * @param excludeNamespaces are prefixes for namespaces that should not be reported.
	 */
	public void replayStartPrefixMappings(final ContentHandler handler, final Set<String> excludeNamespaces) throws SAXException {
		for (Map.Entry<String,LinkedList<String>> e : currentNamespaceMap.entrySet()) {
			final String ns=e.getValue().getFirst();
			if (!excludeNamespaces.contains(ns)) handler.startPrefixMapping(e.getKey(),ns);
		}
	}

	/** Replays all current prefix mappings for another <code>ContentHandler</code> (end mapping).
	 * @param handler the handler which {@link ContentHandler#endPrefixMapping} is called.
	 * @param excludeNamespaces are prefixes for namespaces that should not be reported.
	 */
	public void replayEndPrefixMappings(final ContentHandler handler, final Set<String> excludeNamespaces) throws SAXException {
		for (Map.Entry<String,LinkedList<String>> e : currentNamespaceMap.entrySet()) {
			if (!excludeNamespaces.contains(e.getValue().getFirst())) handler.endPrefixMapping(e.getKey());
		}
	}

	/** Returns <b>all</b> current namespace prefix that are assigned. */
	public Set<String> getCurrentAssignedPrefixes() {
		return Collections.unmodifiableSet(currentNamespaceMap.keySet());
	}

	/** Returns the current namespace URI for the given prefix. */
	public String getCurrentNamespaceForPrefix(String prefix) {
		final LinkedList<String> ns=currentNamespaceMap.get(prefix);
		return (ns==null) ? null : ns.getFirst();
	}

	/** Returns the current namespace prefix mappings as modifiable {@link Map} containing the prefix
	 * and the current namespace assignment (it is just a copy of the internal representation's current mapping). */
	public Map<String,String> getCurrentNamespaceMap() {
		final HashMap<String,String> n=new HashMap<String,String>(currentNamespaceMap.size());
		for (Map.Entry<String,LinkedList<String>> i : currentNamespaceMap.entrySet()) {
			n.put(i.getKey(), i.getValue().getFirst());
		}
		return n;
	}

	/**
	 * Returns the current {@link NamespaceContext} for compiling XPath expressions.
	 * @param strict denotes, that undeclared prefixes throw an {@link IllegalArgumentException}, like XSLT does it.
	 * @param reDefineDefaultPrefix denotes, that the default "xmlns" is reassigned. This conforms to XSLT.
	 */
	public NamespaceContext getCurrentNamespaceContext(boolean strict, boolean reDefineDefaultPrefix) {
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
		return new NamespaceContext() {
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

	/** This rule does nothing. It is needed for giving a rule for uninteresting tags to not throw an exception. */
	public static class DoNothingRule extends Rule {
		// empty, this only makes the class non-abstract
	}

	private static final class InvalidElementRule extends Rule {

		public void begin(String namespace, String name, Attributes attributes) throws Exception {
			throw new SAXException("Unknown element at XML path: '"+digester.getMatch()+"'; tagname: '{"+namespace+"}"+name+"'");
		}

	}
}