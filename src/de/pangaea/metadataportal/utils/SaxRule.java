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

import java.util.*;
import org.apache.commons.collections.ArrayStack;
import de.pangaea.metadataportal.utils.*;
import org.xml.sax.helpers.XMLFilterImpl;
import org.xml.sax.*;

/**
 * This class is used as a rule for included documents during <code>Digester</code> parsing.
 * Whenever this element matches in <code>Digester</code>, <code>begin</code>/<code>end</code> will be called,
 * that then puts all further SAX events to the specified SAX <code>ContentHandler</code>.
 * @author Uwe Schindler
 */
public class SaxRule extends org.apache.commons.digester.Rule {

	/**
	 * @see #setContentHandler
	 */
	protected ContentHandler destContentHandler=null;

	/**
	 * @see #setExcludeNamespaces
	 */
	protected Set<String> excludeNamespaces=Collections.<String>emptySet();

	private ContentHandler lastContentHandler=null;

	/**
	 * Default constructor
	 */
	public SaxRule() {
	}

	/**
	 * Creates an empty "useless" SaxRule. The pupose is to not throw an exception on known but ignored tags (optional with contents).
	 * @return an instance that does nothing by feeding all SAX events to an SAX {@link org.xml.sax.helpers.DefaultHandler}
	 */
	public static SaxRule emptyRule() {
		SaxRule sr=new SaxRule();
		sr.setContentHandler(new org.xml.sax.helpers.DefaultHandler());
		return sr;
	}

	/**
	 * Set the <code>Digester</code> with which this <code>Rule</code> is associated.
	 * @throws IllegalArgumentException if <code>digester</code> is not an {@link ExtendedDigester} instance.
	 */
	@Override
	public void setDigester(org.apache.commons.digester.Digester digester) {
		if (digester instanceof ExtendedDigester) super.setDigester(digester);
		else throw new IllegalArgumentException("You can only use this rule in a "+ExtendedDigester.class.getName()+" instance!");
	}

	/**
	 * Sets the SAX <code>ContentHandler</code> that gets all SAX Events after the <code>startElement</code> event.
	 */
	public void setContentHandler(ContentHandler ch) {
		this.destContentHandler=ch;
	}

	/**
	 * return the current <code>ContentHandler</code>.
	 */
	public ContentHandler getContentHandler() {
		return destContentHandler;
	}

	/**
	 * Sets a {@code Set<String>} containing all Namespace URIs that should not be feed to the target {@code ContentHandler} on match.
	 * Default (or setting to {@code null}) means no restriction: All namespace prefixes visible in the current context will be reported.
	 */
	public void setExcludeNamespaces(Set<String> excludeNamespaces) {
		this.excludeNamespaces=(excludeNamespaces==null) ? Collections.<String>emptySet() : excludeNamespaces;
	}

	/**
	 * Add some tags when document started. The default implementation does nothing.
	 * This method should be overwritten to feed some additional tags after the <code>startDocument</code> SAX event.
	 * @throws SAXException
	 */
	protected void initDocument() throws SAXException {
	}

	/**
	 * Closes the tags created in <code>initDocument()</code>. The default implementation does nothing.
	 * This method should be overwritten to feed some ending tags before the <code>endDocument</code> SAX event.
	 * @throws SAXException
	 */
	protected void finishDocument() throws SAXException {
	}

	@Override
	public void begin(java.lang.String namespace, java.lang.String name, Attributes attributes) throws Exception {
		if (destContentHandler==null) throw new IllegalStateException("You must set a target ContentHandler instance before processing this rule!");
		if (lastContentHandler!=null) throw new IllegalStateException("begin(...) called twice!");

		// initialize target ContentHandler
		SaxFilter filter=new SaxFilter();
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

	@Override
	public void end(java.lang.String namespace, java.lang.String name) throws java.lang.Exception {
		finishDocument();

		// un-register namespace prefixes
		for (Map.Entry<String,ArrayStack> e : ((ExtendedDigester)digester).getCurrentPrefixMappings().entrySet()) {
			if (!excludeNamespaces.contains((String)e.getValue().peek())) destContentHandler.endPrefixMapping(e.getKey());
		}

		destContentHandler.endDocument();
	}

	// the XMLFilter
	private final class SaxFilter extends XMLFilterImpl {

		private int elementCounter=0;

		public SaxFilter() {
			super();
			setContentHandler(destContentHandler);
			if (destContentHandler instanceof EntityResolver) setEntityResolver((EntityResolver)destContentHandler);
			if (destContentHandler instanceof DTDHandler) setDTDHandler((DTDHandler)destContentHandler);
			if (destContentHandler instanceof ErrorHandler) setErrorHandler((ErrorHandler)destContentHandler);
		}

		/* SAX part */
		@Override
		public void endElement(String namespaceURI, String localName, String qName) throws SAXException {
			if (elementCounter==0) {
				// restore original ContentHandler in Digester and resubmit event
				digester.setCustomContentHandler(lastContentHandler);
				lastContentHandler=null;
				digester.endElement(namespaceURI,localName,qName);
			} else {
				super.endElement(namespaceURI,localName,qName);
				elementCounter--;
			}
		}

		@Override
		public void startElement(String namespaceURI, String localName, String qName, Attributes atts) throws SAXException {
			elementCounter++;
			super.startElement(namespaceURI,localName,qName,atts);
		}

		@Override
		public void startDocument() throws SAXException {
			throw new SAXException("Cannot start a new XML document in middle of another document!");
		}

		@Override
		public void endDocument() throws SAXException {
			throw new SAXException("Cannot end current XML document here!");
		}

	}

}