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

import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.XMLConstants;

import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBElement;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Unmarshaller;

/**
 * Used to serialize {@link Node} from a DOM tree to an Object (mainly {@link KeyValuePairs}).
 * This class cannot handle adjacent text nodes, so the DOM tree should
 * be normalized first.
 * Nodes that have child elements or mixed content with get transformed to {@link KeyValuePairs},
 * String only nodes with an {@code xsi:type} attribute get deserialized to the given type, {@link String} otherwise.
 * Empty nodes will return {@code null}.
 * @see Node#normalize()
 * @author Uwe Schindler
 */
public final class XMLToKeyValuePairs {
  private final boolean serializeAttributes;
  private final Unmarshaller jaxbUnmarshaller;
  
  /**
   * If an element has a local name with this prefix, it is converted to a JSON attribute, prefixed by {@code @}.
   */
  public static final String ATTRIBUTE_ELEMENT_PREFIX = "__AT_";
  
  private static final Set<String> HIDDEN_ATTR_NAMESPACES = Set.of(
          XMLConstants.XMLNS_ATTRIBUTE_NS_URI,
          XMLConstants.XML_NS_URI,
          XMLConstants.W3C_XML_SCHEMA_INSTANCE_NS_URI
      );
  private static final Pattern PATTERN_SPECIAL_ATTRIBUTE_ELEMENT = Pattern.compile(Pattern.quote(ATTRIBUTE_ELEMENT_PREFIX).concat("(.*)"));
 
  public XMLToKeyValuePairs(boolean serializeAttributes) throws JAXBException {
    this.serializeAttributes = serializeAttributes;
    this.jaxbUnmarshaller = JAXBContext.newInstance().createUnmarshaller();
  }
  
  /**
   * Convert all children of a node:
   * It first checks if all child nodes are text-only, in that case
   * the whole node is returned as String (using {@link Node#getTextContent()}.
   * In all other cases its starts a new JSON object and converts every
   * contained node. Empty nodes will be returned as {@code null}.
   */
  public Object convertChilds(final Node parentNode) throws JAXBException {
    if (parentNode == null) {
      return null;
    }
    boolean hasText = false, hasElementsOrAttrs = false;
    NamedNodeMap atts = null;
    if (serializeAttributes && parentNode instanceof Element) {
      atts = ((Element)parentNode).getAttributes();
      for (int i = 0, c = atts.getLength(); i < c; i++) {
        if (!isXsiNamespaced(atts.item(i))) {
          hasElementsOrAttrs = true;
        }
      }
    }
    for (Node nod = parentNode.getFirstChild(); nod != null; nod = nod.getNextSibling()) {
      switch (nod.getNodeType()) {
        case Node.ELEMENT_NODE:
          hasElementsOrAttrs = true;
          break;
        case Node.ATTRIBUTE_NODE:
          if (serializeAttributes && !isXsiNamespaced(nod)) {
            hasElementsOrAttrs = true;
          }
          break;
        case Node.TEXT_NODE:
        case Node.CDATA_SECTION_NODE:
          if (!nod.getNodeValue().trim().isEmpty()) {
            hasText = true;
          }
          break;
      }
    }
    if (hasElementsOrAttrs) {
      final KeyValuePairs kv = new KeyValuePairs();
      if (serializeAttributes && atts != null) {
        for (int i = 0, c = atts.getLength(); i < c; i++) {
          convertNode(kv, atts.item(i));
        }
      }
      for (Node nod = parentNode.getFirstChild(); nod != null; nod = nod.getNextSibling()) {
        convertNode(kv, nod);
      }
      return kv.isEmpty() ? null : kv;
    } else if (hasText) {
      // hack type from <String> back to <?> to not cause ClassCastException (because default is String):
      final JAXBElement<?> ele = jaxbUnmarshaller.unmarshal(parentNode, String.class);
      return ele.getValue();
    } else {
      return null;
    }
  }
   
  private boolean isXsiNamespaced(Node n) {
    return HIDDEN_ATTR_NAMESPACES.contains(n.getNamespaceURI());
  }
  
  private void convertNode(final KeyValuePairs kv, final Node n) throws JAXBException {
    if (n == null) return;
    switch (n.getNodeType()) {
      case Node.ELEMENT_NODE:
        String name = n.getLocalName();
        if (serializeAttributes) {
          final Matcher matcher = PATTERN_SPECIAL_ATTRIBUTE_ELEMENT.matcher(name);
          if (matcher.matches()) {
            name = "@".concat(matcher.group(1));
          }
        }
        kv.add(name, convertChilds(n));
        break;
      case Node.DOCUMENT_NODE:
      case Node.DOCUMENT_FRAGMENT_NODE:
        throw new IllegalArgumentException("Invalid node type (DOCUMENT_NODE or DOCUMENT_FRAGMENT_NODE)");
      case Node.ATTRIBUTE_NODE:
        if (isXsiNamespaced(n)) {
          // ignore xsi: attributes
        } else if (serializeAttributes) {
          kv.add("@" + n.getLocalName(), n.getNodeValue());
        }
        break;
      case Node.TEXT_NODE:
      case Node.CDATA_SECTION_NODE:
        throw new IllegalArgumentException("The element contains mixed element/text content, which is not allowed for JSON");
    }
  }
  
}