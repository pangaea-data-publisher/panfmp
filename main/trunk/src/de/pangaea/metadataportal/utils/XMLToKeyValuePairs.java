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

import org.w3c.dom.Node;

/**
 * Used to serialize {@link Node} from a DOM tree to an Object (mainly {@link KeyValuePairs}).
 * This class cannot handle adjacent text nodes, so the DOM tree should
 * be normalized first.
 * Nodes that have child elements or mixed content with get transformed to {@link KeyValuePairs},
 * String only nodes will be returned as String and empty nodes will return {@code null}.
 * @see Node#normalize()
 * @author Uwe Schindler
 */
public final class XMLToKeyValuePairs {
  private final boolean serializeMixedContentText, serializeAttributes;
  
  public XMLToKeyValuePairs(boolean serializeMixedContentText, boolean serializeAttributes) {
    this.serializeMixedContentText = serializeMixedContentText;
    this.serializeAttributes = serializeAttributes;
  }
  
  /**
   * Convert all children of a node:
   * It first checks if all child nodes are text-only, in that case
   * the whole node is returned as String (using {@link Node#getTextContent()}.
   * In all other cases its starts a new JSON object and converts every
   * contained node. Empty nodes will be returned as {@code null}.
   */
  public Object convertChilds(final Node parentNode) {
    if (parentNode == null) {
      return null;
    }
    boolean hasText = false, hasElementsOrAttrs = false;
    for (Node nod = parentNode.getFirstChild(); nod != null; nod = nod.getNextSibling()) {
      switch (nod.getNodeType()) {
        case Node.ELEMENT_NODE:
          hasElementsOrAttrs = true;
          break;
        case Node.ATTRIBUTE_NODE:
          if (serializeAttributes) {
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
      for (Node nod = parentNode.getFirstChild(); nod != null; nod = nod.getNextSibling()) {
        convertNode(kv, nod);
      }
      return kv.isEmpty() ? null : kv;
    } else if (hasText) {
      return parentNode.getTextContent();
    } else {
      return null;
    }
  }
  
  private void convertNode(final KeyValuePairs kv, final Node n) {
    if (n == null) return;
    switch (n.getNodeType()) {
      case Node.ELEMENT_NODE:
        kv.add(n.getLocalName(), convertChilds(n));
        break;
      case Node.DOCUMENT_NODE:
      case Node.DOCUMENT_FRAGMENT_NODE:
        throw new IllegalArgumentException("Invalid node type (DOCUMENT_NODE or DOCUMENT_FRAGMENT_NODE)");
      case Node.ATTRIBUTE_NODE:
        if (serializeAttributes) {
          kv.add("@" + n.getLocalName(), n.getNodeValue());
        }
        break;
      case Node.TEXT_NODE:
      case Node.CDATA_SECTION_NODE:
        if (serializeMixedContentText) {
          kv.add("#text", n.getNodeValue());
        }
        break;
    }
  }
  
}