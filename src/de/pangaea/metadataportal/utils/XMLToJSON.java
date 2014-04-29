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

import java.io.IOException;

import org.w3c.dom.Node;

/**
 * Used to serialize {@link Node} from a DOM tree to a JSON builder.
 * This class cannot handle adjacent text nodes, so the DOM tree should
 * be normalized first.
 * @see Node#normalize()
 * @author Uwe Schindler
 */
public final class XMLToJSON {
  private final boolean serializeMixedContentText, serializeAttributes;
  
  public XMLToJSON(boolean serializeMixedContentText, boolean serializeAttributes) {
    this.serializeMixedContentText = serializeMixedContentText;
    this.serializeAttributes = serializeAttributes;
  }
  
  /**
   * Serialize all children of a node:
   * It first checks if all child nodes are text-only, in that case
   * the whole node is serialized as text (using {@link Node#getTextContent()}.
   * In all other cases its starts a new JSON object and serializes every
   * contained node with {@link #serializeNode(Node)}.
   */
  public Object serializeChilds(final Node parentNode) throws IOException {
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
        serializeNode(kv, nod);
      }
      return kv.isEmpty() ? null : kv;
    } else if (hasText) {
      return parentNode.getTextContent();
    } else {
      return null;
    }
  }
  
  /**
   * Serializes a node depending on its type. In general, this calls
   * {@link #serializeChilds(Node)} for all child nodes, but creates the
   * parent field first.
   * <p>Text nodes get special handling: they are serialized as JSON fields with
   * name &quot;#text&quot;. Attributes are serialized as JSON fields prefixed
   * with &quot;@&quot;.
   */
  private void serializeNode(final KeyValuePairs kv, final Node n) throws IOException {
    if (n == null) return;
    switch (n.getNodeType()) {
      case Node.ELEMENT_NODE:
        kv.add(n.getLocalName(), serializeChilds(n));
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