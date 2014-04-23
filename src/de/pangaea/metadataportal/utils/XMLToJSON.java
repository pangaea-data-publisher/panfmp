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
import java.util.HashSet;
import java.util.Set;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.w3c.dom.Node;

/**
 * Used to serialize {@link Node} from a DOM tree to a JSON builder.
 * This class cannot handle adjacent text nodes, so the DOM tree should
 * be normalized first.
 * @see Node#normalize()
 * @author Uwe Schindler
 */
public final class XMLToJSON {
  private final XContentBuilder builder;
  private final boolean serializeMixedContentText, serializeAttributes;
  
  public XMLToJSON(XContentBuilder builder, boolean serializeMixedContentText, boolean serializeAttributes) {
    this.builder = builder;
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
  public void serializeChilds(Node n) throws IOException {
    if (n == null) {
      builder.nullValue();
      return;
    }
    boolean hasText = false, hasElementsOrAttrs = false;
    for (Node nod = n.getFirstChild(); nod != null; nod = nod.getNextSibling()) {
      switch (nod.getNodeType()) {
        case Node.ELEMENT_NODE:
        case Node.ATTRIBUTE_NODE:
          hasElementsOrAttrs = true;
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
      builder.startObject();
      final Set<String> seen = new HashSet<String>();
      for (Node nod = n.getFirstChild(); nod != null; nod = nod.getNextSibling()) {
        final String localName = nod.getLocalName();
        //NodeList nodes = ((Element) nod).getElementsByTagNameNS("*", localName);
        if (!seen.contains(localName)) {
          // TODO: collect other nodes with same name
          serializeNode(nod);
          // add for next round
          seen.add(localName);
        }
      }
      builder.endObject();
    } else if (hasText) {
      builder.value(n.getTextContent());
    } else {
      builder.nullValue();
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
  public void serializeNode(Node n) throws IOException {
    if (n == null) return;
    switch (n.getNodeType()) {
      case Node.ELEMENT_NODE:
        builder.field(n.getLocalName());
        serializeChilds(n);
        break;
      case Node.DOCUMENT_NODE:
      case Node.DOCUMENT_FRAGMENT_NODE:
        serializeChilds(n);
        break;
      case Node.ATTRIBUTE_NODE:
        if (serializeAttributes) {
          builder.field("@" + n.getLocalName(), n.getNodeValue());
        }
        break;
      case Node.TEXT_NODE:
      case Node.CDATA_SECTION_NODE:
        if (serializeMixedContentText) {
          builder.field("#text", n.getNodeValue());
        }
        break;
    }
  }
  
}