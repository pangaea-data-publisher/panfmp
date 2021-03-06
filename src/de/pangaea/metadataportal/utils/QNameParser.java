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

import javax.xml.XMLConstants;
import javax.xml.namespace.NamespaceContext;
import javax.xml.namespace.QName;

/**
 * Simple static class to parse XPath-like string QNames ("prefix:tagname").
 * 
 * @author Uwe Schindler
 */
public final class QNameParser {
  
  private QNameParser() {}
  
  /**
   * Parses the given string to a {@link QName}. Namespace prefixes are
   * resolvced using the given {@link NamespaceContext}.
   */
  public static QName parseLexicalQName(String nameStr, NamespaceContext ctx) {
    String prefix, localPart;
    String[] parts = nameStr.split(":");
    switch (parts.length) {
      case 1:
        prefix = XMLConstants.DEFAULT_NS_PREFIX;
        localPart = parts[0];
        break;
      case 2:
        prefix = parts[0];
        localPart = parts[1];
        break;
      default:
        throw new IllegalArgumentException("Invalid formatted QName: "
            + nameStr);
    }
    return new QName(ctx.getNamespaceURI(prefix), localPart, prefix);
  }
  
}