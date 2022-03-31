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

package de.pangaea.metadataportal.harvester;

import java.util.Set;

import org.xml.sax.ContentHandler;
import org.xml.sax.helpers.DefaultHandler;

import de.pangaea.metadataportal.utils.SaxRule;

/**
 * This class is used as a rule for the "metadata" element of the OAI response.
 * Whenever this element occurs in Digester, it feeds the SAX events to a
 * content handler and stores the DOM result in the {@link OAIMetadataDocument}
 * on the Digester stack, if rule is not enabled, metadata is fed to nowhere.
 * 
 * @author Uwe Schindler
 */
public final class OAIMetadataSaxRule extends SaxRule {
  
  private OAIMetadataDocument doc = null;
  private boolean enabled = true;
  
  /**
   * Creates a new rule which is enabled by default.
   */
  public OAIMetadataSaxRule() {
    super();
    setExcludeNamespaces(EXCLUDE_NS);
  }
  
  /**
   * If enabled, a DOM tree is build from metadata. If not enabled, metadata is
   * fed to nowhere (for static OAI repositories to filter wrong
   * metadataPrefix).
   */
  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }
  
  /**
   * @return <code>true</code> if storing in DOM tree is enabled
   */
  public boolean isEnabled() {
    return enabled;
  }
  
  // Digester rule part
  
  @Override
  public void begin(java.lang.String namespace, java.lang.String name,
      org.xml.sax.Attributes attributes) throws Exception {
    if (enabled) {
      doc = (OAIMetadataDocument) digester.peek(); // the OAIMetadataDocument is
                                                   // on the stack!!!
      ContentHandler handler = doc.getConverter().getTransformContentHandler();
      setContentHandler(handler);
    } else {
      doc = null;
      setContentHandler(new DefaultHandler());
    }
    super.begin(namespace, name, attributes);
  }
  
  @Override
  public void end(java.lang.String namespace, java.lang.String name)
      throws Exception {
    super.end(namespace, name);
    if (enabled) {
      doc.getConverter().finishTransformation();
      doc = null;
    }
  }
  
  private static final Set<String> EXCLUDE_NS = Set.of(OAIHarvesterBase.OAI_NS,
      OAIHarvesterBase.OAI_STATICREPOSITORY_NS);
  
}