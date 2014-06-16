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

package de.pangaea.metadataportal.config;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.xml.namespace.QName;
import javax.xml.transform.Templates;

import de.pangaea.metadataportal.harvester.Harvester;
import de.pangaea.metadataportal.utils.PublicForDigesterUse;

/**
 * Configuration of a panFMP harvester.
 * 
 * @author Uwe Schindler
 */
public final class HarvesterConfig {
  
  /** Default constructor **/
  public HarvesterConfig(Config root, TargetIndexConfig parent, String id) {
    if (id == null) throw new NullPointerException("Every harvester needs a unique id!");
    this.root = root;
    this.parent = parent;
    this.id = id;
    properties = new Properties(parent.globalHarvesterProperties);
  }
  
  private void checkImmutable() {
    if (checked) throw new IllegalStateException("Harvester configuration cannot be changed anymore!");
  }
  
  /** Sets class name of harvester (called from Digester on config load). **/
  @PublicForDigesterUse
  @Deprecated
  public void setHarvesterClass(String v) throws ClassNotFoundException {
    checkImmutable();
    harvesterClass = Class.forName(v).asSubclass(Harvester.class);
  }

  /** Adds property for harvester (called from Digester on config load). **/
  @PublicForDigesterUse
  @Deprecated
  public void addHarvesterProperty(String value) {
    checkImmutable();
    if (value != null)
      properties.setProperty(parent.root.dig.getCurrentElementName(), value.trim());
  }

  /**
   * Checks, if configuration is ok. After calling this, you are not able to
   * change anything in this instance.
   **/
  public void check() throws Exception {
    Harvester h = harvesterClass.getConstructor(HarvesterConfig.class).newInstance(this);
    Set<String> validProperties = h.getValidHarvesterPropertyNames();
    @SuppressWarnings("unchecked")
    Enumeration<String> en = (Enumeration<String>) properties
        .propertyNames();
    while (en.hasMoreElements()) {
      String prop = en.nextElement();
      if (!validProperties.contains(prop)) throw new IllegalArgumentException(
          "Harvester class '" + harvesterClass.getName() + "' for harvester '" + id
              + "' does not support property '" + prop
              + "'! Supported properties are: " + validProperties);
    }

    checked = true;
  }

  protected boolean checked = false;
  
  // members "the configuration"
  public final String id;
  public final Config root;
  public final TargetIndexConfig parent;
  public final Map<QName,Object> xsltParams = new HashMap<>();

  public Class<? extends Harvester> harvesterClass = null;

  public final Properties properties;

  public Templates xslt = null;
}