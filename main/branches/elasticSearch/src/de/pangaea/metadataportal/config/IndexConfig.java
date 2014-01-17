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

import java.io.File;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.xml.namespace.QName;
import javax.xml.transform.Templates;

import org.apache.lucene.store.Directory;

import de.pangaea.metadataportal.harvester.Harvester;
import de.pangaea.metadataportal.utils.PublicForDigesterUse;

/**
 * Abstract configuration of an panFMP index. It not only configures its
 * properties like name/id, it also contains methods to get some index access
 * objects (IndexReader, Searcher) and status information.
 * 
 * @author Uwe Schindler
 */
public class IndexConfig {
  
  /** Default constructor **/
  public IndexConfig(Config parent) {
    this.parent = parent;
    harvesterProperties = new Properties(parent.globalHarvesterProperties);
  }
  
  /** Sets the ID of this index configuration. **/
  public void setId(String v) {
    if (checked) throw new IllegalStateException(
        "Virtual index configuration cannot be changed anymore!");
    id = v;
  }
  
  /** Sets the user-readable name of this index configuration. **/
  public void setDisplayName(String v) {
    displayName = v;
  }
  
  /** Sets index directory (called from Digester on config load). **/
  public synchronized void setIndexDir(String v)
      throws java.io.IOException {
      if (checked) throw new IllegalStateException(
          "Index configuration cannot be changed anymore!");
      if (indexDirImpl != null) indexDirImpl.close();
      indexDirImpl = null;
      indexDir = v;
    }

  /** Sets class name of harvester (called from Digester on config load). **/
  @PublicForDigesterUse
  @Deprecated
  public void setHarvesterClass(String v) throws ClassNotFoundException {
    if (checked) throw new IllegalStateException(
        "Index configuration cannot be changed anymore!");
    harvesterClass = Class.forName(v).asSubclass(Harvester.class);
  }

  /** Adds property for harvester (called from Digester on config load). **/
  @PublicForDigesterUse
  @Deprecated
  public void addHarvesterProperty(String value) {
    if (checked) throw new IllegalStateException(
        "Index configuration cannot be changed anymore!");
    if (value != null) harvesterProperties.setProperty(
        parent.dig.getCurrentElementName(), value.trim());
  }

  /**
   * Checks, if configuration is ok. After calling this, you are not able to
   * change anything in this instance.
   **/
  public void check() throws Exception {
    if (id == null) throw new IllegalStateException(
        "Every index needs a unique id!");
    if (displayName == null || "".equals(displayName)) throw new IllegalStateException(
        "Index with id=\"" + id + "\" has no displayName!");
    if (indexDir == null || harvesterClass == null) throw new IllegalStateException(
        "Some index configuration fields are missing for index with id=\"" + id
            + "\"!");
    Harvester h = harvesterClass.newInstance();
    Set<String> validProperties = h.getValidHarvesterPropertyNames();
    @SuppressWarnings("unchecked")
    Enumeration<String> en = (Enumeration<String>) harvesterProperties
        .propertyNames();
    while (en.hasMoreElements()) {
      String prop = en.nextElement();
      if (!validProperties.contains(prop)) throw new IllegalArgumentException(
          "Harvester '" + harvesterClass.getName() + "' for index '" + id
              + "' does not support property '" + prop
              + "'! Supported properties are: " + validProperties);
    }

    checked = true;
  }

  /** Returns the local, expanded file system path to index. **/
  public String getFullIndexPath() throws java.io.IOException {
    return parent.makePathAbsolute(indexDir, false);
  }

  /** Returns the directory implementation, that contains the index. **/
  public synchronized Directory getIndexDirectory() throws java.io.IOException {
    if (indexDirImpl == null) indexDirImpl = parent.indexDirImplementation
        .getDirectory(new File(getFullIndexPath()));
    return indexDirImpl;
  }

  protected boolean checked = false;
  
  // members "the configuration"
  public String displayName = null, id = null;
  public final Config parent;

  private String indexDir = null;

  private volatile Directory indexDirImpl = null;

  public Class<? extends Harvester> harvesterClass = null;

  public final Properties harvesterProperties;

  public Templates xslt = null;

  public Map<QName,Object> xsltParams = null;
}