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

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;

import de.pangaea.metadataportal.utils.PublicForDigesterUse;

/**
 * Configuration of an index in Elasticsearch.
 * 
 * @author Uwe Schindler
 */
public final class TargetIndexConfig {
  
  /** Default constructor **/
  public TargetIndexConfig(Config root, String name) {
    if (name == null) throw new NullPointerException("Every target index config needs a unique id!");
    this.root = root;
    this.indexName = name;
  }
  
  private void checkImmutable() {
    if (checked) throw new IllegalStateException("Target index configuration cannot be changed anymore!");
  }
  
  /** Adds property for harvester (called from Digester on config load). **/
  @PublicForDigesterUse
  @Deprecated
  public void addGlobalHarvesterProperty(String value) {
    checkImmutable();
    if (value != null) globalHarvesterProperties.setProperty(
        root.dig.getCurrentElementName(), value.trim());
  }

  /** Adds harvester. **/
  public void addHarvester(HarvesterConfig i) {
    checkImmutable();
    if (!root.harvestersAndIndexes.add(i.id)) throw new IllegalArgumentException(
        "There is already a harvester or targetIndex with id=\"" + i.id + "\" added to configuration!");
    harvesters.put(i.id, i);
  }
  
  /** Adds alias to index. **/
  public void addAlias(String name, String filter) throws IOException {
    checkImmutable();
    if (name == null) {
      throw new NullPointerException("targetIndex alias name cannot be null.");
    }
    name = name.trim();
    if (this.indexName.equals(name) || aliases.containsKey(name)) {
      throw new IllegalArgumentException("targetIndex alias name already exists.");
    }
    if (filter != null && filter.trim().isEmpty()) {
      filter = null;
    }
    if (filter != null) {
      // validate JSON/...
      final XContentParser parser = XContentFactory.xContent(filter).createParser(filter);
      while (parser.nextToken() != null);
    }
    aliases.put(name, filter);
  }
  
  @PublicForDigesterUse
  @Deprecated
  public void setIndexSettings(Settings.Builder bld) {
    checkImmutable();
    if (indexSettings != null)
      throw new IllegalArgumentException("Duplicate <settings/> element");
    // strip the XML matcher path:
    indexSettings = bld.build().getByPrefix(root.dig.getMatch() + "/");
  }
  
  public void setNameSuffix1(String nameSuffix1) {
    checkImmutable();
    this.nameSuffix1 = nameSuffix1;
  }
  
  public void setNameSuffix2(String nameSuffix2) {
    checkImmutable();
    this.nameSuffix2 = nameSuffix2;
  }
  
  /**
   * Checks, if configuration is ok. After calling this, you are not able to
   * change anything in this instance.
   **/
  public void check() throws Exception {
    // *** After loading do final checks ***
    // consistency in harvesters:
    for (HarvesterConfig iconf : harvesters.values()) {
      iconf.check();
    }
    checked = true;
  }
  
  public String getRawIndexName(boolean alternate) {
    return indexName + (alternate ? nameSuffix2 : nameSuffix1);
  }
  
  private boolean checked = false;
  
  // members "the configuration"
  public final String indexName;
  public final Config root;
  public final Properties globalHarvesterProperties = new Properties();
  public final Map<String,HarvesterConfig> harvesters = new LinkedHashMap<>();
  public final Map<String,String> aliases = new LinkedHashMap<>();
  private String nameSuffix1 = "_v1", nameSuffix2 = "_v2";
  public Settings indexSettings = null;
}