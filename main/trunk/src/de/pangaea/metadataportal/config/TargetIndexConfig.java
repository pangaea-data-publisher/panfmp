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
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.indices.IndexAlreadyExistsException;

import de.pangaea.metadataportal.processor.DocumentProcessor;
import de.pangaea.metadataportal.utils.PublicForDigesterUse;

/**
 * Configuration of an index in Elasticsearch.
 * 
 * @author Uwe Schindler
 */
public final class TargetIndexConfig {
  
  private static final org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(TargetIndexConfig.class);
  
  /** Default constructor **/
  public TargetIndexConfig(Config root, String name) {
    if (name == null) throw new NullPointerException("Every target index config needs a unique id!");
    this.root = root;
    this.indexName = name;
  }
  
  /** Adds property for harvester (called from Digester on config load). **/
  @PublicForDigesterUse
  @Deprecated
  public void addGlobalHarvesterProperty(String value) {
    if (checked) throw new IllegalStateException(
        "Target index configuration cannot be changed anymore!");
    if (value != null) globalHarvesterProperties.setProperty(
        root.dig.getCurrentElementName(), value.trim());
  }

  public void addHarvester(HarvesterConfig i) {
    if (checked) throw new IllegalStateException(
        "Target index configuration cannot be changed anymore!");
    if (!root.harvestersAndIndexes.add(i.id)) throw new IllegalArgumentException(
        "There is already a harvester or targetIndex with id=\"" + i.id + "\" added to configuration!");
    harvesters.put(i.id, i);
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
  
  private static XContentBuilder getHarvesterMetadataMapping() throws IOException {
    return XContentFactory.jsonBuilder().startObject()
        .startObject("_source")
        .field("enabled", true)
      .endObject()
      .startObject("_all")
        .field("enabled", false)
      .endObject()
      .startArray("dynamic_templates")
        .startObject()
          .startObject("kv_pairs")
            .field("match", "*")
            .startObject("mapping")
              .field("type", "string").field("index", "no").field("store", false)
            .endObject()
          .endObject()
        .endObject()
      .endArray()
    .endObject();
  }
  
  private void addFieldMappings(Map<String, Object> map) throws IOException {
    if (map.containsKey(root.fieldnameDatestamp) || map.containsKey(root.fieldnameSource) || map.containsKey(root.fieldnameXML)) {
      throw new IllegalArgumentException("The given mapping is not allowed to contain properties for internal field: " +
          Arrays.asList(root.fieldnameDatestamp, root.fieldnameSource, root.fieldnameXML));
    }
    map.put(root.fieldnameDatestamp, ImmutableMap.of(
      "type", "date",
      "format", "dateOptionalTime",
      "precision_step", 8,
      "include_in_all", false
    ));
    map.put(root.fieldnameSource, ImmutableMap.of(
      "type", "string",
      "index", "not_analyzed",
      "include_in_all", false,
      "store", false
    ));
    map.put(root.fieldnameXML, ImmutableMap.of(
      "type", "string",
      "index", "no",
      "include_in_all", false,
      "store", false
    ));
  }
  
  @SuppressWarnings("unchecked")
  private Map<String,Object> getMapping() throws IOException {
    Map<String,Object> mapping, props;
    if (root.esMapping != null) {
      mapping = XContentFactory.xContent(XContentType.JSON).createParser(root.esMapping).mapOrderedAndClose();
      if (mapping.containsKey(root.typeName)) {
        if (mapping.size() != 1) {
          throw new IllegalArgumentException("If the typeName is part of the mapping, it must be the single root element.");
        }
        mapping = (Map<String,Object>) mapping.get(root.typeName);
      }
      props = (Map<String,Object>) mapping.get("properties");
      if (props == null) {
        mapping.put("properties", props = new LinkedHashMap<>());
      }
    } else {
      mapping = new LinkedHashMap<>();
      mapping.put("properties", props = new LinkedHashMap<>());
    }
    addFieldMappings(props);
    return mapping;
  }
  
  /** Creates the index (if needed), configures it (mapping), and creates aliases. The real index name to be used is returned. */
  public String createIndex(Client client, boolean rebuilder) throws IOException {
    final IndicesAdminClient indicesAdmin = client.admin().indices();
    
    log.info("Getting index name for alias='" + indexName + "'...");
    final String realIndexName;
    final Iterator<String> indexes = indicesAdmin.prepareGetAliases(indexName).get().getAliases().keysIt();
    if (indexes.hasNext()) {
      String found = indexes.next();
      log.info("Alias exists and points to index='" + found + "'.");
      if (indexes.hasNext()) {
        throw new IOException("There are more than one index referred by alias='"+indexName+"'");
      }
      if (rebuilder) {
        realIndexName = indexName + (found.equals(indexName + nameSuffix1) ? nameSuffix2 : nameSuffix1);
        log.info("As rebuilding is requested, we create a new index: " + realIndexName);
        final DeleteIndexResponse resp = indicesAdmin.prepareDelete(realIndexName).setIndicesOptions(IndicesOptions.lenientExpandOpen()).get();
        log.info("Pre-existing index deleted: " + resp.isAcknowledged());
      } else {
        realIndexName = found;
      }
    } else {
      log.info("Alias does not yet exist, start fresh...");
      realIndexName = indexName + nameSuffix1;
    }

    log.info("Creating index='" + realIndexName + "'...");
    try {
      final CreateIndexRequestBuilder req = indicesAdmin.prepareCreate(realIndexName)
        .setCause(rebuilder ? "for rebuilding" : "new harvesting")
        .addMapping(DocumentProcessor.HARVESTER_METADATA_TYPE, getHarvesterMetadataMapping())
        .addMapping(root.typeName, getMapping());
      if (!rebuilder) req.addAlias(new Alias(indexName));
      final CreateIndexResponse resp = req.get();
      log.info("Index and mappings created: " + resp.isAcknowledged());
    } catch (IndexAlreadyExistsException e) {
      log.info("Index already exists. Updating mappings for index='" + realIndexName + "'...");
      {
        final PutMappingResponse resp = indicesAdmin.preparePutMapping(realIndexName)
            .setType(DocumentProcessor.HARVESTER_METADATA_TYPE)
            .setSource(getHarvesterMetadataMapping())
            .setIgnoreConflicts(false)
            .get();
        log.info("Harvester metadata mapping updated: " + resp.isAcknowledged());
      }
      {
        final PutMappingResponse resp = indicesAdmin.preparePutMapping(realIndexName)
            .setType(root.typeName)
            .setSource(getMapping())
            .setIgnoreConflicts(false)
            .get();
        log.info("XML metadata mapping updated: " + resp.isAcknowledged());
      }
    }

    return realIndexName;
  }
  
  /** Closes the index after harvesting and update the aliases to point to the active index. */
  public void closeIndex(Client client, String realIndexName, boolean cleanShutdown) throws IOException {
    log.info("Flushing data...");
    client.admin().indices().prepareFlush(realIndexName).get();
    
    final IndicesAdminClient indicesAdmin = client.admin().indices();
    
    final Iterator<String> indexes = indicesAdmin.prepareGetAliases(indexName).get().getAliases().keysIt();
    String aliasedIndex = null;
    if (indexes.hasNext()) {
      aliasedIndex = indexes.next();
      if (indexes.hasNext()) {
        throw new IOException("There are more than one index referred by alias='" + indexName + "'");
      }
    }
    
    if (cleanShutdown && !realIndexName.equals(aliasedIndex)) {
      log.info("Redirecting alias '" + indexName + "' to new index: " + realIndexName);
      {
        IndicesAliasesRequestBuilder req = indicesAdmin.prepareAliases();
        if (aliasedIndex != null) {
          req.removeAlias(aliasedIndex, indexName);
        }
        IndicesAliasesResponse resp = req.addAlias(realIndexName, indexName).get();
        log.info("Aliases redirected: " + resp.isAcknowledged());
      }
      if (aliasedIndex != null) {
        log.info("Deleting orphaned index: " + aliasedIndex);
        DeleteIndexResponse resp = indicesAdmin.prepareDelete(aliasedIndex).get();
        log.info("Index deleted: " + resp.isAcknowledged());
      }
    }
  }
  
  protected boolean checked = false;
  
  // members "the configuration"
  public final String indexName;
  public final Config root;
  public final Properties globalHarvesterProperties = new Properties();
  public final Map<String,HarvesterConfig> harvesters = new LinkedHashMap<String,HarvesterConfig>();
  
  public final String nameSuffix1 = "_v1", nameSuffix2 = "_v2"; // TODO
}