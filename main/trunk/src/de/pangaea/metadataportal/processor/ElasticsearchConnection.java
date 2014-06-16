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

package de.pangaea.metadataportal.processor;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

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
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.indices.IndexAlreadyExistsException;

import de.pangaea.metadataportal.config.Config;
import de.pangaea.metadataportal.config.HarvesterConfig;
import de.pangaea.metadataportal.config.TargetIndexConfig;

/**
 * TODO
 * 
 * @author Uwe Schindler
 */
public final class ElasticsearchConnection implements Closeable {
  private static final org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(ElasticsearchConnection.class);
  
  private Client client;
  private Config conf;

  public static final int ELASTICSEARCH_DEFAULT_PORT = 9300;

  public ElasticsearchConnection(Config config) {
    final Settings settings = config.esSettings == null ? ImmutableSettings.Builder.EMPTY_SETTINGS : config.esSettings;
    log.info("Connecting to Elasticsearch nodes: " + config.esTransports);
    if (log.isDebugEnabled()) {
      log.debug("ES connection settings: " + settings.getAsMap());
    }
    this.conf = config;
    this.client = new TransportClient(settings, false)
      .addTransportAddresses(config.esTransports.toArray(new TransportAddress[config.esTransports.size()]));
  }

  @Override
  public void close() {
    client.close();
    client = null;
    log.info("Closed connection to Elasticsearch.");
  }
  
  private void checkOpen() {
    if (client == null)
      throw new IllegalStateException("Elasticsearch TransportClient is already closed.");
  }
  
  public Client client() {
    checkOpen();
    return client;
  }
  
  public DocumentProcessor getDocumentProcessor(HarvesterConfig iconfig, String targetIndex) throws IOException {
    return new DocumentProcessor(client(), iconfig, targetIndex);
  }
  
  private static final String HARVESTER_METADATA_MAPPING;
  static {
    try {
      HARVESTER_METADATA_MAPPING = XContentFactory.jsonBuilder().startObject()
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
      .endObject().string();
    } catch (IOException ioe) {
      throw new Error(ioe);
    }
  }
  
  @SuppressWarnings("unchecked")
  private String getMapping() throws IOException {
    Map<String,Object> mapping, props;
    if (conf.esMapping != null) {
      mapping = XContentFactory.xContent(conf.esMapping).createParser(conf.esMapping).mapOrderedAndClose();
      if (mapping.containsKey(conf.typeName)) {
        if (mapping.size() != 1) {
          throw new IllegalArgumentException("If the typeName is part of the mapping, it must be the single root element.");
        }
        mapping = (Map<String,Object>) mapping.get(conf.typeName);
      }
    } else {
      mapping = new LinkedHashMap<>();
    }
    props = (Map<String,Object>) mapping.get("properties");
    if (props == null) {
      mapping.put("properties", props = new LinkedHashMap<>());
    }
    if (props.containsKey(conf.fieldnameDatestamp) || props.containsKey(conf.fieldnameSource) || props.containsKey(conf.fieldnameXML)) {
      throw new IllegalArgumentException("The given mapping is not allowed to contain properties for internal field: " +
          Arrays.asList(conf.fieldnameDatestamp, conf.fieldnameSource, conf.fieldnameXML));
    }
    props.put(conf.fieldnameDatestamp, ImmutableMap.of(
      "type", "date",
      "format", "dateOptionalTime",
      "precision_step", 8,
      "include_in_all", false
    ));
    props.put(conf.fieldnameSource, ImmutableMap.of(
      "type", "string",
      "index", "not_analyzed",
      "include_in_all", false,
      "store", false
    ));
    props.put(conf.fieldnameXML, ImmutableMap.of(
      "type", "string",
      "index", "no",
      "include_in_all", false,
      "store", false
    ));
    return XContentFactory.jsonBuilder().value(mapping).string();
  }
  
  /** Creates the index (if needed), configures it (mapping), and creates aliases. The real index name to be used is returned. */
  public String createIndex(TargetIndexConfig ticonf, boolean rebuilder) throws IOException {
    checkOpen();
    
    final IndicesAdminClient indicesAdmin = client.admin().indices();
    
    log.info("Getting index name for alias='" + ticonf.indexName + "'...");
    final String realIndexName;
    final Iterator<String> indexes = indicesAdmin.prepareGetAliases(ticonf.indexName).get().getAliases().keysIt();
    if (indexes.hasNext()) {
      String found = indexes.next();
      log.info("Alias exists and points to index='" + found + "'.");
      if (indexes.hasNext()) {
        throw new IOException("There are more than one index referred by alias='"+ticonf.indexName+"'");
      }
      if (rebuilder) {
        realIndexName = ticonf.indexName + (found.equals(ticonf.indexName + ticonf.nameSuffix1) ? ticonf.nameSuffix2 : ticonf.nameSuffix1);
        log.info("As rebuilding is requested, we create a new index: " + realIndexName);
        final DeleteIndexResponse resp = indicesAdmin.prepareDelete(realIndexName).setIndicesOptions(IndicesOptions.lenientExpandOpen()).get();
        log.info("Pre-existing index deleted: " + resp.isAcknowledged());
      } else {
        realIndexName = found;
      }
    } else {
      log.info("Alias does not yet exist, start fresh...");
      realIndexName = ticonf.indexName + ticonf.nameSuffix1;
    }

    log.info("Creating index='" + realIndexName + "'...");
    try {
      final CreateIndexRequestBuilder req = indicesAdmin.prepareCreate(realIndexName)
        .setCause(rebuilder ? "for rebuilding" : "new harvesting")
        .addMapping(DocumentProcessor.HARVESTER_METADATA_TYPE, HARVESTER_METADATA_MAPPING)
        .addMapping(conf.typeName, getMapping());
      if (ticonf.indexSettings != null) req.setSettings(ticonf.indexSettings);
      if (!rebuilder) req.addAlias(new Alias(ticonf.indexName));
      final CreateIndexResponse resp = req.get();
      log.info("Index and mappings created: " + resp.isAcknowledged());
    } catch (IndexAlreadyExistsException e) {
      log.info("Index already exists. Updating mappings for index='" + realIndexName + "'...");
      {
        final PutMappingResponse resp = indicesAdmin.preparePutMapping(realIndexName)
            .setType(DocumentProcessor.HARVESTER_METADATA_TYPE)
            .setSource(HARVESTER_METADATA_MAPPING)
            .setIgnoreConflicts(false)
            .get();
        log.info("Harvester metadata mapping updated: " + resp.isAcknowledged());
      }
      {
        final PutMappingResponse resp = indicesAdmin.preparePutMapping(realIndexName)
            .setType(conf.typeName)
            .setSource(getMapping())
            .setIgnoreConflicts(false)
            .get();
        log.info("XML metadata mapping updated: " + resp.isAcknowledged());
      }
    }

    return realIndexName;
  }
  
  /** Closes the index after harvesting and update the aliases to point to the active index. */
  public void closeIndex(TargetIndexConfig ticonf, String realIndexName, boolean cleanShutdown) throws IOException {
    checkOpen();
    
    log.info("Flushing data...");
    client.admin().indices().prepareFlush(realIndexName).get();
    
    final IndicesAdminClient indicesAdmin = client.admin().indices();
    
    final Iterator<String> indexes = indicesAdmin.prepareGetAliases(ticonf.indexName).get().getAliases().keysIt();
    String aliasedIndex = null;
    if (indexes.hasNext()) {
      aliasedIndex = indexes.next();
      if (indexes.hasNext()) {
        throw new IOException("There are more than one index referred by alias='" + ticonf.indexName + "'");
      }
    }
    
    if (cleanShutdown && !realIndexName.equals(aliasedIndex)) {
      log.info("Redirecting alias '" + ticonf.indexName + "' to new index: " + realIndexName);
      {
        IndicesAliasesRequestBuilder req = indicesAdmin.prepareAliases();
        if (aliasedIndex != null) {
          req.removeAlias(aliasedIndex, ticonf.indexName);
        }
        IndicesAliasesResponse resp = req.addAlias(realIndexName, ticonf.indexName).get();
        log.info("Aliases redirected: " + resp.isAcknowledged());
      }
      if (aliasedIndex != null) {
        log.info("Deleting orphaned index: " + aliasedIndex);
        DeleteIndexResponse resp = indicesAdmin.prepareDelete(aliasedIndex).get();
        log.info("Index deleted: " + resp.isAcknowledged());
      }
    }
  }
  
}