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
import java.util.List;
import java.util.Map;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceAlreadyExistsException;
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
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

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
  private final Config conf;

  public static final int ELASTICSEARCH_DEFAULT_PORT = 9300;
  
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
                .field("type", "keyword").field("index", false).field("store", false).field("doc_values", false)
              .endObject()
            .endObject()
          .endObject()
        .endArray()
      .endObject().string();
    } catch (IOException ioe) {
      throw new AssertionError("Cannot happen", ioe);
    }
  }
  
  @SuppressWarnings("resource")
  public ElasticsearchConnection(Config config) {
    final Settings settings = config.esSettings == null ? Settings.Builder.EMPTY_SETTINGS : config.esSettings;
    log.info("Connecting to Elasticsearch nodes: " + config.esTransports);
    if (log.isDebugEnabled()) {
      log.debug("ES connection settings: " + settings.getAsMap());
    }
    this.conf = config;
    this.client = new PreBuiltTransportClient(settings)
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
  
  public DocumentProcessor getDocumentProcessor(HarvesterConfig iconfig, String targetIndex) {
    return new DocumentProcessor(client(), iconfig, targetIndex);
  }
  
  public void waitForYellow(TargetIndexConfig ticonf) {
    checkOpen();
    log.info("Waiting for index '" + ticonf.indexName + "' to get available...");
    client.admin().cluster().prepareHealth(ticonf.indexName).setWaitForYellowStatus().get();
  }
  
  private String getAliasedIndex(TargetIndexConfig ticonf) {
    final Iterator<String> indexes = client.admin().indices().prepareGetAliases(ticonf.indexName).get().getAliases().keysIt();
    String aliasedIndex = null;
    if (indexes.hasNext()) {
      aliasedIndex = indexes.next();
      if (indexes.hasNext()) {
        throw new ElasticsearchException("There are more than one index referred by alias='" + ticonf.indexName + "'");
      }
    }
    return aliasedIndex;
  }
  
  /** Creates the index (if needed), configures it (mapping), and creates aliases. The real index name to be used is returned. */
  public String createIndex(TargetIndexConfig ticonf, boolean rebuilder) {
    checkOpen();
    
    final IndicesAdminClient indicesAdmin = client.admin().indices();
    
    log.info("Getting index name for alias='" + ticonf.indexName + "'...");
    final String realIndexName, aliasedIndex = getAliasedIndex(ticonf);
    if (aliasedIndex != null) {
      log.info("Alias exists and points to index='" + aliasedIndex + "'.");
      if (rebuilder) {
        realIndexName = ticonf.getRawIndexName(aliasedIndex.equals(ticonf.getRawIndexName(false)));
        log.info("As rebuilding is requested, we have to create a new index: " + realIndexName);
        final DeleteIndexResponse resp = indicesAdmin.prepareDelete(realIndexName).setIndicesOptions(IndicesOptions.lenientExpandOpen()).get();
        log.info("Pre-existing index deleted: " + resp.isAcknowledged());
      } else {
        realIndexName = aliasedIndex;
      }
    } else {
      log.info("Alias does not yet exist, start fresh...");
      realIndexName = ticonf.getRawIndexName(false);
    }

    log.info("Creating index='" + realIndexName + "' and aliases...");
    final String mapping = getMapping();
    try {
      final CreateIndexRequestBuilder req = indicesAdmin.prepareCreate(realIndexName)
        .setCause(rebuilder ? "for rebuilding" : "new harvesting")
        .addMapping(DocumentProcessor.HARVESTER_METADATA_TYPE, HARVESTER_METADATA_MAPPING)
        .addMapping(conf.typeName, mapping);
      if (ticonf.indexSettings != null) req.setSettings(ticonf.indexSettings);
      if (!rebuilder) {
        req.addAlias(new Alias(ticonf.indexName));
        for (Map.Entry<String,String> e : ticonf.aliases.entrySet()) {
          req.addAlias(new Alias(e.getKey()).filter(e.getValue()));
        }
      }
      final CreateIndexResponse resp = req.get();
      log.info("Index created and mappings assigned: " + resp.isAcknowledged());
    } catch (ResourceAlreadyExistsException e) {
      log.info("Index already exists. Updating mappings for index='" + realIndexName + "'...");
      {
        final PutMappingResponse resp = indicesAdmin.preparePutMapping(realIndexName)
            .setType(DocumentProcessor.HARVESTER_METADATA_TYPE)
            .setSource(HARVESTER_METADATA_MAPPING)
            .get();
        log.info("Harvester metadata mapping updated: " + resp.isAcknowledged());
      }
      {
        final PutMappingResponse resp = indicesAdmin.preparePutMapping(realIndexName)
            .setType(conf.typeName)
            .setSource(mapping)
            .get();
        log.info("XML metadata mapping updated: " + resp.isAcknowledged());
      }
    }

    waitForYellow(ticonf);
    return realIndexName;
  }
  
  /** Closes the index after harvesting and update the aliases to point to the active index. */
  public void closeIndex(TargetIndexConfig ticonf, String realIndexName, boolean cleanShutdown) {
    checkOpen();
    
    final IndicesAdminClient indicesAdmin = client.admin().indices();
    
    log.info("Flushing data...");
    indicesAdmin.prepareFlush(realIndexName).get();
    
    final String aliasedIndex = getAliasedIndex(ticonf);
    if (cleanShutdown && !realIndexName.equals(aliasedIndex)) {
      {
        log.info("Redirecting alias '" + ticonf.indexName + "' to new index: " + realIndexName);
        if (!ticonf.aliases.isEmpty()) {
          log.info("Redirecting additional alias(es) " + ticonf.aliases.keySet() + " to new index: " + realIndexName);
        }
        IndicesAliasesRequestBuilder req = indicesAdmin.prepareAliases();
        if (aliasedIndex != null) {
          req.removeAlias(aliasedIndex, ticonf.indexName);
          for (String a : ticonf.aliases.keySet()) {
            req.removeAlias(aliasedIndex, a);
          }
        }
        req.addAlias(realIndexName, ticonf.indexName);
        for (Map.Entry<String,String> e : ticonf.aliases.entrySet()) {
          req.addAlias(realIndexName, e.getKey(), e.getValue());
        }
        IndicesAliasesResponse resp = req.get();
        log.info("Aliases redirected: " + resp.isAcknowledged());
      }
      if (aliasedIndex != null) {
        log.info("Deleting orphaned index: " + aliasedIndex);
        DeleteIndexResponse resp = indicesAdmin.prepareDelete(aliasedIndex).get();
        log.info("Index deleted: " + resp.isAcknowledged());
      }
    }
  }
  
  /** Closes the index after harvesting and update the aliases to point to the active index. */
  public void updateAliases(TargetIndexConfig ticonf) {
    waitForYellow(ticonf);
    
    final IndicesAdminClient indicesAdmin = client.admin().indices();
    
    final ImmutableOpenMap<String, List<AliasMetaData>> indexCol = indicesAdmin.prepareGetAliases("*")
        .setIndices(ticonf.indexName)
        .setIndicesOptions(IndicesOptions.strictSingleIndexNoExpandForbidClosed())
        .get().getAliases();
    final Iterator<String> indexes = indexCol.keysIt();
    if (indexes.hasNext()) {
      final String aliasedIndex = indexes.next();
      if (indexes.hasNext()) {
        throw new ElasticsearchException("There are more than one index referred by alias='" + ticonf.indexName + "'");
      }
      final List<AliasMetaData> aliases = indexCol.get(aliasedIndex);
      final IndicesAliasesRequestBuilder req = indicesAdmin.prepareAliases();
      for (AliasMetaData a : aliases) {
        final String alias = a.alias();
        if (!(ticonf.indexName.equals(alias) || ticonf.aliases.containsKey(alias))) {
          log.info("Removing outdated alias: " + alias);
          req.removeAlias(aliasedIndex, alias);
        }
      }
      for (Map.Entry<String,String> e : ticonf.aliases.entrySet()) {
        log.info("Updating alias: " + e.getKey());
        req.addAlias(aliasedIndex, e.getKey(), e.getValue());
      }
      final IndicesAliasesResponse resp = req.get();
      log.info("Aliases updated: " + resp.isAcknowledged());

    } else {
      throw new ElasticsearchException("The alias='" + ticonf.indexName + "' does not exist.");
    }
  }
  
  private Map<Object, Object> mapOf(Object... items) {
    if (items.length % 2 != 0) {
      throw new IllegalArgumentException("Invalid number of arguments.");
    }
    final Map<Object, Object> map = new LinkedHashMap<>();
    for (int i = 0; i < items.length; i += 2) {
      map.put(items[i], items[i+1]);
    }
    return map;
  }
  
  @SuppressWarnings("unchecked")
  private String getMapping() {
    try {
      Map<String,Object> mapping, props;
      if (conf.esMapping != null) {
        mapping = XContentFactory.xContent(conf.esMapping).createParser(NamedXContentRegistry.EMPTY, conf.esMapping).mapOrdered();
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
      props.put(conf.fieldnameDatestamp, mapOf(
        "type", "date",
        "format", "dateOptionalTime",
        "index", true,
        "include_in_all", false
      ));
      props.put(conf.fieldnameSource, mapOf(
        "type", "keyword",
        "index", true,
        "include_in_all", false
      ));
      props.put(conf.fieldnameXML, mapOf(
        "type", "keyword",
        "index", false,
        "doc_values", false
      ));
      return XContentFactory.jsonBuilder().map(mapping).string();
    } catch (IOException ioe) {
      throw new AssertionError("Cannot happen, because no IO involved.", ioe);
    }
  }
  
}