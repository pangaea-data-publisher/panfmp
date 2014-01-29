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

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;

import de.pangaea.metadataportal.config.Config;
import de.pangaea.metadataportal.config.IndexConfig;

/**
 * TODO
 * 
 * @author Uwe Schindler
 */
public class ElasticSearchConnection implements Closeable {
  private static final org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(ElasticSearchConnection.class);
  
  private Client client;

  public ElasticSearchConnection(Config config) {
    final Settings settings = config.esSettings == null ? ImmutableSettings.Builder.EMPTY_SETTINGS : config.esSettings;
    log.info("Connecting to ElasticSearch nodes: " + config.esTransports);
    if (log.isDebugEnabled()) {
      log.debug("ES connection settings: " + settings.getAsMap());
    }
    this.client = new TransportClient(settings, false)
      .addTransportAddresses(config.esTransports.toArray(new TransportAddress[config.esTransports.size()]));
  }

  @Override
  public void close() {
    client.close();
    client = null;
    log.info("Closed connection to ElasticSearch.");
  }
  
  public Client client() {
    if (client == null)
      throw new IllegalStateException("ElasticSearch TransportClient is already closed.");
    return client;
  }
  
  public IndexBuilder getIndexBuilder(IndexConfig iconfig) {
    return new IndexBuilder(client(), iconfig);
  }
  
}