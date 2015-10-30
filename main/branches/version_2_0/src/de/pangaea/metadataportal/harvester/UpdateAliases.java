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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import de.pangaea.metadataportal.config.Config;
import de.pangaea.metadataportal.config.TargetIndexConfig;
import de.pangaea.metadataportal.processor.ElasticsearchConnection;

/**
 * TODO
 * 
 * @author Uwe Schindler
 */
public final class UpdateAliases {
  
  private static final org.apache.commons.logging.Log staticLog = org.apache.commons.logging.LogFactory.getLog(UpdateAliases.class);
  
  // main-Methode
  public static void main(String[] args) {
    if (args.length < 1 || args.length > 2) {
      System.err.println("Command line: java " + UpdateAliases.class.getName()
          + " config.xml [elasticsearch-target-index|*]");
      return;
    }
    
    try {
      Config conf = new Config(args[0]);
      runUpdateAliases(conf, (args.length == 2) ? args[1] : null);
    } catch (Exception e) {
      staticLog.fatal("Update alias tool general error:", e);
    }
  }
  
  public static void runUpdateAliases(Config conf, String id) throws IOException {
    final Collection<TargetIndexConfig> subset;
    if (Harvester.isAllIndexes(id)) {
      subset = conf.targetIndexes.values();
    } else {
      if (!conf.targetIndexes.containsKey(id))
        throw new IllegalArgumentException("There is no targetIndex defined with id=\"" + id + "\"!");
      subset = Collections.singletonList(conf.targetIndexes.get(id));
    }
    try (ElasticsearchConnection es = new ElasticsearchConnection(conf)) {
      for (final TargetIndexConfig ticonf : subset) {
        es.updateAliases(ticonf);
      }
    }
  }
  
}
