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

import de.pangaea.metadataportal.config.HarvesterConfig;

/**
 * This harvester can be used to 'disable' harvesting from a source,
 * e.g. to keep the harvester identifier in config file, but use it
 * only in direct push mode or similar.
 * 
 * @author Uwe Schindler
 */
public class NoOpHarvester extends Harvester {
  
  public NoOpHarvester(HarvesterConfig iconfig) {
    super(iconfig);
  }

  @Override
  public void harvest() throws Exception {
    log.info("Nothing to harvest.");
  }
  
}
