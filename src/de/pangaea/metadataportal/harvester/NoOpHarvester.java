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
