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

import java.time.format.DateTimeParseException;
import java.util.LinkedHashSet;
import java.util.Set;

import de.pangaea.metadataportal.config.HarvesterConfig;
import de.pangaea.metadataportal.processor.MetadataDocument;
import de.pangaea.metadataportal.utils.ISODateFormatter;
import de.pangaea.metadataportal.utils.PublicForDigesterUse;

/**
 * Special implementation of {@link MetadataDocument} that adds OAI set support
 * to internal fields
 * 
 * @author Uwe Schindler
 */
public class OAIMetadataDocument extends MetadataDocument {
  
  private static final org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(OAIMetadataDocument.class);
  
  private final String identifierPrefix;
  private final boolean ignoreDatestamps;
  
  /**
   * Constructor, that creates an empty instance for the supplied index
   * configuration.
   */
  public OAIMetadataDocument(HarvesterConfig iconfig, String identifierPrefix, boolean ignoreDatestamps) {
    super(iconfig);
    this.identifierPrefix = identifierPrefix;
    this.ignoreDatestamps = ignoreDatestamps;
  }
  
  @PublicForDigesterUse
  @Deprecated
  public void setHeaderInfo(String status, String identifier, String datestampStr) {
    setDeleted(status != null && status.equals("deleted"));
    setIdentifier(identifierPrefix + identifier);
    try {
      setDatestamp(ISODateFormatter.parseOAIDate(datestampStr));
    } catch (DateTimeParseException pe) {
      if (ignoreDatestamps || deleted) {
        log.warn("Invalid datestamp in OAI response (ignored): " + datestampStr);
      } else {
        throw pe;
      }
    }
  }
  
  /**
   * Adds an OAI set to the set {@link Set}.
   */
  public void addSet(String set) {
    sets.add(set);
  }
  
  /**
   * Returns all OAI sets as {@link Set}.
   */
  public Set<String> getSets() {
    return sets;
  }
  
  @Override
  public String toString() {
    return super.toString() + " sets=" + sets;
  }
  
  /**
   * @see #getSets
   */
  protected Set<String> sets = new LinkedHashSet<>();
  
}
