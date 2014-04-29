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

import java.util.LinkedHashSet;
import java.util.Set;

import org.elasticsearch.search.SearchHit;

import de.pangaea.metadataportal.config.HarvesterConfig;
import de.pangaea.metadataportal.processor.MetadataDocument;
import de.pangaea.metadataportal.utils.ISODateFormatter;
import de.pangaea.metadataportal.utils.KeyValuePairs;
import de.pangaea.metadataportal.utils.PublicForDigesterUse;

/**
 * Special implementation of {@link MetadataDocument} that adds OAI set support
 * to internal fields
 * 
 * @author Uwe Schindler
 */
public class OAIMetadataDocument extends MetadataDocument {
  
  /**
   * Constructor, that creates an empty instance for the supplied index
   * configuration.
   */
  public OAIMetadataDocument(HarvesterConfig iconfig) {
    super(iconfig);
  }
  
  @PublicForDigesterUse
  @Deprecated
  public void setHeaderInfo(String status, String identifier,
      String datestampStr) throws java.text.ParseException {
    setDeleted(status != null && status.equals("deleted"));
    setIdentifier(identifier);
    setDatestamp(ISODateFormatter.parseDate(datestampStr));
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
  public void loadFromElasticSearchHit(SearchHit hit) throws Exception {
    sets.clear();
    super.loadFromElasticSearchHit(hit);
    /* Currently Sets get lost...
    String[] sets = ldoc.getValues(IndexConstants.FIELDNAME_SET);
    if (sets != null) for (String set : sets) {
      if (set != null) addSet(set);
    }
    */
  }
  
  @Override
  protected KeyValuePairs createEmptyKeyValuePairs() throws Exception {
    final KeyValuePairs kv = super.createEmptyKeyValuePairs();
    /* TODO supply as variable!
    if (builder != null) {
      kv.add(IndexConstants.FIELDNAME_SET, sets.toArray(new String[sets.size()]));
    }
    */
    return kv;
  }
  
  @Override
  public String toString() {
    return super.toString() + " sets=" + sets;
  }
  
  /**
   * @see #getSets
   */
  protected Set<String> sets = new LinkedHashSet<String>();
  
}
