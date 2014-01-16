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

import de.pangaea.metadataportal.utils.*;
import de.pangaea.metadataportal.config.IndexConfig;

import java.util.*;

import org.elasticsearch.common.xcontent.XContentBuilder;

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
  public OAIMetadataDocument(IndexConfig iconfig) {
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
  
  /*@Override
  public void loadFromLucene(Document ldoc) throws Exception {
    sets.clear();
    super.loadFromLucene(ldoc);
    String[] sets = ldoc.getValues(IndexConstants.FIELDNAME_SET);
    if (sets != null) for (String set : sets)
      if (set != null) addSet(set);
  }*/
  
  @Override
  protected XContentBuilder createEmptyDocument() throws Exception {
    XContentBuilder builder = super.createEmptyDocument();
    if (builder != null) {
      builder.field(IndexConstants.FIELDNAME_SET, sets.toArray(new String[sets.size()]));
    }
    return builder;
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
