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

import java.time.Instant;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.digester.ExtendedBaseRules;
import org.apache.commons.digester.Rule;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

import de.pangaea.metadataportal.config.HarvesterConfig;
import de.pangaea.metadataportal.processor.ElasticsearchConnection;
import de.pangaea.metadataportal.processor.MetadataDocument;
import de.pangaea.metadataportal.utils.ExtendedDigester;
import de.pangaea.metadataportal.utils.SaxRule;

/**
 * Harvester for OAI static repositories.
 * <p>
 * This harvester supports the following additional <b>harvester properties</b>
 * (please look into {@link OAIHarvesterBase} for further OAI-specific
 * properties):
 * <ul>
 * <li><code>url</code>: URL of static repository</li>
 * </ul>
 * 
 * @author Uwe Schindler
 */
public class OAIStaticRepositoryHarvester extends OAIHarvesterBase {
  // Object members
  private ExtendedDigester dig = null;
  String currMetadataPrefix = null;
  OAIMetadataSaxRule metadataSaxRule = null;
  
  // construtor
  public OAIStaticRepositoryHarvester(HarvesterConfig iconfig) {
    super(iconfig);
    filterIncomingSets = true; // always filter set names
  }

  @Override
  public void open(ElasticsearchConnection es, String targetIndex) throws Exception {
    super.open(es, targetIndex);
    
    if (sets != null) {
      log.warn("Sets are currently not supported by static OAI repositories. "
          + "This may change in future (and so it is implemented in the harvester), "
          + "but may only work with non-conformant repositories, that list setSpecs in metadata headers.");
    }
    
    enableMissingDocumentDelete();
  }

  @Override
  protected void recreateDigester() {
    // *** ListRecords ***
    dig = new ExtendedDigester();
    
    dig.setEntityResolver(getEntityResolver(dig.getEntityResolver()));
    dig.setNamespaceAware(true);
    dig.setValidating(false);
    dig.setXIncludeAware(false);
    dig.setRulesWithInvalidElementCheck(new ExtendedBaseRules());
    
    // *** create rules ***
    dig.setRuleNamespaceURI(OAI_STATICREPOSITORY_NS);
    dig.addDoNothing("Repository");
    
    // *** Identify part (not interesting)
    dig.setRuleNamespaceURI(OAI_STATICREPOSITORY_NS);
    dig.addRule("Repository/Identify", SaxRule.emptyRule());
    
    // *** ListMetadataFormats part (not interesting)
    dig.setRuleNamespaceURI(OAI_STATICREPOSITORY_NS);
    dig.addRule("Repository/ListMetadataFormats", SaxRule.emptyRule());
    
    // *** List Records part
    dig.setRuleNamespaceURI(OAI_STATICREPOSITORY_NS);
    dig.addRule("Repository/ListRecords", new Rule() {
      // special rule that sets current metadataPrefix on begin of ListRecords
      // and unsets it at the end (to be sure, to not harvest documents with
      // missing prefix)
      @Override
      public void begin(String namespace, String name, Attributes attributes)
          throws Exception {
        currMetadataPrefix = attributes.getValue("metadataPrefix");
        if (currMetadataPrefix == null) throw new SAXException(
            "Missing attribute 'metadataPrefix' at '" + digester.getMatch()
                + "'");
        metadataSaxRule.setEnabled(metadataPrefix.equals(currMetadataPrefix));
      }
      
      @Override
      public void end(String namespace, String name) throws Exception {
        currMetadataPrefix = null;
        metadataSaxRule.setEnabled(false);
      }
    });
    
    dig.setRuleNamespaceURI(OAI_NS);
    dig.addFactoryCreate("Repository/ListRecords/record",
        getMetadataDocumentFactory());
    dig.addSetNext("Repository/ListRecords/record", "addDocument");
    
    // setHeaderInfo(boolean deleted, String identifier, String datestampStr)
    dig.addCallMethod("Repository/ListRecords/record/header", "setHeaderInfo",
        3);
    dig.addCallParam("Repository/ListRecords/record/header", 0, "status");
    dig.addCallParam("Repository/ListRecords/record/header/identifier", 1);
    dig.addCallParam("Repository/ListRecords/record/header/datestamp", 2);
    
    dig.addCallMethod("Repository/ListRecords/record/header/setSpec", "addSet",
        0);
    
    // metadata element
    dig.addRule("Repository/ListRecords/record/metadata",
        metadataSaxRule = new OAIMetadataSaxRule());
    metadataSaxRule.setEnabled(false);
    
    // dummy SAX handler to put <about> into trash
    dig.addRule("Repository/ListRecords/record/about", SaxRule.emptyRule());
  }
  
  // harvester code
  @Override
  public void addDocument(MetadataDocument mdoc) throws Exception {
    if (metadataPrefix.equals(currMetadataPrefix)) {
      super.addDocument(mdoc);
    }
  }
  
  @Override
  public void close(boolean cleanShutdown) throws Exception {
    dig = null;
    super.close(cleanShutdown);
  }
  
  @Override
  public void harvest() throws Exception {
    String url = iconfig.properties.getProperty("url");
    if (url == null) throw new NullPointerException(
        "No URL of the OAI static repository was given!");
    
    log.info("Harvesting static repository at \"" + url + "\"...");
    AtomicReference<Instant> modifiedDate = new AtomicReference<>(fromDateReference);
    if (doParse(() -> dig, url, modifiedDate)) {
      // set the date for next harvesting
      setHarvestingDateReference(modifiedDate.get());
    } else {
      log.info("Static OAI repository file was not modified since last harvesting, no need for re-harvesting!");
      cancelMissingDocumentDelete();
    }
  }
  
  @Override
  protected void enumerateValidHarvesterPropertyNames(Set<String> props) {
    super.enumerateValidHarvesterPropertyNames(props);
    props.add("url");
  }
  
}