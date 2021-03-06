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

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.digester.ExtendedBaseRules;

import de.pangaea.metadataportal.config.HarvesterConfig;
import de.pangaea.metadataportal.utils.ExtendedDigester;
import de.pangaea.metadataportal.utils.ISODateFormatter;
import de.pangaea.metadataportal.utils.PublicForDigesterUse;
import de.pangaea.metadataportal.utils.SaxRule;

/**
 * Harvester for OAI-PMH repositories.
 * <p>
 * This harvester supports the following additional <b>harvester properties</b>
 * (please look into {@link OAIHarvesterBase} for further OAI-specific
 * properties):
 * <ul>
 * <li><code>baseUrl</code>: Base URL of OAI-PMH repository.</li>
 * </ul>
 * 
 * @author Uwe Schindler
 */
public class OAIHarvester extends OAIHarvesterBase {
  // Class members
  private static final String[] requestVariables = new String[] {"verb",
      "identifier", "metadataPrefix", "from", "until", "set", "resumptionToken"};
  
  // Object members
  private ExtendedDigester listRecordsDig = null, identifyDig = null;
  private String currResumptionToken = null;
  private Duration currResumptionExpiration = null;
  private Instant currResponseDate = null;
  private boolean fineGranularity = false; // default for OAI 2.0
  
  public OAIHarvester(HarvesterConfig iconfig) {
    super(iconfig);
    this.filterIncomingSets = sets != null && sets.size() > 1;
  }

  @Override
  protected void recreateDigester() {
    // *** ListRecords ***
    listRecordsDig = new ExtendedDigester();
    digesterAddGeneralOAIRules(listRecordsDig);
    
    listRecordsDig.addDoNothing("OAI-PMH/ListRecords");
    listRecordsDig.addFactoryCreate("OAI-PMH/ListRecords/record",
        getMetadataDocumentFactory());
    listRecordsDig.addSetNext("OAI-PMH/ListRecords/record", "addDocument");
    
    // setHeaderInfo(boolean deleted, String identifier, String datestampStr)
    listRecordsDig.addCallMethod("OAI-PMH/ListRecords/record/header",
        "setHeaderInfo", 3);
    listRecordsDig.addCallParam("OAI-PMH/ListRecords/record/header", 0,
        "status");
    listRecordsDig.addCallParam("OAI-PMH/ListRecords/record/header/identifier",
        1);
    listRecordsDig.addCallParam("OAI-PMH/ListRecords/record/header/datestamp",
        2);
    
    listRecordsDig.addCallMethod("OAI-PMH/ListRecords/record/header/setSpec",
        "addSet", 0);
    
    // metadata element
    listRecordsDig.addRule("OAI-PMH/ListRecords/record/metadata",
        new OAIMetadataSaxRule());
    
    // dummy SAX handler to put <about> into trash
    listRecordsDig.addRule("OAI-PMH/ListRecords/record/about",
        SaxRule.emptyRule());
    
    // setResumptionToken(String token, String expirationDateStr, String
    // cursorStr, String completeListSizeStr)
    listRecordsDig.addCallMethod("OAI-PMH/ListRecords/resumptionToken",
        "setResumptionToken", 4);
    listRecordsDig.addCallParam("OAI-PMH/ListRecords/resumptionToken", 0);
    listRecordsDig.addCallParam("OAI-PMH/ListRecords/resumptionToken", 1,
        "expirationDate");
    listRecordsDig.addCallParam("OAI-PMH/ListRecords/resumptionToken", 2,
        "cursor");
    listRecordsDig.addCallParam("OAI-PMH/ListRecords/resumptionToken", 3,
        "completeListSize");
    
    // *** Identify ***
    identifyDig = new ExtendedDigester();
    digesterAddGeneralOAIRules(identifyDig);
    
    identifyDig.addDoNothing("OAI-PMH/Identify");
    identifyDig.addCallMethod("OAI-PMH/Identify/granularity", "setGranularity",
        0);
    
    // dummy SAX handler to put <description> into trash
    identifyDig.addRule("OAI-PMH/Identify/description", SaxRule.emptyRule());
    
    identifyDig.addDoNothing("OAI-PMH/Identify/*");
  }
  
  private void digesterAddGeneralOAIRules(ExtendedDigester dig) {
    dig.setEntityResolver(getEntityResolver(dig.getEntityResolver()));
    dig.setNamespaceAware(true);
    dig.setValidating(false);
    dig.setXIncludeAware(false);
    dig.setRulesWithInvalidElementCheck(new ExtendedBaseRules());
    
    // *** create rules ***
    dig.setRuleNamespaceURI(OAI_NS);
    
    // general info about request
    dig.addDoNothing("OAI-PMH");
    
    dig.addCallMethod("OAI-PMH/responseDate", "setResponseDate", 0);
    
    dig.addObjectCreate("OAI-PMH/request", HashMap.class);
    dig.addSetNext("OAI-PMH/request", "setRequest");
    for (String v : requestVariables) {
      dig.addCallMethod("OAI-PMH/request", "put", 2);
      dig.addObjectParam("OAI-PMH/request", 0, v);
      dig.addCallParam("OAI-PMH/request", 1, v);
    }
    dig.addCallMethod("OAI-PMH/request", "put", 2);
    dig.addObjectParam("OAI-PMH/request", 0, "url");
    dig.addCallParam("OAI-PMH/request", 1);
    
    // errors
    dig.addCallMethod("OAI-PMH/error", "doError", 2);
    dig.addCallParam("OAI-PMH/error", 0, "code");
    dig.addCallParam("OAI-PMH/error", 1);
  }
  
  // Digester entry points
  
  @PublicForDigesterUse
  @Deprecated
  public void setGranularity(String granularity) {
    if ("YYYY-MM-DD".equals(granularity)) this.fineGranularity = false;
    else if ("YYYY-MM-DDThh:mm:ssZ".equals(granularity)) this.fineGranularity = true;
    else throw new IllegalArgumentException(
        "Invalid granularity in identify response: " + granularity);
  }
  
  @PublicForDigesterUse
  @Deprecated
  public void setResumptionToken(String token, String expirationDateStr,
      String cursorStr, String completeListSizeStr) {
    if (token != null && token.equals("")) token = null;
    this.currResumptionToken = token;
    if (expirationDateStr != null && currResponseDate != null) try {
      currResumptionExpiration = Duration.between(currResponseDate, ISODateFormatter.parseOAIDate(expirationDateStr));
      if (currResumptionExpiration.isNegative()) currResumptionExpiration = null;
    } catch (Exception e) {
      currResumptionExpiration = null;
    }
  }
  
  @PublicForDigesterUse
  @Deprecated
  public void doError(String code, String message) throws OAIException {
    if (!"noRecordsMatch".equals(code)) throw new OAIException(code, message);
  }
  
  @PublicForDigesterUse
  @Deprecated
  public void setResponseDate(String date) {
    try {
      currResponseDate = ISODateFormatter.parseOAIDate(date);
    } catch (DateTimeParseException pe) {
      if (!ignoreDatestamps) {
        throw pe;
      } else {
        log.warn("Invalid datestamp in OAI response (ignored): " + date);
      }
    }
  }
  
  @PublicForDigesterUse
  @Deprecated
  public void setRequest(Map<String,String> req) {
  }
  
  // harvester code
  private void readStream(String url) throws Exception {
    log.info("Harvesting \"" + url + "\"...");
    doParse(() -> listRecordsDig, url, null);
  }
  
  private void checkIdentify(String baseURL) throws Exception {
    StringBuilder url = new StringBuilder(baseURL).append("?verb=Identify");
    log.info("Reading identify response from \"" + url + "\"...");
    doParse(() -> identifyDig, url.toString(), null);
    log.info("Repository supports " + (fineGranularity ? "seconds" : "days")
        + "-granularity in selective harvesting.");
  }
  
  @Override
  protected void reset() {
    super.reset();
    currResponseDate = null;
    currResumptionToken = null;
  }
  
  @Override
  public void close(boolean cleanShutdown) throws Exception {
    listRecordsDig = null;
    identifyDig = null;
    super.close(cleanShutdown);
  }
  
  @Override
  public void harvest() throws Exception {
    String baseUrl = iconfig.properties.getProperty("baseUrl");
    if (baseUrl == null) throw new NullPointerException(
        "No baseUrl of the OAI repository was given!");
    checkIdentify(baseUrl);
    reset();
    
    StringBuilder url = new StringBuilder(baseUrl)
      .append("?verb=ListRecords&metadataPrefix=")
      .append(URLEncoder.encode(metadataPrefix, StandardCharsets.UTF_8.name()));
    if (sets != null) {
      if (sets.size() == 1) {
        url.append("&set=").append(
            URLEncoder.encode(sets.iterator().next(), StandardCharsets.UTF_8.name()));
      } else {
        log.warn("More than one set to be harvested - this is not supported by OAI-PMH. Filtering documents during harvesting!");
      }
    }
    if (fromDateReference != null && !ignoreDatestamps) {
      final String from = ISODateFormatter.formatOAIDate(fromDateReference, fineGranularity);
      url.append("&from=").append(URLEncoder.encode(from, StandardCharsets.UTF_8.name()));
    } else {
      enableMissingDocumentDelete();
    }
    readStream(url.toString());
    setHarvestingDateReference(currResponseDate);
    
    while (currResumptionToken != null) {
      if (currResumptionExpiration != null) {
        log.debug("Resumption token expires in " + currResumptionExpiration.getSeconds() + "s");
      }
      url = new StringBuilder(baseUrl)
        .append("?verb=ListRecords&resumptionToken=")
        .append(URLEncoder.encode(currResumptionToken, StandardCharsets.UTF_8.name()));
      reset();
      readStream(url.toString());
    }
    reset();
  }
  
  @Override
  protected void enumerateValidHarvesterPropertyNames(Set<String> props) {
    super.enumerateValidHarvesterPropertyNames(props);
    props.add("baseUrl");
  }
  
}