/*
 *   Copyright 2007 panFMP Developers Team c/o Uwe Schindler
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
import de.pangaea.metadataportal.config.*;
import java.util.*;
import java.io.*;
import java.net.URLEncoder;
import org.apache.commons.digester.*;

/**
 * Harvester for OAI-PMH repositories.
 * <p>This harvester supports the following additional <b>harvester properties</b> (please look into {@link OAIHarvesterBase} for further OAI-specific properties):<ul>
 * <li><code>baseUrl</code>: Base URL of OAI-PMH repository.</li>
 * </ul> 
 * @author Uwe Schindler
 */
public class OAIHarvester extends OAIHarvesterBase {
	// Class members
	private static final String[] requestVariables=new String[]{
		"verb",
		"identifier",
		"metadataPrefix",
		"from",
		"until",
		"set",
		"resumptionToken"
	};

	// Object members
	private ExtendedDigester listRecordsDig=null,identifyDig=null;
	private String currResumptionToken=null;
	private long currResumptionExpiration=-1L;
	private Date currResponseDate=null;
	private Map<String,String> currRequest=null;
	private boolean fineGranularity=false; // default for OAI 2.0

	// construtor
	@Override
	public void open(SingleIndexConfig iconfig) throws Exception {
		super.open(iconfig);

		//*** ListRecords ***
		listRecordsDig=new ExtendedDigester();
		digesterAddGeneralOAIRules(listRecordsDig);

		listRecordsDig.addDoNothing("OAI-PMH/ListRecords");
		listRecordsDig.addObjectCreate("OAI-PMH/ListRecords/record", OAIMetadataDocument.class);
		listRecordsDig.addSetNext("OAI-PMH/ListRecords/record", "addDocument");

		// setHeaderInfo(boolean deleted, String identifier, String datestampStr)
		listRecordsDig.addCallMethod("OAI-PMH/ListRecords/record/header", "setHeaderInfo", 3);
		listRecordsDig.addCallParam("OAI-PMH/ListRecords/record/header", 0, "status");
		listRecordsDig.addCallParam("OAI-PMH/ListRecords/record/header/identifier", 1);
		listRecordsDig.addCallParam("OAI-PMH/ListRecords/record/header/datestamp", 2);

		listRecordsDig.addCallMethod("OAI-PMH/ListRecords/record/header/setSpec", "addSet", 0);

		// metadata element
		listRecordsDig.addRule("OAI-PMH/ListRecords/record/metadata", new OAIMetadataSaxRule(xmlConverter));

		// dummy SAX handler to put <about> into trash
		listRecordsDig.addRule("OAI-PMH/ListRecords/record/about", SaxRule.emptyRule());

		// setResumptionToken(String token, String expirationDateStr, String cursorStr, String completeListSizeStr)
		listRecordsDig.addCallMethod("OAI-PMH/ListRecords/resumptionToken", "setResumptionToken", 4);
		listRecordsDig.addCallParam("OAI-PMH/ListRecords/resumptionToken", 0);
		listRecordsDig.addCallParam("OAI-PMH/ListRecords/resumptionToken", 1, "expirationDate");
		listRecordsDig.addCallParam("OAI-PMH/ListRecords/resumptionToken", 2, "cursor");
		listRecordsDig.addCallParam("OAI-PMH/ListRecords/resumptionToken", 3, "completeListSize");

		//*** Identify ***
		identifyDig=new ExtendedDigester();
		digesterAddGeneralOAIRules(identifyDig);

		identifyDig.addDoNothing("OAI-PMH/Identify");
		identifyDig.addCallMethod("OAI-PMH/Identify/granularity", "setGranularity", 0);

		// dummy SAX handler to put <description> into trash
		identifyDig.addRule("OAI-PMH/Identify/description", SaxRule.emptyRule());

		identifyDig.addDoNothing("OAI-PMH/Identify/*");
	}

	private void digesterAddGeneralOAIRules(ExtendedDigester dig) throws Exception {
		dig.setEntityResolver(getEntityResolver(dig.getEntityResolver()));
		dig.setNamespaceAware(true);
		dig.setValidating(false);
		dig.setRulesWithInvalidElementCheck( new ExtendedBaseRules() );

		//*** create rules ***
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
		if ("YYYY-MM-DD".equals(granularity)) this.fineGranularity=false;
		else if ("YYYY-MM-DDThh:mm:ssZ".equals(granularity)) this.fineGranularity=true;
		else throw new IllegalArgumentException("Invalid granularity in identify response: "+granularity);
	}

	@PublicForDigesterUse
	@Deprecated
	public void setResumptionToken(String token, String expirationDateStr, String cursorStr, String completeListSizeStr) {
		if (token!=null && token.equals("")) token=null;
		this.currResumptionToken=token;
		if (expirationDateStr!=null) try {
			currResumptionExpiration=ISODateFormatter.parseDate(expirationDateStr).getTime()-currResponseDate.getTime();
			if (currResumptionExpiration<=0L) currResumptionExpiration=-1L;
		} catch (Exception e) {
			currResumptionExpiration=-1L;
		}
	}

	@PublicForDigesterUse
	@Deprecated
	public void doError(String code, String message) throws OAIException {
		if (!"noRecordsMatch".equals(code)) throw new OAIException(code,message);
	}

	@PublicForDigesterUse
	@Deprecated
	public void setResponseDate(String date) throws java.text.ParseException {
		currResponseDate=ISODateFormatter.parseDate(date);
	}

	@PublicForDigesterUse
	@Deprecated
	public void setRequest(Map<String,String> req) {
		currRequest=req;
	}

	// harvester code
	private void readStream(String url) throws Exception {
		log.info("Harvesting \""+url+"\"...");
		doParse(listRecordsDig,url,null);
	}

	private void checkIdentify(String baseURL) throws Exception {
		StringBuilder url=new StringBuilder(baseURL);
		url.append("?verb=Identify");
		log.info("Reading identify response from \""+url+"\"...");
		doParse(identifyDig,url.toString(),null);
		log.info("Repository supports "+(fineGranularity?"seconds":"days")+"-granularity in selective harvesting.");
	}

	@Override
	protected void reset() {
		super.reset();
		currResponseDate=null;
		currResumptionToken=null;
		currRequest=null;
	}

	@Override
	public void close() throws Exception {
		listRecordsDig=null;
		identifyDig=null;
		super.close();
	}

	@Override
	public void harvest() throws Exception {
		if (index==null) throw new IllegalStateException("Index not yet opened");

		String baseUrl=iconfig.harvesterProperties.getProperty("baseUrl");
		if (baseUrl==null) throw new NullPointerException("No baseUrl of the OAI repository was given!");
		checkIdentify(baseUrl);
		reset();

		StringBuilder url=new StringBuilder(baseUrl);
		url.append("?verb=ListRecords&metadataPrefix=");
		url.append(URLEncoder.encode(metadataPrefix,"UTF-8"));
		if (sets!=null) {
			if (sets.size()==1) {
				url.append("&set=");
				url.append(URLEncoder.encode(sets.iterator().next(),"UTF-8"));
			} else log.warn("More than one set to be harvested - this is not supported by OAI-PMH. Filtering documents during indexing!");
		}
		if (fromDateReference!=null) {
			url.append("&from=");
			url.append(URLEncoder.encode(fineGranularity?ISODateFormatter.formatLong(fromDateReference):ISODateFormatter.formatShort(fromDateReference),"UTF-8"));
		}
		readStream(url.toString());
		Date lastHarvested=currResponseDate; // store reference date of first harvesting step, to be set at end

		while (currResumptionToken!=null) {
			// checkIndexerBuffer or harvester should max. wait for 1/2 resumption Token expiration!!!
			log.debug("Resumption token expires in "+currResumptionExpiration+" ms");
			index.checkIndexerBuffer();
			url=new StringBuilder(baseUrl);
			url.append("?verb=ListRecords&resumptionToken=");
			url.append(URLEncoder.encode(currResumptionToken,"UTF-8"));
			reset();
			readStream(url.toString());
		}
		reset();

		// set the date for next harvesting
		thisHarvestDateReference=lastHarvested;
	}

	@Override
	protected void enumerateValidHarvesterPropertyNames(Set<String> props) {
		super.enumerateValidHarvesterPropertyNames(props);
		props.add("baseUrl");
	}

}