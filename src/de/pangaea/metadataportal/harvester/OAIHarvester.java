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
import java.net.URL;
import org.xml.sax.*;
import org.apache.commons.digester.*;
import javax.xml.transform.sax.*;

public class OAIHarvester extends AbstractHarvester {
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
    public static final String OAI_NS="http://www.openarchives.org/OAI/2.0/";
    public static final int DEFAULT_RETRY_TIME = 60; // seconds
    public static final int DEFAULT_RETRY_COUNT = 5;

    // Object members
    private ExtendedDigester listRecordsDig=null,identifyDig=null;
    private String currResumptionToken=null;
    private long currResumptionExpiration=-1L;
    private Date currResponseDate=null;
    private Map<String,String> currRequest=null;
    private Set<String> sets=null;
    private int retryCount=DEFAULT_RETRY_COUNT;
    private int retryTime=DEFAULT_RETRY_TIME;
    public boolean fineGranularity=false; // default for OAI 2.0

    // construtor
    public void open(SingleIndexConfig iconfig) throws Exception {
        super.open(iconfig);

        String setSpec=iconfig.harvesterProperties.get("setSpec");
        if (setSpec!=null) {
            sets=new HashSet<String>();
            Collections.addAll(sets,setSpec.split("[\\,\\;\\s]+"));
            if (sets.size()==0) sets=null;
        }

        String retryCountStr=iconfig.harvesterProperties.get("retryCount");
        if (retryCountStr!=null) retryCount=Integer.parseInt(retryCountStr);
        String retryTimeStr=iconfig.harvesterProperties.get("retryAfterSeconds");
        if (retryTimeStr!=null) retryTime=Integer.parseInt(retryTimeStr);

        //*** ListRecords ***
        listRecordsDig=new ExtendedDigester();
        digesterAddGeneralOAIRules(listRecordsDig);

        listRecordsDig.addDoNothing("OAI-PMH/ListRecords");
        listRecordsDig.addObjectCreate("OAI-PMH/ListRecords/record", MetadataDocument.class);
        listRecordsDig.addSetNext("OAI-PMH/ListRecords/record", "addDocument");

        // setHeaderInfo(boolean deleted, String identifier, String datestampStr)
        listRecordsDig.addCallMethod("OAI-PMH/ListRecords/record/header", "setHeaderInfo", 3);
        listRecordsDig.addCallParam("OAI-PMH/ListRecords/record/header", 0, "status");
        listRecordsDig.addCallParam("OAI-PMH/ListRecords/record/header/identifier", 1);
        listRecordsDig.addCallParam("OAI-PMH/ListRecords/record/header/datestamp", 2);

        listRecordsDig.addCallMethod("OAI-PMH/ListRecords/record/header/setSpec", "addSet", 0);

        // metadata element
        listRecordsDig.addRule("OAI-PMH/ListRecords/record/metadata", new OAIMetadataSaxRule(new XMLConverter(iconfig)));

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

    protected void digesterAddGeneralOAIRules(ExtendedDigester dig) throws Exception {
        dig.setEntityResolver(OAIDownload.getEntityResolver(dig.getEntityResolver()));
        dig.setLogger(log.isDebugEnabled()?log:new org.apache.commons.logging.impl.NoOpLog());
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

    public void setGranularity(String granularity) {
        if ("YYYY-MM-DD".equals(granularity)) this.fineGranularity=false;
        else if ("YYYY-MM-DDThh:mm:ssZ".equals(granularity)) this.fineGranularity=true;
        else throw new IllegalArgumentException("Invalid granularity in identify response: "+granularity);
    }

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

    public void doError(String code, String message) throws OAIException {
        if (!"noRecordsMatch".equals(code)) throw new OAIException(code,message);
    }

    public void setResponseDate(String date) throws java.text.ParseException {
        currResponseDate=ISODateFormatter.parseDate(date);
    }

    public void setRequest(Map<String,String> req) {
        currRequest=req;
    }

    public void addDocument(MetadataDocument mdoc) throws Exception {
        if (sets!=null) {
            if (Collections.disjoint(mdoc.sets,sets)) mdoc.deleted=true;
        }
        super.addDocument(mdoc);
    }

    // harvester code

    protected void doParse(ExtendedDigester dig, String url, int retryCount) throws Exception {
        URL u=new URL(url);
        try {
            dig.clear();
            dig.resetRoot();
            dig.push(this);
            dig.parse(OAIDownload.getInputSource(u));
        } catch (IOException ioe) {
            int after=retryTime;
            if (ioe instanceof RetryAfterIOException) {
                if (retryCount==0) throw (IOException)ioe.getCause();
                log.warn("OAI-PMH server returned '503 Service Unavailable' with a 'Retry-After' value being set.");
                after=((RetryAfterIOException)ioe).getRetryAfter();
            } else {
                if (retryCount==0) throw ioe;
                log.error("OAI-PMH server access failed with exception: ",ioe);
            }
            log.info("Retrying after "+after+" seconds ("+retryCount+" retries left)...");
            try { Thread.sleep(1000L*after); } catch (InterruptedException ie) {}
            doParse(dig,url,retryCount-1);
        }
    }

    protected void reset() {
        currResumptionToken=null;
        currResponseDate=null;
        currRequest=null;
    }

    protected void readStream(String url) throws Exception {
        log.info("Harvesting \""+url+"\"...");
        doParse(listRecordsDig,url,retryCount);
        log.info("Harvested "+harvestCount+" documents so far.");
    }

    public void checkIdentify(String baseURL) throws Exception {
        StringBuilder url=new StringBuilder(baseURL);
        url.append("?verb=Identify");
        log.info("Reading identify response from \""+url+"\"...");
        doParse(identifyDig,url.toString(),retryCount);
        log.info("Repository supports "+(fineGranularity?"seconds":"days")+"-granularity in selective harvesting.");
    }

    public void close() throws Exception {
        reset();
        listRecordsDig=null;
        identifyDig=null;
        super.close();
    }

    public void harvest() throws Exception {
        if (index==null) throw new IllegalStateException("Index not yet opened");

        String baseUrl=iconfig.harvesterProperties.get("baseUrl");
        checkIdentify(baseUrl);
        reset();

        index.setChangesBeforeCommit(1000,2000);
        Date from=index.getLastHarvestedFromDisk();
        StringBuilder url=new StringBuilder(baseUrl);
        url.append("?verb=ListRecords&metadataPrefix=");
        url.append(URLEncoder.encode(iconfig.harvesterProperties.get("metadataPrefix"),"UTF-8"));
        if (sets!=null) {
            if (sets.size()==1) {
                url.append("&set=");
                url.append(URLEncoder.encode(sets.iterator().next(),"UTF-8"));
            } else log.warn("More than one set to be harvested - this is not supported by OAI-PMH. Filtering documents during indexing!");
        }
        if (from!=null) {
            url.append("&from=");
            url.append(URLEncoder.encode(fineGranularity?ISODateFormatter.formatLong(from):ISODateFormatter.formatShort(from),"UTF-8"));
        }
        readStream(url.toString());
        Date lastHarvested=currResponseDate;
        if (currResumptionToken!=null) {
            // resize the maximum number for commits to index not to break HTTP downloads!
            if (harvestCount>500) index.setChangesBeforeCommit(harvestCount*2,harvestCount*4);
        }

        while (currResumptionToken!=null) {
            // checkIndexerBuffer or harvester should max. wait for 1/2 resumption Token expiration!!!
            log.debug("Resumption token expires in "+currResumptionExpiration+" ms");
            long maxWaitTime=(currResumptionExpiration>0L) ? currResumptionExpiration/2L : -1;
            index.setMaxWaitForIndexer(maxWaitTime);
            index.checkIndexerBuffer();
            url=new StringBuilder(baseUrl);
            url.append("?verb=ListRecords&resumptionToken=");
            url.append(URLEncoder.encode(currResumptionToken,"UTF-8"));
            reset();
            readStream(url.toString());
        }
        reset();

        index.setLastHarvested(lastHarvested);
    }
}