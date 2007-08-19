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
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.zip.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import javax.xml.transform.sax.SAXSource;
import org.xml.sax.*;
import org.xml.sax.helpers.DefaultHandler;

public class WebCrawlingHarvester extends Harvester {

    public static final int DEFAULT_RETRY_TIME = 60; // seconds
    public static final int DEFAULT_RETRY_COUNT = 5;

    /**
     * This is the parser class used to parse HTML documents to collect URLs for crawling.
     * If this class is not in your classpath, the harvester will fail on startup in {@link #open}.
     * If you change the implementation (possibly in future a HTML parser is embedded in XERCES),
     * change this. Do not forget to revisit the features for this parser in the parsing method.
     */
    public static final String HTML_SAX_PARSER_CLASS="org.cyberneko.html.parsers.SAXParser";

    // Class members
    private String baseURL=null;
    private Pattern filenameFilter=null;
    private Set<String> contentTypes=new HashSet<String>();
    private Set<String> validIdentifiers=null;
    private int retryCount=DEFAULT_RETRY_COUNT;
    private int retryTime=DEFAULT_RETRY_TIME;
    private long pauseBetweenRequests=0;

    private Set<String> harvested=new HashSet<String>();
    private SortedSet<String> needsHarvest=new TreeSet<String>();
    private Date firstDateFound=null;

    private Class<? extends XMLReader> htmlReaderClass=null;

    @Override
    public void open(SingleIndexConfig iconfig) throws Exception {
        super.open(iconfig);

        String s=iconfig.harvesterProperties.getProperty("baseUrl");
        if (s==null) throw new IllegalArgumentException("Missing base URL to start harvesting (property \"baseUrl\")");
        URL u=new URL(s);
        String proto=u.getProtocol().toLowerCase();
        if (!("http".equals(proto) || "https".equals(proto)))
            throw new IllegalArgumentException("WebCrawlingHarvester only allows HTTP(S) as network protocol!");
        baseURL=u.toString();

        s=iconfig.harvesterProperties.getProperty("contentTypes","text/xml,application/xml");
        for (String c : s.split("[\\,\\;\\s]+")) {
            c=c.trim().toLowerCase();
            if (!"".equals(c)) contentTypes.add(c);
        }

        String retryCountStr=iconfig.harvesterProperties.getProperty("retryCount");
        if (retryCountStr!=null) retryCount=Integer.parseInt(retryCountStr);
        String retryTimeStr=iconfig.harvesterProperties.getProperty("retryAfterSeconds");
        if (retryTimeStr!=null) retryTime=Integer.parseInt(retryTimeStr);
        String pauseBetweenRequestsStr=iconfig.harvesterProperties.getProperty("pauseBetweenRequests");
        if (pauseBetweenRequestsStr!=null) pauseBetweenRequests=Long.parseLong(pauseBetweenRequestsStr);

        s=iconfig.harvesterProperties.getProperty("filenameFilter");
        filenameFilter=(s==null) ? null : Pattern.compile(s);

        validIdentifiers=null;
        if (BooleanParser.parseBoolean(iconfig.harvesterProperties.getProperty("deleteMissingDocuments","true"))) validIdentifiers=new HashSet<String>();

        // initialize and test for HTML SAX Parser
        try {
            htmlReaderClass=Class.forName(HTML_SAX_PARSER_CLASS).asSubclass(XMLReader.class);
        } catch (ClassNotFoundException cfe) {
            throw new ClassNotFoundException(getClass().getName()+" needs the NekoHTML parser in classpath!");
        }
    }

    @Override
    public void harvest() throws Exception {
        if (index==null) throw new IllegalStateException("Index not yet opened");

        // process this URL directly and save possible redirect as new base
        String urlStr=baseURL; baseURL=""; // disable base checking for the entry point to follow a initial redirect for sure
        harvested.add(urlStr);
        URL newbaseURL=processURL(new URL(urlStr),retryCount);

        // get an URL that points to the current directory
        // from now on this is used as baseURL
        baseURL = ("".equals(newbaseURL.getPath())) ? newbaseURL.toString() : new URL(newbaseURL,"./").toString();
        log.debug("URL directory which harvesting may not escape: "+baseURL);

        // remove invalid URLs from queued list (because until now we had no baseURL restriction)
        Iterator<String> it=needsHarvest.iterator();
        while (it.hasNext()) {
            if (!it.next().startsWith(baseURL)) it.remove();
        }

        // harvest queue
        while (!needsHarvest.isEmpty()) {
            // waiting
            if (pauseBetweenRequests>0L) try {
                Thread.sleep(pauseBetweenRequests);
            } catch (InterruptedException ie) {}
            // process a new url
            urlStr=needsHarvest.first();
            needsHarvest.remove(urlStr);
            harvested.add(urlStr);
            processURL(new URL(urlStr),retryCount);
        }

        // update this for next harvesting
        setValidIdentifiers(validIdentifiers);
        thisHarvestDateReference=firstDateFound;
    }

    @Override
    public List<String> getValidHarvesterPropertyNames() {
        ArrayList<String> l=new ArrayList<String>(super.getValidHarvesterPropertyNames());
        l.addAll(Arrays.<String>asList(
            "baseUrl",
            "retryCount",
            "retryAfterSeconds",
            "filenameFilter",
            "contentTypes",
            "deleteMissingDocuments",
            "pauseBetweenRequests" /* in milliseconds */
        ));
        return l;
    }

    // internal implementation

    private void queueURL(String url) {
        int p=url.indexOf('#');
        if (p>=0) url=url.substring(0,p);
        // check if it is below base
        if (!url.startsWith(baseURL)) return;
        // was it already harvested?
        if (harvested.contains(url)) return;
        needsHarvest.add(url);
    }

    private InputStream sendHTTPRequest(HttpURLConnection conn, String method) throws IOException {
        try {
            conn.setRequestMethod(method);

            StringBuilder ua=new StringBuilder("Java/");
            ua.append(System.getProperty("java.version"));
            ua.append(" (");
            ua.append(getClass().getName());
            ua.append(')');
            conn.setRequestProperty("User-Agent",ua.toString());

            conn.setRequestProperty("Accept-Encoding","gzip, deflate, identity;q=0.3, *;q=0");
            conn.setRequestProperty("Accept-Charset","utf-8, *;q=0.5");

            StringBuilder ac=new StringBuilder();
            for (String c : contentTypes) ac.append(c+", ");
            ac.append("text/html, *;q=0.1");
            conn.setRequestProperty("Accept",ac.toString());

            conn.setUseCaches(false);
            conn.setFollowRedirects(true);

            log.debug("Opening connection...");
            InputStream in=null;
            try {
                conn.connect();
                in=conn.getInputStream();
            } catch (IOException ioe) {
                int after,code;
                try {
                    after=conn.getHeaderFieldInt("Retry-After",-1);
                    code=conn.getResponseCode();
                } catch (IOException ioe2) {
                    after=-1; code=-1;
                }
                if (code==HttpURLConnection.HTTP_UNAVAILABLE && after>0) throw new RetryAfterIOException(after,ioe);
                throw ioe;
            }

            // cast stream if encoding different from identity
            if (!"HEAD".equals(method)) {
                String encoding=conn.getContentEncoding();
                if (encoding==null) encoding="identity";
                encoding=encoding.toLowerCase();

                log.debug("HTTP server uses "+encoding+" content encoding.");
                if ("gzip".equals(encoding)) in=new GZIPInputStream(in);
                else if ("deflate".equals(encoding)) in=new InflaterInputStream(in);
                else if (!"identity".equals(encoding)) throw new IOException("Server uses an invalid content encoding: "+encoding);
            }

            return in;
        } catch (FileNotFoundException fnfe) {
            log.warn("Cannot find URL '"+conn.getURL()+"'.");
            return null;
        }
    }

    private void analyzeHTML(final URL baseURL, final InputSource source) throws Exception {
        XMLReader r=htmlReaderClass.newInstance();
        r.setFeature("http://xml.org/sax/features/namespaces", true);
        r.setFeature("http://cyberneko.org/html/features/balance-tags", true);
        r.setFeature("http://cyberneko.org/html/features/report-errors",false);
        // these are the defaults for HTML 4.0 and DOM with HTML:
        r.setProperty("http://cyberneko.org/html/properties/names/elems","upper");
        r.setProperty("http://cyberneko.org/html/properties/names/attrs","lower");

        DefaultHandler handler=new DefaultHandler() {

            private URL base=baseURL; // make it unfinal ;-)
            private int inBODY=0;
            private int inFRAMESET=0;
            private int inHEAD=0;

            @Override
            public void startElement(String namespaceURI, String localName, String qName, Attributes atts) throws SAXException {
                String url=null;
                if ("BODY".equals(localName)) {
                    inBODY++;
                } else if ("FRAMESET".equals(localName)) {
                    inFRAMESET++;
                } else if ("HEAD".equals(localName)) {
                    inHEAD++;
                } else if (inHEAD>0) {
                    if ("BASE".equals(localName)) {
                        String newBase=atts.getValue("href");
                        if (newBase!=null) try {
                            base=new URL(base,newBase);
                        } catch (MalformedURLException mue) {
                            // special exception to stop processing
                            log.debug("Found invalid BASE-URL: "+url);
                            throw new SAXException("#panFMP#HTML_INVALID_BASE");
                        }
                    }
                } else {
                    if (inBODY>0) {
                        if ("A".equals(localName) || "AREA".equals(localName)) {
                            url=atts.getValue("href");
                        } else if ("IFRAME".equals(localName)) {
                            url=atts.getValue("src");
                        }
                    }
                    if (inFRAMESET>0) {
                        if ("FRAME".equals(localName)) {
                            url=atts.getValue("src");
                        }
                    }
                }
                // append a possible url to queue
                if (url!=null) try {
                    queueURL(new URL(base,url).toString());
                } catch (MalformedURLException mue) {
                    // there may be javascript:-URLs in the document or something other
                    // we will not throw errors!
                    log.debug("Found invalid URL: "+url);
                }
            }

            @Override
            public void endElement(String namespaceURI, String localName, String qName) throws SAXException {
                if ("BODY".equals(localName)) {
                    inBODY--;
                } else if ("FRAMESET".equals(localName)) {
                    inFRAMESET--;
                } else if ("HEAD".equals(localName)) {
                    inHEAD--;
                }
            }

        };
        r.setContentHandler(handler);
        r.setErrorHandler(handler);
        try {
            r.parse(source);
        } catch (SAXException saxe) {
            // filter special stop criteria
            if ("#panFMP#HTML_INVALID_BASE".equals(saxe.getMessage())) {
                log.warn("HTMLParser detected an invalid URL in HTML 'BASE' tag. Stopped link parsing for this document!");
            } else throw saxe;
        }
    }

    private boolean acceptFile(URL url) {
        if (filenameFilter==null) return true;
        String name=url.getPath();
        int p=name.lastIndexOf('/');
        if (p>=0) name=name.substring(p+1);
        Matcher m=filenameFilter.matcher(name);
        return m.matches();
    }

    private URL processURL(URL url, int retryCount) throws Exception {
        log.info("Requesting props of '"+url+"'...");

        try {
            HttpURLConnection conn=(HttpURLConnection)url.openConnection();
            InputStream in=sendHTTPRequest(conn,"HEAD");
            if (in==null) return url;
            in.close(); // it is empty

            // set the harvest date to the first received timestamp from server
            if (firstDateFound==null && conn.getDate()!=0L) firstDateFound=new java.util.Date(conn.getDate());

            // check connection properties
            String contentType=conn.getContentType();
            String charset=null;
            if (contentType!=null) {
                contentType=contentType.toLowerCase();
                int charsetStart=contentType.indexOf("charset=");
                if (charsetStart>=0) {
                    int charsetEnd=contentType.indexOf(";",charsetStart);
                    if (charsetEnd==-1) charsetEnd=contentType.length();
                    charsetStart+="charset=".length();
                    charset=contentType.substring(charsetStart,charsetEnd).trim();
                }
                int contentEnd=contentType.indexOf(';');
                if (contentEnd>=0) contentType=contentType.substring(0,contentEnd);
                contentType=contentType.trim();
            }
            log.debug("Charset from Content-Type: '"+charset+"'; Type from Content-Type: '"+contentType+"'");
            if (contentType==null) {
                log.warn("Connection to URL '"+url+"' did not return a content-type, skipping.");
                return url;
            }

            // if we got a redirect the new URL is now needed
            URL newurl=conn.getURL();
            if (!url.toString().equals(newurl.toString())) {
                log.debug("Got redirect to: "+newurl);
                url=newurl;
                // check if it is below base
                if (!url.toString().startsWith(baseURL)) return url;
                // was it already harvested?
                if (harvested.contains(url.toString())) return url;
                // clean this new url from lists
                needsHarvest.remove(url.toString());
                harvested.add(url.toString());
            }

            if ("text/html".equals(contentType)) {
                log.info("Analyzing HTML links in '"+url+"'...");

                // reopen for GET
                conn=(HttpURLConnection)url.openConnection();
                in=sendHTTPRequest(conn,"GET");
                if (in!=null) try {
                    InputSource src=new InputSource(in);
                    src.setSystemId(url.toString());
                    src.setEncoding(charset);
                    analyzeHTML(url,src);
                } finally {
                    in.close();
                }
            } else if (contentTypes.contains(contentType)) {
                if (acceptFile(url)) {
                    if (validIdentifiers!=null) validIdentifiers.add(url.toString());

                    Date lastModified=null;
                    if (conn.getLastModified()!=0L) lastModified=new java.util.Date(conn.getLastModified());
                    if (lastModified!=null && fromDateReference!=null && lastModified.getTime()<fromDateReference.getTime()) return url;

                    log.info("Harvesting '"+url+"'...");

                    // reopen for GET and parse as XML
                    conn=(HttpURLConnection)url.openConnection();
                    in=sendHTTPRequest(conn,"GET");
                    if (in!=null) try {
                        MetadataDocument mdoc=new MetadataDocument();
                        mdoc.setIdentifier(url.toString());
                        mdoc.setDatestamp(lastModified);

                        // a SAX InputSource is used because we can set the default encoding from the HTTP headers
                        // if charset is superseded by <?xml ?> declaration, it is changed later by parser
                        InputSource src=new InputSource(in);
                        src.setSystemId(url.toString());
                        src.setEncoding(charset);
                        SAXSource ss=new SAXSource(StaticFactories.xinclSaxFactory.newSAXParser().getXMLReader(), src);
                        mdoc.setDOM(xmlConverter.transform(ss));

                        addDocument(mdoc);
                    } finally {
                        in.close();
                    }
                }
            }
        } catch (IOException ioe) {
            int after=retryTime;
            if (ioe instanceof RetryAfterIOException) {
                if (retryCount==0) throw (IOException)ioe.getCause();
                log.warn("HTTP server returned '503 Service Unavailable' with a 'Retry-After' value being set.");
                after=((RetryAfterIOException)ioe).getRetryAfter();
            } else {
                if (retryCount==0) throw ioe;
                log.error("HTTP server access failed with exception: ",ioe);
            }
            log.info("Retrying after "+after+" seconds ("+retryCount+" retries left)...");
            try { Thread.sleep(1000L*after); } catch (InterruptedException ie) {}
            return processURL(url,retryCount-1);
        }
        return url;
    }

}