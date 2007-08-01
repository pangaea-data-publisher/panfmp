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

    public String HTML_SAX_PARSER_CLASS="org.cyberneko.html.parsers.SAXParser";

    // Class members
    private String baseURL=null;
    private Pattern filenameFilter=null;
    private Set<String> contentTypes=new HashSet<String>();
    private Set<String> validIdentifiers=null;
    private int retryCount=DEFAULT_RETRY_COUNT;
    private int retryTime=DEFAULT_RETRY_TIME;

    private Set<String> harvested=new HashSet<String>();
    private SortedSet<String> needsHarvest=new TreeSet<String>();
    private Date firstDateFound=null;

    private Class<? extends XMLReader> htmlReaderClass=null;

    @Override
    public void open(SingleIndexConfig iconfig) throws Exception {
        // TODO: we want to regenerate the index every time
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
            c=c.trim();
            if (!"".equals(c)) contentTypes.add(c);
        }

        String retryCountStr=iconfig.harvesterProperties.getProperty("retryCount");
        if (retryCountStr!=null) retryCount=Integer.parseInt(retryCountStr);
        String retryTimeStr=iconfig.harvesterProperties.getProperty("retryAfterSeconds");
        if (retryTimeStr!=null) retryTime=Integer.parseInt(retryTimeStr);

        s=iconfig.harvesterProperties.getProperty("filenameFilter");
        filenameFilter=(s==null) ? null : Pattern.compile(s);

        validIdentifiers=null;
        if (Boolean.parseBoolean(iconfig.harvesterProperties.getProperty("deleteMissingDocuments","true"))) validIdentifiers=new HashSet<String>();

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

        // queue this base URL to start with
        queueURL(baseURL);

        // get an URL that points to the current directory
        // from now on this is used as baseURL
        baseURL=new URL(new URL(baseURL),"./").toString();

        // harvest queue
        while (!needsHarvest.isEmpty()) {
            String urlStr=needsHarvest.first();
            processURL(new URL(urlStr),retryCount);
            needsHarvest.remove(urlStr);
            harvested.add(urlStr);
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
            "deleteMissingDocuments"
        ));
        return l;
    }

    // internal implementation

    private void queueURL(String url) {
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
            conn.setRequestProperty("Accept","text/xml, application/xml, text/html, *;q=0.1");

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
            String encoding=conn.getContentEncoding();
            if (encoding==null) encoding="identity";
            encoding=encoding.toLowerCase();

            log.debug("HTTP server uses "+encoding+" content encoding.");
            if ("gzip".equals(encoding)) in=new GZIPInputStream(in);
            else if ("deflate".equals(encoding)) in=new InflaterInputStream(in);
            else if (!"identity".equals(encoding)) throw new IOException("Server uses an invalid content encoding: "+encoding);

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
        r.setProperty("http://cyberneko.org/html/properties/names/elems","lower");
        r.setProperty("http://cyberneko.org/html/properties/names/attrs","lower");
        if (source.getEncoding()!=null) r.setProperty("http://cyberneko.org/html/properties/default-encoding",source.getEncoding());
        DefaultHandler handler=new DefaultHandler() {

            private URL base=baseURL; // make it unfinal ;-)
            private int inBODY=0;
            private int inFRAMESET=0;
            private int inHEAD=0;

            @Override
            public void startElement(String namespaceURI, String localName, String qName, Attributes atts) throws SAXException {
                String url=null;
                if ("body".equals(localName)) {
                    inBODY++;
                } else if ("frameset".equals(localName)) {
                    inFRAMESET++;
                } else if ("head".equals(localName)) {
                    inHEAD++;
                } else if (inHEAD>0) {
                    if ("base".equals(localName)) {
                        String newBase=atts.getValue("href");
                        if (newBase!=null) try {
                            base=new URL(base,newBase);
                        } catch (MalformedURLException mue) {
                            log.warn("HTMLParser detected an invalid URL in <base/>!");
                        }
                    }
                } else {
                    if (inBODY>0) {
                        if ("a".equals(localName) || "area".equals(localName)) {
                            url=atts.getValue("href");
                        }
                        if ("iframe".equals(localName)) {
                            url=atts.getValue("src");
                        }
                    }
                    if (inFRAMESET>0) {
                        if ("frame".equals(localName)) {
                            url=atts.getValue("src");
                        }
                    }
                }
                // append url to queue
                if (url!=null) {
                    int p=url.indexOf('#');
                    if (p>=0) url=url.substring(0,p);
                    try {
                        queueURL(new URL(base,url).toString());
                    } catch (MalformedURLException mue) {}
                }
            }

            @Override
            public void endElement(String namespaceURI, String localName, String qName) throws SAXException {
                if ("body".equals(localName)) {
                    inBODY--;
                } else if ("frameset".equals(localName)) {
                    inFRAMESET--;
                } else if ("head".equals(localName)) {
                    inHEAD--;
                }
            }

        };
        r.setContentHandler(handler);
        r.setErrorHandler(handler);
        r.parse(source);
    }

    private boolean acceptFile(URL url) {
        if (filenameFilter==null) return true;
        String name=url.getPath();
        int p=name.lastIndexOf('/');
        if (p>=0) name=name.substring(p+1);
        Matcher m=filenameFilter.matcher(name);
        return m.matches();
    }

    private void processURL(URL url, int retryCount) throws Exception {
        log.info("Requesting props of '"+url+"'...");

        try {
            HttpURLConnection conn=(HttpURLConnection)url.openConnection();
            InputStream in=sendHTTPRequest(conn,"HEAD");
            if (in==null) return;
            in.close(); // it is empty

            // set the harvest date to the first received timestamp from server
            if (firstDateFound==null && conn.getDate()!=0L) firstDateFound=new java.util.Date(conn.getDate());

            // check connection properties
            String contentType=conn.getContentType();
            String charset=null;
            if (contentType!=null) {
                int charsetStart=contentType.indexOf("charset=");
                if (charsetStart>=0) {
                    int charsetEnd=contentType.indexOf(";",charsetStart);
                    if (charsetEnd==-1) charsetEnd=contentType.length();
                    charsetStart+="charset=".length();
                    charset=contentType.substring(charsetStart,charsetEnd).trim();
                }
                int contentEnd=contentType.indexOf(';');
                if (contentEnd>=0) contentType=contentType.substring(0,contentEnd);
                contentType=contentType.trim().toLowerCase();
            }
            log.debug("Charset from Content-Type: "+charset+"; plain Content-Type without parameters: "+contentType);
            if (contentType==null) {
                log.warn("Connection to URL '"+url+"' did not return a content-type, skipping.");
                return;
            }

            // if we got a redirect the new URL is now needed
            url=conn.getURL();
            // check if it is below base
            if (!url.toString().startsWith(baseURL)) return;
            // was it already harvested?
            if (harvested.contains(url.toString())) return;

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
                    if (lastModified!=null && fromDateReference!=null && lastModified.getTime()<fromDateReference.getTime()) return;

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
            processURL(url,retryCount-1);
        }
    }

}