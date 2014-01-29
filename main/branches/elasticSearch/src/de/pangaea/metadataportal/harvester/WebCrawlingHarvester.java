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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.InflaterInputStream;

import javax.xml.transform.sax.SAXSource;

import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

import de.pangaea.metadataportal.config.IndexConfig;
import de.pangaea.metadataportal.processor.ElasticSearchConnection;
import de.pangaea.metadataportal.utils.SimpleCookieHandler;
import de.pangaea.metadataportal.utils.StaticFactories;

/**
 * Harvester for traversing websites and harvesting XML documents. If the
 * <code>baseURL</code> (from config) contains a XML file with the correct MIME
 * type, it is directly harvested. A html webpage is analyzed and all links are
 * followed and checked for XML files with correct MIME type. This is done
 * recursively, but harvesting does not escape the server and
 * <code>baseURL</code> directory.
 * <p>
 * This harvester supports the following additional <b>harvester properties</b>:
 * <ul>
 * <li><code>baseUrl</code>: URL to start crawling (should point to a HTML
 * page).</li>
 * <li><code>retryCount</code>: how often retry on HTTP errors? (default: 5)</li>
 * <li><code>retryAfterSeconds</code>: time between retries in seconds (default:
 * 60)</li>
 * <li><code>timeoutAfterSeconds</code>: HTTP Timeout for harvesting in seconds</li>
 * <li><code>filenameFilter</code>: regex to match the filename. The regex is
 * applied against the whole filename (this is like ^pattern$)! (default: none)</li>
 * <li><code>contentTypes</code>: MIME types of documents to index (maybe
 * additionally limited by <code>filenameFilter</code>). (default:
 * "text/xml,application/xml")</li>
 * <li><code>excludeUrlPattern</code>: A regex that is applied to all URLs
 * appearing during harvesting process. URLs with matching patterns (partial
 * matches allowed, use ^,$ for start/end matches) are excluded and not further
 * traversed. (default: none)</li>
 * <li><code>pauseBetweenRequests</code>: to not overload server that is
 * harvested, wait XX milliseconds after each HTTP request (default: none)</li>
 * </ul>
 * 
 * @author Uwe Schindler
 */
public class WebCrawlingHarvester extends SingleFileEntitiesHarvester {
  
  public static final int DEFAULT_RETRY_TIME = 60; // seconds
  public static final int DEFAULT_RETRY_COUNT = 5;
  public static final int DEFAULT_TIMEOUT = 180; // seconds
  
  /**
   * This is the parser class used to parse HTML documents to collect URLs for
   * crawling. If this class is not in your classpath, the harvester will fail
   * on startup in {@link #open}. If you change the implementation (possibly in
   * future a HTML parser is embedded in XERCES), change this. Do not forget to
   * revisit the features for this parser in the parsing method.
   */
  public static final String HTML_SAX_PARSER_CLASS = "org.cyberneko.html.parsers.SAXParser";
  public static final Set<String> HTML_CONTENT_TYPES = new HashSet<String>(
      Arrays.asList("text/html", "application/xhtml+xml"));
  
  // Class members
  private String baseURL = null;
  private Pattern filenameFilter = null, excludeUrlPattern = null;
  private Set<String> contentTypes = new HashSet<String>();
  private int retryCount = DEFAULT_RETRY_COUNT;
  private int retryTime = DEFAULT_RETRY_TIME;
  private int timeout = DEFAULT_TIMEOUT;
  private long pauseBetweenRequests = 0;
  
  private Set<String> harvested = new HashSet<String>();
  private SortedSet<String> needsHarvest = new TreeSet<String>();
  
  private Class<? extends XMLReader> htmlReaderClass = null;
  
  @Override
  public void open(ElasticSearchConnection es, IndexConfig iconfig) throws Exception {
    super.open(es, iconfig);
    
    String s = iconfig.harvesterProperties.getProperty("baseUrl");
    if (s == null) throw new IllegalArgumentException(
        "Missing base URL to start harvesting (property \"baseUrl\")");
    URL u = new URL(s);
    String proto = u.getProtocol().toLowerCase(Locale.ROOT);
    if (!("http".equals(proto) || "https".equals(proto))) throw new IllegalArgumentException(
        "WebCrawlingHarvester only allows HTTP(S) as network protocol!");
    baseURL = u.toString();
    
    s = iconfig.harvesterProperties.getProperty("contentTypes",
        "text/xml,application/xml");
    for (String c : s.split("[\\,\\;\\s]+")) {
      c = c.trim().toLowerCase(Locale.ROOT);
      if (!"".equals(c)) contentTypes.add(c);
    }
    
    if ((s = iconfig.harvesterProperties.getProperty("retryCount")) != null) retryCount = Integer
        .parseInt(s);
    if ((s = iconfig.harvesterProperties.getProperty("retryAfterSeconds")) != null) retryTime = Integer
        .parseInt(s);
    if ((s = iconfig.harvesterProperties.getProperty("timeoutAfterSeconds")) != null) timeout = Integer
        .parseInt(s);
    if ((s = iconfig.harvesterProperties.getProperty("pauseBetweenRequests")) != null) pauseBetweenRequests = Long
        .parseLong(s);
    
    s = iconfig.harvesterProperties.getProperty("filenameFilter");
    filenameFilter = (s == null) ? null : Pattern.compile(s);
    
    s = iconfig.harvesterProperties.getProperty("excludeUrlPattern");
    excludeUrlPattern = (s == null) ? null : Pattern.compile(s);
    
    // initialize and test for HTML SAX Parser
    try {
      htmlReaderClass = Class.forName(HTML_SAX_PARSER_CLASS).asSubclass(
          XMLReader.class);
    } catch (ClassNotFoundException cfe) {
      throw new ClassNotFoundException(getClass().getName()
          + " needs the NekoHTML parser in classpath!");
    }
    
    SimpleCookieHandler.INSTANCE.enable();
  }
  
  @Override
  public void close(boolean cleanShutdown) throws Exception {
    SimpleCookieHandler.INSTANCE.disable();
    super.close(cleanShutdown);
  }
  
  @Override
  public void harvest() throws Exception {
    // process this URL directly and save possible redirect as new base
    String urlStr = baseURL;
    baseURL = ""; // disable base checking for the entry point to follow a
                  // initial redirect for sure
    harvested.add(urlStr);
    URL newbaseURL = processURL(new URL(urlStr));
    
    // get an URL that points to the current directory
    // from now on this is used as baseURL
    baseURL = ("".equals(newbaseURL.getPath())) ? newbaseURL.toString()
        : new URL(newbaseURL, "./").toString();
    log.debug("URL directory which harvesting may not escape: " + baseURL);
    
    // remove invalid URLs from queued list (because until now we had no baseURL
    // restriction)
    Iterator<String> it = needsHarvest.iterator();
    while (it.hasNext()) {
      if (!it.next().startsWith(baseURL)) it.remove();
    }
    
    // harvest queue
    while (!needsHarvest.isEmpty()) {
      // waiting
      if (pauseBetweenRequests > 0L) try {
        Thread.sleep(pauseBetweenRequests);
      } catch (InterruptedException ie) {}
      // process a new url
      urlStr = needsHarvest.first();
      needsHarvest.remove(urlStr);
      harvested.add(urlStr);
      processURL(new URL(urlStr));
    }
  }
  
  @Override
  protected void enumerateValidHarvesterPropertyNames(Set<String> props) {
    super.enumerateValidHarvesterPropertyNames(props);
    props.addAll(Arrays.<String> asList("baseUrl", "retryCount",
        "retryAfterSeconds", "timeoutAfterSeconds", "filenameFilter",
        "contentTypes", "excludeUrlPattern", "pauseBetweenRequests" /*
                                                                     * in
                                                                     * milliseconds
                                                                     */
    ));
  }
  
  // internal implementation
  
  private void queueURL(String url) {
    int p = url.indexOf('#');
    if (p >= 0) url = url.substring(0, p);
    // check if it is below base
    if (!url.startsWith(baseURL)) return;
    // was it already harvested?
    if (harvested.contains(url)) return;
    // check pattern
    if (excludeUrlPattern != null) {
      Matcher m = excludeUrlPattern.matcher(url);
      if (m.find()) return;
    }
    needsHarvest.add(url);
  }
  
  private InputStream sendHTTPRequest(HttpURLConnection conn, String method)
      throws IOException {
    try {
      conn.setConnectTimeout(timeout * 1000);
      conn.setReadTimeout(timeout * 1000);
      conn.setRequestMethod(method);
      
      StringBuilder ua = new StringBuilder("Java/")
          .append(System.getProperty("java.version")).append(" (")
          .append(de.pangaea.metadataportal.Package.getProductName())
          .append('/').append(de.pangaea.metadataportal.Package.getVersion())
          .append("; WebCrawlingHarvester)");
      conn.setRequestProperty("User-Agent", ua.toString());
      
      conn.setRequestProperty("Accept-Encoding",
          "gzip, deflate, identity;q=0.3, *;q=0");
      conn.setRequestProperty("Accept-Charset", "utf-8, *;q=0.5");
      
      StringBuilder ac = new StringBuilder();
      for (String c : contentTypes)
        ac.append(c).append(", ");
      for (String c : HTML_CONTENT_TYPES)
        ac.append(c).append(", ");
      ac.append("*;q=0.1");
      conn.setRequestProperty("Accept", ac.toString());
      
      conn.setUseCaches(false);
      conn.setInstanceFollowRedirects(true);
      
      log.debug("Opening connection...");
      InputStream in = null;
      try {
        conn.connect();
        in = conn.getInputStream();
      } catch (IOException ioe) {
        int after, code;
        try {
          after = conn.getHeaderFieldInt("Retry-After", -1);
          code = conn.getResponseCode();
        } catch (IOException ioe2) {
          after = -1;
          code = -1;
        }
        if (code == HttpURLConnection.HTTP_UNAVAILABLE && after > 0) throw new RetryAfterIOException(
            after, ioe);
        throw ioe;
      }
      
      // cast stream if encoding different from identity
      if (!"HEAD".equals(method)) {
        String encoding = conn.getContentEncoding();
        if (encoding == null) encoding = "identity";
        encoding = encoding.toLowerCase(Locale.ROOT);
        
        log.debug("HTTP server uses " + encoding + " content encoding.");
        if ("gzip".equals(encoding)) in = new GZIPInputStream(in);
        else if ("deflate".equals(encoding)) in = new InflaterInputStream(in);
        else if (!"identity".equals(encoding)) throw new IOException(
            "Server uses an invalid content encoding: " + encoding);
      }
      
      return in;
    } catch (FileNotFoundException fnfe) {
      log.warn("Cannot find URL '" + conn.getURL() + "'.");
      return null;
    }
  }
  
  private void analyzeHTML(final URL baseURL, final InputSource source)
      throws Exception {
    XMLReader r = htmlReaderClass.newInstance();
    r.setFeature("http://xml.org/sax/features/namespaces", true);
    r.setFeature("http://cyberneko.org/html/features/balance-tags", true);
    r.setFeature("http://cyberneko.org/html/features/report-errors", false);
    // these are the defaults for HTML 4.0 and DOM with HTML:
    r.setProperty("http://cyberneko.org/html/properties/names/elems", "upper");
    r.setProperty("http://cyberneko.org/html/properties/names/attrs", "lower");
    
    DefaultHandler handler = new DefaultHandler() {
      
      private URL base = baseURL; // make it unfinal ;-)
      private int inBODY = 0;
      private int inFRAMESET = 0;
      private int inHEAD = 0;
      
      @Override
      public void startElement(String namespaceURI, String localName,
          String qName, Attributes atts) throws SAXException {
        String url = null;
        if ("BODY".equals(localName)) {
          inBODY++;
        } else if ("FRAMESET".equals(localName)) {
          inFRAMESET++;
        } else if ("HEAD".equals(localName)) {
          inHEAD++;
        } else if (inHEAD > 0) {
          if ("BASE".equals(localName)) {
            String newBase = atts.getValue("href");
            if (newBase != null) try {
              base = new URL(base, newBase);
            } catch (MalformedURLException mue) {
              // special exception to stop processing
              log.debug("Found invalid BASE-URL: " + url);
              throw new SAXException("#panFMP#HTML_INVALID_BASE");
            }
          }
        } else {
          if (inBODY > 0) {
            if ("A".equals(localName) || "AREA".equals(localName)) {
              url = atts.getValue("href");
            } else if ("IFRAME".equals(localName)) {
              url = atts.getValue("src");
            }
          }
          if (inFRAMESET > 0) {
            if ("FRAME".equals(localName)) {
              url = atts.getValue("src");
            }
          }
        }
        // append a possible url to queue
        if (url != null) try {
          queueURL(new URL(base, url).toString());
        } catch (MalformedURLException mue) {
          // there may be javascript:-URLs in the document or something other
          // we will not throw errors!
          log.debug("Found invalid URL: " + url);
        }
      }
      
      @Override
      public void endElement(String namespaceURI, String localName, String qName)
          throws SAXException {
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
    if (filenameFilter == null) return true;
    String name = url.getPath();
    int p = name.lastIndexOf('/');
    if (p >= 0) name = name.substring(p + 1);
    Matcher m = filenameFilter.matcher(name);
    return m.matches();
  }
  
  @SuppressWarnings("resource")
  private URL processURL(URL url) throws Exception {
    for (int retry = 0; retry <= retryCount; retry++) {
      log.info("Requesting props of '" + url + "'...");
      try {
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        InputStream in = sendHTTPRequest(conn, "HEAD");
        if (in == null) return url;
        in.close(); // it is empty
        
        // check connection properties
        String contentType = conn.getContentType();
        String charset = null;
        if (contentType != null) {
          contentType = contentType.toLowerCase(Locale.ROOT);
          int charsetStart = contentType.indexOf("charset=");
          if (charsetStart >= 0) {
            int charsetEnd = contentType.indexOf(";", charsetStart);
            if (charsetEnd == -1) charsetEnd = contentType.length();
            charsetStart += "charset=".length();
            charset = contentType.substring(charsetStart, charsetEnd).trim();
          }
          int contentEnd = contentType.indexOf(';');
          if (contentEnd >= 0) contentType = contentType.substring(0,
              contentEnd);
          contentType = contentType.trim();
        }
        log.debug("Charset from Content-Type: '" + charset
            + "'; Type from Content-Type: '" + contentType + "'");
        if (contentType == null) {
          log.warn("Connection to URL '" + url
              + "' did not return a content-type, skipping.");
          return url;
        }
        
        // if we got a redirect the new URL is now needed
        URL newurl = conn.getURL();
        if (!url.toString().equals(newurl.toString())) {
          log.debug("Got redirect to: " + newurl);
          url = newurl;
          // check if it is below base
          if (!url.toString().startsWith(baseURL)) return url;
          // was it already harvested?
          if (harvested.contains(url.toString())) return url;
          // clean this new url from lists
          needsHarvest.remove(url.toString());
          harvested.add(url.toString());
        }
        
        if (HTML_CONTENT_TYPES.contains(contentType)) {
          log.info("Analyzing HTML links in '" + url + "'...");
          
          // reopen for GET
          conn = (HttpURLConnection) url.openConnection();
          in = sendHTTPRequest(conn, "GET");
          if (in != null) try {
            InputSource src = new InputSource(in);
            src.setSystemId(url.toString());
            src.setEncoding(charset);
            analyzeHTML(url, src);
          } finally {
            in.close();
          }
        } else if (contentTypes.contains(contentType)) {
          if (acceptFile(url)) {
            long lastModified = conn.getLastModified();
            if (isDocumentOutdated(lastModified)) {
              log.info("Harvesting '" + url + "'...");
              
              // reopen for GET and parse as XML
              conn = (HttpURLConnection) url.openConnection();
              in = sendHTTPRequest(conn, "GET");
              if (in != null) try {
                InputSource src = new InputSource(in);
                src.setSystemId(url.toString());
                src.setEncoding(charset);
                SAXSource saxsrc = new SAXSource(StaticFactories.saxFactory
                    .newSAXParser().getXMLReader(), src);
                addDocument(url.toString(), lastModified, saxsrc);
              } finally {
                in.close();
              }
            } else {
              // add this empty doc here, to update datestamps for next
              // harvesting
              addDocument(url.toString(), lastModified, null);
            }
          }
        }
        return url;
      } catch (IOException ioe) {
        int after = retryTime;
        if (ioe instanceof RetryAfterIOException) {
          if (retry >= retryCount) throw (IOException) ioe.getCause();
          log.warn("HTTP server returned '503 Service Unavailable' with a 'Retry-After' value being set.");
          after = ((RetryAfterIOException) ioe).getRetryAfter();
        } else {
          if (retry >= retryCount) throw ioe;
          log.error("HTTP server access failed with exception: ", ioe);
        }
        log.info("Retrying after " + after + " seconds ("
            + (retryCount - retry) + " retries left)...");
        try {
          Thread.sleep(1000L * after);
        } catch (InterruptedException ie) {}
      }
    }
    throw new IOException("Unable to properly connect HTTP server.");
  }
  
}