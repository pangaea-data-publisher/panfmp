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

import java.io.IOException;
import java.io.InputStream;
import java.net.CookieManager;
import java.net.CookiePolicy;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Redirect;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.xml.transform.sax.SAXSource;

import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

import de.pangaea.metadataportal.config.HarvesterConfig;
import de.pangaea.metadataportal.utils.HttpClientUtils;
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
  public static final Set<String> HTML_CONTENT_TYPES = new HashSet<>(
      Arrays.asList("text/html", "application/xhtml+xml"));
  
  public static final String USER_AGENT = new StringBuilder("Java/")
      .append(Runtime.version()).append(" (")
      .append(de.pangaea.metadataportal.Package.getProductName())
      .append('/').append(de.pangaea.metadataportal.Package.getVersion())
      .append("; WebCrawlingHarvester)").toString();
  
  // Class members
  private String baseURL;
  private final Pattern filenameFilter, excludeUrlPattern;
  private final Set<String> contentTypes = new HashSet<>();
  private final int retryCount;
  private final int retryTime;
  private final Duration timeout;
  private final String authorizationHeader;
  private final long pauseBetweenRequests;
  private final HttpClient httpClient;
    
  private Set<String> harvested = new HashSet<>();
  private SortedSet<String> needsHarvest = new TreeSet<>();
  
  private Class<? extends XMLReader> htmlReaderClass = null;
  
  public WebCrawlingHarvester(HarvesterConfig iconfig) throws Exception {
    super(iconfig);
    
    String s = iconfig.properties.getProperty("baseUrl");
    if (s == null) throw new IllegalArgumentException(
        "Missing base URL to start harvesting (property \"baseUrl\")");
    URI u = new URI(s);
    String proto = u.getScheme().toLowerCase(Locale.ROOT);
    if (!("http".equals(proto) || "https".equals(proto))) throw new IllegalArgumentException(
        "WebCrawlingHarvester only allows HTTP(S) as network protocol!");
    baseURL = u.toString();
    
    s = iconfig.properties.getProperty("contentTypes", "text/xml,application/xml");
    for (String c : s.split("[\\,\\;\\s]+")) {
      c = c.trim().toLowerCase(Locale.ROOT);
      if (!"".equals(c)) contentTypes.add(c);
    }
    
    retryCount = Integer.parseInt(iconfig.properties.getProperty("retryCount", Integer.toString(DEFAULT_RETRY_COUNT)));
    retryTime = Integer.parseInt(iconfig.properties.getProperty("retryAfterSeconds", Integer.toString(DEFAULT_RETRY_TIME)));
    timeout = Duration.ofSeconds(Integer.parseInt(iconfig.properties.getProperty("timeoutAfterSeconds", Integer.toString(DEFAULT_TIMEOUT))));
    authorizationHeader = iconfig.properties.getProperty("authorizationHeader");
    pauseBetweenRequests = Long.parseLong(iconfig.properties.getProperty("pauseBetweenRequests", "0"));
    
    s = iconfig.properties.getProperty("filenameFilter");
    filenameFilter = (s == null) ? null : Pattern.compile(s);
    
    s = iconfig.properties.getProperty("excludeUrlPattern");
    excludeUrlPattern = (s == null) ? null : Pattern.compile(s);
    
    httpClient = HttpClient.newBuilder()
        .followRedirects(Redirect.NORMAL)
        .connectTimeout(timeout)
        .cookieHandler(new CookieManager(null, CookiePolicy.ACCEPT_ORIGINAL_SERVER))
        .build();
    
    // initialize and test for HTML SAX Parser
    try {
      htmlReaderClass = Class.forName(HTML_SAX_PARSER_CLASS).asSubclass(XMLReader.class);
    } catch (ClassNotFoundException cfe) {
      throw new ClassNotFoundException(getClass().getName() + " needs the NekoHTML parser in classpath!");
    }
  }

  @Override
  public void harvest() throws Exception {
    // process this URL directly and save possible redirect as new base
    String urlStr = baseURL;
    baseURL = ""; // disable base checking for the entry point to follow a
                  // initial redirect for sure
    harvested.add(urlStr);
    URI newbaseURL = processURL(new URI(urlStr));
    
    // get an URL that points to the current directory
    // from now on this is used as baseURL
    baseURL = ("".equals(newbaseURL.getPath())) ? newbaseURL.toString()
        : newbaseURL.resolve("./").toString();
    log.debug("URL directory which harvesting may not escape: " + baseURL);
    
    // remove invalid URLs from queued list (because until now we had no baseURL
    // restriction)
    needsHarvest.removeIf(s -> !s.startsWith(baseURL));
    
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
      processURL(new URI(urlStr));
    }
  }
  
  @Override
  protected void enumerateValidHarvesterPropertyNames(Set<String> props) {
    super.enumerateValidHarvesterPropertyNames(props);
    props.addAll(Arrays.asList("baseUrl", "retryCount",
        "retryAfterSeconds", "timeoutAfterSeconds", "filenameFilter",
        "contentTypes", "excludeUrlPattern", "pauseBetweenRequests",
        "authorizationHeader"
    ));
  }
  
  // internal implementation
  
  void queueURL(String url) {
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
  
  private void analyzeHTML(final URI baseURL, final InputSource source)
      throws Exception {
    XMLReader r = htmlReaderClass.getConstructor().newInstance();
    r.setFeature("http://xml.org/sax/features/namespaces", true);
    r.setFeature("http://cyberneko.org/html/features/balance-tags", true);
    r.setFeature("http://cyberneko.org/html/features/report-errors", false);
    // these are the defaults for HTML 4.0 and DOM with HTML:
    r.setProperty("http://cyberneko.org/html/properties/names/elems", "upper");
    r.setProperty("http://cyberneko.org/html/properties/names/attrs", "lower");
    
    DefaultHandler handler = new DefaultHandler() {
      
      private URI base = baseURL; // make it unfinal ;-)
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
              base = base.resolve(newBase);
            } catch (IllegalArgumentException mue) {
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
          queueURL(base.resolve(url).toString());
        } catch (IllegalArgumentException mue) {
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
  
  private boolean acceptFile(URI url) {
    if (filenameFilter == null) return true;
    String name = url.getPath();
    int p = name.lastIndexOf('/');
    if (p >= 0) name = name.substring(p + 1);
    Matcher m = filenameFilter.matcher(name);
    return m.matches();
  }
  
  @SuppressWarnings("resource")
  private URI processURL(URI uri) throws Exception {
    for (int retry = 0; retry <= retryCount; retry++) {
      log.info("Requesting props of '" + uri + "'...");
      var proto = uri.getScheme().toLowerCase(Locale.ROOT);
      if (!("http".equals(proto) || "https".equals(proto))) throw new IllegalArgumentException(
          "WebCrawlingHarvester only allows HTTP(S) as network protocol!");
      final var reqBuilder = HttpRequest.newBuilder(uri).GET()
          .timeout(timeout)
          .setHeader("User-Agent", USER_AGENT)
          .setHeader("Accept-Charset", StandardCharsets.UTF_8.name() + ", *;q=0.5")
          .setHeader("Accept", "text/xml, application/xml, *;q=0.1")
          .setHeader("Accept", Stream.of(contentTypes, HTML_CONTENT_TYPES, Set.of("*;q=0.1"))
              .flatMap(Set::stream).distinct().collect(Collectors.joining(", ")));
      HttpClientUtils.sendCompressionHeaders(reqBuilder);
      if (authorizationHeader != null) {
        reqBuilder.header("Authorization", authorizationHeader);
      }
      
      log.debug("Opening connection...");
      try {
        final HttpResponse<InputStream> resp;
        try {
          resp = httpClient.send(reqBuilder.build(), BodyHandlers.ofInputStream());
        } catch (IOException ioe) {
          throw new RetryAfterIOException(retryTime, ioe);
        }
        final int statusCode = resp.statusCode();
        switch (statusCode) {
          case HttpURLConnection.HTTP_UNAVAILABLE:
            var retryAfter = resp.headers().firstValue("Retry-After").map(Integer::parseInt);
            if (retryAfter.isPresent()) {
              throw new RetryAfterIOException(retryAfter.get(),
                  "Webserver returned '503 Service Unavailable', repeating after " + retryAfter.get() + "s.");
            }
            throw new IOException("Webserver unavailable (status 503)");
          case HttpURLConnection.HTTP_OK:
            break;
          case HttpURLConnection.HTTP_NOT_FOUND:
          case HttpURLConnection.HTTP_GONE:
            log.warn("Cannot find URL '" + resp.uri() + "'.");
            return uri;
          default:
            if (statusCode >= 500) {
              throw new RetryAfterIOException(retryTime, "Webserver returned error code, repeating after " + retryTime + "s: " + statusCode);
            }
            throw new IOException("Webserver returned invalid status code: " + statusCode);
        }
        
        try (final InputStream in = HttpClientUtils.getDecompressingInputStream(resp)) {
          // check connection properties
          String contentType = resp.headers().firstValue("Content-Type").orElse(null);
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
            log.warn("Connection to URL '" + uri
                + "' did not return a content-type, skipping.");
            return uri;
          }
  
          // if we got a redirect the new URL is now needed
          URI newurl = resp.uri();
          if (!uri.toString().equals(newurl.toString())) {
            log.debug("Got redirect to: " + newurl);
            uri = newurl;
            // check if it is below base
            if (!uri.toString().startsWith(baseURL)) return uri;
            // was it already harvested?
            if (harvested.contains(uri.toString())) return uri;
            // clean this new url from lists
            needsHarvest.remove(uri.toString());
            harvested.add(uri.toString());
          }
          
          if (HTML_CONTENT_TYPES.contains(contentType)) {
            log.info("Analyzing HTML links in '" + uri + "'...");
            
            final InputSource src = new InputSource(in);
            src.setSystemId(uri.toString());
            src.setEncoding(charset);
            analyzeHTML(uri, src);
          } else if (contentTypes.contains(contentType)) {
            if (acceptFile(uri)) {
              var lastModified = resp.headers().firstValue("Last-Modified").map(DateTimeFormatter.RFC_1123_DATE_TIME::parse).map(Instant::from).orElse(null);
              if (isDocumentOutdated(lastModified)) {
                log.info("Harvesting '" + uri + "'...");
                
                final InputSource src = new InputSource(in);
                src.setSystemId(uri.toString());
                src.setEncoding(charset);
                final SAXSource saxsrc = new SAXSource(StaticFactories.saxFactory
                    .newSAXParser().getXMLReader(), src);
                addDocument(uri.toString(), lastModified, saxsrc);
              } else {
                // add this empty doc here, to update datestamps for next
                // harvesting
                addDocument(uri.toString(), lastModified, null);
              }
            }
          }
          return uri;
        }
      } catch (RetryAfterIOException ioe) {
        int after = retryTime;
        if (retry >= retryCount) throw (IOException) ioe.getCause();
        log.warn(ioe.getMessage());
        after = ((RetryAfterIOException) ioe).getRetryAfter();
        log.info("Retrying after " + after + " seconds ("
            + (retryCount - retry) + " retries left)...");
        try {
          Thread.sleep(1000L * after);
        } catch (InterruptedException ie) {}
        log.debug("Recreating digester instances to recover from incomplete parsers...");
      }
    }
    throw new IOException("Unable to properly connect HTTP server.");
  }
}