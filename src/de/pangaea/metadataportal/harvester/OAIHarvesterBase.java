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
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Redirect;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.apache.commons.digester.AbstractObjectCreationFactory;
import org.apache.commons.digester.ObjectCreationFactory;
import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import de.pangaea.metadataportal.config.HarvesterConfig;
import de.pangaea.metadataportal.processor.ElasticsearchConnection;
import de.pangaea.metadataportal.processor.MetadataDocument;
import de.pangaea.metadataportal.utils.BooleanParser;
import de.pangaea.metadataportal.utils.ExtendedDigester;
import de.pangaea.metadataportal.utils.HttpClientUtils;
import de.pangaea.metadataportal.utils.HugeStringHashBuilder;

/**
 * Abstract base class for OAI harvesting support in panFMP. Use one of the
 * subclasses for harvesting OAI-PMH or OAI Static Repositories.
 * <p>
 * This harvester supports the following additional <b>harvester properties</b>:
 * <ul>
 * <li><code>setSpec</code>: OAI set to harvest (default: none)</li>
 * <li><code>retryCount</code>: how often retry on HTTP errors? (default: 5)</li>
 * <li><code>retryAfterSeconds</code>: time between retries in seconds (default:
 * 60)</li>
 * <li><code>timeoutAfterSeconds</code>: HTTP Timeout for harvesting in seconds</li>
 * <li><code>authorizationHeader</code>: Optional 'Authorization' HTTP header contents
 * to be sent with request.</li>
 * <li><code>metadataPrefix</code>: OAI metadata prefix to harvest</li>
 * <li><code>identifierPrefix</code>: prepend all identifiers returned by OAI with this string</li>
 * <li><code>ignoreDatestamps</code>: does full harvesting, while ignoring all datestamps. They are saved, but ignored, if invalid.</li>
 * <li><code>deleteMissingDocuments</code>: remove documents after harvesting that were
 * deleted from source (maybe a heavy operation). The harvester only does this on full
 * (not on incremental harvesting). (default: true)</li>
 * </ul>
 * 
 * @author Uwe Schindler
 */
public abstract class OAIHarvesterBase extends Harvester {
  // Class members
  public static final String OAI_NS = "http://www.openarchives.org/OAI/2.0/";
  public static final String OAI_STATICREPOSITORY_NS = "http://www.openarchives.org/OAI/2.0/static-repository";
  
  public static final int DEFAULT_RETRY_TIME = 60; // seconds
  public static final int DEFAULT_RETRY_COUNT = 5;
  public static final int DEFAULT_TIMEOUT = 180; // seconds
  
  public static final String USER_AGENT = new StringBuilder("Java/")
      .append(Runtime.version()).append(" (")
      .append(de.pangaea.metadataportal.Package.getProductName()).append('/')
      .append(de.pangaea.metadataportal.Package.getVersion())
      .append("; OAI downloader)").toString();
  
  /** the used metadata prefix from the configuration */
  protected final String metadataPrefix;
  
  /** prepend all identifiers returned by OAI with this string */
  protected final String identifierPrefix;
  
  /** the sets to harvest from the configuration, <code>null</code> to harvest all */
  protected final Set<String> sets;
  
  /** the retryCount from configuration */
  protected final int retryCount;
  
  /** the retryTime from configuration */
  protected final int retryTime;
  
  /** the timeout from configuration */
  protected final Duration timeout;
  
  /** the authorizationHeader from configuration */
  protected final String authorizationHeader;
  
  /** If enabled, does full harvesting, while ignoring all datestamps (default is {@code false}). They are saved, but ignored, if invalid. */
  protected final boolean ignoreDatestamps;
  
  /** If enabled, on any kind of full harvesting it will track all valid identifiers and delete all of them not seen in index. */
  protected final boolean deleteMissingDocuments;
  
  /** Contains all valid identifiers, if not {@code null}. Will be initialized by subclasses. */
  private HugeStringHashBuilder validIdentifiersBuilder = null;

  /** HttpClient to use, configured with correct connect timeout. */
  protected final HttpClient httpClient;
  
  /**
   * The harvester should filter incoming documents according to its set
   * metadata. Should be disabled for OAI-PMH protocol with only one set.
   * Default is {@code true}.
   */
  protected boolean filterIncomingSets = true;
  
  // constructor
  public OAIHarvesterBase(HarvesterConfig iconfig) {
    super(iconfig);
    
    final String s = iconfig.properties.getProperty("setSpec");
    if (s != null) {
      String[] sets = s.split("[\\,\\;\\s]+");
      this.sets = (sets.length == 0) ? null : Set.of(sets);
    } else {
      this.sets = null;
    }
    
    retryCount = Integer.parseInt(iconfig.properties.getProperty("retryCount", Integer.toString(DEFAULT_RETRY_COUNT)));
    retryTime = Integer.parseInt(iconfig.properties.getProperty("retryAfterSeconds", Integer.toString(DEFAULT_RETRY_TIME)));
    timeout = Duration.ofSeconds(Integer.parseInt(iconfig.properties.getProperty("timeoutAfterSeconds", Integer.toString(DEFAULT_TIMEOUT))));
    authorizationHeader = iconfig.properties.getProperty("authorizationHeader");
    metadataPrefix = iconfig.properties.getProperty("metadataPrefix");
    if (metadataPrefix == null) {
      throw new NullPointerException("No metadataPrefix for the OAI repository was given!");
    }
    identifierPrefix = iconfig.properties.getProperty("identifierPrefix", "");
    ignoreDatestamps = BooleanParser.parseBoolean(iconfig.properties.getProperty("ignoreDatestamps", "false"));
    deleteMissingDocuments = BooleanParser.parseBoolean(iconfig.properties.getProperty("deleteMissingDocuments", "true"));
    
    httpClient = HttpClient.newBuilder()
        .followRedirects(Redirect.NORMAL)
        .connectTimeout(timeout)
        .cookieHandler(new CookieManager(null, CookiePolicy.ACCEPT_ORIGINAL_SERVER))
        .build();
  }

  @Override
  public void open(ElasticsearchConnection es, String targetIndex) throws Exception {
    super.open(es, targetIndex);    
    recreateDigester();
  }
  
  @Override
  public void addDocument(MetadataDocument mdoc) throws Exception {
    if (filterIncomingSets && sets != null && mdoc instanceof OAIMetadataDocument) {
      final OAIMetadataDocument omdoc = (OAIMetadataDocument) mdoc;
      if (Collections.disjoint(omdoc.getSets(), sets)) omdoc.setDeleted(true);
    }
    if (validIdentifiersBuilder != null && !mdoc.isDeleted()) {
      validIdentifiersBuilder.add(mdoc.getIdentifier());
    }
    super.addDocument(mdoc);
  }
  
  @Override
  public MetadataDocument createMetadataDocumentInstance() {
    return new OAIMetadataDocument(iconfig, identifierPrefix, ignoreDatestamps);
  }
  
  /**
   * Returns a factory for creating the {@link MetadataDocument}s in Digester
   * code (using <code>FactoryCreateRule</code>).
   * 
   * @see #createMetadataDocumentInstance
   */
  protected ObjectCreationFactory getMetadataDocumentFactory() {
    return new AbstractObjectCreationFactory() {
      @Override
      public Object createObject(org.xml.sax.Attributes attributes) {
        return createMetadataDocumentInstance();
      }
    };
  }
  
  /**
   * Recreates all digesters that are used by parsing the OAI XML.
   * This method is called initiall once and later on network errors
   * before parsing same document again.
   * This allows to recover from document parsing failing somewhere in 
   * the middle of a document.
   */
  protected abstract void recreateDigester();
  
  /**
   * Harvests a URL using the suplied digester.
   * 
   * @param digSupplier
   *          a {@link Supplier} that gives access to a (possibly recreated)
   *          digester instance.
   * @param url
   *          the URL is parsed by this digester instance.
   * @param checkModifiedDate
   *          for static repositories, it is possible to give a reference to a
   *          {@link Instant} for checking the last modification, in this case
   *          <code>false</code> is returned, if the URL was not modified. If it
   *          was modified, the reference contains a new <code>Date</code>
   *          object with the new modification date. Supply <code>null</code>
   *          for no checking of last modification, a last modification date is
   *          then not returned back (as there is no reference).
   * @return <code>true</code> if harvested, <code>false</code> if not modified
   *         and no harvesting was done.
   */
  protected boolean doParse(Supplier<ExtendedDigester> digSupplier, String url,
      AtomicReference<Instant> checkModifiedDate) throws Exception {
    final URI u = new URI(url);
    for (int retry = 0; retry <= retryCount; retry++) {
      try {
        final ExtendedDigester dig = digSupplier.get();
        dig.clear();
        dig.resetRoot();
        dig.push(this);
        InputSource is = getInputSource(u, checkModifiedDate);
        try {
          if (checkModifiedDate != null && is == null) return false;
          dig.parse(is);
        } finally {
          if (is != null && is.getByteStream() != null) is.getByteStream().close();
        }
        return true;
      } catch (org.xml.sax.SAXException saxe) {
        // throw the real Exception not the digester one
        if (saxe.getException() != null) throw saxe.getException();
        else throw saxe;
      } catch (RetryAfterIOException ioe) {
        int after = retryTime;
        if (retry >= retryCount) throw ioe.getCause();
        log.warn(ioe.getMessage());
        after = ((RetryAfterIOException) ioe).getRetryAfter();
        log.info("Retrying after " + after + " seconds ("
            + (retryCount - retry) + " retries left)...");
        try {
          Thread.sleep(1000L * after);
        } catch (InterruptedException ie) {}
        log.debug("Recreating digester instances to recover from incomplete parsers...");
        recreateDigester();
      }
    }
    throw new IOException("Unable to properly connect OAI server.");
  }
  
  /**
   * Returns an <code>EntityResolver</code> that resolves all HTTP-URLS using
   * {@link #getInputSource}.
   * 
   * @param parent
   *          an <code>EntityResolver</code> that receives all unprocessed
   *          requests
   * @see #getInputSource
   */
  protected EntityResolver getEntityResolver(final EntityResolver parent) {
    return new EntityResolver() {
      @Override
      public InputSource resolveEntity(String publicId, String systemId)
          throws IOException, SAXException {
        try {
          URI uri = new URI(systemId);
          String proto = uri.getScheme().toLowerCase(Locale.ROOT);
          if ("http".equals(proto) || "https".equals(proto)) return getInputSource(
              uri, null);
          else return (parent == null) ? null : parent.resolveEntity(publicId,
              systemId);
        } catch (URISyntaxException e) {
          return (parent == null) ? null : parent.resolveEntity(publicId,
              systemId);
        }
      }
    };
  }
  
  /**
   * Returns a SAX <code>InputSource</code> for retrieving stream data of an
   * URL. It is optimized for compression of the HTTP(S) protocol and timeout
   * checking.
   * 
   * @param url
   *          the URL to open
   * @param checkModifiedDate
   *          for static repositories, it is possible to give a reference to a
   *          {@link Instant} for checking the last modification, in this case
   *          <code>null</code> is returned, if the URL was not modified. If it
   *          was modified, the reference contains a new <code>Date</code>
   *          object with the new modification date. Supply <code>null</code>
   *          for no checking of last modification, a last modification date is
   *          then not returned back (as there is no reference).
   * @see #getEntityResolver
   */
  protected InputSource getInputSource(URI url,
      AtomicReference<Instant> checkModifiedDate) throws IOException {
    String proto = url.getScheme().toLowerCase(Locale.ROOT);
    if (!("http".equals(proto) || "https".equals(proto))) throw new IllegalArgumentException(
        "OAI only allows HTTP(S) as network protocol!");
    final var reqBuilder = HttpRequest.newBuilder(url).GET()
        .timeout(timeout)
        .setHeader("User-Agent", USER_AGENT)
        .setHeader("Accept-Charset", StandardCharsets.UTF_8.name() + ", *;q=0.1")
        .setHeader("Accept", "text/xml, application/xml, *;q=0.1");
    HttpClientUtils.sendCompressionHeaders(reqBuilder);
    if (authorizationHeader != null) {
      reqBuilder.header("Authorization", authorizationHeader);
    }
    if (checkModifiedDate != null && checkModifiedDate.get() != null) {
      reqBuilder.setHeader("If-Modified-Since", DateTimeFormatter.RFC_1123_DATE_TIME.format(checkModifiedDate.get().atOffset(ZoneOffset.UTC)));
    }

    log.debug("Opening connection...");
    final HttpResponse<InputStream> resp;
    try {
      resp = HttpClientUtils.sendHttpRequestWithRetry(httpClient, reqBuilder.build(), BodyHandlers.ofInputStream());
    } catch (IOException ioe) {
      throw new RetryAfterIOException(retryTime, ioe);
    }
    boolean success = false;
    try {
      final int statusCode = resp.statusCode();
      switch (statusCode) {
        case HttpURLConnection.HTTP_UNAVAILABLE:
          final var ioe1 = new IOException("OAI server returned '503 Service Unavailable'");
          var retryAfter = resp.headers().firstValue("Retry-After").map(Integer::parseInt);
          if (retryAfter.isPresent()) {
            throw new RetryAfterIOException(retryAfter.get(),
                "OAI server returned '503 Service Unavailable', repeating after " + retryAfter.get() + "s.", ioe1);
          }
          throw new RetryAfterIOException(retryTime, "OAI server returned error code, repeating after " + retryTime + "s: " + statusCode, ioe1);
        case HttpURLConnection.HTTP_NOT_MODIFIED:
          if (checkModifiedDate != null) {
            log.debug("File not modified since " + checkModifiedDate.get());
            return null;
          }
          throw new IOException("OAI service returned 'not modified', although we did no 'If-Modified-Since' request.");
        case HttpURLConnection.HTTP_OK:
          break;
        default:
          final var ioe2 = new IOException("OAI service returned invalid status code: " + statusCode);
          if (statusCode >= 500) {
            throw new RetryAfterIOException(retryTime, "OAI Server returned error code, repeating after " + retryTime + "s: " + statusCode, ioe2);
          }
          throw ioe2;
      }
      
      if (checkModifiedDate != null) {
        var d = resp.headers().firstValue("Last-Modified").map(DateTimeFormatter.RFC_1123_DATE_TIME::parse).map(Instant::from).orElse(null);
        checkModifiedDate.set(d);
      }
      
      // get charset from content-type to fill into InputSource to prevent
      // SAXParser from guessing it
      // if charset is superseded by <?xml ?> declaration, it is changed later by
      // parser
      final String charset = resp.headers().firstValue("Content-Type").map(contentType -> {
        contentType = contentType.toLowerCase(Locale.ROOT);
        int charsetStart = contentType.indexOf("charset=");
        if (charsetStart >= 0) {
          int charsetEnd = contentType.indexOf(";", charsetStart);
          if (charsetEnd == -1) charsetEnd = contentType.length();
          charsetStart += "charset=".length();
          return contentType.substring(charsetStart, charsetEnd).trim();
        }
        return null;
      }).orElse(null);
      log.debug("Charset from Content-Type: '" + charset + "'");
      
      final InputSource src = new InputSource(HttpClientUtils.getDecompressingInputStream(resp));
      src.setSystemId(url.toString());
      src.setEncoding(charset);
      success = true;
      return src;
    } finally {
      if (!success) try {
        resp.body().close();
      } catch (IOException ioe) {
        // ignore
      }
    }
  }
  
  /** Resets the internal variables. */
  protected void reset() {}
  
  /**
   * Enable unseen document deletes. This should be enabled by harvester
   * before calling {@link #addDocument(MetadataDocument)}, so tracking
   * can be enabled.
   */
  protected void enableMissingDocumentDelete() {
    if (validIdentifiersBuilder == null && deleteMissingDocuments) {
      log.info("Tracking of seen document identifiers enabled.");
      validIdentifiersBuilder = new HugeStringHashBuilder();
    }
  }
  
  /**
   * Disable the property "deleteMissingDocuments" for this instance. This can
   * be used, when the container (like a ZIP file was not modified), and all
   * containing documents are not enumerated. To prevent deletion of all these
   * documents call this.
   */
  protected void cancelMissingDocumentDelete() {
    log.info("Tracking of seen document identifiers cancelled, no deletions will happen.");
    validIdentifiersBuilder = null;
  }
  
  @Override
  public void close(boolean cleanShutdown) throws Exception {
    if (cleanShutdown && validIdentifiersBuilder != null) {
      setValidIdentifiers(validIdentifiersBuilder.build());
    }
    reset();
    super.close(cleanShutdown);
  }
  
  @Override
  protected void enumerateValidHarvesterPropertyNames(Set<String> props) {
    super.enumerateValidHarvesterPropertyNames(props);
    props.addAll(Arrays.asList("setSpec", "retryCount",
        "retryAfterSeconds", "timeoutAfterSeconds", "metadataPrefix",
        "identifierPrefix", "ignoreDatestamps", "deleteMissingDocuments",
        "authorizationHeader"));
  }
  
}