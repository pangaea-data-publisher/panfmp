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
import de.pangaea.metadataportal.config.*;
import java.util.*;
import java.net.*;
import java.io.*;
import java.util.zip.*;
import java.util.concurrent.atomic.AtomicReference;
import org.xml.sax.InputSource;
import org.xml.sax.EntityResolver;
import org.xml.sax.SAXException;
import org.apache.commons.digester.*;

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
 * <li><code>metadataPrefix</code>: OAI metadata prefix to harvest</li>
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
  
  /** the used metadata prefix from the configuration */
  protected String metadataPrefix = null;
  /**
   * the sets to harvest from the configuration, <code>null</code> to harvest
   * all
   */
  protected Set<String> sets = null;
  /** the retryCount from configuration */
  protected int retryCount = DEFAULT_RETRY_COUNT;
  /** the retryTime from configuration */
  protected int retryTime = DEFAULT_RETRY_TIME;
  /** the timeout from configuration */
  protected int timeout = DEFAULT_TIMEOUT;
  
  /**
   * The harvester should filter incoming documents according to its set
   * metadata. Should be disabled for OAI-PMH protocol with only one set.
   * Default is {@code true}.
   */
  protected boolean filterIncomingSets = true;
  
  // construtor
  @Override
  public void open(SingleIndexConfig iconfig) throws Exception {
    super.open(iconfig);
    
    String s = iconfig.harvesterProperties.getProperty("setSpec");
    if (s != null) {
      sets = new HashSet<String>();
      Collections.addAll(sets, s.split("[\\,\\;\\s]+"));
      if (sets.isEmpty()) sets = null;
    }
    
    if ((s = iconfig.harvesterProperties.getProperty("retryCount")) != null) retryCount = Integer
        .parseInt(s);
    if ((s = iconfig.harvesterProperties.getProperty("retryAfterSeconds")) != null) retryTime = Integer
        .parseInt(s);
    if ((s = iconfig.harvesterProperties.getProperty("timeoutAfterSeconds")) != null) timeout = Integer
        .parseInt(s);
    metadataPrefix = iconfig.harvesterProperties.getProperty("metadataPrefix");
    if (metadataPrefix == null) throw new NullPointerException(
        "No metadataPrefix for the OAI repository was given!");
    
    SimpleCookieHandler.INSTANCE.enable();
  }
  
  @Override
  public void addDocument(MetadataDocument mdoc)
      throws IndexBuilderBackgroundFailure, InterruptedException {
    if (filterIncomingSets && sets != null) {
      if (Collections.disjoint(((OAIMetadataDocument) mdoc).getSets(), sets)) mdoc
          .setDeleted(true);
    }
    super.addDocument(mdoc);
  }
  
  @Override
  protected MetadataDocument createMetadataDocumentInstance() {
    return new OAIMetadataDocument(iconfig);
  }
  
  /**
   * Returns a factory for creating the {@link MetadataDocument}s in Digester
   * code (using <code>FactoryCreateRule</code>).
   * 
   * @see #createMetadataDocumentInstance
   */
  protected ObjectCreationFactory getMetadataDocumentFactory() {
    return new AbstractObjectCreationFactory() {
      public Object createObject(org.xml.sax.Attributes attributes) {
        return createMetadataDocumentInstance();
      }
    };
  }
  
  /**
   * Harvests a URL using the suplied digester.
   * 
   * @param dig
   *          the digester instance.
   * @param url
   *          the URL is parsed by this digester instance.
   * @param checkModifiedDate
   *          for static repositories, it is possible to give a reference to a
   *          {@link Date} for checking the last modification, in this case
   *          <code>false</code> is returned, if the URL was not modified. If it
   *          was modified, the reference contains a new <code>Date</code>
   *          object with the new modification date. Supply <code>null</code>
   *          for no checking of last modification, a last modification date is
   *          then not returned back (as there is no reference).
   * @return <code>true</code> if harvested, <code>false</code> if not modified
   *         and no harvesting was done.
   */
  protected boolean doParse(ExtendedDigester dig, String url,
      AtomicReference<Date> checkModifiedDate) throws Exception {
    URL u = new URL(url);
    for (int retry = 0; retry <= retryCount; retry++) {
      try {
        dig.clear();
        dig.resetRoot();
        dig.push(this);
        InputSource is = getInputSource(u, checkModifiedDate);
        try {
          if (checkModifiedDate != null && is == null) return false;
          dig.parse(is);
        } finally {
          if (is != null && is.getByteStream() != null) is.getByteStream()
              .close();
        }
        return true;
      } catch (org.xml.sax.SAXException saxe) {
        // throw the real Exception not the digester one
        if (saxe.getException() != null) throw saxe.getException();
        else throw saxe;
      } catch (IOException ioe) {
        int after = retryTime;
        if (ioe instanceof RetryAfterIOException) {
          if (retry >= retryCount) throw (IOException) ioe.getCause();
          log.warn("OAI server returned '503 Service Unavailable' with a 'Retry-After' value being set.");
          after = ((RetryAfterIOException) ioe).getRetryAfter();
        } else {
          if (retry >= retryCount) throw ioe;
          log.error("OAI server access failed with exception: ", ioe);
        }
        log.info("Retrying after " + after + " seconds ("
            + (retryCount - retry) + " retries left)...");
        try {
          Thread.sleep(1000L * after);
        } catch (InterruptedException ie) {}
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
      public InputSource resolveEntity(String publicId, String systemId)
          throws IOException, SAXException {
        try {
          URL url = new URL(systemId);
          String proto = url.getProtocol().toLowerCase(Locale.ENGLISH);
          if ("http".equals(proto) || "https".equals(proto)) return getInputSource(
              url, null);
          else return (parent == null) ? null : parent.resolveEntity(publicId,
              systemId);
        } catch (MalformedURLException malu) {
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
   *          {@link Date} for checking the last modification, in this case
   *          <code>null</code> is returned, if the URL was not modified. If it
   *          was modified, the reference contains a new <code>Date</code>
   *          object with the new modification date. Supply <code>null</code>
   *          for no checking of last modification, a last modification date is
   *          then not returned back (as there is no reference).
   * @see #getEntityResolver
   */
  protected InputSource getInputSource(URL url,
      AtomicReference<Date> checkModifiedDate) throws IOException {
    String proto = url.getProtocol().toLowerCase(Locale.ENGLISH);
    if (!("http".equals(proto) || "https".equals(proto))) throw new IllegalArgumentException(
        "OAI only allows HTTP(S) as network protocol!");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setConnectTimeout(timeout * 1000);
    conn.setReadTimeout(timeout * 1000);
    
    StringBuilder ua = new StringBuilder("Java/")
        .append(System.getProperty("java.version")).append(" (")
        .append(de.pangaea.metadataportal.Package.getProductName()).append('/')
        .append(de.pangaea.metadataportal.Package.getVersion())
        .append("; OAI downloader)");
    conn.setRequestProperty("User-Agent", ua.toString());
    
    conn.setRequestProperty("Accept-Encoding",
        "gzip, deflate, identity;q=0.3, *;q=0");
    conn.setRequestProperty("Accept-Charset", "utf-8, *;q=0.1");
    conn.setRequestProperty("Accept", "text/xml, application/xml, *;q=0.1");
    
    if (checkModifiedDate != null && checkModifiedDate.get() != null) conn
        .setIfModifiedSince(checkModifiedDate.get().getTime());
    
    conn.setUseCaches(false);
    conn.setFollowRedirects(true);
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
    
    if (checkModifiedDate != null) {
      if (conn.getResponseCode() == HttpURLConnection.HTTP_NOT_MODIFIED) {
        log.debug("File not modified since " + checkModifiedDate.get());
        if (in != null) in.close();
        return null;
      }
      long d = conn.getLastModified();
      checkModifiedDate.set((d == 0L) ? null : new Date(d));
    }
    
    String encoding = conn.getContentEncoding();
    if (encoding == null) encoding = "identity";
    encoding = encoding.toLowerCase(Locale.ENGLISH);
    log.debug("HTTP server uses " + encoding + " content encoding.");
    if ("gzip".equals(encoding)) in = new GZIPInputStream(in);
    else if ("deflate".equals(encoding)) in = new InflaterInputStream(in);
    else if (!"identity".equals(encoding)) throw new IOException(
        "Server uses an invalid content encoding: " + encoding);
    
    // get charset from content-type to fill into InputSource to prevent
    // SAXParser from guessing it
    // if charset is superseded by <?xml ?> declaration, it is changed later by
    // parser
    String contentType = conn.getContentType();
    String charset = null;
    if (contentType != null) {
      contentType = contentType.toLowerCase(Locale.ENGLISH);
      int charsetStart = contentType.indexOf("charset=");
      if (charsetStart >= 0) {
        int charsetEnd = contentType.indexOf(";", charsetStart);
        if (charsetEnd == -1) charsetEnd = contentType.length();
        charsetStart += "charset=".length();
        charset = contentType.substring(charsetStart, charsetEnd).trim();
      }
    }
    log.debug("Charset from Content-Type: '" + charset + "'");
    
    InputSource src = new InputSource(in);
    src.setSystemId(url.toString());
    src.setEncoding(charset);
    return src;
  }
  
  /** Resets the internal variables. */
  protected void reset() {}
  
  @Override
  public void close(boolean cleanShutdown) throws Exception {
    reset();
    SimpleCookieHandler.INSTANCE.disable();
    super.close(cleanShutdown);
  }
  
  @Override
  protected void enumerateValidHarvesterPropertyNames(Set<String> props) {
    super.enumerateValidHarvesterPropertyNames(props);
    props.addAll(Arrays.<String> asList("setSpec", "retryCount",
        "retryAfterSeconds", "timeoutAfterSeconds", "metadataPrefix"));
  }
  
}