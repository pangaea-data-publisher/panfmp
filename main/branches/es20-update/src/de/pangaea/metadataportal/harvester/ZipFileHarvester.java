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
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Date;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipInputStream;

import javax.xml.transform.stream.StreamSource;

import de.pangaea.metadataportal.config.HarvesterConfig;
import de.pangaea.metadataportal.processor.ElasticsearchConnection;
import de.pangaea.metadataportal.utils.BooleanParser;
import de.pangaea.metadataportal.utils.NoCloseInputStream;

/**
 * Harvester for unzipping ZIP files and reading their contents. Identifiers
 * look like: &quot;zip:&lt;identifierPrefix&gt;&lt;entryFilename&gt;&quot;
 * <p>
 * This harvester supports the following additional <b>harvester properties</b>:
 * <ul>
 * <li><code>zipFile</code>: filename or URL of ZIP file to harvest</li>
 * <li><code>identifierPrefix</code>: This prefix is appended before all
 * identifiers (that are the identifiers of the documents) (default: "")</li>
 * <li><code>filenameFilter</code>: regex to match the entry filename (default:
 * none)</li>
 * <li><code>useZipFileDate</code>: if "yes", check the modification date of the
 * ZIP file and re-harvest in complete; if "no", look at each file in the
 * archive and store its modification date in index. For ZIP files from network
 * connections that seldom change use "yes" as it prevents scanning the ZIP file
 * in complete. "No" is recommended for large local files with much
 * modifications in only some files (default: yes)</li>
 * <li><code>retryCount</code>: how often retry on HTTP errors? (default: 5)</li>
 * <li><code>retryAfterSeconds</code>: time between retries in seconds (default:
 * 60)</li>
 * <li><code>timeoutAfterSeconds</code>: HTTP Timeout for harvesting in seconds</li>
 * </ul>
 * 
 * @author Uwe Schindler
 */
public class ZipFileHarvester extends SingleFileEntitiesHarvester {
  
  private String zipFile = null;
  
  private final Pattern filenameFilter;
  private final String identifierPrefix;
  private final boolean useZipFileDate;
  
  public static final int DEFAULT_RETRY_TIME = 60; // seconds
  public static final int DEFAULT_RETRY_COUNT = 5;
  public static final int DEFAULT_TIMEOUT = 180; // seconds
  
  /** the retryCount from configuration */
  protected final int retryCount;
  /** the retryTime from configuration */
  protected final int retryTime;
  /** the timeout from configuration */
  protected final int timeout;
  
  public ZipFileHarvester(HarvesterConfig iconfig) {
    super(iconfig);
    
    identifierPrefix = iconfig.properties.getProperty("identifierPrefix", "");
    
    String s = iconfig.properties.getProperty("filenameFilter");
    filenameFilter = (s == null) ? null : Pattern.compile(s);
    
    retryCount = Integer.parseInt(iconfig.properties.getProperty("retryCount", Integer.toString(DEFAULT_RETRY_COUNT)));
    retryTime = Integer.parseInt(iconfig.properties.getProperty("retryAfterSeconds", Integer.toString(DEFAULT_RETRY_TIME)));
    timeout = Integer.parseInt(iconfig.properties.getProperty("timeoutAfterSeconds", Integer.toString(DEFAULT_TIMEOUT)));
    useZipFileDate = BooleanParser.parseBoolean(iconfig.properties.getProperty("useZipFileDate", "true"));
  }

  @Override
  public void open(ElasticsearchConnection es, String targetIndex) throws Exception {
    super.open(es, targetIndex);
    
    String zipFile = iconfig.properties.getProperty("zipFile");
    if (zipFile == null) throw new IllegalArgumentException(
        "Missing name / URL of ZIP file to harvest (property \"zipFile\")");
    this.zipFile = iconfig.root.makePathAbsolute(zipFile, true);
  }

  @Override
  public void harvest() throws Exception {
    StringBuilder logstr = new StringBuilder("Opening and reading ZIP file \"")
        .append(zipFile).append("\" (useZipFileDate=").append(useZipFileDate);
    if (filenameFilter != null) logstr.append(", filter=\"")
        .append(filenameFilter).append("\"");
    logstr.append(")...");
    log.info(logstr);
    
    try (final InputStream is = openStream()) {
      if (is != null) {
        try (final ZipInputStream zis = new ZipInputStream(is)) {
          ZipEntry ze = null;
          int count = 0;
          while ((ze = zis.getNextEntry()) != null) {
            try {
              count++;
              if (ze.isDirectory()) continue;
              if (filenameFilter != null) {
                String name = ze.getName();
                int p = name.lastIndexOf('/');
                if (p >= 0) name = name.substring(p + 1);
                Matcher m = filenameFilter.matcher(name);
                if (!m.matches()) continue;
              }
              log.debug("Processing ZipEntry: " + ze);
              processFile(new NoCloseInputStream(zis), ze);
            } finally {
              zis.closeEntry();
            }
          }
          if (count <= 0) throw new ZipException(
              "The file seems to be no ZIP file, it contains no file entries.");
          log.info("Finished reading contents of ZIP file '" + zipFile + "'.");
        }
      } else {
        cancelMissingDocumentDelete();
        log.info("ZIP file '" + zipFile + "' not modified!");
      }
    }
  }
  
  @Override
  protected void enumerateValidHarvesterPropertyNames(Set<String> props) {
    super.enumerateValidHarvesterPropertyNames(props);
    props.addAll(Arrays.<String> asList("zipFile", "identifierPrefix",
        "filenameFilter", "useZipFileDate", "retryCount", "retryAfterSeconds",
        "timeoutAfterSeconds"));
  }
  
  private InputStream openStream() throws IOException {
    for (int retry = 0; retry <= retryCount; retry++)
      try {
        URLConnection conn = (new URL(zipFile)).openConnection();
        conn.setConnectTimeout(timeout * 1000);
        conn.setReadTimeout(timeout * 1000);
        
        if (conn instanceof HttpURLConnection) {
          StringBuilder ua = new StringBuilder("Java/")
              .append(System.getProperty("java.version")).append(" (")
              .append(de.pangaea.metadataportal.Package.getProductName())
              .append('/')
              .append(de.pangaea.metadataportal.Package.getVersion())
              .append("; ZipFileHarvester)");
          conn.setRequestProperty("User-Agent", ua.toString());
          
          conn.setRequestProperty("Accept-Encoding", "identity, *;q=0");
          conn.setRequestProperty("Accept", "application/zip, *;q=0.1");
          ((HttpURLConnection) conn).setInstanceFollowRedirects(true);
          
          // currently only for HTTP enabled
          if (fromDateReference != null && useZipFileDate) conn
              .setIfModifiedSince(fromDateReference.getTime());
        }
        
        conn.setUseCaches(false);
        log.debug("Opening connection...");
        InputStream in = null;
        try {
          conn.connect();
          in = conn.getInputStream();
        } catch (IOException ioe) {
          int after = -1, code = -1;
          if (conn instanceof HttpURLConnection) try {
            after = conn.getHeaderFieldInt("Retry-After", -1);
            code = ((HttpURLConnection) conn).getResponseCode();
          } catch (IOException ioe2) {
            after = -1;
            code = -1;
          }
          if (code == HttpURLConnection.HTTP_UNAVAILABLE && after > 0) throw new RetryAfterIOException(
              after, ioe);
          throw ioe;
        }
        
        long lastModified = conn.getLastModified();
        if (fromDateReference != null && useZipFileDate) {
          if ((conn instanceof HttpURLConnection && ((HttpURLConnection) conn)
              .getResponseCode() == HttpURLConnection.HTTP_NOT_MODIFIED)
              || !isDocumentOutdated(lastModified)) {
            log.debug("File not modified since " + fromDateReference);
            if (in != null) in.close();
            return null;
          }
        }
        if (useZipFileDate) setHarvestingDateReference((lastModified == 0L) ? null : new Date(lastModified));
        return in;
      } catch (MalformedURLException urle) {
        // normal file
        Path f = Paths.get(zipFile);
        long lastModified = Files.getLastModifiedTime(f).toMillis();
        if (useZipFileDate) setHarvestingDateReference((lastModified == 0L) ? null : new Date(lastModified));
        if (useZipFileDate && !isDocumentOutdated(lastModified)) return null;
        return Files.newInputStream(f);
      } catch (NoSuchFileException nsfe) {
        throw nsfe;
      } catch (IOException ioe) {
        int after = retryTime;
        if (ioe instanceof RetryAfterIOException) {
          if (retry >= retryCount) throw (IOException) ioe.getCause();
          log.warn("HTTP server returned '503 Service Unavailable' with a 'Retry-After' value being set.");
          after = ((RetryAfterIOException) ioe).getRetryAfter();
        } else {
          if (retry >= retryCount) throw ioe;
          log.error("Server access failed with exception: ", ioe);
        }
        log.info("Retrying after " + after + " seconds ("
            + (retryCount - retry) + " retries left)...");
        try {
          Thread.sleep(1000L * after);
        } catch (InterruptedException ie) {}
      }
    throw new IOException("Could not open stream.");
  }
  
  private void processFile(InputStream is, ZipEntry ze) throws Exception {
    String identifier = "zip:" + identifierPrefix + ze.getName();
    addDocument(identifier, useZipFileDate ? -1L : ze.getTime(),
        new StreamSource(is, identifier));
  }
  
}