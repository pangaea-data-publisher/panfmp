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

import de.pangaea.metadataportal.config.*;
import de.pangaea.metadataportal.utils.BooleanParser;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import javax.xml.transform.stream.StreamSource;
import java.util.zip.*;

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
  
  // Class members
  private String zipFile = null;
  private Pattern filenameFilter = null;
  private String identifierPrefix = "";
  private boolean useZipFileDate = true;
  
  public static final int DEFAULT_RETRY_TIME = 60; // seconds
  public static final int DEFAULT_RETRY_COUNT = 5;
  public static final int DEFAULT_TIMEOUT = 180; // seconds
  
  /** the retryCount from configuration */
  protected int retryCount = DEFAULT_RETRY_COUNT;
  /** the retryTime from configuration */
  protected int retryTime = DEFAULT_RETRY_TIME;
  /** the timeout from configuration */
  protected int timeout = DEFAULT_TIMEOUT;
  
  @Override
  public void open(IndexConfig iconfig) throws Exception {
    super.open(iconfig);
    
    zipFile = iconfig.harvesterProperties.getProperty("zipFile");
    if (zipFile == null) throw new IllegalArgumentException(
        "Missing name / URL of ZIP file to harvest (property \"zipFile\")");
    zipFile = iconfig.parent.makePathAbsolute(zipFile, true);
    
    identifierPrefix = iconfig.harvesterProperties.getProperty(
        "identifierPrefix", "");
    
    String s = iconfig.harvesterProperties.getProperty("filenameFilter");
    filenameFilter = (s == null) ? null : Pattern.compile(s);
    
    if ((s = iconfig.harvesterProperties.getProperty("retryCount")) != null) retryCount = Integer
        .parseInt(s);
    if ((s = iconfig.harvesterProperties.getProperty("retryAfterSeconds")) != null) retryTime = Integer
        .parseInt(s);
    if ((s = iconfig.harvesterProperties.getProperty("timeoutAfterSeconds")) != null) timeout = Integer
        .parseInt(s);
    if ((s = iconfig.harvesterProperties.getProperty("useZipFileDate")) != null) useZipFileDate = BooleanParser
        .parseBoolean(s);
  }
  
  @Override
  public void harvest() throws Exception {
    StringBuilder logstr = new StringBuilder("Opening and reading ZIP file \"")
        .append(zipFile).append("\" (useZipFileDate=").append(useZipFileDate);
    if (filenameFilter != null) logstr.append(", filter=\"")
        .append(filenameFilter).append("\"");
    logstr.append(")...");
    log.info(logstr);
    
    InputStream is = null;
    ZipInputStream zis = null;
    try {
      is = openStream();
      if (is != null) {
        zis = new ZipInputStream(is);
        ZipEntry ze = null;
        int count = 0;
        while ((ze = zis.getNextEntry()) != null)
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
        if (count <= 0) throw new ZipException(
            "The file seems to be no ZIP file, it contains no file entries.");
        log.info("Finished reading contents of ZIP file '" + zipFile + "'.");
      } else {
        cancelMissingDocumentDelete();
        log.info("ZIP file '" + zipFile + "' not modified!");
      }
    } finally {
      if (zis != null) zis.close();
      if (is != null) is.close();
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
        if (useZipFileDate) setHarvestingDateReference((lastModified == 0L) ? null
            : new Date(lastModified));
        return in;
      } catch (MalformedURLException urle) {
        // normal file
        File f = new File(zipFile);
        long lastModified = f.lastModified();
        if (useZipFileDate) setHarvestingDateReference((lastModified == 0L) ? null
            : new Date(lastModified));
        if (useZipFileDate && !isDocumentOutdated(lastModified)) return null;
        return new FileInputStream(f);
      } catch (FileNotFoundException fne) {
        throw fne;
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
  
  private static final class NoCloseInputStream extends FilterInputStream {
    
    public NoCloseInputStream(InputStream is) {
      super(is);
    }
    
    @Override
    public void close() throws IOException {
      // ignore close request
    }
    
  }
  
}