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

package de.pangaea.metadataportal.utils;

import java.io.IOException;
import java.io.InputStream;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Locale;
import java.util.zip.GZIPInputStream;
import java.util.zip.InflaterInputStream;

/**
 * Some utility methods for decompressing {@link HttpResponse}
 * 
 * @author Uwe Schindler
 */
public final class HttpClientUtils {
  private static final org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory
      .getLog(HttpClientUtils.class);
  
  private HttpClientUtils() {}
  
  /** Returns an InputStream which decodes with header "Content-Encoding" */
  public static InputStream getDecompressingInputStream(final HttpResponse<InputStream> resp) throws IOException {
    final String encoding = resp.headers().firstValue("Content-Encoding").orElse("identity").toLowerCase(Locale.ROOT).trim();
    log.debug("HTTP server uses " + encoding + " content encoding.");
    switch (encoding) {
      case "gzip": return new GZIPInputStream(resp.body());
      case "deflate": return new InflaterInputStream(resp.body());
      case "identity": return resp.body();
    }
    throw new IOException("Server uses an invalid content encoding: " + encoding);
  }
  
  /** Sends "Accept-Encoding" header to ask server to compress result.
   * The response can later be parsed with {@link #getDecompressingInputStream(HttpResponse)} */
  public static void sendCompressionHeaders(final HttpRequest.Builder builder) {
    builder.setHeader("Accept-Encoding", "gzip, deflate, identity;q=0.3, *;q=0");
  }
  
  /** Workaround for: https://stackoverflow.com/questions/55087292/how-to-handle-http-2-goaway-with-httpclient */
  public static <T> HttpResponse<T> sendHttpRequestWithRetry(HttpClient client, HttpRequest request,
      HttpResponse.BodyHandler<T> responseBodyHandler) throws IOException {
    try {
      try {
        return client.send(request, responseBodyHandler);
      } catch (IOException e) {
        if (e.getMessage() != null && e.getMessage().contains("GOAWAY")) {
          return client.send(request, responseBodyHandler);
        }
        throw e;
      }
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new IOException("Connection interrupted.");
    }
  }
    
}