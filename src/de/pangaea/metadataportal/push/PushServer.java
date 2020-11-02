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

package de.pangaea.metadataportal.push;

import java.io.InputStream;
import java.time.Instant;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.xml.transform.sax.SAXSource;
import org.xml.sax.InputSource;

import de.pangaea.metadataportal.config.Config;

import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.handlers.GracefulShutdownHandler;
import io.undertow.util.DateUtils;
import io.undertow.util.Headers;
import io.undertow.util.PathTemplateMatch;
import io.undertow.util.StatusCodes;

/**
 * Provides a simple HTTP server that accepts push requests. It can be
 * used as a replacement for a real harvester. In such cases, it is recommended
 * to use {@link de.pangaea.metadataportal.harvester.NoOpHarvester} in the config.
 * Once started, the server by default listens on 127.0.0.1, port 8089.
 * <p>
 * You can index a document by doing a {@code PUT} request and delete documents
 * by doing a {@code DELETE} request. The path pattern is:
 * {@code /harvesterID/identifier}. If the harvester does not exist a 404 Not Found
 * is returned. The push server 
 * <p>
 * To configure host and port, pass system properties {@code server.host} and
 * {@code server.port}. If you want to prepend all path names with some constant
 * prefix, pass system property {@code server.rootPath}. This prepends this
 * path to the above pattern.
 * 
 * @see de.pangaea.metadataportal.harvester.NoOpHarvester
 * @author Uwe Schindler
 */
public class PushServer {
  
  private static final org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(PushServer.class);
  
  private final Config conf;
  private final Map<String,PushWrapperHarvester> harvesters = new ConcurrentHashMap<>();
  
  private final String host;
  private final int port;
  private final String rootPath;
  
  public static void main(String... args) throws Exception {
    if (args.length < 1 || args.length > 2) {
      System.err.println("Command line: java " + PushServer.class.getName() + " config.xml");
      return;
    }
    
    try {
      final Config conf = new Config(args[0]);
      new PushServer(conf,
          System.getProperty("server.host", "127.0.0.1"),
          Integer.getInteger("server.port", 8089),
          System.getProperty("server.rootPath")).runServer();
    } catch (Exception e) {
      log.fatal("PushServer general error:", e);
    }
  }

  public PushServer(Config conf, String host, int port, String rootPath) {
    this.conf = conf;
    this.host = host;
    this.port = port;
    this.rootPath = rootPath;
  }

  private void runServer() {
    log.info(String.format(Locale.ENGLISH, "Starting panFMP push server and listening on %s:%d...", host, port));
    HttpHandler handler = Handlers.routing(false)
        .put("/{harvester}/*", this::handlePut)
        .delete("/{harvester}/*", this::handleDelete)
        .post("/{harvester}/_commit", this::handleCommit);
    final GracefulShutdownHandler shutdownHandler;
    handler = shutdownHandler = Handlers.gracefulShutdown(handler);
    if (rootPath != null) {
      handler = Handlers.path().addPrefixPath(rootPath, handler);
    }
    // register a shutdown hook to commit any changes on Ctrl-C:
    Runtime.getRuntime().addShutdownHook(new Thread(() -> shutdownHook(shutdownHandler)));
    // start server and go into event loop:
    Undertow.builder()
      .addHttpListener(port, host)
      .setHandler(handler)
      .build()
      .start();
  }
  
  private void handlePut(HttpServerExchange e) {
    if (e.isInIoThread()) {
      e.dispatch(this::handlePut);
      return;
    }
    e.startBlocking();
    final Map<String,String> params = e.getAttachment(PathTemplateMatch.ATTACHMENT_KEY).getParameters();
    final String harvester = params.get("harvester");
    final String documentId = params.get("*");
    final String lastModHdr = e.getRequestHeaders().getFirst(Headers.LAST_MODIFIED);
    final Instant lastMod;
    try {
      lastMod = (lastModHdr == null) ? null : DateUtils.parseDate(lastModHdr).toInstant();
    } catch (NullPointerException npe) {
      log.error("Invalid last modified date in HTTP request: " + lastModHdr);
      e.setStatusCode(StatusCodes.BAD_REQUEST);
      e.endExchange();
      return;
    }
    
    if (log.isTraceEnabled()) {
      log.trace("Index object for harvester " + harvester + ": " + documentId);
    }
    try (final InputStream in = e.getInputStream()) {
      final InputSource saxSrc = new InputSource(in);
      saxSrc.setSystemId(documentId);
      saxSrc.setEncoding(e.getRequestCharset());
      getHarvester(harvester).addDocument(documentId, lastMod, new SAXSource(saxSrc));
      e.setStatusCode(StatusCodes.ACCEPTED);
    } catch (Exception ex) {
      log.error(ex);
      e.setStatusCode(getStatusCode(ex));
    }
    e.endExchange();
  }
  
  private void handleDelete(HttpServerExchange e) {
    final Map<String,String> params = e.getAttachment(PathTemplateMatch.ATTACHMENT_KEY).getParameters();
    final String harvester = params.get("harvester");
    final String documentId = params.get("*");
    
    if (log.isTraceEnabled()) {
      log.trace("Delete object for harvester " + harvester + ": " + documentId);
    }
    try {
      getHarvester(harvester).deleteDocument(documentId);
      e.setStatusCode(StatusCodes.ACCEPTED);
    } catch (Exception ex) {
      log.error(ex);
      e.setStatusCode(getStatusCode(ex));
    }
    e.endExchange();
  }
  
  private void handleCommit(HttpServerExchange e) {
    final Map<String,String> params = e.getAttachment(PathTemplateMatch.ATTACHMENT_KEY).getParameters();
    final String harvester = params.get("harvester");
    
    if (PushWrapperHarvester.isValidHarvesterId(conf, harvester)) {
      if (harvesters.containsKey(harvester)) {
        log.info("Commit changes for harvester: " + harvester);
        try {
          getHarvester(harvester).commitAndClose();
          e.setStatusCode(StatusCodes.NO_CONTENT);
        } catch (Exception ex) {
          log.error(ex);
          e.setStatusCode(getStatusCode(ex));
        }
      } else {
        log.info("Harvester not open, commit ignored: " + harvester);
        e.setStatusCode(StatusCodes.NO_CONTENT);
      }
    } else {
      e.setStatusCode(StatusCodes.NOT_FOUND);
    }
    e.endExchange();
  }
  
  private void shutdownHook(GracefulShutdownHandler rootHandler) {
    log.info("Shutting down...");
    rootHandler.shutdown();
    rootHandler.addShutdownListener(this::shutdownNow);
    try {
      rootHandler.awaitShutdown();
    } catch (InterruptedException ie) {
      log.warn("Shutdown interrupted, state of push server unknown!", ie);
    }
    log.info("Shutdown of push server completed.");
  }
  
  private void shutdownNow(boolean shutdownSuccessful) {
    for (final Map.Entry<String,PushWrapperHarvester> he : harvesters.entrySet()) try {
      log.info("Commit changes for harvester for shutdown: " + he.getKey());
      he.getValue().commitAndClose();
    } catch (Exception e) {
      log.error("Error shutting down and commiting changes for harvester: " + he.getKey(), e);
    }
  }
  
  private PushWrapperHarvester getHarvester(String id) {
    return harvesters.computeIfAbsent(id, key -> PushWrapperHarvester.initializeWrapper(conf, key, h -> harvesters.remove(id)));
  }
  
  private int getStatusCode(Throwable e) {
    if (e instanceof IllegalArgumentException) {
      return StatusCodes.NOT_FOUND;
    }
    return StatusCodes.INTERNAL_SERVER_ERROR;
  }
  
}
