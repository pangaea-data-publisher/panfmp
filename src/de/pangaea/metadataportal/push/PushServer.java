package de.pangaea.metadataportal.push;

import java.io.InputStream;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.xml.transform.Source;
import javax.xml.transform.sax.SAXSource;

import org.xml.sax.InputSource;

import de.pangaea.metadataportal.config.Config;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.DateUtils;
import io.undertow.util.Headers;
import io.undertow.util.PathTemplateMatch;
import io.undertow.util.StatusCodes;

/**
 * TODO
 * 
 * @author Uwe Schindler
 */
public class PushServer {
  
  private static final org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(PushServer.class);
  
  private final Config conf;
  private final Map<String,PushWrapperHarvester> harvesters = new ConcurrentHashMap<>();
  
  private PushServer(Config conf) {
    this.conf = conf;
  }

  public static void main(String... args) throws Exception {
    if (args.length < 1 || args.length > 2) {
      System.err.println("Command line: java " + PushServer.class.getName() + " config.xml");
      return;
    }
    
    try {
      final Config conf = new Config(args[0]);
      new PushServer(conf).runServer();
    } catch (Exception e) {
      log.fatal("PushServer general error:", e);
    }
  }
  
  private PushWrapperHarvester getHarvester(String id) {
    return harvesters.computeIfAbsent(id, key -> PushWrapperHarvester.initializeWrapper(conf, key, h -> harvesters.remove(id)));
  }
  
  private void runServer() {
    final HttpHandler handler = Handlers.routing(false)
        .put("/{harvester}/*", this::handlePut)
        .delete("/{harvester}/*", this::handleDelete)
        .post("/{harvester}/_commit", this::handleCommit);
    Undertow.builder()
      .addHttpListener(8089, "127.0.0.1")
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
      log.error("Invald last modified date in HTTP request: " + lastModHdr);
      e.setStatusCode(StatusCodes.BAD_REQUEST);
      e.endExchange();
      return;
    }
    try (InputStream in = e.getInputStream()) {
      final InputSource saxSrc = new InputSource(in);
      saxSrc.setSystemId(documentId);
      saxSrc.setEncoding(e.getRequestCharset());
      final Source src = new SAXSource(saxSrc);
      log.info("Index object for harvester " + harvester + ": " + documentId);
      getHarvester(harvester).addDocument(documentId, lastMod, src);
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
    log.info("Delete object for harvester " + harvester + ": " + documentId);
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
  
  private int getStatusCode(Throwable e) {
    if (e instanceof IllegalArgumentException) {
      return StatusCodes.NOT_FOUND;
    }
    return StatusCodes.INTERNAL_SERVER_ERROR;
  }
  
}
