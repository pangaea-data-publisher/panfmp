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

package de.pangaea.metadataportal.search;

import java.util.*;
import de.pangaea.metadataportal.config.*;

public class LuceneCache {

    private static org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(LuceneCache.class);

    // Singleton per configuration
    private LuceneCache() {}

    private LuceneCache(Config config) {
        this.config=config;
    }

    private static final Map<String,LuceneCache> instances=new HashMap<String,LuceneCache>();

    // get an instance of per-file-singletons
    public static synchronized LuceneCache getInstance(String cfgFile) throws Exception {
        LuceneCache instance=instances.get(cfgFile);
        if (instance==null) {
            de.pangaea.metadataportal.config.Config cfg=new de.pangaea.metadataportal.config.Config(cfgFile,Config.ConfigMode.SEARCH);
            instance=new LuceneCache(cfg);
            instances.put(cfgFile,instance);
            log.info("New LuceneCache instance for config file '"+cfgFile+"' created.");
        }
        return instance;
    }

    // Queries
    public synchronized LuceneSession getLuceneResult(SearchRequest req) throws Exception {
        LuceneSession sess=sessions.get(req);
        if (sess==null) {
            sess=new LuceneSession(this,req);
            sessions.put(req,sess);
            log.info("New LuceneSession for SearchRequest: "+req);
        } else {
            sess.logAccess();
        }
        return sess;
    }

    public synchronized void cleanupCache() throws java.io.IOException {
        // get defaults for cache age etc.
        int cacheMaxAge=Integer.parseInt(config.searchProperties.getProperty("cacheMaxAge",Integer.toString(DEFAULT_CACHE_MAX_AGE)));
        int cacheMaxSessions=Integer.parseInt(config.searchProperties.getProperty("cacheMaxSessions",Integer.toString(DEFAULT_CACHE_MAX_SESSIONS)));
        int reloadIndexIfChangedAfter=Integer.parseInt(config.searchProperties.getProperty("reloadIndexIfChangedAfter",Integer.toString(DEFAULT_RELOAD_AFTER)));

        long now=new java.util.Date().getTime();

        // check indices for changes and queue re-open
        if (!indexChanged) {
            boolean changed=false;
            for (IndexConfig cfg : config.indices.values()) {
                if (cfg instanceof SingleIndexConfig && !cfg.isIndexCurrent()) {
                    changed=true;
                    break;
                }
            }
            if (changed) {
                indexChangedAt=now;
                log.info("Detected change in one of the configured indices. Preparing for reload in "+reloadIndexIfChangedAfter+"s.");
            }
            indexChanged=changed;
        }

        // reopen indices after RELOAD_AFTER secs. from detection of change
        boolean doReopen=(indexChanged && now-indexChangedAt>((long)reloadIndexIfChangedAfter)*1000L);

        if (doReopen) {
            for (IndexConfig cfg : config.indices.values()) {
                // only scan real indices, the others will be implicitely reopened
                if (cfg instanceof SingleIndexConfig) {
                    log.info("Reopening index '"+cfg.id+"'.");
                    cfg.reopenIndex();
                }
            }
        }

        // now really clean up sessions
        LuceneSession oldest=null;
        while (oldest==null /* always for first time */ || sessions.size()>cacheMaxSessions /* when too many sessions */ ) {
            // iterate over sessions and look for oldest one, delete outdated ones
            Iterator<LuceneSession> entries=sessions.values().iterator();
            while (entries.hasNext()) {
                LuceneSession e=entries.next();
                if (doReopen || now-e.lastAccess>((long)cacheMaxAge)*1000L) {
                    entries.remove();
                    log.info("Removed LuceneSession for SearchRequest: "+e.req);
                } else {
                    if (oldest==null || e.lastAccess<oldest.lastAccess) oldest=e;
                }
            }
            // delete oldest if needed
            if (oldest==null) break;
            if (sessions.size()>cacheMaxSessions) {
                sessions.values().remove(oldest);
                log.info("Removed LuceneSession for SearchRequest: "+oldest.req);
            }
        }

        if (doReopen) indexChanged=false; // reset flag

        if (log.isDebugEnabled()) log.debug("Cache after cleanup: "+sessions);
    }

    private HashMap<SearchRequest,LuceneSession> sessions=new HashMap<SearchRequest,LuceneSession>();
    private boolean indexChanged=false;
    private long indexChangedAt;

    protected de.pangaea.metadataportal.config.Config config=null;

    public static final int DEFAULT_CACHE_MAX_SESSIONS=Integer.MAX_VALUE; // infinite
    public static final int DEFAULT_CACHE_MAX_AGE=5*60; // default 5 minutes
    public static final int DEFAULT_RELOAD_AFTER=1*60; // reload changed index after 1 minutes

    /* config options:
        <cacheMaxAge>300</cacheMaxAge>
        <cacheMaxSessions>10</cacheMaxSessions>
        <reloadIndexIfChangedAfter>60</reloadIndexIfChangedAfter>
    */
}