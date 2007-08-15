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
import de.pangaea.metadataportal.utils.IndexConstants;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.document.*;

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
    public synchronized Session getSession(IndexConfig index, Query query, Sort sort) throws java.io.IOException {
        Identifier id=new Identifier(index.id,query,sort);
        Session sess=sessions.get(id);
        if (sess==null) {
            sess=new Session(config,index.newSearcher(),query,sort);
            sessions.put(id,sess);
            log.info("Session for query={"+query.toString(IndexConstants.FIELDNAME_CONTENT)+"}; sorting={"+sort+"}");
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

        // check indexes for changes and queue re-open
        if (!indexChanged) {
            boolean changed=false;
            for (IndexConfig cfg : config.indexes.values()) {
                if (cfg instanceof SingleIndexConfig && !cfg.isIndexCurrent()) {
                    changed=true;
                    break;
                }
            }
            if (changed) {
                indexChangedAt=now;
                log.info("Detected change in one of the configured indexes. Preparing for reload in "+reloadIndexIfChangedAfter+"s.");
            }
            indexChanged=changed;
        }

        // reopen indexes after RELOAD_AFTER secs. from detection of change
        boolean doReopen=(indexChanged && now-indexChangedAt>((long)reloadIndexIfChangedAfter)*1000L);

        if (doReopen) {
            for (IndexConfig cfg : config.indexes.values()) {
                // only scan real indexes, the others will be implicitely reopened
                if (cfg instanceof SingleIndexConfig) {
                    log.info("Reopening index '"+cfg.id+"'.");
                    cfg.reopenIndex();
                }
            }
        }

        // now really clean up sessions
        Session oldest=null;
        while (oldest==null /* always for first time */ || sessions.size()>cacheMaxSessions /* when too many sessions */ ) {
            // iterate over sessions and look for oldest one, delete outdated ones
            Iterator<Session> entries=sessions.values().iterator();
            while (entries.hasNext()) {
                Session e=entries.next();
                if (doReopen || now-e.lastAccess>((long)cacheMaxAge)*1000L) {
                    entries.remove();
                    //log.info("Removed Session for Query: "+e.query);
                } else {
                    if (oldest==null || e.lastAccess<oldest.lastAccess) oldest=e;
                }
            }
            // delete oldest if needed
            if (oldest==null) break;
            if (sessions.size()>cacheMaxSessions) {
                sessions.values().remove(oldest);
                //log.info("Removed Session for SearchRequest: "+oldest.req);
            }
        }

        if (doReopen) indexChanged=false; // reset flag

        if (log.isDebugEnabled()) log.debug("Cache after cleanup: "+sessions);
    }

    public static FieldSelector getFieldSelector(Config config, boolean loadXml, Collection<String> fieldsToLoad) {
        if (fieldsToLoad!=null) {

            // check fields
            for (String fieldName : fieldsToLoad) {
                FieldConfig f=config.fields.get(fieldName);
                if (f==null) throw new IllegalFieldConfigException("Field name '"+fieldName+"' is unknown!");
                if (!f.lucenestorage) throw new IllegalFieldConfigException("Field '"+fieldName+"' is not a stored field!");
            }

            HashSet<String> set=new HashSet<String>(loadXml?FIELDS_XML:FIELDS_DEFAULT);
            set.addAll(fieldsToLoad);
            return new SetBasedFieldSelector(set,Collections.<String>emptySet());
        } else {
            if (!loadXml) throw new IllegalArgumentException("If you want to load all fields (fieldsToLoad==null), XML must be loaded, too!");
            return null;
        }
    }

    private HashMap<Identifier,Session> sessions=new HashMap<Identifier,Session>();
    private boolean indexChanged=false;
    private long indexChangedAt;

    protected de.pangaea.metadataportal.config.Config config=null;

    public static final int DEFAULT_CACHE_MAX_SESSIONS=Integer.MAX_VALUE; // infinite
    public static final int DEFAULT_CACHE_MAX_AGE=5*60; // default 5 minutes
    public static final int DEFAULT_RELOAD_AFTER=1*60; // reload changed index after 1 minutes

    private static final Set<String> FIELDS_DEFAULT=Collections.singleton(IndexConstants.FIELDNAME_IDENTIFIER);
    private static final Set<String> FIELDS_XML=new HashSet<String>(FIELDS_DEFAULT);
    static {
        FIELDS_XML.add(IndexConstants.FIELDNAME_XML);
    }

    protected static final class Identifier {

        // we use the String representations, because e.g. Sort has Buggy or missing equals()/hashCode()

        public Identifier(String index, Query query, Sort sort) {
            this.index=index;
            this.query=(query==null) ? null : query.toString();
            this.sort=(sort==null) ? null : sort.toString();
        }

        @Override
        public final int hashCode() {
            return ((index==null)?0:index.hashCode()^0x0f134aff) + ((query==null)?0:query.hashCode()^0x43615555) + ((sort==null)?0:sort.hashCode());
        }

        @Override
        public final boolean equals(Object o) {
            if (!(o instanceof Identifier)) return false;
            Identifier i=(Identifier)o;
            return (index==i.index || index.equals(i.index)) && (query==i.query || query.equals(i.query)) && (sort==i.sort || sort.equals(i.sort));
        }

        public String index,query,sort;
    }

    protected static final class Session {

        protected Session(Config config, Searcher searcher, Query query, Sort sort) throws java.io.IOException {
            lastAccess=queryTime=new java.util.Date().getTime();
            this.config=config;
            this.searcher=searcher;
            if (sort!=null) hits=searcher.search(query,sort);
            else hits=searcher.search(query);
            queryTime=new java.util.Date().getTime()-queryTime;
        }

        protected synchronized void logAccess() {
            lastAccess=new java.util.Date().getTime();
        }

        public SearchResultList getSearchResultList(boolean loadXml, Collection<String> fieldsToLoad) {
            return new SearchResultList(this, getFieldSelector(config,loadXml,fieldsToLoad));
        }

        protected Config config;
        protected Searcher searcher;
        protected Hits hits;
        protected long lastAccess;
        protected long queryTime;
    }

    /* config options:
        <cacheMaxAge>300</cacheMaxAge>
        <cacheMaxSessions>10</cacheMaxSessions>
        <reloadIndexIfChangedAfter>60</reloadIndexIfChangedAfter>
    */
}