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
import de.pangaea.metadataportal.utils.HashGenerator;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.document.*;
import org.apache.commons.collections.map.LRUMap;

/**
 * Implementation of the caching algorithm behind the <b>panFMP</b> search engine.
 * This class is for internal use only.
 * <p>To configure the cache use the following search properties in your config file <em>(these are the defaults):</em></p>
 *<pre>{@literal
 *<cacheMaxAge>300</cacheMaxAge>
 *<cacheMaxSessions>30</cacheMaxSessions>
 *<reloadIndexIfChangedAfter>60</reloadIndexIfChangedAfter>
 *<maxStoredQueries>200</maxStoredQueries>
 *}</pre>
 * @author Uwe Schindler
 */
public class LuceneCache {

    private static org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(LuceneCache.class);

    private LuceneCache(Config config) {
        this.config=config;
        int maxStoredQueries=Integer.parseInt(config.searchProperties.getProperty("maxStoredQueries",Integer.toString(DEFAULT_MAX_STORED_QUERIES)));
        @SuppressWarnings("unchecked") Map<String,Query> storedQueries=(Map<String,Query>)new LRUMap(maxStoredQueries);
        this.storedQueries=Collections.synchronizedMap(storedQueries);
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

    // Stored Queries
    public String storeQuery(Query query) {
        String hash=HashGenerator.sha1(query.toString());
        storedQueries.put(hash,query);
        return hash;
    }

    public Query readStoredQuery(String hash) {
        return storedQueries.get(hash);
    }

    // Cache of Hits
    public synchronized Session getSession(IndexConfig index, Query query, Sort sort) throws java.io.IOException {
        // generate an unique identifier
        String id="index="+index.id+"\000query="+query.toString(IndexConstants.FIELDNAME_CONTENT)+"\000sort="+sort;
        // look for identifier in cache
        Session sess=sessions.get(id);
        if (sess==null) {
            sess=new Session(this,index.newSearcher(),query,sort);
            sessions.put(id,sess);
            log.info("Created session: "+sess);
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
                    log.info("Removed session: "+e);
                } else {
                    if (oldest==null || e.lastAccess<oldest.lastAccess) oldest=e;
                }
            }
            // delete oldest if needed
            if (oldest==null) break;
            if (sessions.size()>cacheMaxSessions) {
                sessions.values().remove(oldest);
                log.info("Removed session: "+oldest);
            }
        }

        if (doReopen) indexChanged=false; // reset flag

        if (log.isDebugEnabled()) log.debug("Session cache after cleanup: "+sessions);
    }

    public FieldSelector getFieldSelector(boolean loadXml, Collection<String> fieldsToLoad) {
        HashSet<String> set=new HashSet<String>(loadXml?FIELDS_XML:FIELDS_DEFAULT);
        if (fieldsToLoad==null) {
            for (FieldConfig f : config.fields.values()) {
                if (f.lucenestorage!=Field.Store.NO) set.add(f.name);
            }
        } else {
            // check fields
            for (String fieldName : fieldsToLoad) {
                FieldConfig f=config.fields.get(fieldName);
                if (f==null) throw new IllegalFieldConfigException("Field name '"+fieldName+"' is unknown!");
                if (f.lucenestorage==Field.Store.NO) throw new IllegalFieldConfigException("Field '"+fieldName+"' is not a stored field!");
            }
            set.addAll(fieldsToLoad);
        }
        return new SetBasedFieldSelector(set,Collections.<String>emptySet());
    }

    private boolean indexChanged=false;
    private long indexChangedAt;
    private Map<String,Query> storedQueries;
    private Map<String,Session> sessions=new HashMap<String,Session>();

    protected Config config;

    public static final int DEFAULT_CACHE_MAX_AGE=5*60; // default 5 minutes
    public static final int DEFAULT_RELOAD_AFTER=1*60; // reload changed index after 1 minutes

    public static final int DEFAULT_MAX_STORED_QUERIES=200;
    public static final int DEFAULT_CACHE_MAX_SESSIONS=30;

    private static final Set<String> FIELDS_DEFAULT=Collections.singleton(IndexConstants.FIELDNAME_IDENTIFIER);
    private static final Set<String> FIELDS_XML=new TreeSet<String>(FIELDS_DEFAULT);
    static {
        FIELDS_XML.add(IndexConstants.FIELDNAME_XML);
    }

    /**
     * Implementation of a cache entry.
     */
    protected static final class Session {

        protected Session(LuceneCache parent, Searcher searcher, Query query, Sort sort) throws java.io.IOException {
            identifier="query={"+query.toString(IndexConstants.FIELDNAME_CONTENT)+"}; sorting={"+sort+"}";
            lastAccess=queryTime=new java.util.Date().getTime();
            this.parent=parent;
            this.searcher=searcher;
            if (sort!=null) hits=searcher.search(query,sort);
            else hits=searcher.search(query);
            queryTime=new java.util.Date().getTime()-queryTime;
        }

        protected synchronized void logAccess() {
            lastAccess=new java.util.Date().getTime();
        }

        public SearchResultList getSearchResultList(boolean loadXml, Collection<String> fieldsToLoad) {
            return new SearchResultList(this, parent.getFieldSelector(loadXml,fieldsToLoad));
        }

        @Override
        public String toString() {
            return identifier;
        }

        protected LuceneCache parent;
        protected String identifier;
        protected Searcher searcher;
        protected Hits hits;
        protected long lastAccess;
        protected long queryTime;
    }

}