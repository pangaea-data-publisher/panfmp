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

package de.pangaea.metadataportal.config;

import java.util.*;

public class VirtualIndexConfig extends IndexConfig {

    public VirtualIndexConfig() {
        super();
    }

    // Digester set methods
    public void addIndex(String v) {
        if (checked) throw new IllegalStateException("Virtual index configuration cannot be changed anymore!");
        v=v.trim();
        if (v==null || "".equals(v)) throw new IllegalArgumentException("The index list inside a virtual index must contain references to other index instances!");
        if (indexIds.contains(v)) throw new IllegalArgumentException("Virtual index already contains index id=\""+v+"\"!");
        indexIds.add(v);
    }

    public void addIndexCollection(Collection<String> v) {
        if (checked) throw new IllegalStateException("Virtual index configuration cannot be changed anymore!");
        for (String s : v) addIndex(s);
    }

    public void setThreaded(String v) {
        threaded=Boolean.parseBoolean(v.trim());
    }

    public void check() {
        super.check();
        if (indexIds.size()==0) throw new IllegalStateException("Virtual index with id=\""+id+"\" does not reference any index!");
        indices=new IndexConfig[indexIds.size()];
        int i=0;
        for (Iterator<String> it=indexIds.iterator(); it.hasNext(); i++) {
            String s=it.next();
            IndexConfig iconf=parent.indices.get(s);
            if (iconf==null) throw new IllegalStateException("Virtual index with id=\""+id+"\" references not existing index \""+s+"\"!");
            if (iconf==this) throw new IllegalStateException("Virtual index with id=\""+id+"\" references itsself!");
            iconf.check();
            indices[i]=iconf;
        }
    }

    // Searcher
    public org.apache.lucene.search.Searcher newSearcher() throws java.io.IOException {
        if (indices==null) throw new IllegalStateException("Virtual index configuration with id=\""+id+"\" not yet checked and initialized!");
        org.apache.lucene.search.Searchable[] l=new org.apache.lucene.search.Searchable[indices.length];
        for (int i=0, c=indices.length; i<c; i++) l[i]=indices[i].newSearcher();
        if (threaded) {
            return new org.apache.lucene.search.ParallelMultiSearcher(l);
        } else {
            return new org.apache.lucene.search.MultiSearcher(l);
        }
    }

    // Reader
    public org.apache.lucene.index.IndexReader getIndexReader() throws java.io.IOException {
        if (indices==null) throw new IllegalStateException("Virtual index configuration with id=\""+id+"\" not yet checked and initialized!");
        org.apache.lucene.index.IndexReader[] l=new org.apache.lucene.index.IndexReader[indices.length];
        for (int i=0, c=indices.length; i<c; i++) l[i]=indices[i].getIndexReader();
        return new org.apache.lucene.index.MultiReader(l);
    }

    // check if current opened reader is current
    public synchronized boolean isIndexCurrent() throws java.io.IOException {
        if (indices==null) throw new IllegalStateException("Virtual index configuration with id=\""+id+"\" not yet checked and initialized!");
        boolean ok=true;
        for (int i=0, c=indices.length; i<c; i++) ok&=indices[i].isIndexCurrent();
        return ok;
    }

    public synchronized void reopenIndex() throws java.io.IOException {
        if (indices==null) throw new IllegalStateException("Virtual index configuration with id=\""+id+"\" not yet checked and initialized!");
        for (int i=0, c=indices.length; i<c; i++) indices[i].reopenIndex();
        closeIndex();
    }

    private Set<String> indexIds=new HashSet<String>();

    // members "the configuration"
    public IndexConfig[] indices=null;
    public boolean threaded=false;
}