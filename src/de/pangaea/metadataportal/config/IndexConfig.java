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

public abstract class IndexConfig {

    // Digester set methods
    public void setId(String v) {
        if (checked) throw new IllegalStateException("Virtual index configuration cannot be changed anymore!");
        id=v;
    }

    public void setDisplayName(String v) {
        displayName=v;
    }

    public void check() {
        checked=true;
        if (id==null) throw new IllegalStateException("Every index needs a unique id!");
        if (displayName==null || "".equals(displayName)) throw new IllegalStateException("Index with id=\""+id+"\" has no displayName!");
    }

    // Searcher
    public abstract org.apache.lucene.search.Searcher newSearcher() throws java.io.IOException;
    // Reader
    public abstract org.apache.lucene.index.IndexReader getIndexReader() throws java.io.IOException;
    // check if current opened reader is current
    public abstract boolean isIndexCurrent() throws java.io.IOException;
    public abstract void reopenIndex() throws java.io.IOException;

    public void closeIndex() throws java.io.IOException {
    }

    protected boolean checked=false;

    // members "the configuration"
    public String displayName=null,id=null;
    public Config parent=null;
}