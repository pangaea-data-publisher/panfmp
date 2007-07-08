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
import de.pangaea.metadataportal.utils.*;
import javax.xml.transform.*;

public class SingleIndexConfig extends IndexConfig {

    public SingleIndexConfig() {
        super();
    }

    // Digester set methods
    public void setIndexDir(String v) throws java.io.IOException {
        indexDir=v;
    }

    public void setAutoOptimize(String v) {
        autoOptimize=Boolean.parseBoolean(v);
    }

    public void setValidate(String v) {
        validate=Boolean.parseBoolean(v);
    }

    public void setHarvesterClass(String v) throws ClassNotFoundException {
        Class<?> c=Class.forName(v);
        harvesterClass=c.asSubclass(de.pangaea.metadataportal.harvester.AbstractHarvester.class);
    }

    public void addHarvesterProperty(ExtendedDigester dig, String value) {
        harvesterProperties.setProperty(dig.getCurrentElementName(),value);
    }

    public void check() {
        super.check();
        if (indexDir==null || harvesterClass==null)
            throw new IllegalStateException("Some index configuration fields are missing for index with id=\""+id+"\"!");
    }

    // Searcher
    public synchronized org.apache.lucene.search.Searcher newSearcher() throws java.io.IOException {
        if (indexReader==null) indexReader=org.apache.lucene.index.IndexReader.open(getFullIndexPath());
        return new org.apache.lucene.search.IndexSearcher(indexReader);
    }

    // Reader
    public synchronized org.apache.lucene.index.IndexReader getIndexReader() throws java.io.IOException {
        if (indexReader==null) indexReader=org.apache.lucene.index.IndexReader.open(getFullIndexPath());
        return indexReader;
    }

    public org.apache.lucene.index.IndexReader getUncachedIndexReader() throws java.io.IOException {
        return org.apache.lucene.index.IndexReader.open(getFullIndexPath());
    }

    public boolean isIndexAvailable() throws java.io.IOException {
        return org.apache.lucene.index.IndexReader.indexExists(getFullIndexPath());
    }

    public String getFullIndexPath() throws java.io.IOException {
        return parent.makePathAbsolute(indexDir);
    }

    // check if current opened reader is current
    public synchronized boolean isIndexCurrent() throws java.io.IOException {
        if (indexReader==null) return true;
        return indexReader.isCurrent();
    }

    public synchronized void reopenIndex() throws java.io.IOException {
        closeIndex();
    }

    protected void finalize() throws java.io.IOException {
        closeIndex();
    }

    public synchronized void closeIndex() throws java.io.IOException {
        if (indexReader!=null) indexReader.close();
        indexReader=null;
    }

    protected org.apache.lucene.index.IndexReader indexReader=null;

    // members "the configuration"
    private String indexDir=null;
    public boolean autoOptimize=false;
    public boolean validate=true;
    public Class<? extends de.pangaea.metadataportal.harvester.AbstractHarvester> harvesterClass=null;
    public InheritedProperties harvesterProperties=new InheritedProperties();
    public Templates xslt=null;
}