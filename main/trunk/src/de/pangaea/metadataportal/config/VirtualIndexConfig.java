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

import de.pangaea.metadataportal.utils.PublicForDigesterUse;
import de.pangaea.metadataportal.utils.BooleanParser;
import java.util.*;

/**
 * Configuration of a virtual lucene index. Such indexes can not be the target of a harvest operation.
 * It is only a collection of {@link SingleIndexConfig}.
 * It supplies methods to get some index access objects (IndexReader, Searcher) and status information.
 * Because of that it is possible to use these virtual indexes for searching like one single index.
 * @author Uwe Schindler
 */
public class VirtualIndexConfig extends IndexConfig {

	public VirtualIndexConfig() {
		super();
	}

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

	@PublicForDigesterUse
	@Deprecated
	public void setThreaded(String v) {
		threaded=BooleanParser.parseBoolean(v.trim());
	}

	@Override
	public void check() {
		super.check();
		if (indexIds.size()==0) throw new IllegalStateException("Virtual index with id=\""+id+"\" does not reference any index!");
		indexes=new IndexConfig[indexIds.size()];
		int i=0;
		for (Iterator<String> it=indexIds.iterator(); it.hasNext(); i++) {
			String s=it.next();
			IndexConfig iconf=parent.indexes.get(s);
			if (iconf==null) throw new IllegalStateException("Virtual index with id=\""+id+"\" references not existing index \""+s+"\"!");
			if (iconf==this) throw new IllegalStateException("Virtual index with id=\""+id+"\" references itsself!");
			iconf.check();
			indexes[i]=iconf;
		}
	}

	// Searcher
	@Override
	public org.apache.lucene.search.Searcher newSearcher() throws java.io.IOException {
		if (indexes==null) throw new IllegalStateException("Virtual index configuration with id=\""+id+"\" not yet checked and initialized!");
		org.apache.lucene.search.Searchable[] l=new org.apache.lucene.search.Searchable[indexes.length];
		for (int i=0, c=indexes.length; i<c; i++) l[i]=indexes[i].newSearcher();
		if (threaded) {
			return new org.apache.lucene.search.ParallelMultiSearcher(l);
		} else {
			return new org.apache.lucene.search.MultiSearcher(l);
		}
	}

	// Reader
	@Override
	public org.apache.lucene.index.IndexReader getIndexReader() throws java.io.IOException {
		if (indexes==null) throw new IllegalStateException("Virtual index configuration with id=\""+id+"\" not yet checked and initialized!");
		org.apache.lucene.index.IndexReader[] l=new org.apache.lucene.index.IndexReader[indexes.length];
		for (int i=0, c=indexes.length; i<c; i++) l[i]=indexes[i].getIndexReader();
		return new org.apache.lucene.index.MultiReader(l);
	}

	@Override
	public org.apache.lucene.index.IndexReader getUncachedIndexReader() throws java.io.IOException {
		if (indexes==null) throw new IllegalStateException("Virtual index configuration with id=\""+id+"\" not yet checked and initialized!");
		org.apache.lucene.index.IndexReader[] l=new org.apache.lucene.index.IndexReader[indexes.length];
		for (int i=0, c=indexes.length; i<c; i++) l[i]=indexes[i].getUncachedIndexReader();
		return new org.apache.lucene.index.MultiReader(l);
	}

	// check if current opened reader is current
	@Override
	public synchronized boolean isIndexCurrent() throws java.io.IOException {
		if (indexes==null) throw new IllegalStateException("Virtual index configuration with id=\""+id+"\" not yet checked and initialized!");
		boolean ok=true;
		for (int i=0, c=indexes.length; i<c; i++) ok&=indexes[i].isIndexCurrent();
		return ok;
	}

	@Override
	public boolean isIndexAvailable() throws java.io.IOException {
		if (indexes==null) throw new IllegalStateException("Virtual index configuration with id=\""+id+"\" not yet checked and initialized!");
		boolean ok=true;
		for (int i=0, c=indexes.length; i<c; i++) ok&=indexes[i].isIndexAvailable();
		return ok;
	}

	@Override
	public synchronized void reopenIndex() throws java.io.IOException {
		if (indexes==null) throw new IllegalStateException("Virtual index configuration with id=\""+id+"\" not yet checked and initialized!");
		for (int i=0, c=indexes.length; i<c; i++) indexes[i].reopenIndex();
		closeIndex();
	}

	private Set<String> indexIds=new HashSet<String>();

	// members "the configuration"
	public IndexConfig[] indexes=null;
	public boolean threaded=false;
}