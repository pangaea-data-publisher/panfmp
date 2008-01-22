/*
 *   Copyright 2007-2008 panFMP Developers Team c/o Uwe Schindler
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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.IndexSearcher;

/**
 * Abstract configuration of an panFMP index. It not only configures its properties like name/id,
 * it also contains methods to get some index access objects (IndexReader, Searcher) and status information.
 * @author Uwe Schindler
 */
public abstract class IndexConfig {

	public void setId(String v) {
		if (checked) throw new IllegalStateException("Virtual index configuration cannot be changed anymore!");
		id=v;
	}

	public void setDisplayName(String v) {
		displayName=v;
	}

	public void check() {
		if (id==null) throw new IllegalStateException("Every index needs a unique id!");
		if (displayName==null || "".equals(displayName)) throw new IllegalStateException("Index with id=\""+id+"\" has no displayName!");
		checked=true;
	}
	
	@Override
	protected void finalize() throws java.io.IOException {
		closeIndex();
	}	

	public Searcher newSearcher() throws java.io.IOException {
		if (!checked) throw new IllegalStateException("Index config not initialized and checked!");
		return new IndexSearcher(getIndexReader());
	}

	public synchronized boolean isIndexCurrent() throws java.io.IOException {
		if (!checked) throw new IllegalStateException("Index config not initialized and checked!");
		if (indexReader==null) return true;
		return indexReader.isCurrent();
	}

	public synchronized void reopenIndex() throws java.io.IOException {
		if (!checked) throw new IllegalStateException("Index config not initialized and checked!");
		if (indexReader!=null) {
			IndexReader n=indexReader.reopen();
			if (n!=indexReader) try {
				// reader was really reopened
				indexReader.close();
			} finally {
				indexReader=n;
			}
		}
	}

	public synchronized void closeIndex() throws java.io.IOException {
		if (!checked) throw new IllegalStateException("Index config not initialized and checked!");
		if (indexReader!=null) indexReader.close();
		indexReader=null;
	}
	
	// Reader
	public abstract IndexReader getIndexReader() throws java.io.IOException;
	public abstract IndexReader getUncachedIndexReader() throws java.io.IOException;
	public abstract boolean isIndexAvailable() throws java.io.IOException;

	protected IndexReader indexReader=null;
	protected boolean checked=false;

	// members "the configuration"
	public String displayName=null,id=null;
	public Config parent=null;
}