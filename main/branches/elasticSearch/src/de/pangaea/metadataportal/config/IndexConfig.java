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

package de.pangaea.metadataportal.config;

import java.lang.ref.WeakReference;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import de.pangaea.metadataportal.utils.AutoCloseIndexReader;

/**
 * Abstract configuration of an panFMP index. It not only configures its properties like name/id,
 * it also contains methods to get some index access objects (IndexReader, Searcher) and status information.
 * @author Uwe Schindler
 */
public abstract class IndexConfig {

	private static org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(IndexConfig.class);
	
	/** Default constructor **/
	public IndexConfig(Config parent) {
		this.parent=parent;
	}

	/** Sets the ID of this index configuration. **/
	public void setId(String v) {
		if (checked) throw new IllegalStateException("Virtual index configuration cannot be changed anymore!");
		id=v;
	}

	/** Sets the user-readable name of this index configuration. **/
	public void setDisplayName(String v) {
		displayName=v;
	}

	/** Checks, if configuration is ok. After calling this, you are not able to change anything in this instance. **/
	public void check() throws Exception {
		if (id==null) throw new IllegalStateException("Every index needs a unique id!");
		if (displayName==null || "".equals(displayName)) throw new IllegalStateException("Index with id=\""+id+"\" has no displayName!");
		checked=true;
	}
	
	/** returns a IndexSearcher on the shared IndexReader, should be closed after using. **/
	public IndexSearcher newSearcher() throws java.io.IOException {
		if (!checked) throw new IllegalStateException("Index config not initialized and checked!");
		IndexSearcher searcher=new IndexSearcher(getSharedIndexReader());
		searcher.setDefaultFieldSortScoring(true,true);
		return searcher;
	}

	/** checks, if shared IndexReader is current and the underlying disk store was not changed **/
	public synchronized boolean isSharedIndexCurrent() throws java.io.IOException {
		if (!checked) throw new IllegalStateException("Index config not initialized and checked!");
		if (indexReader==null) return true;
		return indexReader.isCurrent();
	}
	
	/** returns a shared, read-only IndexReader. This reader may not be closed by {@link IndexReader#close()}.  **/
	public abstract IndexReader getSharedIndexReader() throws java.io.IOException;

	/**  returns a new IndexReader. This reader must be closed after using. **/
	public abstract IndexReader newIndexReader() throws java.io.IOException;

	/** checks, if index is available (a segment file is available) **/
	public abstract boolean isIndexAvailable() throws java.io.IOException;

	/** reopens the shared index reader. **/
	public abstract void reopenSharedIndex() throws java.io.IOException;
	
	/** called by SearchService to warm the shared index reader during webapp initialization **/
	public abstract void warmSharedIndexReader() throws java.io.IOException;
	
	/** called by {@link de.pangaea.metadataportal.search.LuceneCache} to release the old reader, if not done automatically by GC. **/
	public synchronized void releaseOldSharedReader() throws java.io.IOException {
		if (oldReaderRef!=null) {
			AutoCloseIndexReader old=oldReaderRef.get();
			try {
				if (old!=null) old.hardClose();
			} finally {
				old=null;
				oldReaderRef.clear();
				oldReaderRef=null;
			}
		}
	}
	
	/** this saves the old indexReader instance in a weak reference and replaces by new (reopened) one **/
	protected synchronized void replaceSharedIndexReader(AutoCloseIndexReader indexReader) throws java.io.IOException {
		releaseOldSharedReader();
		oldReaderRef=new WeakReference<AutoCloseIndexReader>(this.indexReader);
		this.indexReader=indexReader;
	}

	@Override
	protected void finalize() throws Throwable {
		try {
			releaseOldSharedReader();
		} finally {
			super.finalize();
		}
	}
	
	protected volatile AutoCloseIndexReader indexReader=null;
	protected volatile WeakReference<AutoCloseIndexReader> oldReaderRef=null;
	protected boolean checked=false;

	// members "the configuration"
	public String displayName=null,id=null;
	public final Config parent;
}