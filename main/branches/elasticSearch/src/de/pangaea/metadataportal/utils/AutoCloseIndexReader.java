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

package de.pangaea.metadataportal.utils;

import org.apache.lucene.index.*;
import java.io.IOException;

/**
 * <code>AutoCloseIndexReader</code> is used by {@link de.pangaea.metadataportal.config.IndexConfig#getSharedIndexReader} to make IndexReaders cleanup by GC possible.
 * @author Uwe Schindler
 */
public final class AutoCloseIndexReader extends FilterIndexReader {

	private static org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(AutoCloseIndexReader.class);

	public AutoCloseIndexReader(final IndexReader in, final String name) {
		this(in,name,false);
	}
	
	private AutoCloseIndexReader(final IndexReader in, final String name, final boolean reopened) {
		super(in);
		this.name=name;
		log.info((reopened?"Softly reopened":"Opened")+" shared reader of index '"+name+"'.");
	}
	
	@Override
	protected synchronized final void doClose() throws IOException {
		// the index may only be closed on finalization
		if (!finalizationStarted) throw new UnsupportedOperationException("This IndexReader should not be closed.");
		// if in finalization, close normal
		super.doClose();
	}

	@Override
	protected IndexReader doOpenIfChanged() throws CorruptIndexException, IOException {
		final IndexReader n=IndexReader.openIfChanged(in);
		return (n==null) ? null : new AutoCloseIndexReader(n,name,true);
	}

	@Override @Deprecated
	protected IndexReader doOpenIfChanged(boolean openReadOnly) throws CorruptIndexException, IOException {
		final IndexReader n=IndexReader.openIfChanged(in,openReadOnly);
		return (n==null) ? null : new AutoCloseIndexReader(n,name,true);
	}

	@Override
	protected IndexReader doOpenIfChanged(final IndexCommit commit) throws CorruptIndexException, IOException {
		final IndexReader n=IndexReader.openIfChanged(in,commit);
		return (n==null) ? null : new AutoCloseIndexReader(n,name,true);
	}

	@Override
	protected IndexReader doOpenIfChanged(IndexWriter writer, boolean applyAllDeletes) throws CorruptIndexException, IOException {
		final IndexReader n=IndexReader.openIfChanged(in,writer,applyAllDeletes);
		return (n==null) ? null : new AutoCloseIndexReader(n,name,true);
	}

	public final synchronized void hardClose() throws CorruptIndexException, IOException {
		finalizationStarted=true;
		try {
			log.info("Closing unused shared reader of index '"+name+"'.");
			close();
		} catch (IOException ioe) {
			finalizationStarted=false;
			throw ioe;
		}
	}

	/** Close the index automatically. */
	@Override
	protected synchronized void finalize() throws Throwable {
		try {
			if (finalizationStarted) return;
			hardClose();
		} catch (IOException ioe) {
			log.error("Closing index '"+name+"' in finalization failed.",ioe);
		} finally {
			super.finalize();
		}
	}

	private boolean finalizationStarted=false;
	private final String name;
}
