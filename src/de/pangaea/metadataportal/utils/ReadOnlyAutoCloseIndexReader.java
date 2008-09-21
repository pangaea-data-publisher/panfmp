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

package de.pangaea.metadataportal.utils;

import org.apache.lucene.index.*;
import java.io.IOException;

/**
 * <code>ReadOnlyAutoCloseIndexReader</code> prevents the target index to be modified. It is used by {@link de.pangaea.metadataportal.config.IndexConfig#getIndexReader}
 * to prevent index modification. To do reopens of indexes, the index cannot be closed by <code>IndexConfig</code>, so it is openend until finalized by GC.
 * @author Uwe Schindler
 */
public final class ReadOnlyAutoCloseIndexReader extends FilterIndexReader {

	private static org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(ReadOnlyAutoCloseIndexReader.class);

	public ReadOnlyAutoCloseIndexReader(final IndexReader in, final String name) {
		super(in);
		this.name=name;
	}

	@Override
	protected void doUndeleteAll() throws CorruptIndexException, IOException {
		throw new UnsupportedOperationException("This IndexReader is read-only.");
	}

	@Override
	protected void doSetNorm(int d, String f, byte b) throws CorruptIndexException, IOException {
		throw new UnsupportedOperationException("This IndexReader is read-only.");
	}

	@Override
	protected void doDelete(int n) throws  CorruptIndexException, IOException {
		throw new UnsupportedOperationException("This IndexReader is read-only.");
	}
	
	@Override
	protected void doCommit() throws IOException { 
		throw new UnsupportedOperationException("This IndexReader is read-only.");
	}
	
	@Override
	protected void doClose() throws IOException {
		// the index may only be closed on finalization
		if (!finalizationStarted) throw new UnsupportedOperationException("This IndexReader should not be closed.");
		// if in finalization, close normal
		super.doClose();
	}

	@Override
	public IndexReader reopen() throws CorruptIndexException, IOException {
		final IndexReader n=in.reopen();
		if (n!=in) return new ReadOnlyAutoCloseIndexReader(n,name);
		return this;
	}

	/** Close the index automatically. */
	@Override
	protected void finalize() throws Throwable {
		finalizationStarted=true;
		try {
			log.info("Auto-closing orphaned instance of index '"+name+"' in finalizer.");
			this.close();
		} catch (IOException ioe) {
			log.error("Closing index '"+name+"' in finalization failed.",ioe);
		} finally {
			super.finalize();
		}
	}

	private volatile boolean finalizationStarted=false;
	private final String name;
}
