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

import java.util.*;
import de.pangaea.metadataportal.utils.*;
import javax.xml.transform.Templates;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.Directory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Configuration of a real lucene index. Such indexes can be the target of a harvest operation.
 * @author Uwe Schindler
 */
public class SingleIndexConfig extends IndexConfig {

	private static Log log = LogFactory.getLog(SingleIndexConfig.class);

	/** Default constructor **/
	public SingleIndexConfig(Config parent) {
		super(parent);
	}

	/** Sets index directory (called from Digester on config load). **/
	public synchronized void setIndexDir(String v) throws java.io.IOException {
		if (checked) throw new IllegalStateException("Index configuration cannot be changed anymore!");
		if (indexDirImpl!=null) indexDirImpl.close();
		indexDirImpl=null;
		indexDir=v;
	}

	/** Sets class name of harvester (called from Digester on config load). **/
	@PublicForDigesterUse
	@Deprecated
	public void setHarvesterClass(String v) throws ClassNotFoundException {
		if (checked) throw new IllegalStateException("Index configuration cannot be changed anymore!");
		harvesterClass=Class.forName(v).asSubclass(de.pangaea.metadataportal.harvester.Harvester.class);
	}

	/** Adds property for harvester (called from Digester on config load). **/
	@PublicForDigesterUse
	@Deprecated
	public void addHarvesterProperty(ExtendedDigester dig, String value) {
		if (checked) throw new IllegalStateException("Index configuration cannot be changed anymore!");
		if (value!=null) harvesterProperties.setProperty(dig.getCurrentElementName(),value.trim());
	}

	@Override
	public void check() {
		super.check();
		if (indexDir==null || harvesterClass==null)
			throw new IllegalStateException("Some index configuration fields are missing for index with id=\""+id+"\"!");
	}

	/** Checks all harvester properties for validity. **/
	public void checkProperties() throws Exception {
		de.pangaea.metadataportal.harvester.Harvester h=harvesterClass.newInstance();
		Set<String> validProperties=h.getValidHarvesterPropertyNames();
		@SuppressWarnings("unchecked") Enumeration<String> en=(Enumeration<String>)harvesterProperties.propertyNames();
		while (en.hasMoreElements()) {
			String prop=en.nextElement();
			if (!validProperties.contains(prop))
				throw new IllegalArgumentException("Harvester '"+harvesterClass.getName()+"' for index '"+id+"' does not support property '"+prop+"'! Supported properties are: "+validProperties);
		}
	}

	/** Returns the local, expanded file system path to index. **/
	public String getFullIndexPath() throws java.io.IOException {
		return parent.makePathAbsolute(indexDir,false);
	}

	/** Returns the directory implementation, that contains the index. **/
	public synchronized Directory getIndexDirectory() throws java.io.IOException {
		if (indexDirImpl==null) indexDirImpl=FSDirectory.getDirectory(getFullIndexPath());
		return indexDirImpl;
	}

	@Override
	public synchronized IndexReader getSharedIndexReader() throws java.io.IOException {
		if (!checked) throw new IllegalStateException("Index config not initialized and checked!");
		if (indexReader==null) indexReader=new AutoCloseIndexReader(IndexReader.open(getIndexDirectory(),true),id);
		return indexReader;
	}

	@Override
	public IndexReader newIndexReader(final boolean readOnly) throws java.io.IOException {
		if (!checked) throw new IllegalStateException("Index config not initialized and checked!");
		return IndexReader.open(getIndexDirectory(),readOnly);
	}
	
	/** Opens an IndexWriter for adding Documents to Index. **/
	public IndexWriter newIndexWriter(boolean create) throws java.io.IOException {
		if (!checked) throw new IllegalStateException("Index config not initialized and checked!");
		final IndexWriter writer=new IndexWriter(getIndexDirectory(), parent.getAnalyzer(), create, IndexWriter.MaxFieldLength.UNLIMITED);
		final Log iwlog=LogFactory.getLog(writer.getClass());
		if (iwlog.isDebugEnabled()) writer.setInfoStream(LogUtil.getDebugStream(iwlog));
		writer.setUseCompoundFile(true);
		return writer;
	}

	@Override
	public boolean isIndexAvailable() throws java.io.IOException {
		if (!checked) throw new IllegalStateException("Index config not initialized and checked!");
		return IndexReader.indexExists(getIndexDirectory());
	}
	
	@Override
	public synchronized void reopenSharedIndex() throws java.io.IOException {
		if (!checked) throw new IllegalStateException("Index config not initialized and checked!");
		if (indexReader!=null) {
			IndexReader n=indexReader.reopen();
			if (n!=indexReader) {
				replaceSharedIndexReader((AutoCloseIndexReader)n); // should be castable!
			} else if (!indexReader.isCurrent()) { // it should be current now... (we hope)
				log.warn("Index '"+id+"' was reopened but is still not up-to-date (maybe a bug in Lucene, we try to investigate this). Doing a hard reopen (close & open later).");
				replaceSharedIndexReader(null);
			}
		}
	}

	@Override
	protected void finalize() throws Throwable {
		try {
			if (indexDirImpl!=null) indexDirImpl.close();
			indexDirImpl=null;
		} finally {
			super.finalize();
		}
	}
	
	// members "the configuration"
	private String indexDir=null;
	private volatile Directory indexDirImpl=null;
	public Class<? extends de.pangaea.metadataportal.harvester.Harvester> harvesterClass=null;
	public final InheritedProperties harvesterProperties=new InheritedProperties();
	public Templates xslt=null;
}
