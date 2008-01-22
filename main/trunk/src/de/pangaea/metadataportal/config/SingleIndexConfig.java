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

/**
 * Configuration of a real lucene index. Such indexes can be the target of a harvest operation.
 * @author Uwe Schindler
 */
public class SingleIndexConfig extends IndexConfig {

	public SingleIndexConfig() {
		super();
	}

	public void setIndexDir(String v) throws java.io.IOException {
		indexDir=v;
	}

	@PublicForDigesterUse
	@Deprecated
	public void setHarvesterClass(String v) throws ClassNotFoundException {
		harvesterClass=Class.forName(v).asSubclass(de.pangaea.metadataportal.harvester.Harvester.class);
	}

	@PublicForDigesterUse
	@Deprecated
	public void addHarvesterProperty(ExtendedDigester dig, String value) {
		harvesterProperties.setProperty(dig.getCurrentElementName(),value);
	}

	@Override
	public void check() {
		super.check();
		if (indexDir==null || harvesterClass==null)
			throw new IllegalStateException("Some index configuration fields are missing for index with id=\""+id+"\"!");
	}

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

	public String getFullIndexPath() throws java.io.IOException {
		return parent.makePathAbsolute(indexDir);
	}

	@Override
	public synchronized IndexReader getIndexReader() throws java.io.IOException {
		if (!checked) throw new IllegalStateException("Index config not initialized and checked!");
		if (indexReader==null) indexReader=IndexReader.open(getFullIndexPath());
		return indexReader;
	}

	@Override
	public IndexReader getUncachedIndexReader() throws java.io.IOException {
		if (!checked) throw new IllegalStateException("Index config not initialized and checked!");
		return IndexReader.open(getFullIndexPath());
	}

	@Override
	public boolean isIndexAvailable() throws java.io.IOException {
		if (!checked) throw new IllegalStateException("Index config not initialized and checked!");
		return IndexReader.indexExists(getFullIndexPath());
	}

	// members "the configuration"
	private String indexDir=null;
	public Class<? extends de.pangaea.metadataportal.harvester.Harvester> harvesterClass=null;
	public InheritedProperties harvesterProperties=new InheritedProperties();
	public Templates xslt=null;
}