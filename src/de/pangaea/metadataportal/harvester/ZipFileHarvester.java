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

package de.pangaea.metadataportal.harvester;

import de.pangaea.metadataportal.config.*;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import javax.xml.transform.stream.StreamSource;
import java.util.zip.*;

/**
 * Harvester for unzipping ZIP files and reading their contents. Identifiers look like: &quot;zip:&lt;identifierPrefix&gt;&lt;entryFilename&gt;&quot;
 * <p>This harvester supports the following additional <b>harvester properties</b>:<ul>
 * <li><code>zipFile</code>: filename or URL of ZIP file to harvest</li>
 * <li><code>identifierPrefix</code>: This prefix is appended before all identifiers (that are the identifiers of the documents) (default: "")</li>
 * <li><code>filenameFilter</code>: regex to match the entry filename (default: none)</li>
 * </ul>
 * TODO: Check date stamp of ZIP file / ZIP URL directly and stop harvesting if older. Currently files are filtered by date-stamp in ZipEntry.
 * @author Uwe Schindler
 */
public class ZipFileHarvester extends SingleFileEntitiesHarvester {

	// Class members
	private String zipFile=null;
	private Pattern filenameFilter=null;
	private String identifierPrefix="";

	@Override
	public void open(SingleIndexConfig iconfig) throws Exception {
		super.open(iconfig);

		zipFile=iconfig.harvesterProperties.getProperty("zipFile");
		if (zipFile==null) throw new IllegalArgumentException("Missing name / URL of ZIP file to harvest (property \"zipFile\")");
		zipFile=zipFile.trim();
		try {
			new URL(zipFile); // just test
		} catch (MalformedURLException urle) {
			zipFile=iconfig.parent.makePathAbsolute(zipFile);
		}

		identifierPrefix=iconfig.harvesterProperties.getProperty("identifierPrefix","").trim();

		String s=iconfig.harvesterProperties.getProperty("filenameFilter");
		filenameFilter=(s==null) ? null : Pattern.compile(s);
	}

	@Override
	public void harvest() throws Exception {
		log.info("Harvesting contents of ZIP file '"+zipFile+"'...");
		InputStream is=null;
		ZipInputStream zis=null;
		try {
			try {
				is=new URL(zipFile).openStream();
			} catch (MalformedURLException urle) {
				is=new FileInputStream(zipFile);
			}
			zis=new ZipInputStream(is);
			ZipEntry ze=null;
			int count=0;
			while ((ze=zis.getNextEntry())!=null) try {
				count++;
				if (ze.isDirectory()) continue;
				if (filenameFilter!=null) {
					String name=ze.getName();
					int p=name.lastIndexOf('/');
					if (p>=0) name=name.substring(p+1);
					Matcher m=filenameFilter.matcher(name);
					if (!m.matches()) continue;
				} 
				log.debug("Processing ZipEntry: "+ze);
				processFile(new NoCloseInputStream(zis),ze);
			} finally {
				zis.closeEntry();
			}
			if (count<=0) throw new ZipException("The file seems to be no ZIP file, it contains no file entries.");
		} finally {
			if (zis!=null) zis.close();
			if (is!=null) is.close();
		}
		log.info("Finished reading contents of ZIP file '"+zipFile+"'.");
	}

	@Override
	protected void enumerateValidHarvesterPropertyNames(Set<String> props) {
		super.enumerateValidHarvesterPropertyNames(props);
		props.addAll(Arrays.<String>asList(
			"zipFile",
			"identifierPrefix",
			"filenameFilter"
		));
	}

	private void processFile(InputStream is, ZipEntry ze) throws Exception {
		String identifier="zip:"+identifierPrefix+ze.getName();
		addDocument(identifier,ze.getTime(),new StreamSource(is));
	}
	
	private static final class NoCloseInputStream extends FilterInputStream {
		
		public NoCloseInputStream(InputStream is) {
			super(is);
		}
		
		@Override
		public void close() throws IOException {
			// ignore close request
		}
	
	}

}