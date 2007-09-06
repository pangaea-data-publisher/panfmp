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

package de.pangaea.metadataportal.harvester;

import de.pangaea.metadataportal.utils.*;
import de.pangaea.metadataportal.config.*;
import java.io.*;
import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import javax.xml.transform.stream.StreamSource;

public class DirectoryHarvester extends Harvester implements FilenameFilter {

	// Class members
	private File directory=null;
	private boolean recursive=false;
	private Pattern filenameFilter=null;
	private String identifierPrefix="";

	private Set<String> validIdentifiers=null;

	@Override
	public void open(SingleIndexConfig iconfig) throws Exception {
		super.open(iconfig);

		String s=iconfig.harvesterProperties.getProperty("directory");
		if (s==null) throw new IllegalArgumentException("Missing directory name to start harvesting (property \"directory\")");

		directory=new File(iconfig.parent.makePathAbsolute(s));
		recursive=BooleanParser.parseBoolean(iconfig.harvesterProperties.getProperty("recursive","false"));
		identifierPrefix=iconfig.harvesterProperties.getProperty("identifierPrefix","").trim();

		s=iconfig.harvesterProperties.getProperty("filenameFilter");
		filenameFilter=(s==null) ? null : Pattern.compile(s);

		validIdentifiers=null;
		if (BooleanParser.parseBoolean(iconfig.harvesterProperties.getProperty("deleteMissingDocuments","true"))) validIdentifiers=new HashSet<String>();
	}

	@Override
	public void harvest() throws Exception {
		if (index==null) throw new IllegalStateException("Index not yet opened");

		java.util.Date startDate=new java.util.Date(); // store reference date of first harvesting step, to be set at end

		processDirectory(directory);

		// set the this for next harvesting
		setValidIdentifiers(validIdentifiers);
		thisHarvestDateReference=startDate;
	}

	@Override
	public List<String> getValidHarvesterPropertyNames() {
		ArrayList<String> l=new ArrayList<String>(super.getValidHarvesterPropertyNames());
		l.addAll(Arrays.<String>asList(
			"directory",
			"recursive",
			"identifierPrefix",
			"filenameFilter",
			"deleteMissingDocuments"
		));
		return l;
	}

	public boolean accept(File dir, String name) {
		File file=new File(dir,name);
		if (file.isDirectory()) return (recursive && !".".equals(name) && !"..".equals(name));
		if (filenameFilter==null) return true;
		Matcher m=filenameFilter.matcher(name);
		return m.matches();
	}

	private void processFile(File file) throws Exception {
		String identifier="file:"+identifierPrefix+directory.toURI().normalize().relativize(file.toURI().normalize()).toString();
		if (validIdentifiers!=null) validIdentifiers.add(identifier);

		if (fromDateReference!=null && fromDateReference.getTime()>file.lastModified()) return;

		MetadataDocument mdoc=new MetadataDocument();
		mdoc.setIdentifier(identifier);
		mdoc.setDatestamp(new java.util.Date(file.lastModified()));
		mdoc.setDOM(xmlConverter.transform(new StreamSource(file)));
		addDocument(mdoc);
	}

	private void processDirectory(File dir) throws Exception {
		log.info("Walking into directory \""+dir+"\" (recursive="+recursive+",filter=\""+filenameFilter+"\")...");
		File[] files=dir.listFiles(this);
		if (files==null) return;
		for (File f : files) {
			if (f.isDirectory()) processDirectory(f);
			else if (f.isFile()) processFile(f);
		}
		log.info("Finished directory \""+dir+"\".");
	}

}