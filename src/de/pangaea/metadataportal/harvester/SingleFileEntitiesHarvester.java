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

import java.util.*;
import de.pangaea.metadataportal.config.SingleIndexConfig;
import de.pangaea.metadataportal.utils.*;
import javax.xml.transform.Source;

/**
 * Abstract harvester class for single file entities (like files from web page or from a local directory). The harvester makes it possible
 * to add XML documents given by a {@link Source} to the index. These are harvested, but if an fatal parse error occurs, the harvester will then stop harvesting
 * (like it would be with OAI-PMH), ignore the document, or delete it (if existent in index) depending on the harvester property "parseErrorAction".
 * <p>This panFMP harvester supports the following <b>harvester properties</b> in adidition to the default ones:<ul>
 * <li><code>parseErrorAction</code>: What to do if a parse error occurs?
 *    Can be <code>FATALEXIT</code>, <code>IGNOREDOCUMENT</code>, <code>DELETEDOCUMENT</code> (default is to ignore the document)</li>
 * <li><code>deleteMissingDocuments</code>: remove documents after harvesting that were deleted from source (maybe a heavy operation). (default: true)</li>
 * </ul>
 * @author Uwe Schindler
 */
public abstract class SingleFileEntitiesHarvester extends Harvester {

	/** 
	 * Enumeration that specifies what action should be taken on a parse error. 
	 * @see #parseErrorAction
	 */
	public static enum ParseErrorAction { STOP, IGNOREDOCUMENT, DELETEDOCUMENT };
	
	private ParseErrorAction parseErrorAction=ParseErrorAction.IGNOREDOCUMENT;
	private Set<String> validIdentifiers=null;
	private long newestDatestamp=-1;

	@Override
	public void open(SingleIndexConfig iconfig) throws Exception {
		super.open(iconfig);
		
		String s=iconfig.harvesterProperties.getProperty("parseErrorAction");
		if (s!=null) try {
			parseErrorAction=ParseErrorAction.valueOf(s.toUpperCase());
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException("Invalid value '"+s+"' for harvester property 'parseErrorAction', valid ones are: "+Arrays.asList(ParseErrorAction.values()).toString());
		}

		validIdentifiers=null;
		if (BooleanParser.parseBoolean(iconfig.harvesterProperties.getProperty("deleteMissingDocuments","true"))) validIdentifiers=new HashSet<String>();
	}

	@Override
	public void close(boolean cleanShutdown) throws Exception {
		if (cleanShutdown) {
			index.setValidIdentifiers(validIdentifiers);
		}
		super.close(cleanShutdown);
	}
	
	/**
	 * Adds a document to the {@link #index} working in the background. If a parsing error occurs the document is handled according to {@link #parseErrorAction}.
	 * It is also added to the valid identifiers (if unseen documents should be deleted).
	 * @param identifier is the document's identifier in the index
	 * @param lastModified is the last-modification date which is used to calculate the next harvesting start date. If document is older that the last harvesting, it is skipped.
	 * @param xml is the transformer source of the document, <code>null</code> to only update document status (lastModified) and adding to valid identifiers
	 * @see #addDocument(MetadataDocument)
	 */
	protected void addDocument(String identifier, Date lastModified, Source xml) throws Exception {
		addDocument(identifier, (lastModified==null)?-1L:lastModified.getTime(), xml);
	}
	
	/**
	 * Adds a document to the {@link #index} working in the background.
	 * @see #addDocument(String,Date,Source)
	 */
	protected void addDocument(String identifier, long lastModified, Source xml) throws Exception {
		if (validIdentifiers!=null) validIdentifiers.add(identifier);
		
		if (lastModified>0L) {
			if (newestDatestamp<=0L || newestDatestamp<lastModified) setHarvestingDateReference(new Date((newestDatestamp=lastModified)+1L));
			if (fromDateReference!=null && fromDateReference.getTime()>lastModified) return;
		}
		
		if (xml==null) return;
		
		MetadataDocument mdoc=new MetadataDocument();
		mdoc.setIdentifier(identifier);
		mdoc.setDatestamp(new java.util.Date(lastModified));
		
		Exception e=null; String errstr=null;
		try {
			mdoc.setDOM(xmlConverter.transform(xml));
		} catch (org.xml.sax.SAXParseException saxe) {
			e=saxe;
			errstr="Harvesting object '"+identifier+"' failed due to SAX parse error in \""+saxe.getSystemId()+"\", line "+saxe.getLineNumber()+", column "+saxe.getColumnNumber();
		} catch (javax.xml.transform.TransformerException transfe) {
			e=transfe;
			String loc=transfe.getLocationAsString();
			errstr="Harvesting object '"+identifier+"' failed due to transformer/parse error"+((loc!=null)?(" at "+loc):"");
		}
		
		// handle exception
		if (e!=null && errstr!=null) switch (parseErrorAction) {
			case IGNOREDOCUMENT: 
				log.error(errstr+" (object ignored):",e);
				return;
			case DELETEDOCUMENT:
				log.error(errstr+" (object marked deleted):",e);
				mdoc.setDOM(null);
				mdoc.deleted=true;
				break; // continue normal
			default:
				throw e;
		}

		addDocument(mdoc);
	}

	@Override
	protected void enumerateValidHarvesterPropertyNames(Set<String> props) {
		super.enumerateValidHarvesterPropertyNames(props);
		props.addAll(Arrays.<String>asList(
			"parseErrorAction",
			"deleteMissingDocuments"
		));
	}

}