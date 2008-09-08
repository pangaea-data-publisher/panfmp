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
import de.pangaea.metadataportal.utils.*;
import org.apache.lucene.index.*;
import org.apache.lucene.document.*;
import java.util.*;

/**
 * Maintenance component for removing duplicate documents in index. Called from command line.
 * @author Uwe Schindler
 */
public class RemoveDuplicates {

	protected static org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(RemoveDuplicates.class);

	private RemoveDuplicates() {}

	protected static int removeDuplicates(IndexReader reader, Term t) throws java.io.IOException {
		ArrayList<Integer> list=new ArrayList<Integer>();
		TermDocs td=reader.termDocs(t);
		try {
			while (td.next()) {
				int docid=td.doc();
				log.debug("Found docid="+docid+" for identifier='"+t.text()+"'.");
				list.add(docid);
			}
		} finally {
			td.close();
		}

		int removed=0;
		if (list.size()>1) {
			log.warn("Found duplicate(s) of identifer '"+t.text()+"' - Keeping newest document...");
			Iterator<Integer> it=list.iterator();
			while (it.hasNext()) {
				int id=it.next();
				if (it.hasNext()) {
					reader.deleteDocument(id);
					log.debug("Removed duplicate docid="+id+" for identifier='"+t.text()+"'.");
					removed++;
				}
			}
			log.info("Removed "+removed+" duplicates of identifer '"+t.text()+"'.");
		}
		return removed;
	}

	// main-Methode
	public static void main (String[] args) {
		if (args.length<1 || args.length>2) {
			System.err.println("Command line: java "+RemoveDuplicates.class.getName()+" config.xml [index-name|*]");
			return;
		}

		try {
			Config conf=new Config(args[0],Config.ConfigMode.HARVESTING);
			Collection<IndexConfig> indexList=null;
			if (args.length==1 || "*".equals(args[1])) {
				indexList=conf.indexes.values();
			} else {
				IndexConfig iconf=conf.indexes.get(args[1]);
				if (iconf==null || !(iconf instanceof SingleIndexConfig)) throw new IllegalArgumentException("There is no index defined with id=\""+args[1]+"\"!");
				indexList=Collections.singletonList(iconf);
			}

			for (IndexConfig iconf : indexList) if (iconf instanceof SingleIndexConfig) {
				IndexReader reader=null;
				SingleIndexConfig siconf=(SingleIndexConfig)iconf;
				int foundDupl=0,foundDocs=0;
				try {
					// und los gehts
					log.info("Opening index \""+iconf.id+"\" for removing duplicate documents...");
					reader = siconf.getUncachedIndexReader();

					Term base=new Term(IndexConstants.FIELDNAME_IDENTIFIER,"");
					TermEnum terms=reader.terms(base);
					try {
						do {
							Term t=terms.term();
							if (t!=null && base.field()==t.field()) {
							    // check frequency
							    String identifier=t.text();
							    int count=terms.docFreq();
							    if (count>1) {
							        int c=removeDuplicates(reader,t);
							        foundDupl+=c;
							        if (c>0) foundDocs++;
							    }
							} else break;
						} while (terms.next());
					} finally {
						terms.close();
					}
					log.info("Finished removing duplicates. "+foundDupl+" duplicates of "+foundDocs+" documents removed!");
				} catch (java.io.IOException e) {
					log.fatal("Exception during removing duplicates:",e);
				} finally {
					if (reader!=null) reader.close();
				}
			}
		} catch (Exception e) {
			log.fatal("General error during removing duplicates:",e);
		}
	}
}