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
import org.apache.lucene.store.FSDirectory;
import java.util.*;
import java.io.PrintStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Index optimizer. To be called from command line.
 * @author Uwe Schindler
 */
public class Checker {

	protected static Log log = LogFactory.getLog(Checker.class);

	private Checker() {}

	// main-Methode
	public static void main (String[] args) {
		if (args.length<2 || args.length>3) {
			System.err.println("Command line: java "+Checker.class.getName()+" config.xml index-name|* [-fix]");
			return;
		}

		try {
			boolean fix;
			if (args.length==3) {
				if (!"-fix".equals(args[2])) {
					System.err.println("Third parameter must be either '-fix' or missing!");
					return;
				}
				fix=true;
			} else fix=false;

			Config conf=new Config(args[0],Config.ConfigMode.HARVESTING);
			Collection<IndexConfig> indexList=null;
			if ("*".equals(args[1])) {
				indexList=conf.indexes.values();
			} else {
				IndexConfig iconf=conf.indexes.get(args[1]);
				if (iconf==null || !(iconf instanceof SingleIndexConfig)) throw new IllegalArgumentException("There is no index defined with id=\""+args[1]+"\"!");
				indexList=Collections.singletonList(iconf);
			}
			
			PrintStream oldCheckIndexStream=CheckIndex.out;
			try {
				CheckIndex.out=LogUtil.getInfoStream(LogFactory.getLog(CheckIndex.class));

				for (IndexConfig iconf : indexList) if (iconf instanceof SingleIndexConfig) {
					IndexWriter writer=null;
					SingleIndexConfig siconf=(SingleIndexConfig)iconf;
					try {
						// und los gehts
						log.info("Checking index \""+iconf.id+"\"...");
						FSDirectory dir=FSDirectory.getDirectory(siconf.getFullIndexPath());
						if (!siconf.isIndexAvailable()) throw new java.io.FileNotFoundException("Index directory with segments file does not exist: "+dir.toString());
						boolean result=CheckIndex.check(dir,fix);
						if (result)
							log.info("Finished checking of index \""+iconf.id+"\": Index is clean.");
						else 
							log.warn("Finished checking of index \""+iconf.id+"\": Index "+(fix?"was":"is")+" corrupt.");
					} catch (java.io.IOException e) {
						log.fatal("Exception during index checking.",e);
					} finally {
						if (writer!=null) writer.close();
					}
				}
			} finally {
				CheckIndex.out=oldCheckIndexStream;
			}
		} catch (Exception e) {
			log.fatal("Checker general error:",e);
		}
	}

}