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
		try {
			boolean fix=false;
			String configFile=null,indexId=null;
			for (String arg : args) {
				if ("-fix".equals(arg)) fix=true;
				else if (configFile==null) configFile=arg;
				else if (indexId==null) indexId=arg;
				else {
					System.err.println("Command line: java "+Checker.class.getName()+" [-fix] config.xml [index-name|*]");
					return;
				}
			}

			Config conf=new Config(configFile,Config.ConfigMode.HARVESTING);
			Collection<IndexConfig> indexList=null;
			if (indexId==null || "*".equals(indexId)) {
				indexList=conf.indexes.values();
			} else {
				IndexConfig iconf=conf.indexes.get(indexId);
				if (iconf==null || !(iconf instanceof SingleIndexConfig)) throw new IllegalArgumentException("There is no index defined with id=\""+indexId+"\"!");
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
						log.info("Checking index \""+iconf.id+'"'+(fix?" with repairing errors":"")+"...");
						if (!siconf.isIndexAvailable()) throw new java.io.FileNotFoundException("Index directory with segments file does not exist.");
						CheckIndexStatus result=CheckIndex.check(siconf.getIndexDirectory(),false);
						if (result.clean) {
							log.info("Finished checking of index \""+iconf.id+"\": Index is clean.");
						} else {
							log.warn("Finished checking of index \""+iconf.id+"\": Index is corrupt"+(fix?", fixing it":"")+'.');
							if (fix) {
								CheckIndex.fix(result);
								log.info("Index \""+iconf.id+"\" was fixed.");
							}
						}
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