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

package de.pangaea.metadataportal.harvester;

import de.pangaea.metadataportal.config.*;
import de.pangaea.metadataportal.utils.*;
import org.apache.lucene.index.*;
import java.util.*;
import java.io.PrintStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Index checker. To be called from command line.
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
			
			for (IndexConfig iconf : indexList) if (iconf instanceof SingleIndexConfig) {
				IndexWriter writer=null;
				SingleIndexConfig siconf=(SingleIndexConfig)iconf;
				try {
					// und los gehts
					log.info("Checking index \""+iconf.id+'"'+(fix?" with repairing errors":"")+"...");
					if (!siconf.isIndexAvailable()) throw new java.io.FileNotFoundException("Index directory with segments file does not exist.");
					CheckIndex checker=new CheckIndex(siconf.getIndexDirectory());
					PrintStream out=LogUtil.getInfoStream(LogFactory.getLog(CheckIndex.class));
					checker.setInfoStream(out);
					CheckIndex.Status result=checker.checkIndex();
					if (result!=null && result.clean) {
						log.info("Finished checking of index \""+iconf.id+"\": Index is clean.");
					} else {
						if (result==null) fix=false;
						log.warn("Finished checking of index \""+iconf.id+"\": Index is corrupt"+(fix?", fixing it":"")+'.');
						if (fix) {
							try {
								// remove the last harvested file, because corrupt index means missing documents
								siconf.getIndexDirectory().deleteFile(IndexConstants.FILENAME_LASTHARVESTED);
							} catch (java.io.IOException ioe) {}
							checker.fixIndex(result);
							log.info("Index \""+iconf.id+"\" was fixed.");
						}
					}
					checker.setInfoStream(null);
					out.close();
				} catch (java.io.IOException e) {
					log.fatal("Exception during index checking.",e);
				} finally {
					if (writer!=null) writer.close();
				}
			}
		} catch (Exception e) {
			log.fatal("Checker general error:",e);
		}
	}

}