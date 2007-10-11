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

import de.pangaea.metadataportal.config.*;
import de.pangaea.metadataportal.utils.*;
import org.apache.lucene.index.*;
import org.apache.lucene.store.FSDirectory;
import java.util.*;

/**
 * Index optimizer. To be called from command line.
 * @author Uwe Schindler
 */
public class Optimizer {

	protected static org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(Optimizer.class);

	private Optimizer() {}

	// main-Methode
	public static void main (String[] args) {
		if (args.length!=2) {
			System.err.println("Command line: java "+Optimizer.class.getName()+" config.xml index-name|*");
			return;
		}

		try {
			Config conf=new Config(args[0],Config.ConfigMode.HARVESTING);
			Collection<IndexConfig> indexList=null;
			if ("*".equals(args[1])) {
				indexList=conf.indexes.values();
			} else {
				IndexConfig iconf=conf.indexes.get(args[1]);
				if (iconf==null || !(iconf instanceof SingleIndexConfig)) throw new IllegalArgumentException("There is no index defined with id=\""+args[1]+"\"!");
				indexList=Collections.singletonList(iconf);
			}

			for (IndexConfig iconf : indexList) if (iconf instanceof SingleIndexConfig) {
				IndexWriter writer=null;
				SingleIndexConfig siconf=(SingleIndexConfig)iconf;
				try {
					// und los gehts
					log.info("Opening index \""+iconf.id+"\" for optimizing...");
					FSDirectory dir=FSDirectory.getDirectory(siconf.getFullIndexPath());
					if (!siconf.isIndexAvailable()) throw new java.io.FileNotFoundException("Index directory with segments file does not exist: "+dir.toString());
					writer = new IndexWriter(dir, conf.getAnalyzer(), false);
					log.info("Optimizing...");
					writer.optimize();
					log.info("Finished index optimizing of index \""+iconf.id+"\".");
				} catch (java.io.IOException e) {
					log.fatal("Exception during index optimization.",e);
				} finally {
					if (writer!=null) writer.close();
				}
			}
		} catch (Exception e) {
			log.fatal("Optimizer general error:",e);
		}
	}

}