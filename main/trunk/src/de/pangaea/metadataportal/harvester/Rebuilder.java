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
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.store.FSDirectory;
import java.util.*;

public class Rebuilder extends AbstractHarvester {

    private static org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(Rebuilder.class);

    // harvester interface
    private IndexReader reader=null;

    public void open(SingleIndexConfig iconfig) throws Exception {
        log.info("Opening index \""+iconfig.id+"\" for harvesting all documents...");
        reader = iconfig.getUncachedIndexReader();
        super.open(iconfig);
    }

    public void close() throws Exception {
        if (reader!=null) reader.close();
        reader=null;
        super.close();
    }

    public void harvest() throws Exception {
        if (reader==null) throw new IllegalStateException("Rebuilder was not opened!");
        for (int i=0, c=reader.maxDoc(); i<c; i++) {
            if (!reader.isDeleted(i)) addDocument(MetadataDocument.createInstanceFromLucene(iconfig,reader.document(i)));
        }
    }

    // main-Methode
    public static void main (String[] args) {
        if (args.length!=2) {
            System.err.println("Command line: java "+Rebuilder.class.getName()+" config.xml index-name|*");
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
                SingleIndexConfig siconf=(SingleIndexConfig)iconf;

                log.info("Rebuilding index \""+siconf.id+"\"...");
                AbstractHarvester h=new Rebuilder();
                try {
                    h.open(siconf);
                    h.harvest();
                } catch (org.xml.sax.SAXParseException saxe) {
                    log.fatal("Harvesting documents into index \""+siconf.id+"\" failed due to SAX parse error in \""+saxe.getSystemId()+"\", line "+saxe.getLineNumber()+", column "+saxe.getColumnNumber()+".",saxe);
                } catch (Exception e) {
                    if (e.getCause() instanceof org.xml.sax.SAXParseException) {
                        org.xml.sax.SAXParseException saxe=(org.xml.sax.SAXParseException)e.getCause();
                        log.fatal("Harvesting documents into index \""+siconf.id+"\" failed due to SAX parse error in \""+saxe.getSystemId()+"\", line "+saxe.getLineNumber()+", column "+saxe.getColumnNumber()+".",saxe);
                    } else log.fatal("Harvesting documents into index \""+siconf.id+"\" failed!",e);
                } finally {
                    h.close();
                }
                log.info("Finished rebuilding of index \""+siconf.id+"\".");
            }
        } catch (Exception e) {
            log.fatal("Exception during index rebuild.",e);
        }
    }
}