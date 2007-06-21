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
import javax.xml.transform.stream.StreamSource;
import javax.xml.transform.dom.DOMResult;
import java.io.StringReader;
import java.util.*;

public class Rebuilder {

    protected static org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(Rebuilder.class);

    private Rebuilder() {}

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
                indexList=conf.indices.values();
            } else {
                IndexConfig iconf=conf.indices.get(args[1]);
                if (iconf==null || !(iconf instanceof SingleIndexConfig)) throw new IllegalArgumentException("There is no index defined with id=\""+args[1]+"\"!");
                indexList=Collections.singletonList(iconf);
            }

            for (IndexConfig iconf : indexList) if (iconf instanceof SingleIndexConfig) {
                IndexBuilder builder=null;
                IndexReader reader=null;
                int count=0;
                SingleIndexConfig siconf=(SingleIndexConfig)iconf;
                try {
                    // und los gehts
                    log.info("Opening index \""+iconf.id+"\" for harvesting all documents...");
                    builder = new IndexBuilder(false,siconf);
                    reader = siconf.getIndexReader();
                    log.info("Harvesting documents...");
                    for (int i=0, c=reader.maxDoc(); i<c; i++) {
                        if (!reader.isDeleted(i)) {
                            MetadataDocument mdoc=new RebuilderMetadataDocument(reader.document(i));
                            builder.addDocument(mdoc);
                            count++;
                            if (count%1000==0) log.info(count+" documents harvested from index so far.");
                        }
                    }
                    log.info(count+" documents harvested from index and queued for reindexing.");
                } finally {
                    if (reader!=null) reader.close();
                    if (builder!=null) builder.close();
                }
                log.info("Finished index rebuilding.");
            }
        } catch (Exception e) {
            log.fatal("Exception during index rebuild.",e);
        }
    }

    public static final class RebuilderMetadataDocument extends MetadataDocument {

        public RebuilderMetadataDocument(Document ldoc) throws Exception {
            xml=ldoc.get(IndexConstants.FIELDNAME_XML);
            identifier=ldoc.get(IndexConstants.FIELDNAME_IDENTIFIER);
            try {
                String d=ldoc.get(IndexConstants.FIELDNAME_DATESTAMP);
                if (d!=null) datestamp=LuceneConversions.luceneToDate(d);
            } catch (NumberFormatException ne) {
                log.warn("Datestamp of document '"+identifier+"' is invalid. Deleting datestamp!",ne);
            }
            String[] sets=ldoc.getValues(IndexConstants.FIELDNAME_SET);
            if (sets!=null) for (String set : sets) if (set!=null) addSet(set);

            // build DOM tree for XPath
            dom=StaticFactories.dombuilder.newDocument();
            StreamSource s=new StreamSource(new StringReader(xml),identifier);
            DOMResult r=new DOMResult(dom,identifier);
            StaticFactories.transFactory.newTransformer().transform(s,r);
        }

        public String getXML() {
            return xml;
        }

        // cache!
        private String xml=null;

    }

}