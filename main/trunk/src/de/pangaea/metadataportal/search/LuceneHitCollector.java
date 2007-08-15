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

package de.pangaea.metadataportal.search;

import de.pangaea.metadataportal.config.Config;
import org.apache.lucene.search.*;
import org.apache.lucene.document.*;
import java.io.IOException;
import java.util.*;

public final class LuceneHitCollector extends HitCollector {

    private static org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(LuceneHitCollector.class);

    protected LuceneHitCollector(int bufferSize, SearchResultCollector coll, Config config, Searcher searcher, FieldSelector fields) {
        if (bufferSize<=0) throw new IllegalArgumentException("Buffer must have a size >0");
        buffer=new Item[bufferSize];
        this.coll=coll;
        this.config=config;
        this.searcher=searcher;
        this.fields=fields;
    }

    protected synchronized void flushBuffer() {
        if (log.isDebugEnabled()) log.debug("Flushing buffer containing "+count+" search results...");
        try {
            // we do the buffer in index order which is less IO expensive!
            Arrays.sort(buffer,0,count);
            try {
                for (int i=0; i<count; i++) {
                    if (!coll.collect(new SearchResultItem(config, buffer[i].score, searcher.doc(buffer[i].doc, fields) ))) throw new StopException();
                }
            } finally {
                count=0;
            }
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public synchronized void collect(int doc, float score) {
        buffer[count++]=new Item(doc,score);
        if (count==buffer.length) flushBuffer();
    }

    private int count=0;
    private Item[] buffer;
    SearchResultCollector coll;
    private Config config;
    private Searcher searcher;
    private FieldSelector fields;

    protected static final class StopException extends RuntimeException {
        protected StopException() {
            super();
        }
    }

    private static final class Item implements Comparable<Item> {
        protected int doc;
        protected float score;

        protected Item(int doc, float score) {
            this.doc=doc;
            this.score=score;
        }

        public int compareTo(Item o) {
            return Integer.valueOf(doc).compareTo(Integer.valueOf(o.doc));
        }
    }

}