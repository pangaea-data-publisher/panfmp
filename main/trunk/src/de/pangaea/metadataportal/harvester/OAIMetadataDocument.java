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
import de.pangaea.metadataportal.config.SingleIndexConfig;
import java.util.HashSet;
import org.apache.lucene.document.*;

/**
 * Special implementation of {@link MetadataDocument} that adds OAI set support to internal fields
 */
public class OAIMetadataDocument extends MetadataDocument {

    @PublicForDigesterUse
    public void setHeaderInfo(String status, String identifier, String datestampStr) throws java.text.ParseException {
        this.deleted=(status!=null && status.equals("deleted"));
        this.identifier=identifier;
        this.datestamp=ISODateFormatter.parseDate(datestampStr);
    }

    public void addSet(String set) {
        sets.add(set);
    }

    @Override
    public void loadFromLucene(SingleIndexConfig iconf, Document ldoc) throws Exception {
        sets.clear();
        super.loadFromLucene(iconf,ldoc);
        String[] sets=ldoc.getValues(IndexConstants.FIELDNAME_SET);
        if (sets!=null) for (String set : sets) if (set!=null) addSet(set);
    }

    @Override
    protected Document createEmptyDocument() throws Exception {
        Document ldoc=super.createEmptyDocument();
        if (ldoc!=null) {
            for (String set : sets) ldoc.add(new Field(IndexConstants.FIELDNAME_SET, set, Field.Store.YES, Field.Index.UN_TOKENIZED));
        }
        return ldoc;
    }

    @Override
    public String toString() {
        return super.toString()+" sets="+sets;
    }

    public HashSet<String> sets=new HashSet<String>();

}
