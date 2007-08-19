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

import de.pangaea.metadataportal.config.*;
import de.pangaea.metadataportal.utils.*;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import java.io.IOException;
import java.util.*;

/**
 * This class holds one search result item.
 * @author Uwe Schindler
 */
public class SearchResultItem {

    private static org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(SearchResultItem.class);

    /**
     * Internal use only!
     */
    protected SearchResultItem(Config config, float score, Document doc) {
        this.config=config;
        this.score=score;
        this.doc=doc;
    }

    /**
     * Gets score of item (<code>0.0&lt;score&lt;=1.0</code>).
     */
    public float getScore() {
        return score;
    }

    /**
     * Returns the XML source code as String. Can be fed into a XML parser via {@link java.io.StringReader}.
     */
    public String getXml() {
        return doc.get(IndexConstants.FIELDNAME_XML);
    }

    /**
     * Returns the identifier of the search result in the index.
     */
    public String getIdentifier() {
        return doc.get(IndexConstants.FIELDNAME_IDENTIFIER);
    }

    /**
     * Returns the values of the supplied field name.
     * @param fieldName name of field
     * @return an array of {@link String}, {@link Double}, or {@link Date} values depending on the data type
     * ({@link de.pangaea.metadataportal.config.FieldConfig.DataType}) in field configuration. Returns <code>null</code>
     * if the document does not contain a value for this field.
     * @throws IllegalFieldConfigException if <code>fieldName</code> is not a stored field
     */
    public synchronized Object[] getField(String fieldName) {
        FieldConfig f=config.fields.get(fieldName);
        if (f==null) throw new IllegalFieldConfigException("Field name '"+fieldName+"' is unknown!");
        if (f.lucenestorage==Field.Store.NO) throw new IllegalFieldConfigException("Field '"+fieldName+"' is not stored!");

        if (fieldCache!=null) return fieldCache.get(fieldName);
        else return getField(f);
    }

    /**
     * Returns a mapping of <b>all</b> field names to values.
     * @return a {@link Map} mapping field names to
     * arrays of {@link String}, {@link Double}, or {@link Date} values depending on the data type
     * ({@link de.pangaea.metadataportal.config.FieldConfig.DataType}) in field configuration
     */
    public synchronized java.util.Map<String,Object[]> getFields() {
        if (fieldCache!=null) return Collections.unmodifiableMap(fieldCache);

        fieldCache=new HashMap<String,Object[]>();
        for (FieldConfig f : config.fields.values()) if (f.lucenestorage!=Field.Store.NO) {
            Object[] vals=getField(f);
            if (vals!=null) fieldCache.put(f.name,vals);
        }
        return Collections.unmodifiableMap(fieldCache);
    }

    private Object[] getField(FieldConfig f) {
        String[] data=doc.getValues(f.name);
        if (data!=null) {
            java.util.ArrayList<Object> vals=new java.util.ArrayList<Object>(data.length);
            for (String val : data) try {
                switch(f.datatype) {
                    case TOKENIZEDTEXT:
                    case STRING:
                    case XML:
                    case XHTML:
                        vals.add(val); break;
                    case NUMBER:
                        vals.add(new Double(LuceneConversions.luceneToDouble(val))); break;
                    case DATETIME:
                        vals.add(LuceneConversions.luceneToDate(val)); break;
                }
            } catch (NumberFormatException ex) {
                // ignore the field if conversion exception
                log.warn(ex);
            }
            return (vals.size()>0) ? vals.toArray() : null;
        } else return null;
    }

    // information
    private float score;
    private Document doc;
    private Map<String,Object[]> fieldCache=null;
    protected Config config;
}