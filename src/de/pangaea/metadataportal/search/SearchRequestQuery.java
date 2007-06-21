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

import de.pangaea.metadataportal.utils.*;
import de.pangaea.metadataportal.config.Config;
import de.pangaea.metadataportal.config.Config.Config_Field;

public class SearchRequestQuery implements java.io.Serializable {

    // default for axis
    public SearchRequestQuery() {
    }

    // simple easy-to-use constructor
    public SearchRequestQuery(String field, String query) {
        this.fieldName=field.intern();
        this.query=query;
        this.anyof=false;
    }

    public SearchRequestQuery(String field, String query, boolean anyof) {
        this.fieldName=field.intern();
        this.query=query;
        this.anyof=anyof;
    }

    public void setField(String v) { fieldName=v.intern(); }
    public void setQuery(String v) { query=v; }
    public void setAnyOf(boolean v) { anyof=v; }

    // cast the min max values to correct types
    protected void normalize(Config config) {
        if (fieldName==null) {
            fieldName=IndexConstants.FIELDNAME_CONTENT;
            isStringField=new Boolean(false);
            if (query==null) throw new IllegalArgumentException("A query string must be given for default field!");
        } else {
            Config_Field f=config.fields.get(fieldName);
            if (f==null) throw new IllegalArgumentException("Field name '"+fieldName+"' is unknown!");
            if (!f.luceneindexed) throw new IllegalArgumentException("Field '"+fieldName+"' is not searchable!");
            if (f.datatype!=Config.DataType.tokenizedText && f.datatype!=Config.DataType.string)
                throw new IllegalArgumentException("Field '"+fieldName+"' is not of type string or tokenizedText!");
            isStringField=new Boolean(f.datatype==Config.DataType.string);
            if (query==null) throw new IllegalArgumentException("A query string must be given for field '"+fieldName+"'!");
        }
    }

    // hashing & compare & string representation
    public boolean equals(Object o) {
        if (o!=null && o instanceof SearchRequestQuery) {
            SearchRequestQuery s=(SearchRequestQuery)o;
            boolean ok=true;
            ok&=(fieldName==s.fieldName);
            ok&=(query==s.query || ((s.query!=null) && s.query.equals(query)));
            ok&=(anyof==s.anyof);
            return ok;
        } else return false;
    }

    public int hashCode() {
        int hashCode=0;
        if (fieldName!=null) hashCode^=fieldName.hashCode();
        if (query!=null) hashCode^=query.hashCode();
        hashCode^=new Boolean(anyof).hashCode();
        return hashCode;
    }

    public String toString() {
        return ((fieldName==null || fieldName==IndexConstants.FIELDNAME_CONTENT)?("'"+query+"'"):(fieldName+"='"+query+"'"))+(anyof?"(anyOf)":"");
    }

    // members
    protected String fieldName=null,query=null;
    protected boolean anyof=false;
    protected Boolean isStringField=null;

}