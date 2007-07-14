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
import de.pangaea.metadataportal.config.*;

public class SearchRequestRange implements java.io.Serializable {

    // default for axis
    public SearchRequestRange() {
    }

    // simple easy-to-use constructor
    public SearchRequestRange(String field, Object min, Object max) {
        this.fieldName=field.intern();
        this.min=min;
        this.max=max;
    }

    public void setField(String v) { fieldName=v.intern(); }
    public void setMin(Object v) { min=v; }
    public void setMax(Object v) { max=v; }

    // cast the min max values to correct types
    protected void normalize(Config config) {
        if (fieldName==null) throw new IllegalArgumentException("Field name is missing!");
        FieldConfig f=config.fields.get(fieldName);
        if (f==null) throw new IllegalArgumentException("Field name '"+fieldName+"' is unknown!");
        if (!f.luceneindexed) throw new IllegalArgumentException("Field '"+fieldName+"' is not searchable!");
        if (min==null && max==null) throw new IllegalArgumentException("A min or max value must be given for field '"+fieldName+"'!");
        switch (f.datatype) {
            case NUMBER:
                try {
                    if (min!=null && !(min instanceof Number)) min=new Double(min.toString());
                    if (max!=null && !(max instanceof Number)) max=new Double(max.toString());
                } catch (NumberFormatException ne) {
                    throw new IllegalArgumentException("Field '"+fieldName+"' is not a correct number!");
                }
                break;
            case DATETIME:
                try {
                    if (min!=null) {
                        if (min instanceof java.util.Calendar) min=((java.util.Calendar)min).getTime();
                        if (!(min instanceof java.util.Date)) min=LenientDateParser.parseDate(min.toString());
                    }
                    if (max!=null) {
                        if (max instanceof java.util.Calendar) max=((java.util.Calendar)max).getTime();
                        if (!(max instanceof java.util.Date)) max=LenientDateParser.parseDate(max.toString());
                    }
                } catch (java.text.ParseException ne) {
                    throw new IllegalArgumentException("Field '"+fieldName+"' is not a correct dateTime!");
                }
                break;
            default:
                throw new IllegalArgumentException("Field '"+fieldName+"' is not of type number or dateTime!");
        }
    }

    // hashing & compare & string representation
    public final boolean equals(Object o) {
        if (o!=null && o instanceof SearchRequestRange) {
            SearchRequestRange s=(SearchRequestRange)o;
            boolean ok=true;
            ok&=(fieldName==s.fieldName);
            ok&=(min==s.min || ((s.min!=null) && s.min.equals(min)));
            ok&=(max==s.max || ((s.max!=null) && s.max.equals(max)));
            return ok;
        } else return false;
    }

    public final int hashCode() {
        int hashCode=0;
        if (fieldName!=null) hashCode^=fieldName.hashCode()^0x8774ab42;
        if (min!=null) hashCode^=min.hashCode()^0x0a456ff33;
        if (max!=null) hashCode^=max.hashCode()^0x0b5afd345;
        return hashCode;
    }

    public String toString() {
        return fieldName+"=["+min+".."+max+"]";
    }

    // members
    protected String fieldName=null;
    protected Object min=null,max=null;

}