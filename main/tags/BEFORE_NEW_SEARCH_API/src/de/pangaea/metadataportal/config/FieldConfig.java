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

package de.pangaea.metadataportal.config;

import de.pangaea.metadataportal.utils.*;

/**
 * Config element that contains the definition of a field. It contains its name and some
 * properties like name, datatype and Lucene indexing flags.
 * @author Uwe Schindler
 */
public class FieldConfig extends ExpressionConfig {

    public void setName(String v) {
        name=v;
    }

    @PublicForDigesterUse
    @Deprecated
    public void setDatatype(String v) {
        try {
            datatype=DataType.valueOf(v.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid value '"+v+"' for attribute datatype!");
        }
    }

    @PublicForDigesterUse
    @Deprecated
    public void setLucenestorage(String v) {
        lucenestorage=Boolean.parseBoolean(v);
    }

    @PublicForDigesterUse
    @Deprecated
    public void setLuceneindexed(String v) {
        luceneindexed=Boolean.parseBoolean(v);
    }

    public void setDefault(String v) {
        defaultValue=v;
    }

    @Override
    public String toString() {
        return name;
    }

    // members "the configuration"
    public String name=null;
    public String defaultValue=null;
    public DataType datatype=DataType.TOKENIZEDTEXT;
    public boolean lucenestorage=true;
    public boolean luceneindexed=true;

    public static enum DataType { TOKENIZEDTEXT,STRING,NUMBER,DATETIME };
}
