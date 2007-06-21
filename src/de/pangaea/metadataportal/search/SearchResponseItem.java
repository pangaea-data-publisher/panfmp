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

public class SearchResponseItem implements java.io.Serializable {
    /**
     * @return
     */
    public float getScore() {
        return score;
    }

    /**
     * @return
     */
    public String getXml() {
        return xml;
    }

    /**
     * @return
     */
    public String getIdentifier() {
        return identifier;
    }

    /**
     * @return
     */
    public java.util.Map<String,Object[]> getFields() {
        return fields;
    }

    // information
    protected float score=0.0f;
    protected String xml=null,identifier=null;
    protected java.util.Map<String,Object[]> fields=new java.util.HashMap<String,Object[]>();
}