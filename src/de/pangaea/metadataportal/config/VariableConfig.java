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
import javax.xml.XMLConstants;
import javax.xml.namespace.QName;

public class VariableConfig extends AnyExpressionConfig {

    public void setName(ExtendedDigester dig, String nameStr) {
        if ("".equals(nameStr)) return; // Exception throws the Config.addVariable() method
        // current namespace context with strict=true (display errors when namespace declaration is missing [non-standard!])
        // and with possibly declared default namespace is redefined/deleted to "" (according to XSLT specification,
        // where this is also mandatory).
        this.name=QNameParser.parseLexicalQName(nameStr,dig.getCurrentNamespaceContext(true,true));
    }

    public String toString() {
        return new StringBuilder(name.toString()).append(" (").append(super.toString()).append(')').toString();
    }

    // members "the configuration"
    public QName name=null;
}

