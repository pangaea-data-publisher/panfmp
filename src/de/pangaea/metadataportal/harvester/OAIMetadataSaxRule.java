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
import org.xml.sax.ContentHandler;

/***
This class is used as a rule for the "metadata" element of the OAI response.
Whenever this element occurs in digester, begin/end will be called, that then puts all further SAX events to the specified ContentHandler
***/

public class OAIMetadataSaxRule extends de.pangaea.metadataportal.utils.SaxRule {

    private MetadataDocument doc=null;
    private XMLConverter trans=null;

    public OAIMetadataSaxRule(XMLConverter trans) {
        super();
        this.trans=trans;
    }

    // Digester rule part

    @Override
    public void begin(java.lang.String namespace, java.lang.String name, org.xml.sax.Attributes attributes) throws Exception {
        doc=(MetadataDocument)digester.peek(); // the MetadataDocument is on the stack!!!
        ContentHandler handler=trans.getTransformContentHandler(doc.getIdentifier());
        setExcludeNamespaces(java.util.Collections.singleton(OAIHarvester.OAI_NS));
        setContentHandler(handler);
        super.begin(namespace,name,attributes);
    }

    @Override
    public void end(java.lang.String namespace, java.lang.String name) throws Exception {
        super.end(namespace,name);
        doc.setDOM(trans.finishTransformation());
        doc=null;
    }

}