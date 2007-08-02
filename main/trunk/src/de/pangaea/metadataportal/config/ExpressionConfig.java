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
import javax.xml.xpath.*;
import javax.xml.transform.*;

/**
 * Generic XPath/XSLT config element. This class contains a XPath expression <b>OR</b> a XSLT Template.
 * @author Uwe Schindler
 */
public class ExpressionConfig {

    @PublicForDigesterUse
    @Deprecated
    public void setXPath(ExtendedDigester dig, String xpath) throws XPathExpressionException {
        if ("".equals(xpath)) return; // Exception throws the Config.addField() method
        XPath x=StaticFactories.xpathFactory.newXPath();
        x.setXPathFunctionResolver(de.pangaea.metadataportal.harvester.XPathResolverImpl.getInstance());
        x.setXPathVariableResolver(de.pangaea.metadataportal.harvester.XPathResolverImpl.getInstance());
        // current namespace context with strict=true (display errors when namespace declaration is missing [non-standard!])
        // and with possibly declared default namespace is redefined/deleted to "" (according to XSLT specification,
        // where this is also mandatory).
        x.setNamespaceContext(dig.getCurrentNamespaceContext(true,true));
        xPathExpr=x.compile(xpath);
        cachedXPath=xpath;
    }

    public void setTemplate(Templates xslt) {
        this.xslt=xslt;
    }

    @Override
    public String toString() {
        return (xPathExpr==null) ? "?template?" : cachedXPath;
    }

    public XPathExpression xPathExpr=null;
    public Templates xslt=null;
    private String cachedXPath=null;
}
