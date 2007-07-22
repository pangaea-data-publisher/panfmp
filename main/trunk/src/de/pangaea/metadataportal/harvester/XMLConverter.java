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
import de.pangaea.metadataportal.config.*;
import javax.xml.transform.*;
import javax.xml.transform.sax.*;
import javax.xml.transform.dom.*;
import javax.xml.validation.*;
import org.w3c.dom.Document;
import org.xml.sax.*;
import java.io.IOException;

/*
This class handles the transformation from any source to the "official" metadata format and can even validate it
*/

public class XMLConverter  {

    private static org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(XMLConverter.class);

    private SingleIndexConfig iconfig;
    private boolean validate=true;

    public XMLConverter(SingleIndexConfig iconfig) {
        this.iconfig=iconfig;
        String v=iconfig.harvesterProperties.getProperty("validate");
        if (iconfig.parent.schema==null) {
            if (v!=null) throw new IllegalStateException("The <validate> harvester property is only allowed if a XML schema is set in the metadata properties!");
            validate=false; // no validation if no schema available
        } else {
            if (v==null) validate=true; // validate by default
            else validate=Boolean.parseBoolean(v);
        }
    }

    private DOMResult validate(final DOMSource ds, final boolean wasTransformed) throws SAXException,IOException {
        if (!validate) {
            return DOMSource2Result(ds);
        } else {
            if (log.isDebugEnabled()) log.debug("Validating '"+ds.getSystemId()+"'...");
            Validator val=iconfig.parent.schema.newValidator();
            val.setErrorHandler(new ErrorHandler() {
                public void warning(SAXParseException e) throws SAXException {
                    log.warn("Validation warning in "+(wasTransformed?"XSL transformed ":"")+"document '"+ds.getSystemId()+"': "+e.getMessage());
                }

                public void error(SAXParseException e) throws SAXException {
                    String msg="Validation error in "+(wasTransformed?"XSL transformed ":"")+"document '"+ds.getSystemId()+"': "+e.getMessage();
                    if (iconfig.parent.haltOnSchemaError) throw new SAXException(msg);
                    log.error(msg);
                }

                public void fatalError(SAXParseException e) throws SAXException {
                    throw new SAXException("Fatal validation error in "+(wasTransformed?"XSL transformed ":"")+"document '"+ds.getSystemId()+"': "+e.getMessage());
                }
            });
            DOMResult dr=emptyDOMResult(ds.getSystemId());
            val.validate(ds,dr);
            return dr;
        }
    }

    public static DOMSource DOMResult2Source(DOMResult dr) {
        return new DOMSource(dr.getNode(),dr.getSystemId());
    }

    public static DOMResult DOMSource2Result(DOMSource ds) {
        return new DOMResult(ds.getNode(),ds.getSystemId());
    }

    public static DOMResult emptyDOMResult(String systemId) {
        return new DOMResult(StaticFactories.dombuilder.newDocument(),systemId);
    }

    // Transforms a Source to a DOM w/wo transformation
    public Document transform(Source s) throws TransformerException,SAXException,IOException {
        DOMResult dr;
        if (iconfig.xslt==null && s instanceof DOMSource) {
            dr=DOMSource2Result((DOMSource)s);
        } else {
            if (log.isDebugEnabled()) log.debug("XSL-Transforming '"+s.getSystemId()+"'...");
            Transformer trans=(iconfig.xslt==null) ? StaticFactories.transFactory.newTransformer() : iconfig.xslt.newTransformer();
            trans.setErrorListener(new LoggingErrorListener(getClass()));
            dr=emptyDOMResult(s.getSystemId());
            trans.transform(s,dr);
        }
        Document dom=(Document)(validate(DOMResult2Source(dr), iconfig.xslt!=null).getNode());
        dom.normalize();
        return dom;
    }

    // ContentHandler part (gets events and converts it to DOM w/wo transformation)
    private DOMResult dr=null;

    public ContentHandler getTransformContentHandler(String systemId) throws TransformerException {
        if (dr!=null) throw new IllegalStateException("XMLConverter is currently convertig a SAX document, you cannot get a new ContentHandler!");

        if (iconfig.xslt!=null && log.isDebugEnabled()) log.debug("XSL-Transforming '"+systemId+"'...");

        TransformerHandler handler=(iconfig.xslt==null)?StaticFactories.transFactory.newTransformerHandler():StaticFactories.transFactory.newTransformerHandler(iconfig.xslt);
        handler.getTransformer().setErrorListener(new LoggingErrorListener(getClass()));
        dr=emptyDOMResult(systemId);
        handler.setResult(dr);
        return handler;
    }

    public Document finishTransformation() throws TransformerException,SAXException,IOException {
        if (dr==null) throw new IllegalStateException("XMLConverter is not convertig a SAX document, you cannot get a result DOM tree!");

        Document dom=(Document)(validate(DOMResult2Source(dr), iconfig.xslt!=null).getNode());
        dom.normalize();
        dr=null;
        return dom;
    }

}