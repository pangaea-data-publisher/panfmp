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
import java.io.StringWriter;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import javax.xml.transform.*;
import javax.xml.transform.dom.*;
import javax.xml.transform.stream.*;
import javax.xml.namespace.QName;
import org.apache.lucene.document.*;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.DocumentFragment;

public class MetadataDocument {
    private static org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(MetadataDocument.class);

    public MetadataDocument() {
    }

    public static final MetadataDocument createInstanceFromLucene(SingleIndexConfig iconf, Document ldoc) throws Exception {
        String mdocImpl=ldoc.get(IndexConstants.FIELDNAME_MDOC_IMPL);
        Class<? extends MetadataDocument> cls=MetadataDocument.class;
        if (mdocImpl==null) {
            // guess type for backwards compatibility (to be removed in future)
            if (ldoc.get(IndexConstants.FIELDNAME_SET)!=null) cls=OAIMetadataDocument.class;
        } else try {
            if (mdocImpl!=null) cls=Class.forName(mdocImpl).asSubclass(MetadataDocument.class);
        } catch (ClassNotFoundException cnfe) {
            throw new ClassNotFoundException("There exists no class "+mdocImpl+" for rebuilding index. "+
                "This error occurs if there was an incompatible change of panFMP. You have to reharvest from the original source and cannot rebuild your index!");
        }
        if (log.isDebugEnabled()) log.debug("Using MetadataDocument class: "+cls.getName());
        MetadataDocument mdoc=cls.newInstance();
        mdoc.loadFromLucene(iconf,ldoc);
        return mdoc;
    }

    public void loadFromLucene(SingleIndexConfig iconf, Document ldoc) throws Exception {
        deleted=false; datestamp=null;
        xmlCache=ldoc.get(IndexConstants.FIELDNAME_XML);
        identifier=ldoc.get(IndexConstants.FIELDNAME_IDENTIFIER);
        try {
            String d=ldoc.get(IndexConstants.FIELDNAME_DATESTAMP);
            if (d!=null) datestamp=LuceneConversions.luceneToDate(d);
        } catch (NumberFormatException ne) {
            log.warn("Datestamp of document '"+identifier+"' is invalid. Deleting datestamp!",ne);
        }

        if (identifier==null || xmlCache==null)
            throw new IllegalArgumentException("Tried to load incomplete document from index.");

        // build DOM tree for XPath
        dom=StaticFactories.dombuilder.newDocument();
        StreamSource s=new StreamSource(new StringReader(xmlCache),identifier);
        DOMResult r=new DOMResult(dom,identifier);
        Transformer trans=StaticFactories.transFactory.newTransformer();
        trans.setErrorListener(new LoggingErrorListener(getClass()));
        trans.transform(s,r);
    }

    public String getXML() throws Exception {
        if (deleted || dom==null) return null;
        if (xmlCache!=null) return xmlCache;

        // convert DOM
        StringWriter xmlWriter=new StringWriter();
        Transformer trans=StaticFactories.transFactory.newTransformer();
        trans.setErrorListener(new LoggingErrorListener(getClass()));
        trans.setOutputProperty(OutputKeys.INDENT,"no");
        trans.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION,"yes");
        DOMSource in=new DOMSource(dom,identifier);
        StreamResult out=new StreamResult(xmlWriter);
        trans.transform(in,out);
        xmlWriter.close();
        return xmlCache=xmlWriter.toString();
    }

    public void setDOM(org.w3c.dom.Document  dom) {
        this.dom=dom;
        xmlCache=null;
    }

    public org.w3c.dom.Document getDOM() {
        return dom;
    }

    public void setDeleted(boolean deleted) {
        this.deleted=deleted;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public void setDatestamp(java.util.Date datestamp) {
        this.datestamp=datestamp;
    }

    public java.util.Date getDatestamp() {
        return datestamp;
    }

    public void setIdentifier(String identifier) {
        this.identifier=identifier;
    }

    public String getIdentifier() {
        return identifier;
    }

    @Override
    public String toString() {
        return "identifier="+identifier+" deleted="+deleted+" datestamp="+((datestamp!=null)?ISODateFormatter.formatLong(datestamp):(String)null);
    }

    protected Document createEmptyDocument() throws Exception {
        if (identifier==null || "".equals(identifier))
            throw new IllegalArgumentException("The identifier of a document may not be empty!");

        // make a new, empty document
        if (deleted) {
            return null; // to delete
        } else {
            Document ldoc = new Document();
            ldoc.add(new Field(IndexConstants.FIELDNAME_IDENTIFIER, identifier, Field.Store.YES, Field.Index.UN_TOKENIZED));
            ldoc.add(new Field(IndexConstants.FIELDNAME_MDOC_IMPL, getClass().getName(), Field.Store.YES, Field.Index.NO));
            if (datestamp!=null) {
                String val;
                ldoc.add(new Field(IndexConstants.FIELDNAME_DATESTAMP, val=LuceneConversions.dateToLucene(datestamp), Field.Store.YES, Field.Index.UN_TOKENIZED));
                LuceneConversions.addTrieIndexEntries(ldoc,IndexConstants.FIELDNAME_DATESTAMP,val);
            }
            return ldoc;
        }
    }

    protected void addDefaultField(SingleIndexConfig iconfig, Document ldoc) throws Exception {
        StringBuilder sb=new StringBuilder();
        if (iconfig.parent.defaultField==null) {
            walkNodeTexts(sb, dom.getDocumentElement(),true);
        } else {
            NodeList nodes=(NodeList)iconfig.parent.defaultField.xPathExpr.evaluate(dom, javax.xml.xpath.XPathConstants.NODESET);
            for (int i=0,c=nodes.getLength(); i<c; i++) {
                walkNodeTexts(sb,nodes.item(i),true);
                sb.append('\n');
            }
        }
        if (log.isTraceEnabled()) log.trace("DefaultField: "+sb.toString());
        ldoc.add(new Field(IndexConstants.FIELDNAME_CONTENT, sb.toString(), Field.Store.NO, Field.Index.TOKENIZED));
    }

    protected void addFields(SingleIndexConfig iconfig, Document ldoc) throws Exception {
        for (FieldConfig f : iconfig.parent.fields.values()) {
            boolean needDefault=true;
            Object value=null;
            if (f.xPathExpr!=null) {
                try {
                    // First: try to get XPath result as Nodelist if that fails (because result is #STRING): fallback
                    // TODO: Looking for a better system to detect return type of XPath :-( [slowdown by this?]
                    value=f.xPathExpr.evaluate(dom, javax.xml.xpath.XPathConstants.NODESET);
                } catch (javax.xml.xpath.XPathExpressionException ex) {
                    // Fallback: if return type of XPath is a #STRING (for example from a substring() routine)
                    value=f.xPathExpr.evaluate(dom, javax.xml.xpath.XPathConstants.STRING);
                }
            } else if (f.xslt!=null) {
                value=evaluateTemplate(f);
            } else throw new NullPointerException("Both XPath and template are NULL for field "+f.name);

            // interpret result
            if (value instanceof NodeList) {
                NodeList nodes=(NodeList)value;
                for (int i=0,c=nodes.getLength(); i<c; i++) {
                    StringBuilder sb=new StringBuilder();
                    walkNodeTexts(sb,nodes.item(i),true);
                    String val=sb.toString().trim();
                    if (!"".equals(val)) {
                        internalAddField(ldoc,f,val);
                        needDefault=false;
                    }
                }
            } else if (value instanceof String) {
                String s=(String)value;
                s=s.trim();
                if (!"".equals(s)) {
                    internalAddField(ldoc,f,s);
                    needDefault=false;
                }
            } else throw new UnsupportedOperationException("Invalid Java data type of expression result: "+value.getClass().getName());

            if (needDefault && f.defaultValue!=null) internalAddField(ldoc,f,f.defaultValue);
        }
    }

    protected void processDocumentBoost(SingleIndexConfig iconfig, Document ldoc) throws Exception {
        float boost=1.0f;
        if (iconfig.parent.documentBoost!=null && iconfig.parent.documentBoost.xPathExpr!=null) {
            Number n=(Number)iconfig.parent.documentBoost.xPathExpr.evaluate(dom, javax.xml.xpath.XPathConstants.NUMBER);
            if (n==null) throw new javax.xml.xpath.XPathExpressionException("The XPath for document boost did not return a valid NUMBER value!");
            boost=n.floatValue();
            if (Float.isNaN(boost) || boost<=0.0f || Float.isInfinite(boost))
                throw new javax.xml.xpath.XPathExpressionException("The XPath for document boost did not return a positive, finite NUMBER (default=1.0)!");
        }
        if (log.isTraceEnabled()) log.trace("DocumentBoost: "+boost);
        ldoc.setBoost(boost);
    }

    protected boolean processFilters(SingleIndexConfig iconfig) throws Exception {
        boolean accept=(iconfig.parent.filterDefault==FilterConfig.FilterType.ACCEPT);
        for (FilterConfig f : iconfig.parent.filters) {
            if (f.xPathExpr==null) throw new NullPointerException("Filters need to contain a XPath expression, which is NULL!");
            Boolean b=(Boolean)f.xPathExpr.evaluate(dom, javax.xml.xpath.XPathConstants.BOOLEAN);
            if (b==null) throw new javax.xml.xpath.XPathExpressionException("The filter XPath did not return a valid BOOLEAN value!");
            if (b && log.isTraceEnabled()) log.trace("FilterMatch: "+f);
            switch (f.type) {
                case ACCEPT:
                    if (b) accept=true;
                    break;
                case DENY:
                    if (b) accept=false;
                    break;
                default:
                    throw new IllegalArgumentException("Invalid filter type (should never happen!)");
            }
        }
        return accept;
    }

    protected void addSystemVariables(SingleIndexConfig iconfig, Map<QName,Object> vars) {
        vars.put(XPathResolverImpl.VARIABLE_INDEX_ID,iconfig.id);
        vars.put(XPathResolverImpl.VARIABLE_INDEX_DISPLAYNAME,iconfig.displayName);
        vars.put(XPathResolverImpl.VARIABLE_DOC_IDENTIFIER,identifier);
        vars.put(XPathResolverImpl.VARIABLE_DOC_DATESTAMP,(datestamp==null)?"":ISODateFormatter.formatLong(datestamp));
    }

    protected final void processXPathVariables(SingleIndexConfig iconfig) throws Exception {
        // put map of variables in thread local storage of index config
        boolean needCleanup=true;
        Map<QName,Object> data=XPathResolverImpl.getInstance().initVariables();
        try {
            addSystemVariables(iconfig,data);

            // variables in config
            for (VariableConfig f : iconfig.parent.xPathVariables) {
                Object value=null;
                if (f.xPathExpr!=null) {
                    try {
                        // First: try to get XPath result as Nodelist if that fails (because result is #STRING): fallback
                        // TODO: Looking for a better system to detect return type of XPath :-( [slowdown by this?]
                        value=f.xPathExpr.evaluate(dom, javax.xml.xpath.XPathConstants.NODESET);
                    } catch (javax.xml.xpath.XPathExpressionException ex) {
                        // Fallback: if return type of XPath is a #STRING (for example from a substring() routine)
                        value=f.xPathExpr.evaluate(dom, javax.xml.xpath.XPathConstants.STRING);
                    }
                } else if (f.xslt!=null) {
                    value=evaluateTemplate(f);
                } else throw new NullPointerException("Both XPath and template are NULL for variable "+f.name);

                if (value!=null) {
                    if (log.isTraceEnabled()) log.trace("Variable: "+f.name+"="+value);
                    data.put(f.name,value);
                }
            }

            needCleanup=false;
        } finally {
            // we need to cleanup on any Exception to keep config in consistent state
            if (needCleanup) XPathResolverImpl.getInstance().unsetVariables();
        }
    }

    public Document getLuceneDocument(SingleIndexConfig iconfig) throws Exception {
        Document ldoc = createEmptyDocument();
        if (!deleted) {
            if (dom==null) throw new NullPointerException("The DOM-Tree of document may not be 'null'!");
            processXPathVariables(iconfig);
            try {
                boolean filterAccepted=processFilters(iconfig);
                if (!filterAccepted) {
                    log.debug("Document filtered: "+identifier);
                    return null;
                }
                addDefaultField(iconfig,ldoc);
                addFields(iconfig,ldoc);
                processDocumentBoost(iconfig,ldoc);
            } finally {
                XPathResolverImpl.getInstance().unsetVariables();
            }
            ldoc.add(new Field(IndexConstants.FIELDNAME_XML, this.getXML(), Field.Store.COMPRESS, Field.Index.NO));
        }
        return ldoc;
    }

    protected NodeList evaluateTemplate(ExpressionConfig expr) throws TransformerException {
        Transformer trans=expr.xslt.newTransformer();
        trans.setErrorListener(new LoggingErrorListener(getClass()));

        // set variables in transformer
        Map<QName,Object> vars=XPathResolverImpl.getInstance().getCurrentVariableMap();
        for (Map.Entry<QName,Object> entry : vars.entrySet()) {
            trans.setParameter(entry.getKey().toString(),entry.getValue());
        }

        // transform
        DocumentFragment df=dom.createDocumentFragment();
        trans.transform(new DOMSource(dom,identifier),new DOMResult(df));
        return df.getChildNodes();
    }

    protected void walkNodeTexts(StringBuilder sb, Node n, boolean topLevel) {
        if (n==null) return;
        switch (n.getNodeType()) {
            case Node.ELEMENT_NODE:
            case Node.DOCUMENT_NODE:
            case Node.DOCUMENT_FRAGMENT_NODE:
                NodeList childs=n.getChildNodes();
                for (int i=0,c=childs.getLength(); i<c; i++) {
                    walkNodeTexts(sb, childs.item(i),false);
                    sb.append('\n');
                }
                break;
            case Node.ATTRIBUTE_NODE:
                // This is special: Attributes are normally not converted to String, only if the XPath goes directly to the attribute
                // If this is the case the Attribute is topLevel in the recursion!
                if (!topLevel) break;
            case Node.TEXT_NODE:
            case Node.CDATA_SECTION_NODE:
                sb.append(n.getNodeValue());
                break;
        }
    }

    protected void internalAddField(Document ldoc, FieldConfig f, String val) throws Exception {
        if (log.isTraceEnabled()) log.trace("AddField: "+f.name+'='+val);
        boolean token=false;
        switch(f.datatype) {
            case NUMBER: val=LuceneConversions.doubleToLucene(Double.parseDouble(val));break;
            case DATETIME: val=LuceneConversions.dateToLucene(LenientDateParser.parseDate(val));break;
            case TOKENIZEDTEXT: token=true;
        }
        Field.Index in=Field.Index.NO;
        if (f.luceneindexed) in=token?Field.Index.TOKENIZED:Field.Index.UN_TOKENIZED;
        ldoc.add(new Field(f.name, val, f.lucenestorage?Field.Store.YES:Field.Store.NO, in));
        if (f.luceneindexed && (f.datatype==FieldConfig.DataType.NUMBER || f.datatype==FieldConfig.DataType.DATETIME))
            LuceneConversions.addTrieIndexEntries(ldoc,f.name,val);
    }

    protected boolean deleted=false;
    protected java.util.Date datestamp=null;
    protected String identifier=null;

    private org.w3c.dom.Document dom=null;
    private String xmlCache=null;
}