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
import java.util.HashSet;
import java.util.HashMap;
import javax.xml.transform.*;
import javax.xml.transform.dom.*;
import javax.xml.transform.stream.*;
import javax.xml.namespace.QName;
import org.apache.lucene.document.*;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;

public class MetadataDocument {
    private static org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(MetadataDocument.class);

    public void setHeaderInfo(String status, String identifier, String datestampStr) throws java.text.ParseException {
        this.deleted=(status!=null && status.equals("deleted"));
        this.identifier=identifier;
        this.datestamp=ISODateFormatter.parseDate(datestampStr);
    }

    public void addSet(String set) {
        sets.add(set);
    }

    public String getXML() throws Exception {
        if (deleted || dom==null) return null;

        // convert DOM
        StringWriter xmlWriter=new StringWriter();
        Transformer trans=StaticFactories.transFactory.newTransformer();
        trans.setOutputProperty(OutputKeys.INDENT,"no");
        trans.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION,"yes");
        DOMSource in=new DOMSource(dom,identifier);
        StreamResult out=new StreamResult(xmlWriter);
        trans.transform(in,out);
        xmlWriter.close();
        return xmlWriter.toString();
    }

    public String toString() {
        return "identifier="+identifier+" deleted="+deleted+" datestamp="+((datestamp!=null)?ISODateFormatter.formatLong(datestamp):(String)null)+" sets="+sets;
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
            if (datestamp!=null) {
                String val;
                ldoc.add(new Field(IndexConstants.FIELDNAME_DATESTAMP, val=LuceneConversions.dateToLucene(datestamp), Field.Store.YES, Field.Index.UN_TOKENIZED));
                LuceneConversions.addTrieIndexEntries(ldoc,IndexConstants.FIELDNAME_DATESTAMP,val);
            }
            for (String set : sets) ldoc.add(new Field(IndexConstants.FIELDNAME_SET, set, Field.Store.YES, Field.Index.UN_TOKENIZED));
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
        for (Config.Config_Field f : iconfig.parent.fields.values()) {
            boolean needDefault=true;
            try {
                // First: try to get XPath result as Nodelist if that fails (because result is #STRING): fallback
                // TODO: Looking for a better system to detect return type of XPath :-( [slowdown by this?]
                NodeList nodes=(NodeList)f.xPathExpr.evaluate(dom, javax.xml.xpath.XPathConstants.NODESET);
                for (int i=0,c=nodes.getLength(); i<c; i++) {
                    StringBuilder sb=new StringBuilder();
                    walkNodeTexts(sb,nodes.item(i),true);
                    String val=sb.toString().trim();
                    if (!"".equals(val)) {
                        internalAddField(ldoc,f,val);
                        needDefault=false;
                    }
                }
            } catch (javax.xml.xpath.XPathExpressionException ex) {
                // Fallback: if return type of XPath is a #STRING (for example from a substring() routine)
                String val=f.xPathExpr.evaluate(dom).trim();
                if (!"".equals(val)) {
                    internalAddField(ldoc,f,val);
                    needDefault=false;
                }
            }
            if (needDefault && f.defaultValue!=null) internalAddField(ldoc,f,f.defaultValue);
        }
    }

    protected void processXPathVariables(SingleIndexConfig iconfig) throws Exception {
        // put map of variables in thread local storage of index config
        HashMap<QName,Object> data=new HashMap<QName,Object>();
        iconfig.parent.xPathVariableData.set(data);

        // TODO: default variables
        data.put(new QName("index"),iconfig.id);
        data.put(new QName("indexDisplayName"),iconfig.displayName);

        // variables in config file
        for (Config.Config_XPathVariable f : iconfig.parent.xPathVariables) {
            Object value=null;
            try {
                // First: try to get XPath result as Nodelist if that fails (because result is #STRING): fallback
                // TODO: Looking for a better system to detect return type of XPath :-( [slowdown by this?]
                value=f.xPathExpr.evaluate(dom, javax.xml.xpath.XPathConstants.NODESET);
            } catch (javax.xml.xpath.XPathExpressionException ex) {
                // Fallback: if return type of XPath is a #STRING (for example from a substring() routine)
                value=f.xPathExpr.evaluate(dom, javax.xml.xpath.XPathConstants.STRING);
            }
            data.put(f.name,value);
        }
    }

    protected void unsetXPathVariables(SingleIndexConfig iconfig) {
        // unset the map from the local thread storage of index config
        iconfig.parent.xPathVariableData.set(null); // unset variables
    }

    public Document getLuceneDocument(SingleIndexConfig iconfig) throws Exception {
        Document ldoc = createEmptyDocument();
        if (!deleted) {
            if (dom==null) throw new NullPointerException("The DOM-Tree of document may not be 'null'!");
            ldoc.add(new Field(IndexConstants.FIELDNAME_XML, this.getXML(), Field.Store.COMPRESS, Field.Index.NO));

            processXPathVariables(iconfig);
            addDefaultField(iconfig,ldoc);
            addFields(iconfig,ldoc);
            unsetXPathVariables(iconfig);
        }
        return ldoc;
    }

/*

    // this transforms a validated document (that is also stored in index) to the final doc to be used by xPath)
    public DOMResult transform4XPath(DOMSource ds) throws TransformerException {
        if (iconfig.parent.xslt==null) return DOMSource2Result(ds);
        else {
            Transformer trans=iconfig.parent.xslt.newTransformer();
            DOMResult dr=emptyDOMResult(ds.getSystemId());
            trans.transform(ds,dr);
            return dr;
        }
    }

*/

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

    protected void internalAddField(Document ldoc, Config.Config_Field f, String val) throws Exception {
        if (log.isTraceEnabled()) log.trace("AddField: "+f.name+'='+val);
        boolean token=false;
        switch(f.datatype) {
            case number: val=LuceneConversions.doubleToLucene(Double.parseDouble(val));break;
            case dateTime: val=LuceneConversions.dateToLucene(LenientDateParser.parseDate(val));break;
            case tokenizedText: token=true;
        }
        Field.Index in=Field.Index.NO;
        if (f.luceneindexed) in=token?Field.Index.TOKENIZED:Field.Index.UN_TOKENIZED;
        ldoc.add(new Field(f.name, val, f.lucenestorage?Field.Store.YES:Field.Store.NO, in));
        if (f.luceneindexed && (f.datatype==Config.DataType.number || f.datatype==Config.DataType.dateTime))
            LuceneConversions.addTrieIndexEntries(ldoc,f.name,val);
    }

    public boolean deleted=false;
    public java.util.Date datestamp=null;
    public String identifier=null;
    public org.w3c.dom.Document dom=null;
    public HashSet<String> sets=new HashSet<String>();
}