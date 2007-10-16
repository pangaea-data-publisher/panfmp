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
import java.util.Map;
import javax.xml.transform.*;
import javax.xml.transform.dom.*;
import javax.xml.transform.stream.*;
import javax.xml.namespace.QName;
import org.apache.lucene.document.*;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.DocumentFragment;

/**
 * This class holds all information harvested and provides methods for {@link IndexBuilder} to create
 * a Lucene {@link Document} instance from it.
 * @author Uwe Schindler
 */
public class MetadataDocument {
	private static org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(MetadataDocument.class);

	/**
	 * Default constructor, that creates an empty instance.
	 */
	public MetadataDocument() {
	}

	/**
	 * This static method "harvests" a stored Lucene {@link Document} from index for re-parsing.
	 * The class name for the correct <code>MetadataDocument</code> class extension is read from
	 * field {@link IndexConstants#FIELDNAME_MDOC_IMPL}.
	 * When the correct instance is created, it sets the {@link SingleIndexConfig} and calls {@link #loadFromLucene}.
	 *
	 * <P>This method is used by the {@link Rebuilder}.
	 *
	 * @return An instance of a subclass of <code>MetadataDocument</code>
	 */
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
				"This error occurs if there was an incompatible change of panFMP. You have to reharvest from the original source and recreate your index!");
		}
		if (log.isDebugEnabled()) log.debug("Using MetadataDocument class: "+cls.getName());
		MetadataDocument mdoc=cls.newInstance();
		mdoc.setIndexConfig(iconf);
		mdoc.loadFromLucene(ldoc);
		return mdoc;
	}

	/**
	 * Sets the {@link SingleIndexConfig} to be used for transforming the document to a Lucene {@link Document}.
	 */
	public void setIndexConfig(SingleIndexConfig iconfig) {
		this.iconfig=iconfig;
	}

	/**
	 * "Harvests" a stored Lucene {@link Document} from index for re-parsing.
	 * Extracts XML blob, identifier and datestamp from <code>Document</code>.
	 * Stored fields are not restored. They are regenerated by re-executing all
	 * XPath and Templates.
	 * {@link SingleIndexConfig} is used for index specific conversions.
	 */
	public void loadFromLucene(Document ldoc) throws Exception {
		deleted=false; datestamp=null;
		identifier=ldoc.get(IndexConstants.FIELDNAME_IDENTIFIER);
		if (identifier==null)
			log.warn("Loaded document without identifier from index.");
		try {
			String d=ldoc.get(IndexConstants.FIELDNAME_DATESTAMP);
			if (d!=null) datestamp=LuceneConversions.luceneToDate(d);
		} catch (NumberFormatException ne) {
			log.warn("Datestamp of document '"+identifier+"' is invalid. Deleting datestamp!",ne);
		}
		setXML(ldoc.get(IndexConstants.FIELDNAME_XML));
	}

	/**
	 * Sets XML contents as String (used by {@link #loadFromLucene}).
	 */
	public void setXML(String xml) throws Exception {
		xmlCache=xml;
		if (xml==null) {
			dom=null;
		} else {
			dom=StaticFactories.dombuilder.newDocument();
			StreamSource s=new StreamSource(new StringReader(xml),identifier);
			DOMResult r=new DOMResult(dom,identifier);
			Transformer trans=StaticFactories.transFactory.newTransformer();
			trans.setErrorListener(new LoggingErrorListener(getClass()));
			trans.transform(s,r);
		}
	}

	/**
	 * Returns XML contents as String (a cache is used).
	 */
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

	/**
	 * Sets XML contents as DOM tree. Invalidates cache.
	 */
	public void setDOM(org.w3c.dom.Document  dom) {
		this.dom=dom;
		xmlCache=null;
	}

	/**
	 * Returns XML contents as DOM tree.
	 */
	public org.w3c.dom.Document getDOM() {
		return dom;
	}

	/**
	 * Marks a harvested document as deleted. A deleted document is not indexed and
	 * will be explicitely deleted from index.
	 * A deleted document should not contain XML data, if there is XML data it will be ignored.
	 */
	public void setDeleted(boolean deleted) {
		this.deleted=deleted;
	}

	/**
	 * Returns deletion status.
	 * @see #setDeleted
	 */
	public boolean isDeleted() {
		return deleted;
	}

	/**
	 * Set the datestamp (last modification time of document file).
	 */
	public void setDatestamp(java.util.Date datestamp) {
		this.datestamp=datestamp;
	}

	/**
	 * @see #setDatestamp
	 */
	public java.util.Date getDatestamp() {
		return datestamp;
	}

	/**
	 * Set the document identifier.
	 */
	public void setIdentifier(String identifier) {
		this.identifier=identifier;
	}

	/**
	 * @see #setIdentifier
	 */
	public String getIdentifier() {
		return identifier;
	}

	@Override
	public String toString() {
		return "identifier="+identifier+" deleted="+deleted+" datestamp="+((datestamp!=null)?ISODateFormatter.formatLong(datestamp):(String)null);
	}

	/**
	 * Converts this instance to a Lucene {@link Document}.
	 * @return Lucene {@link Document} or <code>null</code>, if doc was deleted.
	 * @throws Exception if an exception occurs during transformation (various types of exceptions can be thrown).
	 * @throws IllegalStateException if index configuration is unknown
	 * @see #setIndexConfig
	 */
	public Document getLuceneDocument() throws Exception {
		if (iconfig==null) throw new IllegalStateException("An index configuration must be set before calling getLuceneDocument().");

		Document ldoc = createEmptyDocument();
		if (!deleted) {
			if (dom==null) throw new NullPointerException("The DOM-Tree of document may not be 'null'!");
			processXPathVariables();
			try {
				boolean filterAccepted=processFilters();
				if (!filterAccepted) {
					log.debug("Document filtered: "+identifier);
					return null;
				}
				addDefaultField(ldoc);
				addFields(ldoc);
				processDocumentBoost(ldoc);
			} finally {
				XPathResolverImpl.getInstance().unsetVariables();
			}
			ldoc.add(new Field(IndexConstants.FIELDNAME_XML,
				this.getXML(),
				(BooleanParser.parseBoolean(iconfig.harvesterProperties.getProperty("compressXML","true")) ? Field.Store.COMPRESS : Field.Store.YES),
				Field.Index.NO
			));
		}
		return ldoc;
	}

	/**
	 * Helper method that generates an empty Lucene {@link Document} instance.
	 * The standard fields are set to the doc properties (identifier, datestamp)
	 * @return Lucene {@link Document} or <code>null</code>, if doc was deleted.
	 * @throws Exception if an exception occurs during transformation (various types of exceptions can be thrown).
	 * @throws IllegalStateException if identifier is empty.
	 */
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
			if (datestamp!=null) LuceneConversions.addTrieDocumentField(ldoc,IndexConstants.FIELDNAME_DATESTAMP,datestamp,true,Field.Store.YES);
			return ldoc;
		}
	}

	/**
	 * Helper method that adds the default field to the given Lucene {@link Document} instance.
	 * This method executes the XPath for the default field.
	 * @throws Exception if an exception occurs during transformation (various types of exceptions can be thrown).
	 */
	protected void addDefaultField(Document ldoc) throws Exception {
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

	/**
	 * Helper method that adds all fields to the given Lucene {@link Document} instance.
	 * This method executes all XPath/Templates and converts the results.
	 * @throws Exception if an exception occurs during transformation (various types of exceptions can be thrown).
	 */
	protected void addFields(Document ldoc) throws Exception {
		for (FieldConfig f : iconfig.parent.fields.values()) {
			if (f.datatype==FieldConfig.DataType.XHTML) {
				addField(ldoc,f,evaluateTemplateAsXHTML(f));
			} else {
				boolean needDefault=(f.datatype==FieldConfig.DataType.NUMBER || f.datatype==FieldConfig.DataType.DATETIME);
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
					Transformer trans=null;
					if (f.datatype==FieldConfig.DataType.XML) {
						trans=StaticFactories.transFactory.newTransformer();
						trans.setErrorListener(new LoggingErrorListener(getClass()));
						trans.setOutputProperty(OutputKeys.INDENT,"no");
						trans.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION,"yes");
					}
					NodeList nodes=(NodeList)value;
					for (int i=0,c=nodes.getLength(); i<c; i++) {
						if (f.datatype==FieldConfig.DataType.XML) {
							DOMSource in=new DOMSource(nodes.item(i));
							if (in.getNode().getNodeType()!=Node.ELEMENT_NODE) continue; // only element nodes are valid XML document roots!
							StringWriter xmlWriter=new StringWriter();
							StreamResult out=new StreamResult(xmlWriter);
							trans.transform(in,out);
							xmlWriter.close();
							addField(ldoc,f,xmlWriter.toString());
						} else {
							StringBuilder sb=new StringBuilder();
							walkNodeTexts(sb,nodes.item(i),true);
							String val=sb.toString().trim();
							if (!"".equals(val)) {
							    addField(ldoc,f,val);
							    needDefault=false;
							}
						}
					}
				} else if (value instanceof String) {
					if (f.datatype==FieldConfig.DataType.XML) throw new UnsupportedOperationException("Fields with datatype XML may only return NODESETs on evaluation!");
					String s=(String)value;
					s=s.trim();
					if (!"".equals(s)) {
						addField(ldoc,f,s);
						needDefault=false;
					}
				} else throw new UnsupportedOperationException("Invalid Java data type of expression result: "+value.getClass().getName());

				if (needDefault && f.defaultValue!=null) addField(ldoc,f,f.defaultValue);
			}
		}
	}

	/**
	 * Helper method that evaluates the document boost for the Lucene {@link Document} instance.
	 * This method executes the XPath and converts the results to a float (default is 1.0f).
	 * @throws Exception if an exception occurs during transformation (various types of exceptions can be thrown).
	 */
	protected void processDocumentBoost(Document ldoc) throws Exception {
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

	/**
	 * Helper method that evaluates all filters.
	 * This method executes the XPath and converts the results to a boolean.
	 * The results of all filters are combined according to the ACCEPT/DENY type.
	 * @throws Exception if an exception occurs during transformation (various types of exceptions can be thrown).
	 */
	protected boolean processFilters() throws Exception {
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

	/**
	 * Helper method to register all standard variables for the XPath/Templates evaluation.
	 * Overwrite this method to register any special variables dependent on the <code>MetadataDocument</code> implementation.
	 * The variables must be registered in the supplied {@link Map}.
	 */
	protected void addSystemVariables(Map<QName,Object> vars) {
		vars.put(XPathResolverImpl.VARIABLE_INDEX_ID,iconfig.id);
		vars.put(XPathResolverImpl.VARIABLE_INDEX_DISPLAYNAME,iconfig.displayName);
		vars.put(XPathResolverImpl.VARIABLE_DOC_IDENTIFIER,identifier);
		vars.put(XPathResolverImpl.VARIABLE_DOC_DATESTAMP,(datestamp==null)?"":ISODateFormatter.formatLong(datestamp));
	}

	/**
	 * Helper method to process all user supplied variables for the XPath/Templates evaluation.
	 * The variables are stored in thread local storage.
	 * @throws Exception if an exception occurs during transformation (various types of exceptions can be thrown).
	 */
	protected final void processXPathVariables() throws Exception {
		// put map of variables in thread local storage of index config
		boolean needCleanup=true;
		Map<QName,Object> data=XPathResolverImpl.getInstance().initVariables();
		try {
			addSystemVariables(data);

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

	/**
	 * Helper method to evaluate a template.
	 * This method is called by variables and fields, when a template is used instead of a XPath.
	 * <P>For internal use only!
	 */
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

	/**
	 * Helper method to evaluate a template and return result as XHTML.
	 * This method is called by fields with datatype XHTML.
	 * <P>For internal use only!
	 */
	protected String evaluateTemplateAsXHTML(FieldConfig expr) throws TransformerException,java.io.IOException {
		if (expr.datatype!=FieldConfig.DataType.XHTML) throw new IllegalArgumentException("Datatype must be XHTML for evaluateTemplateAsXHTML()");
		Transformer trans=expr.xslt.newTransformer();
		trans.setErrorListener(new LoggingErrorListener(getClass()));
		trans.setOutputProperty(OutputKeys.METHOD,"xml");
		trans.setOutputProperty(OutputKeys.DOCTYPE_PUBLIC,"-//W3C//DTD XHTML 1.0 Transitional//EN");
		trans.setOutputProperty(OutputKeys.INDENT,"no");
		trans.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION,"yes");

		// set variables in transformer
		Map<QName,Object> vars=XPathResolverImpl.getInstance().getCurrentVariableMap();
		for (Map.Entry<QName,Object> entry : vars.entrySet()) {
			trans.setParameter(entry.getKey().toString(),entry.getValue());
		}

		StringWriter xmlWriter=new StringWriter();
		StreamResult out=new StreamResult(xmlWriter);
		trans.transform(new DOMSource(dom,identifier),out);
		xmlWriter.close();
		return xmlWriter.toString();
	}

	/**
	 * Helper method to walk through a DOM tree node (n) and collect strings.
	 * <P>For internal use only!
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

	/**
	 * Helper method to add a field in the correct format to given Lucene {@link Document}.
	 * The format is defined by the {@link FieldConfig}. The value is given as string.
	 * <P>For internal use only!
	 * @throws Exception if an exception occurs during transformation (various types of exceptions can be thrown).
	 */
	protected void addField(Document ldoc, FieldConfig f, String val) throws Exception {
		if (log.isTraceEnabled()) log.trace("AddField: "+f.name+'='+val);
		boolean token=false;
		switch(f.datatype) {
			case NUMBER:
				LuceneConversions.addTrieDocumentField(ldoc, f.name, Double.parseDouble(val), f.luceneindexed, f.lucenestorage);
				break;
			case DATETIME:
				LuceneConversions.addTrieDocumentField(ldoc, f.name, LenientDateParser.parseDate(val), f.luceneindexed, f.lucenestorage);
				break;
			case TOKENIZEDTEXT: 
				token=true;
				// fall-through
			default:
				Field.Index in=Field.Index.NO;
				if (f.luceneindexed) in=token?Field.Index.TOKENIZED:Field.Index.UN_TOKENIZED;
				ldoc.add(new Field(f.name, val, f.lucenestorage, in));
		}
	}

	/**
	 * @see #setDeleted
	 */
	protected boolean deleted=false;

	/**
	 * @see #setDatestamp
	 */
	protected java.util.Date datestamp=null;

	/**
	 * @see #setIdentifier
	 */
	protected String identifier=null;

	/**
	 * @see #setIndexConfig
	 */
	protected SingleIndexConfig iconfig=null;

	private org.w3c.dom.Document dom=null;
	private String xmlCache=null;
}