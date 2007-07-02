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

//import org.apache.lucene.index.IndexReader;
import java.util.*;
import javax.xml.namespace.QName;
import javax.xml.xpath.*;
import de.pangaea.metadataportal.config.*;
import de.pangaea.metadataportal.utils.*;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.Term;

public final class XPathResolverImpl implements XPathFunctionResolver,XPathVariableResolver {

    private static org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(XPathResolverImpl.class);

    public static XPathResolverImpl getInstance() {
        return instance;
    }

    private XPathResolverImpl() {
        try {
            parent=XPathFunctionResolver.class.cast(Class.forName("org.apache.xalan.extensions.XPathFunctionResolverImpl").newInstance());
        } catch (ClassNotFoundException ce) {
            log.warn("org.apache.xalan.extensions.XPathFunctionResolverImpl not found, extensions to XPath disabled!");
        } catch (Exception oe) {
            log.warn("org.apache.xalan.extensions.XPathFunctionResolverImpl not working, extensions to XPath disabled!");
        }
    }

    // XPathFunctionResolver
    public XPathFunction resolveFunction(QName functionName, int arity) {
        if (FUNCTION_DOC_UNIQUE.equals(functionName)) {
            // FUNCTION: test if identifier of current document is unique
            return new XPathFunction() {
                public Object evaluate(List args) throws XPathFunctionException {
                    IndexBuilder index=currentIndexBuilder.get();
                    if (index==null)
                        throw new IllegalStateException("There is no IndexBuilder instance in thread local storage!");
                    String identifier=(String)resolveVariable(VARIABLE_DOC_IDENTIFIER);
                    if (identifier==null) throw new IllegalStateException("Missing variable "+VARIABLE_DOC_IDENTIFIER+" in thread local storage!");
                    HashSet<String> indexIds=new HashSet<String>();
                    try {
                        if (args.size()==0) {
                            // collect all indexes, excluding the current one
                            for (IndexConfig iconfig : index.iconfig.parent.indices.values()) {
                                if (iconfig==index.iconfig) continue;
                                if (!(iconfig instanceof SingleIndexConfig)) continue;
                                if (!((SingleIndexConfig)iconfig).isIndexAvailable()) continue;
                                indexIds.add(iconfig.id);
                            }
                        } else {
                            // collect indices by id, excluding the current one
                            for (Object o : args) {
                                if (!(o instanceof String))
                                    throw new XPathFunctionException(FUNCTION_DOC_UNIQUE.toString()+" only allows type STRING as parameters (which are index ids, or empty for all indices)!");
                                String s=(String)o;
                                if (s.equals(index.iconfig.id)) continue;
                                IndexConfig iconfig=index.iconfig.parent.indices.get(s);
                                if (!(iconfig instanceof SingleIndexConfig))
                                    throw new XPathFunctionException(FUNCTION_DOC_UNIQUE.toString()+" does not support index '"+s+"' (not defined or wrong type)!");
                                if (!((SingleIndexConfig)iconfig).isIndexAvailable()) continue;
                                indexIds.add(s);
                            }
                        }
                        // if no other indexes present, identifier is unique!
                        if (indexIds.isEmpty()) return new Boolean(true);
                        // fetch a MultiReader from cache to search for identifiers
                        IndexReader reader=openIndexReader(index.iconfig.parent,indexIds);
                        Term t=new Term(IndexConstants.FIELDNAME_IDENTIFIER,identifier);
                        TermDocs td=reader.termDocs(t);
                        try {
                            return new Boolean(!td.next());
                        } finally {
                            td.close();
                        }
                    } catch (java.io.IOException ioe) {
                        throw new XPathFunctionException("Error accessing index: "+ioe);
                    }
                }
            };
        }
        if (parent!=null) {
            // try the Xalan Function Resolver
            return parent.resolveFunction(functionName,arity);
        }
        // no function found
        return null;
    }

    // XPathVariableResolver
    public Object resolveVariable(QName variableName) {
        Map<QName,Object> map=xPathVariableData.get();
        if (map==null) throw new IllegalStateException("There is no variables map in thread local storage!");
        return map.get(variableName);
    }

    // private
    private IndexReader openIndexReader(Config conf, Set<String> ids) throws java.io.IOException {
        HashMap<Set<String>,IndexReader> ci=cachedIndexes.get();
        if (ci==null) throw new IllegalStateException("There is no correct data in thread local storage!");
        IndexReader reader=ci.get(ids);
        if (reader==null) {
            log.info("Opening virtual index reader containing "+ids+" for duplicates checking...");
            IndexReader[] l=new IndexReader[ids.size()];
            Iterator<String> it=ids.iterator();
            for (int i=0; it.hasNext(); i++) l[i]=conf.indices.get(it.next()).getUncachedIndexReader();
            ci.put(ids,reader=new org.apache.lucene.index.MultiReader(l));
        }
        return reader;
    }

    // API
    public synchronized Map<QName,Object> createVariableMap() {
        HashMap<QName,Object> data=new HashMap<QName,Object>();
        xPathVariableData.set(data);
        return data;
    }

    public synchronized void unsetVariables() {
        xPathVariableData.set(null);
    }

    public synchronized void setIndexBuilder(IndexBuilder index) {
        currentIndexBuilder.set(index);
        cachedIndexes.set(new HashMap<Set<String>,IndexReader>());
    }

    public synchronized void unsetIndexBuilder() {
        currentIndexBuilder.set(null);
        for (IndexReader v : cachedIndexes.get().values()) try {
            v.close();
        } catch (java.io.IOException ioe) {
            log.warn("Could not close one of the opened foreign indexes.");
        }
        cachedIndexes.set(null);
    }

    // class members
    public static final String INDEX_BUILDER_NAMESPACE="urn:java:"+IndexBuilder.class.getName();

    public static final QName FUNCTION_DOC_UNIQUE=new QName(INDEX_BUILDER_NAMESPACE,"isDocIdentifierUnique");
    public static final QName VARIABLE_DOC_IDENTIFIER=new QName(INDEX_BUILDER_NAMESPACE,"docIdentifier");
    public static final QName VARIABLE_DOC_DATESTAMP=new QName(INDEX_BUILDER_NAMESPACE,"docDatestamp");
    public static final QName VARIABLE_INDEX_DISPLAYNAME=new QName(INDEX_BUILDER_NAMESPACE,"indexDisplayName");
    public static final QName VARIABLE_INDEX_ID=new QName(INDEX_BUILDER_NAMESPACE,"index");

    private static XPathResolverImpl instance=new XPathResolverImpl();

    // object members
    private ThreadLocal<HashMap<QName,Object>> xPathVariableData=new ThreadLocal<HashMap<QName,Object>>();
    private ThreadLocal<IndexBuilder> currentIndexBuilder=new ThreadLocal<IndexBuilder>();
    private ThreadLocal<HashMap<Set<String>,IndexReader>> cachedIndexes=new ThreadLocal<HashMap<Set<String>,IndexReader>>();

    private XPathFunctionResolver parent=null;

}