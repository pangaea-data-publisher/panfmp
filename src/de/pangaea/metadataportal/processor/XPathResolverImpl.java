/*
 *   Copyright panFMP Developers Team c/o Uwe Schindler
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

package de.pangaea.metadataportal.processor;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.xml.namespace.QName;
import javax.xml.xpath.XPathFunction;
import javax.xml.xpath.XPathFunctionResolver;
import javax.xml.xpath.XPathVariableResolver;

import org.apache.xalan.extensions.XPathFunctionResolverImpl;

/**
 * Helper class that implements several XPath interfaces to supply variables and
 * functions to XPath expressions. This is a singleton!
 * 
 * @author Uwe Schindler
 */
public final class XPathResolverImpl implements XPathFunctionResolver, XPathVariableResolver {
  
  public static XPathResolverImpl getInstance() {
    return instance;
  }
  
  private XPathResolverImpl() {
    parent = new XPathFunctionResolverImpl();
  }
  
  // XPathFunctionResolver
  @Override
  public XPathFunction resolveFunction(QName functionName, int arity) {
    /*if (FUNCTION_DOC_UNIQUE.equals(functionName)) {
      // FUNCTION: isDocIdentifierUnique() -- test if identifier of current
      // document is unique
      return new XPathFunction() {
        @SuppressWarnings("rawtypes")
        public Object evaluate(List args) throws XPathFunctionException {
          return isDocIdentifierUnique(args);
        }
      };
    }*/
    return parent.resolveFunction(functionName, arity);
  }
  
  // XPathVariableResolver
  @Override
  public Object resolveVariable(QName variableName) {
    Map<QName,Object> map = xPathVariableData.get();
    if (map == null) throw new IllegalStateException(
        "There is no variables map in thread local storage!");
    return map.get(variableName);
  }
    
  // API
  public synchronized Map<QName,Object> initVariables() {
    HashMap<QName,Object> data = new HashMap<>();
    xPathVariableData.set(data);
    return data;
  }
  
  public synchronized Map<QName,Object> getCurrentVariableMap() {
    return xPathVariableData.get();
  }
  
  public synchronized void unsetVariables() {
    xPathVariableData.remove();
  }
  
  // class members
  public static final String DOCUMENT_PROCESSOR_NAMESPACE = "urn:java:"
      + DocumentProcessor.class.getName();
  
  public static final QName VARIABLE_DOC_IDENTIFIER = new QName(
      DOCUMENT_PROCESSOR_NAMESPACE, "docIdentifier");
  public static final QName VARIABLE_DOC_DATESTAMP = new QName(
      DOCUMENT_PROCESSOR_NAMESPACE, "docDatestamp");
  public static final QName VARIABLE_HARVESTER_ID = new QName(
      DOCUMENT_PROCESSOR_NAMESPACE, "harvesterIdentifier");
  
  public static final Set<QName> BASE_VARIABLES = Set.of(
          VARIABLE_DOC_IDENTIFIER, VARIABLE_DOC_DATESTAMP,
          VARIABLE_HARVESTER_ID);
  
  private static XPathResolverImpl instance = new XPathResolverImpl();
  
  // object members
  private ThreadLocal<Map<QName,Object>> xPathVariableData = new ThreadLocal<>();
  
  private final XPathFunctionResolver parent;
  
}