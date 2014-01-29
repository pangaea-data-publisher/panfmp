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

/**
 * Some constants used by <b>panFMP</b>.
 * 
 * @author Uwe Schindler
 */
public final class IndexConstants {
  
  private IndexConstants() {} // no instance
  
  public static final String FIELD_INTERNAL_PREFIX = "internal-";
  
  public static final String FIELDNAME_CONTENT = "_all";
  public static final String FIELDNAME_XML = "xml";
  
  public static final String FIELDNAME_SOURCE = FIELD_INTERNAL_PREFIX + "source";
  public static final String FIELDNAME_SET = FIELD_INTERNAL_PREFIX + "set";
  public static final String FIELDNAME_DATESTAMP = FIELD_INTERNAL_PREFIX + "datestamp";
  public static final String FIELDNAME_MDOC_IMPL = FIELD_INTERNAL_PREFIX + "mdoc-impl";
}