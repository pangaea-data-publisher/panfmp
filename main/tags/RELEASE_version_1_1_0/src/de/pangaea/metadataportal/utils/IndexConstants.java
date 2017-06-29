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

package de.pangaea.metadataportal.utils;

/**
 * Some constants used by <b>panFMP</b>.
 * @author Uwe Schindler
 */
public final class IndexConstants {

	private IndexConstants() {} // no instance

	public static final String FIELD_PREFIX = "internal-";

	public static final String FIELDNAME_CONTENT    = "textcontent".intern();

	public static final String FIELDNAME_IDENTIFIER = (FIELD_PREFIX+"identifier").intern();
	public static final String FIELDNAME_BOOST      = (FIELD_PREFIX+"boost").intern();
	public static final String FIELDNAME_SET        = (FIELD_PREFIX+"set").intern();
	public static final String FIELDNAME_DATESTAMP  = (FIELD_PREFIX+"datestamp").intern();
	public static final String FIELDNAME_XML        = (FIELD_PREFIX+"xml").intern();
	public static final String FIELDNAME_MDOC_IMPL  = (FIELD_PREFIX+"mdoc-impl").intern();

	public static final String FILENAME_LASTHARVESTED="lastharvested";
}