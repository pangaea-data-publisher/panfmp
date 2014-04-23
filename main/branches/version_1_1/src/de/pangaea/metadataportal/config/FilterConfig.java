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

package de.pangaea.metadataportal.config;

import de.pangaea.metadataportal.utils.*;
import javax.xml.transform.*;
import java.util.Locale;

/**
 * A filter config element that filters harvested documents by a XPath expression
 * that returns a {@link javax.xml.xpath.XPathConstants#BOOLEAN} value.
 * @author Uwe Schindler
 */
public class FilterConfig extends ExpressionConfig {

	@PublicForDigesterUse
	@Deprecated
	public void setType(String v) {
		try {
			type=FilterType.valueOf(v.toUpperCase(Locale.ENGLISH));
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException("Invalid filter type: '"+v+"'");
		}
	}

	@Override
	public void setTemplate(Templates xslt) {
		throw new UnsupportedOperationException("Cannot assign a template to a filter!");
	}

	@Override
	public String toString() {
		return new StringBuilder().append(type).append('(').append(super.toString()).append(')').toString();
	}

	// members "the configuration"
	public FilterType type=null;

	public static enum FilterType { ACCEPT,DENY };
}
