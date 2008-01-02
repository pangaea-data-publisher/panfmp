/*
 *   Copyright 2007-2008 panFMP Developers Team c/o Uwe Schindler
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

package de.pangaea.metadataportal.search.axis;

import de.pangaea.metadataportal.search.SearchResultItem;

public final class SearchResponseItem {

	protected SearchResponseItem(SearchResultItem item, boolean returnXML, boolean returnStoredFields) {
		this.item=item;
		this.returnXML=returnXML;
		this.returnStoredFields=returnStoredFields;
	}

	public float getScore() {
		return item.getScore();
	}

	public String getXml() {
		return returnXML ? item.getXml() : null;
	}

	public String getIdentifier() {
		return item.getIdentifier();
	}

	public java.util.Map<String,Object[]> getFields() {
		return returnStoredFields ? item.getFields() : null;
	}

	// information
	private SearchResultItem item;
	private boolean returnXML,returnStoredFields;
}