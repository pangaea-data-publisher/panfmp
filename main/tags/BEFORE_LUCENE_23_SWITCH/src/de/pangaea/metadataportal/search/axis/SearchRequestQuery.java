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

public final class SearchRequestQuery implements java.io.Serializable {

	// default for axis
	public SearchRequestQuery() {
	}

	// simple easy-to-use constructor
	public SearchRequestQuery(String field, String query) {
		this.fieldName=field.intern();
		this.query=query;
		this.anyof=false;
	}

	public SearchRequestQuery(String field, String query, boolean anyof) {
		this.fieldName=field.intern();
		this.query=query;
		this.anyof=anyof;
	}

	public void setField(String v) { fieldName=v.intern(); }
	public void setQuery(String v) { query=v; }
	public void setAnyOf(boolean v) { anyof=v; }

	// members
	protected String fieldName=null,query=null;
	protected boolean anyof=false;

}