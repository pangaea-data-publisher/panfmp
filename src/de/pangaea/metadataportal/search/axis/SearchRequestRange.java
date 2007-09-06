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

package de.pangaea.metadataportal.search.axis;

public final class SearchRequestRange implements java.io.Serializable {

	// default for axis
	public SearchRequestRange() {
	}

	// simple easy-to-use constructor
	public SearchRequestRange(String field, Object min, Object max) {
		this.fieldName=field.intern();
		this.min=min;
		this.max=max;
	}

	public void setField(String v) { fieldName=v.intern(); }
	public void setMin(Object v) { min=v; }
	public void setMax(Object v) { max=v; }

	// members
	protected String fieldName=null;
	protected Object min=null,max=null;

}