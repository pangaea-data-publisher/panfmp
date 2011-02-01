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

package de.pangaea.metadataportal.search.axis;

import de.pangaea.metadataportal.search.*;
import java.util.List;

public final class SearchResponse {

	protected SearchResponse(SearchResultList list, int offset, int count, boolean returnXML, boolean returnStoredFields) {
		this.list=list;
		this.offset=offset;
		this.count=count;
		this.returnXML=returnXML;
		this.returnStoredFields=returnStoredFields;
	}

	public int getOffset() {
		return offset;
	}

	public SearchResponseItem[] getResults() throws java.io.IOException {
		List<SearchResultItem> page=list.subList(Math.min(offset,list.size()), Math.min(offset+count,list.size()));
		SearchResponseItem[] results=new SearchResponseItem[page.size()];
		int i=0;
		for (SearchResultItem item : page) {
			results[i++]=new SearchResponseItem(item,returnXML,returnStoredFields);
		}
		return results;
	}

	public long getQueryTime() throws java.io.IOException {
		return list.getQueryTime();
	}

	public int getTotalCount() {
		return list.size();
	}

	// data
	private int offset,count;
	private SearchResultList list;
	private boolean returnXML,returnStoredFields;
}