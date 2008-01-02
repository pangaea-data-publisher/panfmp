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

package de.pangaea.metadataportal.search;

/**
 * Implement this interface to collect search results.
 * @author Uwe Schindler
 */
public interface SearchResultCollector {

	/**
	 * This method is called every time a search result item was found during search.
	 * @param item the found search result item (contained fields and XML blob depends on
	 * parameters of used search method in {@link SearchService})
	 * @return <code>true</code>, if further results should be collected &ndash;
	 * <code>false</code>, if collecting must be stopped (use this to signal that you got enough items)
	 */
	public boolean collect(SearchResultItem item);

}