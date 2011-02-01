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

import java.util.*;

/**
 * Simple LRUMap based on {@link LinkedHashMap}.
 * The maximum size is given in constructor, the eldest entry (not accessed for the longest time) is removed.
 * @author Uwe Schindler
 */
public class LRUMap<K,V> extends LinkedHashMap<K,V> {

	/**
	 * Creates the LRUMap using the given size as maximum entry count.
	 */
	public LRUMap(final int maxSize) {
		super(Math.max(maxSize/2, 16), 0.75f, true);
		this.maxSize=maxSize;
	}
	
	@Override
	protected boolean removeEldestEntry(Map.Entry<K,V> eldest) {
		return size() > maxSize;
	}
	
	protected int maxSize;
}