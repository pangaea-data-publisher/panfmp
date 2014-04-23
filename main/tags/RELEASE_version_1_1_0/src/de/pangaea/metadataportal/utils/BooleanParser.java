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

import java.util.Locale;

/**
 * <code>BooleanParser</code> is a simple static class supplying a method to parse booleans.
 * @author Uwe Schindler
 */
public final class BooleanParser {

	private BooleanParser() {}

	/** Parses a boolean value expressed as String
	 * @throws IllegalArgumentException if uppercase <code>v</code> is not "TRUE", "YES", "ON", "FALSE", "NO", "OFF"
	 */
	public static boolean parseBoolean(String v) {
		v=v.toUpperCase(Locale.ENGLISH);
		if ("TRUE".equals(v) || "YES".equals(v) || "ON".equals(v)) return true;
		else if ("FALSE".equals(v) || "NO".equals(v) || "OFF".equals(v)) return false;
		else throw new IllegalArgumentException("Boolean value must be one of: [YES,TRUE,ON]; [NO,FALSE,OFF]");
	}

}