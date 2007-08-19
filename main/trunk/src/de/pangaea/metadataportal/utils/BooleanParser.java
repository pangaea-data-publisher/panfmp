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

package de.pangaea.metadataportal.utils;

import javax.xml.XMLConstants;
import javax.xml.namespace.*;

public final class BooleanParser {

    private BooleanParser() {}

    public static boolean parseBoolean(String v) {
        v=v.toUpperCase();
        if ("TRUE".equals(v) || "YES".equals(v) || "ON".equals(v)) return true;
        else if ("FALSE".equals(v) || "NO".equals(v) || "OFF".equals(v)) return false;
        else throw new IllegalArgumentException("Boolean value must be one of: [YES,TRUE,ON]; [NO,FALSE,OFF]");
    }

}