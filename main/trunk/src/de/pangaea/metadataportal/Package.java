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

package de.pangaea.metadataportal;

public final class Package {

    private Package() {}

    public static java.lang.Package get() {
        return Package.class.getPackage();
    }

    public static String getVersion() {
        java.lang.Package pkg=get();
        return (pkg==null) ? null : pkg.getImplementationVersion();
    }

    public static void checkMinimumRequirements() {
        java.lang.Package lpkg=org.apache.lucene.LucenePackage.get();
        if (lpkg==null || !lpkg.isCompatibleWith("2.2.0"))
            throw new RuntimeException("panFMP only runs with Apache Lucene 2.2.0 as a minimum requirement!");
    }

}
