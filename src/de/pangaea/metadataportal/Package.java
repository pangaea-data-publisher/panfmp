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

/**
 * Class to get version information about panFMP.
 * @author Uwe Schindler
 */
public final class Package {

    private Package() {}

    /** Gets package object from classloader. */
    public static java.lang.Package get() {
        return Package.class.getPackage();
    }

    /** Gets version of panFMP. */
    public static String getVersion() {
        java.lang.Package pkg=get();
        return (pkg==null) ? null : pkg.getImplementationVersion();
    }

    /** Gets product name ("panFMP"). */
    public static String getProductName() {
        java.lang.Package pkg=get();
        return (pkg==null) ? null : pkg.getImplementationTitle();
    }

    /** Gets product vendor (the developer team). */
    public static String getProductVendor() {
        java.lang.Package pkg=get();
        return (pkg==null) ? null : pkg.getImplementationVendor();
    }

    /** Gets a version string to print out. */
    public static String getFullPackageDescription() {
        java.lang.Package pkg=get();
        if (pkg==null) return null;
        StringBuilder sb=new StringBuilder();
        sb.append(pkg.getImplementationTitle());
        sb.append(" version ");
        sb.append(pkg.getImplementationVersion());
        sb.append(" (");
        sb.append(pkg.getImplementationVendor());
        sb.append(")");
        return sb.toString();
    }

    /** Checks the minimum requirements (Lucene package, etc.).
     * @throws RuntimeException if the version of Lucene is too old.
     */
    public static void checkMinimumRequirements() {
        java.lang.Package lpkg=org.apache.lucene.LucenePackage.get();
        if (lpkg==null || !lpkg.isCompatibleWith("2.2.0"))
            throw new RuntimeException(getProductName()+" only runs with Apache Lucene 2.2.0 as a minimum requirement!");
    }

}
