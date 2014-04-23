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

package de.pangaea.metadataportal;

/**
 * Class to get version information about panFMP.
 * 
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
    java.lang.Package pkg = get();
    return (pkg == null || pkg.getImplementationVersion() == null) ? "undefined" : pkg.getImplementationVersion();
  }
  
  /** Gets product name ("panFMP"). */
  public static String getProductName() {
    java.lang.Package pkg = get();
    return (pkg == null || pkg.getImplementationTitle() == null) ? "panFMP" : pkg.getImplementationTitle();
  }
  
  /** Gets product vendor (the developer team). */
  public static String getProductVendor() {
    java.lang.Package pkg = get();
    return (pkg == null) ? null : pkg.getImplementationVendor();
  }
  
  /** Gets a version string to print out. */
  public static String getFullPackageDescription() {
    return new StringBuilder().append(getProductName()).append(" version ")
        .append(getVersion()).append(" (")
        .append(getProductVendor()).append(")").toString();
  }
  
}
