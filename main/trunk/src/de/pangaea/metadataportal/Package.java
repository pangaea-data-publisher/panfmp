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

package de.pangaea.metadataportal;

/**
 * Class to get version information about panFMP.
 * @author Uwe Schindler
 */
public final class Package {

	private static org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(Package.class);

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
	
	private static boolean isVersionCompatible(String pkgVersion, String desired) {
		if (pkgVersion == null || pkgVersion.length() < 1) return false;

		// pkgVersion may be bad formatted (may contain suffixes with letters etc.)
		String [] sa = pkgVersion.split("\\.", -1);
		int [] si = new int[sa.length];
		for (int i = 0; i < sa.length; i++) {
			sa[i]=sa[i].replaceFirst("\\D.*$","");
			si[i]=(sa[i].length()>0) ? Integer.parseInt(sa[i]) : 0;
			if (si[i]<0) si[i]=0;
		}

		// desired is always formatted correctly
		String [] da = desired.split("\\.", -1);
		int [] di = new int[da.length];
		for (int i = 0; i < da.length; i++) di[i]=Integer.parseInt(da[i]);
		
		for (int i = 0, len = Math.max(di.length, si.length); i < len; i++) {
			int d = (i<di.length) ? di[i] : 0;
			int s = (i<si.length) ? si[i] : 0;
			if (s < d) return false;
			if (s > d) return true;
		}
		return true;
	}
	
	private static void checkPackage(java.lang.Package pkg, String name, String desired) {
		if (pkg==null) {
			log.warn(
				"Cannot determine version of component '"+name+"'. "+getProductName()+
				" may work, but it would be better to check that version "+desired+" is installed in classpath!"
			);
		} else if (!(
			isVersionCompatible(pkg.getSpecificationVersion(),desired) ||
			isVersionCompatible(pkg.getImplementationVersion(),desired)
		)) throw new RuntimeException(getProductName()+" only runs with '"+name+"' version "+desired+" as a minimum requirement!");
	}

	/** Checks the minimum requirements (Lucene package, etc.).
	 * @throws RuntimeException if the version of Lucene is too old.
	 */
	public static void checkMinimumRequirements() {
		checkPackage(org.apache.lucene.LucenePackage.get(),"Apache Lucene","2.3");
		checkPackage(java.lang.Package.getPackage("org.apache.commons.digester"),"Apache Commons Digester","1.7");
	}

}
