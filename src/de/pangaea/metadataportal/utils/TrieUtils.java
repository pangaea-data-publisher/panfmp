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

package de.pangaea.metadataportal.utils;

import java.util.Date;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

/**
 *This is a helper class to construct the trie-based index entries for numerical values.
 * <p>This is part of the central implementation of <b>panFMP</b>'s
 * high performance range queries on numeric and date/time fields using &quot;trie memory&quot; structures.
 * See the publication about <b>panFMP</b>:
 * <blockquote><strong>Schindler, U, Diepenbroek, M</strong>, 2008. <em>Generic XML-based Framework for Metadata Portals.</em>
 * Computers &amp; Geosciences 34 (12), 1947-1955.
 * <a href="http://dx.doi.org/10.1016/j.cageo.2008.02.023" target="_blank">doi:10.1016/j.cageo.2008.02.023</a></blockquote>
 * <p>For more information, how the query works, see {@link de.pangaea.metadataportal.search.TrieRangeQuery}. The format of how the
 * numerical values are stored in index is documented here:
 * <p>All numerical values are first converted to special <code>unsigned long</code>s by applying some bit-wise transformations. This means:<ul>
 * <li>{@link Date}s are casted to unix timestamps (milliseconds since 1970-01-01, this is how Java represents date/time
 * internally): {@link Date#getTime()}. The resulting <code>signed long</code> is transformed to the unsigned form like so:</li>
 * <li><code>signed long</code>s are shifted, so that {@link Long#MIN_VALUE} is mapped to <code>0x0000000000000000</code>,
 * {@link Long#MAX_VALUE} is mapped to <code>0xffffffffffffffff</code>.</li>
 * <li><code>double</code>s are converted by getting their IEEE 754 floating-point "double format" bit layout and then some bits
 * are swapped, to be able to compare the result as <code>unsigned long</code>s.</li>
 * </ul><p>Each half-byte of this <code>unsigned long</code> (starting with the most-significant half-byte) is converted to ASCII chars
 * between 'a' ({@link #TRIE_CODED_SYMBOL_MIN}) and <code>'o'=='a'+0x0f</code> ({@link #TRIE_CODED_SYMBOL_MAX}).
 * The resulting {@link String} is comparable like the corresponding <code>unsigned long</code>.
 * <p>To store the different precisions of the long values (from one byte [only the most significant one] to the full eight bytes),
 * each lower precision is prefixed by the length ({@link #TRIE_CODED_PADDING_START}<code>+precision == '0'+precision</code>).
 * The full long is not prefixed at all. By this it is possible to get the correct enumeration of terms in correct precision
 * of the term list by just jumping to the correct prefix.
 * The full precision value may also be stored in the document.
 * @author Uwe Schindler
 */
public final class TrieUtils {

	/** Marker (PADDING)  before lower-precision (LOOSE) trie entries to signal the precision value */
	public static final char TRIE_CODED_PADDING_START='0';

	/** Characters used as lower end ['a' to ('a'+0x0f)] */
	public static final char TRIE_CODED_SYMBOL_MIN='a';
	/** Characters used as upper end ['a' to ('a'+0x0f)] */
	public static final char TRIE_CODED_SYMBOL_MAX=(char)(TRIE_CODED_SYMBOL_MIN+0x0f);

	/** minimum encoded value of a numerical index entry: {@link Long#MIN_VALUE} */
	public static String TRIE_CODED_NUMERIC_MIN=longToTrieCoded(Long.MIN_VALUE);
	/** maximum encoded value of a numerical index entry: {@link Long#MAX_VALUE} */
	public static String TRIE_CODED_NUMERIC_MAX=longToTrieCoded(Long.MAX_VALUE);

	private TrieUtils() {} // no instance

	// internal conversion to/from strings

	private static final String internalLongToTrieCoded(final long l) {
		final StringBuilder sb=new StringBuilder(16);
		for (int j=60; j>=0; j-=4) sb.append((char)(TRIE_CODED_SYMBOL_MIN+((l >>> j) & 0x0f)));
		return sb.toString();
	}

	private static final long internalTrieCodedToLong(final String s) {
		if (s==null) throw new NullPointerException("Trie encoded string may not be NULL");
		final int len=s.length();
		if (len!=16) throw new NumberFormatException("Invalid trie encoded numerical value representation: "+s+" (incompatible length, must be 16)");
		long l=0L;
		for (int i=0; i<len; i++) {
			char ch=s.charAt(i);
			int b;
			if (ch>=TRIE_CODED_SYMBOL_MIN && ch<=TRIE_CODED_SYMBOL_MAX) b=(int)(ch-TRIE_CODED_SYMBOL_MIN);
			else throw new NumberFormatException("Invalid trie encoded numerical value representation: "+s+" (char '"+ch+"' is invalid)");
			l = (l << 4) | b;
		}
		return l;
	}

	// Long's

	/** Converts a <code>long</code> value encoded to a <code>String</code> (with 16 bytes). This data type is currently not used by <b>panFMP</b>. */
	public static String longToTrieCoded(final long l) {
		return internalLongToTrieCoded(l ^ 0x8000000000000000L);
	}

	/** Converts a encoded <code>String</code> (16 bytes) value back to a <code>long</code>. This data type is currently not used by <b>panFMP</b>. */
	public static long trieCodedToLong(final String s) {
		return internalTrieCodedToLong(s) ^ 0x8000000000000000L;
	}

	// Double's

	/** Converts a <code>double</code> value encoded to a <code>String</code> (with 16 bytes). */
	public static String doubleToTrieCoded(final double d) {
		long l=Double.doubleToLongBits(d);
		if ((l & 0x8000000000000000L) == 0L) {
			// >0
			l |= 0x8000000000000000L;
		} else {
			// <0
			l = ~l;
		}
		return internalLongToTrieCoded(l);
	}

	/** Converts a encoded <code>String</code> (16 bytes) value back to a <code>double</code>. */
	public static double trieCodedToDouble(final String s) {
		long l=internalTrieCodedToLong(s);
		if ((l & 0x8000000000000000L) != 0L) {
			// >0
			l &= 0x7fffffffffffffffL;
		} else {
			// <0
			l = ~l;
		}
		return Double.longBitsToDouble(l);
	}

	// Date's

	/** Converts a <code>Date</code> value encoded to a <code>String</code> (with 16 bytes). */
	public static String dateToTrieCoded(final Date d) {
		return longToTrieCoded(d.getTime());
	}

	/** Converts a encoded <code>String</code> (16 bytes) value back to a <code>Date</code>. */
	public static Date trieCodedToDate(final String s) {
		return new Date(trieCodedToLong(s));
	}

	// increment / decrement

	/** Increments a encoded String value by 1. Needed by {@link de.pangaea.metadataportal.search.TrieRangeQuery}. */
	public static String incrementTrieCoded(final String v) {
		final int l=v.length();
		final StringBuilder sb=new StringBuilder(l);
		boolean inc=true;
		for (int i=l-1; i>=0; i--) {
			int b=v.charAt(i)-TRIE_CODED_SYMBOL_MIN;
			if (inc) b++;
			if (inc=(b>0x0f)) b=0;
			sb.insert(0,(char)(TRIE_CODED_SYMBOL_MIN+b));
		}
		return sb.toString();
	}

	/** Decrements a encoded String value by 1. Needed by {@link de.pangaea.metadataportal.search.TrieRangeQuery}. */
	public static String decrementTrieCoded(final String v) {
		final int l=v.length();
		final StringBuilder sb=new StringBuilder(l);
		boolean dec=true;
		for (int i=l-1; i>=0; i--) {
			int b=v.charAt(i)-TRIE_CODED_SYMBOL_MIN;
			if (dec) b--;
			if (dec=(b<0)) b=0x0f;
			sb.insert(0,(char)(TRIE_CODED_SYMBOL_MIN+b));
		}
		return sb.toString();
	}

	private static void addConvertedTrieCodedDocumentField(final Document ldoc, final String fieldname, final String val, final boolean index, final Field.Store store) {
		Field f=new Field(fieldname, val, store, index?Field.Index.NOT_ANALYZED:Field.Index.NO);
		if (index) f.setOmitTf(true);
		ldoc.add(f);
		if (index) for (int i=7; i>0; i--) {
			f=new Field(fieldname, new StringBuilder(i*2+1).append((char)(TRIE_CODED_PADDING_START+i)).append(val.substring(0,i*2)).toString(), Field.Store.NO, Field.Index.NOT_ANALYZED);
			f.setOmitTf(true);
			ldoc.add(f);
		}
	}

	/**
	 * Stores a double value in trie-form in document for indexing.
	 * This is done by stripping off 1 to 7 words from the end and adding each stripped value as a new
	 * term with a marker prefix {@link #TRIE_CODED_PADDING_START} marking its precision to the document.
	 * The full-precision value is indexed without a marker value and is also stored as given by the <code>store</code> parameter.
	 * If the field should not be searchable, set <code>index</code> to <code>false</code>. It is then only stored (for convenience).
	 * Fields added to a document using this method can be queried by {@link de.pangaea.metadataportal.search.TrieRangeQuery}
	 */
	public static void addDoubleTrieCodedDocumentField(final Document ldoc, final String fieldname, final double val, final boolean index, final Field.Store store) {
		addConvertedTrieCodedDocumentField(ldoc, fieldname, doubleToTrieCoded(val), index, store);
	}

	/**
	 * Stores a Date value in trie-form in document for indexing.
	 * This is done by stripping off 1 to 7 words from the end and adding each stripped value as a new
	 * term with a marker prefix {@link #TRIE_CODED_PADDING_START} marking its precision to the document.
	 * The full-precision value is indexed without a marker value and is also stored as given by the <code>store</code> parameter.
	 * If the field should not be searchable, set <code>index</code> to <code>false</code>. It is then only stored (for convenience).
	 * Fields added to a document using this method can be queried by {@link de.pangaea.metadataportal.search.TrieRangeQuery}
	 */
	public static void addDateTrieCodedDocumentField(final Document ldoc, final String fieldname, final Date val, final boolean index, final Field.Store store) {
		addConvertedTrieCodedDocumentField(ldoc, fieldname, dateToTrieCoded(val), index, store);
	}

	/**
	 * Stores a long (this data type is currently not used by <b>panFMP</b>) value in trie-form in document for indexing.
	 * This is done by stripping off 1 to 7 words from the end and adding each stripped value as a new
	 * term with a marker prefix {@link #TRIE_CODED_PADDING_START} marking its precision to the document.
	 * The full-precision value is indexed without a marker value and is also stored as given by the <code>store</code> parameter.
	 * If the field should not be searchable, set <code>index</code> to <code>false</code>. It is then only stored (for convenience).
	 * Fields added to a document using this method can be queried by {@link de.pangaea.metadataportal.search.TrieRangeQuery}
	 */
	public static void addLongTrieCodedDocumentField(final Document ldoc, final String fieldname, final long val, final boolean index, final Field.Store store) {
		addConvertedTrieCodedDocumentField(ldoc, fieldname, longToTrieCoded(val), index, store);
	}

	// test
	/*
	public static void main(String[] argv) {
		System.err.println("min="+TRIE_CODED_NUMERIC_MIN);
		System.err.println("max="+TRIE_CODED_NUMERIC_MAX);
		System.err.println();
		java.util.ArrayList<Double> testlist=new java.util.ArrayList<Double>();
		for (double x=1E-100; x<1; x=x*10000000.0) for (double d=-100; d<=+100; d+=7) testlist.add(x*d);
		java.util.Collections.sort(testlist);
		for (Double d : testlist) {
			String s=doubleToTrieCoded(d);
			System.err.println(d+" == "+s+" == "+trieCodedToDouble(s));
		}
		System.err.println();
		for (long l=-400L; l<=+400L; l+=7L) {
			String s=longToTrieCoded(l);
			System.err.println(l+" == "+s+" == "+trieCodedToLong(s));
		}
	}
	*/

}
