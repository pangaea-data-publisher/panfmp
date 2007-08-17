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

import java.util.Date;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

public final class LuceneConversions {

    public static final char LUCENE_LOOSE_PADDING_START='0';
    public static final char LUCENE_SYMBOL_MIN='a';
    public static final char LUCENE_SYMBOL_MAX=(char)(LUCENE_SYMBOL_MIN+0x0f);

    public static String LUCENE_NUMERIC_MIN=longToLucene(Long.MIN_VALUE);
    public static String LUCENE_NUMERIC_MAX=longToLucene(Long.MAX_VALUE);

    private static final String ERR_STRING = "Invalid Lucene numerical value representation: ";

    private LuceneConversions() {} // no instance

    // internal conversion to/from strings

    private static String internalLongToLucene(long l) {
        StringBuilder sb=new StringBuilder(16);
        for (int j=60; j>=0; j-=4) sb.append((char)(LUCENE_SYMBOL_MIN+((l >>> j) & 0x0f)));
        return sb.toString();
    }

    private static long internalLuceneToLong(String s) {
        if (s==null) throw new NumberFormatException(ERR_STRING+s);
        int len=s.length();
        if (len!=16) throw new NumberFormatException(ERR_STRING+s);
        long l=0L;
        for (int i=0; i<len; i++) {
            char ch=s.charAt(i);
            int b;
            if (ch>=LUCENE_SYMBOL_MIN && ch<=LUCENE_SYMBOL_MAX) b=(int)(ch-LUCENE_SYMBOL_MIN);
            else throw new NumberFormatException(ERR_STRING+s);
            l = (l << 4) | b;
        }
        return l;
    }

    // Long's

    public static String longToLucene(long l) {
        return internalLongToLucene(l ^ 0x8000000000000000L);
    }

    public static long luceneToLong(String s) throws NumberFormatException {
        return internalLuceneToLong(s) ^ 0x8000000000000000L;
    }

    // Double's

    public static String doubleToLucene(double d) {
        long l=Double.doubleToLongBits(d);
        if ((l & 0x8000000000000000L) == 0L) {
            // >0
            l |= 0x8000000000000000L;
        } else {
            // <0
            l = ~l;
        }
        return internalLongToLucene(l);
    }

    public static double luceneToDouble(String s) {
        long l=internalLuceneToLong(s);
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

    public static String dateToLucene(Date d) {
        return longToLucene(d.getTime());
    }

    public static Date luceneToDate(String s) {
        return new Date(luceneToLong(s));
    }

    // increment / decrement

    public static String incrementLucene(String v) {
        int l=v.length();
        StringBuilder sb=new StringBuilder(l);
        boolean inc=true;
        for (int i=l-1; i>=0; i--) {
            int b=v.charAt(i)-LUCENE_SYMBOL_MIN;
            if (inc) b++;
            if (inc=(b>0x0f)) b=0;
            sb.insert(0,(char)(LUCENE_SYMBOL_MIN+b));
        }
        return sb.toString();
    }

    public static String decrementLucene(String v) {
        int l=v.length();
        StringBuilder sb=new StringBuilder(l);
        boolean dec=true;
        for (int i=l-1; i>=0; i--) {
            int b=v.charAt(i)-LUCENE_SYMBOL_MIN;
            if (dec) b--;
            if (dec=(b<0)) b=0x0f;
            sb.insert(0,(char)(LUCENE_SYMBOL_MIN+b));
        }
        return sb.toString();
    }

    // helpers for searches for shorter lucene converted numeric values

    public static void addTrieIndexEntries(Document ldoc, String fieldname, String val) {
        for (int i=7; i>0; i--)
            ldoc.add(new Field(fieldname, new StringBuilder(i*2+1).append((char)(LUCENE_LOOSE_PADDING_START+i)).append(val.substring(0,i*2)).toString(), Field.Store.NO, Field.Index.UN_TOKENIZED));
    }

    // test
    /*
    public static void main(String[] argv) {
        System.err.println("min="+LUCENE_NUMERIC_MIN);
        System.err.println("max="+LUCENE_NUMERIC_MAX);
        System.err.println();
        java.util.ArrayList<Double> testlist=new java.util.ArrayList<Double>();
        for (double x=1E-100; x<1; x=x*10000000.0) for (double d=-100; d<=+100; d+=7) testlist.add(x*d);
        java.util.Collections.sort(testlist);
        for (Double d : testlist) {
            String s=doubleToLucene(d);
            System.err.println(d+" == "+s+" == "+luceneToDouble(s));
        }
        System.err.println();
        for (long l=-400L; l<=+400L; l+=7L) {
            String s=longToLucene(l);
            System.err.println(l+" == "+s+" == "+luceneToLong(s));
        }
    }
    */

}

