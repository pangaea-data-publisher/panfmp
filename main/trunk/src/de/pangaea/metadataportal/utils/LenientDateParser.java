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

import java.util.Date;
import java.util.TimeZone;
import java.util.Locale;
import java.util.regex.Pattern;
import java.text.*;

// TODO !!!!!!!!!!!!!!!!!

/**
 * Simple static class to parse date/times <b>very</b> lenient. This class will be changed
 * in future, as date/time parsing will be refactored in <b>panFMP</b>.
 * @author Uwe Schindler
 */
public final class LenientDateParser {

	private LenientDateParser() {} // no instance

	/**Parses a string to a {@link Date} */
	public static synchronized Date parseDate(String date) throws ParseException {
		if (date==null) return null;

		// try to remove invalid time zone (remove dot)
		date=tzpat.matcher(date).replaceFirst("$1$2");

		// parse
		ParsePosition pp=new ParsePosition(0);
		Date d1=null,d2=null;

		for (int i=0,c=dateFormats.length; i<c; i++) {
			DateFormat df=dateFormats[i];
			ParsePosition akt=new ParsePosition(pp.getIndex());
			d1=df.parse(date,akt);
			if (akt.getErrorIndex()<0) {
				pp=akt;
				break;
			} else d1=null;
		}

		for (int i=0,c=timeFormats.length; i<c; i++) {
			DateFormat df=timeFormats[i];
			ParsePosition akt=new ParsePosition(pp.getIndex());
			d2=df.parse(date,akt);
			if (akt.getErrorIndex()<0) {
				pp=akt;
				break;
			} else d2=null;
		}

		//System.err.println(d1);System.err.println(d2);System.err.println(pp);
		if ((d1==null && d2==null) || pp.getIndex()!=date.length()) throw new ParseException("Invalid date/time: "+date,pp.getIndex());
		if (d1==null) return d2;
		if (d2==null) return d1;
		return new Date(d1.getTime()+d2.getTime());
	}

	// static constants
	private static Pattern tzpat=Pattern.compile("([\\+\\-]\\d\\d)\\:(\\d\\d)\\z");
	private static DateFormat[] dateFormats={
		new SimpleDateFormat("yyyy-MM-dd'T'",Locale.US),
		new SimpleDateFormat("yyyy-MM-dd",Locale.US),
		DateFormat.getDateInstance(DateFormat.FULL,Locale.US),
		DateFormat.getDateInstance(DateFormat.MEDIUM,Locale.US),
		DateFormat.getDateInstance(DateFormat.SHORT,Locale.US),
		new SimpleDateFormat("yyyy-MM",Locale.US),
		new SimpleDateFormat("yyyy",Locale.US),
	};
	private static DateFormat[] timeFormats={
		new SimpleDateFormat("HH:mm:ss.SSS'Z'",Locale.US),
		new SimpleDateFormat("HH:mm:ss'Z'",Locale.US),
		new SimpleDateFormat("HH:mm'Z'",Locale.US),
		new SimpleDateFormat("HH:mm:ss.SSSZ",Locale.US),
		new SimpleDateFormat("HH:mm:ssZ",Locale.US),
		new SimpleDateFormat("HH:mmZ",Locale.US),
		new SimpleDateFormat("HH:mm:ss.SSS z",Locale.US),
		new SimpleDateFormat("HH:mm:ss z",Locale.US),
		new SimpleDateFormat("HH:mm z",Locale.US),
		new SimpleDateFormat("HH:mm:ss.SSS",Locale.US),
		new SimpleDateFormat("HH:mm:ss",Locale.US),
		new SimpleDateFormat("HH:mm",Locale.US),
		DateFormat.getTimeInstance(DateFormat.FULL,Locale.US),
		DateFormat.getTimeInstance(DateFormat.MEDIUM,Locale.US),
		DateFormat.getTimeInstance(DateFormat.SHORT,Locale.US),
	};
	static {
		TimeZone UTC=TimeZone.getTimeZone("UTC");
		for (int i=0,c=dateFormats.length; i<c; i++) {
			DateFormat df=dateFormats[i];
			df.setTimeZone(UTC);
			df.setLenient(true);
			//System.err.println(((SimpleDateFormat)df).toPattern());
		}
		TimeZone tz=TimeZone.getDefault();
		for (int i=0,c=timeFormats.length; i<c; i++) {
			DateFormat df=timeFormats[i];
			df.setTimeZone(tz);
			df.setLenient(true);
			//System.err.println(((SimpleDateFormat)df).toPattern());
		}
		timeFormats[0].setTimeZone(UTC);
		timeFormats[0].setLenient(false);
		timeFormats[1].setTimeZone(UTC);
		timeFormats[1].setLenient(false);
		timeFormats[2].setTimeZone(UTC);
		timeFormats[2].setLenient(false);
	}

}