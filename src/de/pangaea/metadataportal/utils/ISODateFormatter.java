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
import java.util.TimeZone;
import java.util.Locale;
import java.text.*;

/**
 * Simple static class to create and parse ISO-8601 date stamps (used by OAI harvester):
 * The used date formats are:<ul>
 * <li>Long date: <code>yyyy-MM-dd'T'HH:mm:ss'Z'</code></li>
 * <li>Short date: <code>yyyy-MM-dd</code></li>
 * </ul>
 * @author Uwe Schindler
 */
public final class ISODateFormatter {

	private ISODateFormatter() {} // no instance

	/** Parses the given string into a {@link Date}. It accepts short and long dates (with time) */
	public static synchronized Date parseDate(String date) throws ParseException {
		if (date==null) return null;
		ParsePosition pp=new ParsePosition(0);
		try {
			Date d=longDate.parse(date,pp);
			if (pp.getIndex()!=date.length()) throw new ParseException("Invalid datestamp",pp.getIndex());
			return d;
		} catch (java.text.ParseException e) {
			pp=new ParsePosition(0);
			Date d=shortDate.parse(date,pp);
			if (pp.getIndex()!=date.length()) throw new ParseException("Invalid datestamp",pp.getIndex());
			return d;
		}
	}

	/** Formats a long date. */
	public static synchronized String formatLong(Date date) {
		return longDate.format(date);
	}

	/** Formats a short date. */
	public static synchronized String formatShort(Date date) {
		return shortDate.format(date);
	}

	private static SimpleDateFormat longDate=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'",Locale.US);
	private static SimpleDateFormat shortDate=new SimpleDateFormat("yyyy-MM-dd",Locale.US);
	static {
		longDate.setTimeZone(TimeZone.getTimeZone("UTC"));
		longDate.setLenient(false);
		shortDate.setTimeZone(TimeZone.getTimeZone("UTC"));
		shortDate.setLenient(false);
	}
}