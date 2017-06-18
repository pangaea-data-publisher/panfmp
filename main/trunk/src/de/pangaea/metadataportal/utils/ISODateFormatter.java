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

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;

/**
 * Simple static class to create and parse ISO-8601 date stamps (used by OAI
 * harvester): The used date formats are:
 * <ul>
 * <li>Long date: <code>uuuu-MM-dd'T'HH:mm:ss'Z'</code></li>
 * <li>Short date: <code>uuuu-MM-dd</code></li>
 * </ul>
 * 
 * @author Uwe Schindler
 */
public final class ISODateFormatter {
  
  private ISODateFormatter() {} // no instance
  
  /**
   * Parses the given string into a {@link Instant}. It accepts short and long
   * dates (with time)
   */
  public static Instant parseOAIDate(String date) {
    return parseOAIDate(date, false);
  }
  
  /**
   * Parses the given string into a {@link Instant}. It accepts short and long
   * dates (with time). If roundUp is {@code true} it round the date up to
   * one millisecond before next.
   */
  public static Instant parseOAIDate(String date, boolean roundUp) {
    if (date == null) {
      return null;
    }
    try {
      Instant d = SHORT_DATE_FORMAT.parse(date, LocalDate::from).atStartOfDay().toInstant(ZoneOffset.UTC);
      if (roundUp) d = d.plus(1L, ChronoUnit.DAYS).minusMillis(1L);
      return d;
    } catch (DateTimeParseException e) {
      Instant d = LONG_DATE_FORMAT.parse(date, Instant::from);
      if (roundUp) d = d.truncatedTo(ChronoUnit.SECONDS).plus(1L, ChronoUnit.SECONDS).minusMillis(1L);
      return d;
    }
  }
  
  /** Formats a long OAI date. */
  public static String formatLong(TemporalAccessor date) {
    return LONG_DATE_FORMAT.format(date);
  }
  
  /** Formats a short OAI date. */
  public static String formatShort(TemporalAccessor date) {
    return SHORT_DATE_FORMAT.format(date);
  }
  
  /** Formats an Elasticsearch date. */
  public static String formatElasticsearch(TemporalAccessor date) {
    return ELASTIC_DATE_FORMAT.format(date);
  }
  
  private static final DateTimeFormatter ELASTIC_DATE_FORMAT =
      DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.ROOT).withZone(ZoneOffset.UTC);
  private static final DateTimeFormatter LONG_DATE_FORMAT =
      DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss'Z'", Locale.ROOT).withZone(ZoneOffset.UTC);
  private static final DateTimeFormatter SHORT_DATE_FORMAT =
      DateTimeFormatter.ofPattern("uuuu-MM-dd", Locale.ROOT).withZone(ZoneOffset.UTC);
}