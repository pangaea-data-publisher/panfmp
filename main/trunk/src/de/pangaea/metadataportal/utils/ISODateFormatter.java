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
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
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
   * Parses the given string from the OAI protocol into an {@link Instant}. It accepts short and long
   * dates (with time)
   */
  public static Instant parseOAIDate(String date) {
    final TemporalAccessor temporal = PARSE_DATE_FORMAT.parse(date);
    LocalTime time = temporal.query(TemporalQueries.localTime());
    if (time == null) {
      time = LocalTime.MIDNIGHT;
    }
    return LocalDate.from(temporal).atTime(time).toInstant(ZoneOffset.UTC);
  }
  
  /** Formats an ISO date, according to the OAI granularity. */
  public static String formatOAIDate(TemporalAccessor date, boolean fineGranularity) {
    return (fineGranularity ? LONG_DATE_FORMAT : SHORT_DATE_FORMAT).format(date);
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
  
  private static final DateTimeFormatter PARSE_DATE_FORMAT = new DateTimeFormatterBuilder()
      .appendPattern("uuuu-MM-dd")
      .optionalStart()
      .appendLiteral('T')
      .appendPattern("HH:mm:ss")
      .appendLiteral('Z')
      .toFormatter(Locale.ROOT).withResolverStyle(ResolverStyle.STRICT).withZone(ZoneOffset.UTC);
}