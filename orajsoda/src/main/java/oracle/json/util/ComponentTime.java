/* Copyright (c) 2014, 2024, Oracle and/or its affiliates. */
/* All rights reserved.*/

/*
   DESCRIPTION
    Manages a date and time stamp with microsecond precision.
 */

/**
 * This class is not part of the public API, and is
 * subject to change.
 *
 * Do not rely on it in your application code.
 *
 * @author  Doug McMahon
 */

package oracle.json.util;

import java.util.Formatter;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.Date;
import java.util.Locale;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;

import java.lang.NumberFormatException;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

// ### Dependency on common package should be eliminated from util
import oracle.json.common.Message;

public final class ComponentTime
{
  public static final String LOWEST_TIME    = "0001-01-01T00:00:00.000000";
  public static final String INFINITY_TIME  = "9999-12-31T23:59:59.999999";

  public static final String DEFAULT_OFORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
  public static final String DEFAULT_IFORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS";

  private static final String HTTP_FORMAT = "EEE, dd MMM yyyy HH:mm:ss z";

  public static final TimeZone GMT_ZONE = TimeZone.getTimeZone("GMT");

  private static final int YPOS = 46;
  private static final int MPOS = 42;
  private static final int DPOS = 37;
  private static final int HPOS = 32;
  private static final int IPOS = 26;
  private static final int SPOS = 20;
  private static final int UPOS = 0;

  // Prevent instances
  private ComponentTime()
  {
  }

  private static void throwException(String badDate)
  {
    throw(new IllegalArgumentException(Message.EX_ILLEGAL_DATE_TIME.get(badDate)));
  }

  /**
   * Make a formatter for the default format and the GMT timezone.
   * The output formatter appends the trailing Z.
   */
  public static SimpleDateFormat makeOutputFormatter()
  {
    SimpleDateFormat fmt = new SimpleDateFormat(DEFAULT_OFORMAT);
    fmt.setTimeZone(GMT_ZONE);
    return(fmt);
  }

  /**
   * Make a formatter for the default format and the GMT timezone.
   * The input formatter doesn't include the trailing Z.
   */
  public static SimpleDateFormat makeInputFormatter()
  {
    SimpleDateFormat fmt = new SimpleDateFormat(DEFAULT_IFORMAT);
    fmt.setTimeZone(GMT_ZONE);
    return(fmt);
  }

  /**
   * Convert a Java date to a DB time string
   * This includes a trailing Z for the UTC timezone
   */
  public static String dateToString(Date dat)
  {
    SimpleDateFormat fmt = ComponentTime.makeOutputFormatter();
    return(fmt.format(dat));
  }

  /**
   * Convert a date in ISO 8601 format to the format needed
   * for an HTTP header.
   */
  public static String dateToHeaderString(Date dat)
  {
    SimpleDateFormat fmt =  new SimpleDateFormat(HTTP_FORMAT, Locale.US);
    fmt.setTimeZone(GMT_ZONE);
    return(fmt.format(dat));
  }

  /**
   * Convert a date in ISO 8601 format to the format needed
   * for an HTTP header.
   */
  public static Date dateFromHeaderString(String datstr)
  {
    SimpleDateFormat fmt =  new SimpleDateFormat(HTTP_FORMAT, Locale.US);
    ParsePosition    zero = new ParsePosition(0);
    fmt.setTimeZone(GMT_ZONE);
    return(fmt.parse(datstr, zero));
  }

  /**
   * Convert a String to a Java Date
   * ### Obsolete API - remove it? Use stringToMillis() instead?
   */
  public static Date stringToDate(String datstr)
  {
    SimpleDateFormat fmt = ComponentTime.makeInputFormatter();
    ParsePosition    zero = new ParsePosition(0);

    int len = datstr.length();

    // Strip any trailing UTC zone
    if (datstr.endsWith("Z"))
      datstr = datstr.substring(0, --len);

    // ### The formatter has a bug if given an overly precise
    //     timestamp - if the fractional seconds exceed 3 digits
    //     it effectively multiplies the value by 1000ths, and
    //     therefore can add minutes/hours to the result. To
    //     avoid this the timestamp is truncated to 23 digits.
    if (len > 23)
      datstr = datstr.substring(0,23);

    // The formatter won't work if there's no time component
    if (len == 10) datstr += "T00:00:00.000";

    int pos = datstr.indexOf('.');
    if ((pos < 0) && (len == 19))
    {
      // ### The formatter has another bug: if the date string doesn't
      // ### have any fractional seconds, it just quits and returns a null.
      datstr = datstr + ".000";
    }

    return fmt.parse(datstr, zero);
  }

  /**
   * Convert a Java millisecond timestamp to a DB time string
   * This has a trailing Z for UTC
   */
  public static String millisToString(long millis)
  {
    return Instant.ofEpochMilli(millis).toString();
  }

  /**
   * Convert a DB time string to a Java millisecond timestamp
   */
  public static long stringToMillis(String isostr)
    throws IllegalArgumentException
  {
    if (isostr != null)
    {
      try
      {
        Instant ival = ComponentTime.stringToInstant(isostr);
        return ival.toEpochMilli();
      }
      catch (DateTimeParseException e)
      {
        // ### Should just let the DateTimeParseException go.
        // ### This is to prop up legacy code for now.
        ComponentTime.throwException(isostr);
      }
    }
    // NOTREACHED
    return Integer.MIN_VALUE;
  }

  /**
   * Convert a DB time string to an Instant
   * Strings that don't have a zone are assumed to be in UTC.
   */
  public static Instant stringToInstant(String isostr)
    throws DateTimeParseException
  {
    // ### Bug: it won't parse without some time component
    if (isostr.length() == 10) isostr += "T00:00:00.000";
    // Try a parse without the time zone, assuming it's UTC
    if (!isostr.endsWith("Z"))
    {
      try
      {
        LocalDateTime lval = LocalDateTime.parse(isostr);
        return lval.toInstant(ZoneOffset.UTC);
      }
      catch (DateTimeParseException e)
      {
        // Drop down and try the zone-aware parse
      }
    }
    // Try a parse with an optional time zone
    OffsetDateTime oval = OffsetDateTime.parse(isostr);
    return oval.toInstant();
  }

  private static final DateTimeFormatter ISO_FORMATTER =
      DateTimeFormatter.ofPattern("YYYY-MM-dd'T'HH:mm:ss.nnnnnnnnn");
  
  private static final DateTimeFormatter ISO_FORMATTER_SIX_DIGITS =
      DateTimeFormatter.ofPattern("YYYY-MM-dd'T'HH:mm:ss.SSSSSS");

  /**
   * Convert an Instant to an ISO 8601 string matching the old
   * SODA format. This includes trailing fractional digits out to
   * 6 places, and a trailing Z for UTC.
   * Note: if the instant has signficant fractional digits to 9 places,
   * they will be printed.
   */
  public static String instantToString(Instant ival)
  {
    return ComponentTime.instantToString(ival, true);
  }

  /**
   * ISO 8601 string with or without the Z (based on the withZone parameter).
   */
  public static String instantToString(Instant ival, boolean withZone)
  {
    return ComponentTime.instantToString(ival, withZone, false);
  }
  
  public static String instantToString(Instant ival, boolean withZone, boolean truncateMillis)
  {
    return ComponentTime.instantToString(ival, withZone, withZone, false);
  }

  public static String instantToString(Instant ival,
                                       boolean withZone,
                                       boolean truncateMillis,
                                       boolean useSixDigits)
  {
    LocalDateTime dt = LocalDateTime.ofInstant(ival, ZoneOffset.UTC);
    String result = dt.format(useSixDigits ? ISO_FORMATTER_SIX_DIGITS : ISO_FORMATTER);
    if (truncateMillis)
      result = result.substring(0, result.indexOf('.'));
    else if (result.endsWith("000")) // Remove nanos if they're zero
      result = result.substring(0, result.length()-3);
    if (withZone)
      result = result + "Z";
    return result;
  }

  /**
   * Round an instant up to the largest instant that is just lower
   * than CEIL(seconds) - that is, just a tick below the nearest
   * second, rounded up.
   * ### Forced to run in microsecond precison to match old code/tests.
   */
  public static Instant maxFractional(Instant ival)
  {
    long nanos = 999999999L - ival.getNano();
    // ### Might this add too many trailing digits?
    // ### To prop up old code, wipe off excess digits.
    // ### We can remove this later, I hope.
    if (nanos < 999L)
    {
      nanos = 999L - nanos;
      ival = ival.minusNanos(nanos);
    }
    else if (nanos > 999L)
    {
      nanos = nanos - 999L;
      ival = ival.plusNanos(nanos);
    }
    return ival;
  }

  /**
   *
   *  Construct a long-integer timestamp with the following fields:
   *
   *     0-19  microsecond (0-999999)
   *    20-25  second      (0-60)
   *    26-31  minute      (0-59)
   *    32-36  hour        (0-23)
   *    37-41  day         (1-31)
   *    42-45  month       (1-12)
   *    46-59  year        (0-9999)
   *
   *  Note that nanosecond precision is lost.
   *
   *  ### Unclear if years B.C. will work correctly - don't use them
   */
  public static long instantToStamp(Instant ival)
  {
    LocalDateTime dt = LocalDateTime.ofInstant(ival, ZoneOffset.UTC);
    long year   = (long)dt.getYear();
    long month  = (long)dt.getMonthValue();
    long day    = (long)dt.getDayOfMonth();
    long hour   = (long)dt.getHour();
    long minute = (long)dt.getMinute();
    long second = (long)dt.getSecond();
    long micros = (long)dt.getNano() / 1000L;

    long tstamp = micros; // UPOS

    tstamp |= (year   << YPOS);
    tstamp |= (month  << MPOS);
    tstamp |= (day    << DPOS);
    tstamp |= (hour   << HPOS);
    tstamp |= (minute << IPOS);
    tstamp |= (second << SPOS);

    return tstamp;
  }
}
