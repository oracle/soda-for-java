/* Copyright (c) 2014, 2018, Oracle and/or its affiliates. 
All rights reserved.*/

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

// ### Dependency on common package should be
//     eliminated from util
import oracle.json.common.Message;

public final class ComponentTime implements Comparable<ComponentTime>
{
  public static final String LOWEST_TIME    = "0001-01-01T00:00:00.000000";
  public static final String INFINITY_TIME  = "9999-12-31T23:59:59.999999";

  public static final String DEFAULT_OFORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
  public static final String DEFAULT_IFORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS";

  private static final String HTTP_FORMAT = "EEE, dd MMM yyyy HH:mm:ss z";

  public static final TimeZone GMT_ZONE = TimeZone.getTimeZone("GMT");

  // These times are invalid and can be used as "markers"
  public static final ComponentTime UNKNOWN_STAMP =
                  new ComponentTime(0L);
  public static final ComponentTime EXPIRED_STAMP =
                  new ComponentTime(Long.MAX_VALUE);

  // These capture negative and positive "infinity" times
  public static final ComponentTime LOWEST_STAMP =
                  new ComponentTime(LOWEST_TIME);
  public static final ComponentTime HIGHEST_STAMP =
                  new ComponentTime(INFINITY_TIME);

  private static final int YPOS = 46;
  private static final int MPOS = 42;
  private static final int DPOS = 37;
  private static final int HPOS = 32;
  private static final int IPOS = 26;
  private static final int SPOS = 20;
  private static final int UPOS = 0;

  private static final int YMASK = 0x3FFF;
  private static final int MMASK = 0xF;
  private static final int DMASK = 0x1F;
  private static final int HMASK = 0x1F;
  private static final int IMASK = 0x3F;
  private static final int SMASK = 0x3F;
  private static final int UMASK = 0xFFFFF;

  private static final int YIDX = 0;
  private static final int MIDX = 1;
  private static final int DIDX = 2;
  private static final int HIDX = 3;
  private static final int IIDX = 4;
  private static final int SIDX = 5;
  private static final int UIDX = 6;
  private static final int NUMIDX = 7;

  private final long timestamp;

  // Maxim allowed value for fractional seconds
  // (we use TIMESTAMP(6) in the RDBMS).
  private static final long MAX_MICROS = 1000000L;

  /**
   * Construct from a date in this format:
   *   YYYY-MM-DD'T'HH:MI:SS.FFFFFF
   */
  public ComponentTime(String strdate)
  {
    timestamp = ComponentTime.stringToStamp(strdate);
  }

  /**
   * Construct a component date from a long integer bit field.
   * ### No attempt is made to validate it
   */
  public ComponentTime(long tstamp)
  {
    timestamp = tstamp;
  }

  /**
   * Make a formatter for the default format and the GMT timezone.
   * The output formatter appends the trailing Z.
   */
  public static SimpleDateFormat makeOutputFormatter()
  {
    SimpleDateFormat fmt =  new SimpleDateFormat(DEFAULT_OFORMAT);
    fmt.setTimeZone(GMT_ZONE);
    return(fmt);
  }

  /**
   * Make a formatter for the default format and the GMT timezone.
   * The input formatter doesn't include the trailing Z.
   */
  public static SimpleDateFormat makeInputFormatter()
  {
    SimpleDateFormat fmt =  new SimpleDateFormat(DEFAULT_IFORMAT);
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
   */
  public static Date stringToDate(String datstr)
  {
    SimpleDateFormat fmt = ComponentTime.makeInputFormatter();
    ParsePosition    zero = new ParsePosition(0);
    // ### The formatter has a bug if given an overly precise
    //     timestamp - if the fractional seconds exceed 3 digits
    //     it effectively multiplies the value by 1000ths, and
    //     therefore can add minutes/hours to the result. To
    //     avoid this the timestamp is truncated to 23 digits.
    if (datstr.length() > 23) datstr = datstr.substring(0,23);
    return(fmt.parse(datstr, zero));
  }

  /**
   * Convert a Java millisecond timestamp to a DB time string
   */
  public static String millisToString(long millis)
  {
    return(ComponentTime.dateToString(new Date(millis)));
  }

  /**
   * Get a string rendition of this date/time value
   */
  public String toString()
  {
    return(ComponentTime.stampToString(timestamp));
  }

  /**
   * Get the date/time value as a long integer
   */
  public long getValue()
  {
    return(timestamp);
  }

  public Date toDate()
  {
    return(ComponentTime.stringToDate(this.toString().substring(0,23)));
  }

  /**
   *  Construct a long-integer timestamp with the following fields:
   *
   *     0-19  microsecond (0-999999)
   *    20-25  second      (0-63)
   *    26-31  minute      (0-59)
   *    32-36  hour        (0-23)
   *    37-41  day         (1-31)
   *    42-45  month       (1-12)
   *    46-59  year        (0-9999)
   *    60-62  (reserved for "exponent")
   *    63     (reserved for BC dates)
   */
  private static long packBits(int year, int month, int day,
                               int hour, int minute, int second,
                               int microsecond)
  {
    long tstamp = (long)microsecond; // UPOS

    tstamp |= ((long)year)   << YPOS;
    tstamp |= ((long)month)  << MPOS;
    tstamp |= ((long)day)    << DPOS;
    tstamp |= ((long)hour)   << HPOS;
    tstamp |= ((long)minute) << IPOS;
    tstamp |= ((long)second) << SPOS;

    return(tstamp);
  }

  /**
   * Unpack a bit-packed timestamp
   * ### Unpack into an int array. An alternative would
   *     be to use an inner class to represent the timestamp.
   */
  private static void unpackBits(long tstamp, int[] ymd_hms_f)
  {
    int year   = (int)(tstamp >> YPOS) & YMASK;
    int month  = (int)(tstamp >> MPOS) & MMASK;
    int day    = (int)(tstamp >> DPOS) & DMASK;
    int hour   = (int)(tstamp >> HPOS) & HMASK;
    int minute = (int)(tstamp >> IPOS) & IMASK;
    int second = (int)(tstamp >> SPOS) & SMASK;
    int microsec = (int)(tstamp) & UMASK;

    ymd_hms_f[YIDX] = year;
    ymd_hms_f[MIDX] = month;
    ymd_hms_f[DIDX] = day;
    ymd_hms_f[HIDX] = hour;
    ymd_hms_f[IIDX] = minute;
    ymd_hms_f[SIDX] = second;
    ymd_hms_f[UIDX] = microsec;
  }

  private static void throwException(String badDate)
  {
    throw(new IllegalArgumentException(Message.EX_ILLEGAL_DATE_TIME.get(badDate)));
  }

  /**
   * Convenience method for converting string dates to long integers
   */
  public static long stringToStamp(String strdate)
  {
    int year = 1;
    int month = 1;
    int day = 1;
    int hour = 0;
    int minute = 0;
    int second = 0;
    int microsecond = 0;

    int len = (strdate == null) ? 0 : strdate.length();

    try
    {
      if (len == 0)
        throw (new IllegalArgumentException(Message.EX_ILLEGAL_DATE_TIME2.get()));
        
      else if (len < 4)
        year = Integer.parseInt(strdate.substring(0, len));
      else
        year = Integer.parseInt(strdate.substring(0, 4));

      if (len >= 9)
      {
        if ((strdate.charAt(4) != '-') || (strdate.charAt(7) != '-'))
          throwException(strdate);
        month = Integer.parseInt(strdate.substring(5, 7));
        day   = Integer.parseInt(strdate.substring(8, 10));
      }

      if (len >= 16)
      {
        if (((strdate.charAt(10) != 'T') && (strdate.charAt(10) != 't') &&
             (strdate.charAt(10) != ' ')) || (strdate.charAt(13) != ':'))
          throwException(strdate);
        hour   = Integer.parseInt(strdate.substring(11, 13));
        minute = Integer.parseInt(strdate.substring(14, 16));
      }

      if (len >= 19)
      {
        if (strdate.charAt(16) != ':')
          throwException(strdate);
        second = Integer.parseInt(strdate.substring(17, 19));
      }

      if (len > 20)
      {
        /* Strip off TZ if present */
        int zpos = (len > 26) ? 26 : len;
        if (strdate.charAt(zpos-1) == 'Z') --zpos;

        if (strdate.charAt(19) != '.')
          throwException(strdate);
        microsecond = Integer.parseInt(strdate.substring(20, zpos));
        for (zpos -= 20; zpos < 6; ++zpos)
          microsecond *= 10;
      }
    }
    catch (NumberFormatException e)
    {
      throwException(strdate);
    }

    if ((year > 9999) || (day > 31) || (month > 12) ||
        (hour > 23) || (minute > 59) || (second > 62) ||
        (year <= 0) || (day <= 0) || (month <= 0) ||
        (hour < 0) || (minute < 0) || (second < 0))
      throwException(strdate);

    return(ComponentTime.packBits(year, month, day,
                                  hour, minute, second,
                                  microsecond));
  }

  /**
   * Convenience method for converting component time longs to strings
   */
  private static String stampToString(long tstamp, String formatMask)
  {
    StringBuilder sb  = new StringBuilder(30);
    // ### Default locale is probably OK given
    //     our format mask. But force the locale
    //     to US just to be sure.
    Formatter     fmt = new Formatter(sb, Locale.US);

    int[] pieces = new int[NUMIDX];
    ComponentTime.unpackBits(tstamp, pieces);

    fmt.format(formatMask,
               pieces[YIDX], pieces[MIDX], pieces[DIDX],
               pieces[HIDX], pieces[IIDX], pieces[SIDX], pieces[UIDX]);
    fmt.close();

    return(sb.toString());
  }
  
  /**
   * Convenience method for converting component time longs to strings
   */
  public static String stampToString(long tstamp)
  {
    return(stampToString(tstamp, "%04d-%02d-%02dT%02d:%02d:%02d.%06dZ"));
  }

  /**
   * Max out the fractional seconds of a timestamp in case
   * we're working with an inaccurate reader. Optionally keep the
   * milliseconds and max the digits up to microseconds after that.
   */
  public static long maxFractionalSeconds(long tstamp, boolean keepMillis)
  {
    int microsec = (int)(tstamp) & UMASK; // Keep the microseconds
    tstamp &= (long)(~UMASK);             // Strip them off the stamp
    if (!keepMillis)                      // If stripping all sub-second info
      microsec = 999999;                  // Maximum microseconds
    else                                  // Else keeping up to millis
      microsec = (microsec % 1000) + 999; // Max the micros after the millis
    tstamp |= (long)microsec;             // Restore the bitfield
    return(tstamp);
  }

  /**
   * Increment a component timestamp by a specifed number of microseconds
   * (must be less than one full second)
   */
  public static long plus(long tstamp, long microseconds)
  {
    long newmicros = tstamp & ((long)UMASK);

    // If the addition doesn't roll over past fractional seconds we can add it
    if ((newmicros + microseconds) < MAX_MICROS)
    {
      tstamp += microseconds;
    }
    // Otherwise we'll need to perform a more complex rolling-field increment
    else
    {
      int[] pieces = new int[NUMIDX];
      ComponentTime.unpackBits(tstamp, pieces);
      int year   = pieces[YIDX];
      int month  = pieces[MIDX];
      int day    = pieces[DIDX];
      int hour   = pieces[HIDX];
      int minute = pieces[IIDX];
      int second = pieces[SIDX];

      // Add the microseconds
      newmicros += microseconds;

      // Compute the number of seconds of overflow
      int overseconds = (int)(newmicros / MAX_MICROS);
      // ### Revisit: what happens if this overflows remaining seconds 

      // Strip microseconds to the trailing amount
      newmicros = newmicros % MAX_MICROS;

      // Use a calendar to handle leap-years and leap-seconds.
      // Note that we subtract 1 from the month, because DB
      // uses 1 to 12 for month numbering but GregorianCalendar
      // uses 0 to 11.
      GregorianCalendar cal = new GregorianCalendar(year, month - 1, day,
                                                    hour, minute, second);

      cal.add(Calendar.SECOND, overseconds);

      second = cal.get(Calendar.SECOND);
      minute = cal.get(Calendar.MINUTE);
      hour = cal.get(Calendar.HOUR_OF_DAY);
      day = cal.get(Calendar.DAY_OF_MONTH);
      // Note that we add 1 to month, because GregorianCalendar
      // uses 0 to 11 for month numbering, but DB uses 1 to 12.
      month = cal.get(Calendar.MONTH) + 1;
      year = cal.get(Calendar.YEAR);

      tstamp = newmicros;
      tstamp |= ((long)year)   << YPOS;
      tstamp |= ((long)month)  << MPOS;
      tstamp |= ((long)day)    << DPOS;
      tstamp |= ((long)hour)   << HPOS;
      tstamp |= ((long)minute) << IPOS;
      tstamp |= ((long)second) << SPOS;
    }

    return(tstamp);
  }

  /**
   * Increment a component timestamp by a specifed number of microseconds
   * (must be less than one full second).
   * If this can't be done cheaply, returns -1L.
   */
  public static long increment(long tstamp, long microseconds)
  {
    long newmicros = tstamp & ((long)UMASK);

    // If the addition doesn't roll over past fractional seconds we can add it
    if ((newmicros + microseconds) > MAX_MICROS) return(-1L);

    return(tstamp + microseconds);
  }

  @Override
  public boolean equals(Object obj)
  {
    if (obj == null) return(false);
    if (!(obj instanceof ComponentTime)) return(false);
    ComponentTime t = (ComponentTime)obj;
    return((this.timestamp == t.timestamp));
  }

  @Override
  public int hashCode()
  {
    return((new Long(timestamp)).hashCode());
  }

  public int compareTo(ComponentTime t)
  {
    if (t == null)
      return(1);
    if (this.timestamp > t.timestamp)
      return(1);
    else if (this.timestamp < t.timestamp)
      return(-1);
    return(0);
  }
}
