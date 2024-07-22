/* Copyright (c) 2015, 2024, Oracle and/or its affiliates. */
/* All rights reserved.*/

/*
   DESCRIPTION
     Holds a value with the data type and count of instances
 */

/**
 * This class is not part of the public API, and is
 * subject to change.
 *
 * Do not rely on it in your application code.
 *
 *  @author  Doug McMahon
 */

package oracle.json.sodacommon;

import java.math.BigDecimal;
import java.util.Date;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.Instant;
import java.time.ZoneOffset;

import oracle.json.sodautil.ScalarValue;

public class DistinctValue extends ScalarValue
{
  public final int count;

  private DistinctValue(BigDecimal nval, int count)
  {
    super(nval);
    this.count = count;
  }

  private DistinctValue(String sval, int count)
  {
    super(sval);
    this.count = count;
  }

  private DistinctValue(Instant tval, int count)
  {
    super(tval);
    this.count = count;
  }

  private DistinctValue(int count)
  {
    super();
    this.count = count;
  }

  private DistinctValue(boolean bval, int count)
  {
    super(bval);
    this.count = count;
  }

  private DistinctValue(ScalarValue oldval, int count)
  {
    super(oldval);
    this.count = count;
  }

  private static int datatypeFrom(ScalarValue oldval)
  {
    if (oldval.isString())   return ScalarValue.STRING;
    if (oldval.isNumber())   return ScalarValue.NUMBER;
    if (oldval.isNull())     return ScalarValue.NULL;
    if (oldval.isBoolean())  return ScalarValue.BOOLEAN;
    if (oldval.isDateTime()) return ScalarValue.DATETIME;
    return ScalarValue.BINARY;
  }

  public int datatype()
  {
    // ### Just to prop up old callers
    return DistinctValue.datatypeFrom(this);
  }
  
  public static DistinctValue createString(String sval, int count)
  {
    return new DistinctValue(sval, count);
  }

  public static DistinctValue createNumber(BigDecimal nval, int count)
  {
    return new DistinctValue(ScalarValue.numberFrom(nval), count);
  }

  public static DistinctValue createDateTime(String sval, int count)
  {
    return new DistinctValue(ScalarValue.timestampFrom(sval), count);
  }

  public static DistinctValue createBoolean(boolean bval, int count)
  {
    return new DistinctValue(bval, count);
  }

  public static DistinctValue createNull(int count)
  {
    return new DistinctValue(count);
  }
      
  /**
   * This converts the value to a Java "native" type.
   *   null      => null
   *   string    => String
   *   boolean   => Boolean
   *   date/time => Instant
   *   number    => int/long/double/BigDecimal
   * Numeric conversions are "best-effort".
   */
  public Object getNativeValue()
  {
    if (this.isString())
      return this.stringValue();
    else if (this.isBoolean())
      return this.booleanValue();
    else if (this.isDate())
      return this.timestampValue();
    else if (this.isDateTime())
      return this.timestampValue();
    else if (this.isBinary())
      return this.binaryValue();
    else if (this.isNull())
      return null;
    else if (this.isInteger())
      return this.val;
    else if (this.isLong())
      return this.val;
    else if (this.isDouble())
      return this.val;
    else if (this.isFloat())
      return this.val;
    else if (this.isDecimalNumber())
      return this.val;
    else if (!this.isNumber())
      return null;                  // ### Really an error

    BigDecimal nval = this.numberValue();

    try
    {
      // Try directly using the subtype information
      if (subType == ScalarValue.DECIMAL)
        return nval;
      else if (subType == ScalarValue.INTEGER)
        return new Integer(nval.intValueExact());
      else if (subType == ScalarValue.LONG)
        return new Long(nval.longValueExact());
      else if (subType == ScalarValue.FLOAT)
        return new Float(nval.floatValue());
      else if (subType == ScalarValue.DOUBLE)
        return new Double(nval.doubleValue());
      // Otherwise use best-fit for the NUMBER case
    }
    catch (ArithmeticException e)
    {
      // Ignore and try best-fit heuristics
    }
    // Otherwise we need to find a "best-fit" Java numeric type

    BigDecimal canonicalValue = nval.stripTrailingZeros();
    int        precision      = canonicalValue.precision();
    int        scale          = canonicalValue.scale();

    // If it's got fractional digits, or a lot of significant digits,
    // it can't be an integer or long
    if ((scale > 0) || (precision > 34))
    {
      // For less precise values, attempt to convert them to double
      // Also if the precision has too many digits for a decimal128
      if ((precision <= 15) || (precision > 34))
      {
        double dbl = nval.doubleValue();
        if ((dbl != Double.NEGATIVE_INFINITY) && 
            (dbl != Double.POSITIVE_INFINITY))
          return new Double(dbl);
        // If this hit a range boundary, fall through to BigDecimal
      }
    }
    // Otherwise it might fit in an integer or long
    else
    {
      int idigits = precision - scale;

      // See if it fits as an integer
      if (idigits <= 10)
      {
        try
        {
          int ival = nval.intValueExact();
          return new Integer(ival);
        }
        catch (ArithmeticException e)
        {
          // Fall through to long
        }
      }
      // See if it fits as a long
      if (idigits <= 19)
      {
        try
        {
          long lval = nval.longValueExact();
          return new Long(lval);
        }
        catch (ArithmeticException e)
        {
          // Fall through to BigDecimal
        }
      }
    }
    return nval;
  }
}
