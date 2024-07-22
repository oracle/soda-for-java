/* Copyright (c) 2015, 2024, Oracle and/or its affiliates. */
/* All rights reserved.*/

/*
   DESCRIPTION
     Holds a scalar value that may be one of several supported
     types (e.g. an UntypedAtomic value). This is mainly intended
     to convey values for use as bind variables in SQL. Another use
     may be to convey distinct return values from SELECT statements.

   NOTES
     The types are grouped into families for the purposes of
     collation/comparison semantics as follows:
     - boolean
     - numeric
     - string
     - date/time
     - binary/raw
 */

/**
 * This class is not part of the public API, and is
 * subject to change.
 *
 * Do not rely on it in your application code.
 *
 *  @author  Doug McMahon
 */

package oracle.json.sodautil;

import java.math.BigDecimal;
import java.util.Date;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.Instant;
import java.time.ZoneOffset;

import oracle.json.sodautil.ComponentTime;

public class ScalarValue implements Comparable
{
  private static final ScalarValue NULL_VALUE  = new ScalarValue();
  private static final ScalarValue FALSE_VALUE = new ScalarValue(false);
  private static final ScalarValue TRUE_VALUE  = new ScalarValue(true);

  protected final Object  val;

  // Internally all date and timestamps are stored as Instants
  // and all numeric values are stored as BigDecimals. So the
  // original sub-type is recorded here.

  protected final int subType;

  public static final int NULL      = 0;

  public static final int BOOLEAN   = 1;

  public static final int INTEGER   = 2;
  public static final int LONG      = 3;
  public static final int FLOAT     = 4;
  public static final int DOUBLE    = 5;
  public static final int DECIMAL   = 6;

  public static final int DATE      = 7;
  public static final int TIMESTAMP = 8;

  public static final int STRING    = 9;
  public static final int BINARY    = 10;

  public static final int NUMBER    = 11;  /* Any numeric type */
  public static final int DATETIME  = 12;  /* Any date/time type */

  protected ScalarValue(ScalarValue old)
  {
    this.val = old.val;
    this.subType = old.subType;
  }

  /**
   * Create a scalar value representing a null
   */
  protected ScalarValue()
  {
    this.val = null;
    this.subType = ScalarValue.NULL;
  }

  public ScalarValue(Boolean v)
  {
    this(v.booleanValue());
  }

  public ScalarValue(boolean v)
  {
    this.val = v ? Boolean.TRUE : Boolean.FALSE;
    this.subType = ScalarValue.BOOLEAN;
  }

  public ScalarValue(Integer v)
  {
    this.val = v;
    this.subType = ScalarValue.INTEGER;
  }

  public ScalarValue(int v)
  {
    this(new Integer(v));
  }

  public ScalarValue(Long v)
  {
    this.val = v;
    this.subType = ScalarValue.LONG;
  }

  public ScalarValue(long v)
  {
    this(new Long(v));
  }

  public ScalarValue(Float v)
  {
    this.val = v;
    this.subType = ScalarValue.FLOAT;
  }

  public ScalarValue(float v)
  {
    this(new Float(v));
  }

  public ScalarValue(Double v)
  {
    this.val = v;
    this.subType = ScalarValue.DOUBLE;
  }

  public ScalarValue(double v)
  {
    this(new Double(v));
  }

  public ScalarValue(String v)
  {
    this.val = v;
    this.subType = ScalarValue.STRING;
  }

  private ScalarValue(BigDecimal v, int subType)
  {
    this.val = v;
    this.subType = subType;
  }

  public ScalarValue(BigDecimal v)
  {
    this(v, ScalarValue.DECIMAL);
  }

  private ScalarValue(Instant v, int subType)
  {
    this.val = v;
    this.subType = subType;
  }

  /* Internally this is treated as a date */
  public ScalarValue(Date v)
  {
    this(ComponentTime.stringToInstant(ComponentTime.dateToString(v)),
         ScalarValue.DATE);
  }

  /* Internally this is treated as a timestamp */
  public ScalarValue(Instant v)
  {
    this(v, ScalarValue.TIMESTAMP);
  }

  public ScalarValue(OffsetDateTime v)
  {
    this(v.toInstant());
  }

  public ScalarValue(byte[] v)
  {
    this.val = v;
    this.subType = ScalarValue.BINARY;
  }

  public boolean isNull()
  {
    return (val == null);
  }

  public boolean isBoolean()
  {
    if (val == null) return false;
    return (val instanceof Boolean);
  }

  public boolean isString()
  {
    if (val == null) return false;
    return (val instanceof String);
  }

  /* Returns true if this is one of the numeric types */
  public boolean isNumber()
  {
    if (val == null) return false;
    if (val instanceof BigDecimal) return true;
    if (val instanceof Double) return true;
    if (val instanceof Float) return true;
    if (val instanceof Integer) return true;
    if (val instanceof Long) return true;
    return false;
  }

  /* Returns true if this is one of the date/timestamp types */
  public boolean isDateTime()
  {
    if (val == null) return false;
    return (val instanceof Instant);
  }

  public boolean isBinary()
  {
    if (val == null) return false;
    return (val instanceof byte[]);
  }

  /* Return the boolean value, or null if not a boolean */
  public Boolean booleanValue()
  {
    if (!isBoolean()) return null;
    return (Boolean)val;
  }

  /* Return the string value, or null if not a string */
  public String stringValue()
  {
    if (!isString()) return null;
    return (String)val;
  }

  /* Return the numeric value as a BigDecimal, or null if not a number */
  public BigDecimal numberValue()
  {
    if (!isNumber())
      return null;
    else if (val instanceof Double)
      return BigDecimal.valueOf((Double)val);
    else if (val instanceof Float)
      return BigDecimal.valueOf((Float)val);
    else if (val instanceof Integer)
      return BigDecimal.valueOf((Integer)val);
    else if (val instanceof Long)
      return BigDecimal.valueOf((Long)val);
    return (BigDecimal)val;
  }

  /* Return a date/time value as a LocalDateTime, or null if not a date/time */
  public LocalDateTime dateTimeValue()
  {
    if (!isDateTime()) return null;
    return ((Instant)val).atOffset(ZoneOffset.UTC).toLocalDateTime();
  }

  /* Return a date/time value as an Instant, or null if not a date/time */
  protected Instant timestampValue()
  {
    if (!isDateTime()) return null;
    return (Instant)val;
  }

  /* Return the binary value as a byte array, or null if not binary */
  public byte[] binaryValue()
  {
    if (!isBinary()) return null;
    return (byte[])val;
  }

  /**
   * Returns true if this is a BigDecimal, false otherwise
   * (i.e. for int/long/float/double). Typically this would
   * be used to bind a BINARY_DOUBLE instead of a NUMBER.
   */
  public boolean isDecimalNumber()
  {
    return (subType == ScalarValue.DECIMAL);
  }

  /* Returns true if this is the double type */
  public boolean isDouble()
  {
    return (subType == ScalarValue.DOUBLE);
  }

  /* Returns true if this is the float type */
  public boolean isFloat()
  {
    return (subType == ScalarValue.FLOAT);
  }

  /* Returns true if this is the integer type */
  public boolean isInteger()
  {
    return (subType == ScalarValue.INTEGER);
  }

  /* Returns true if this is the long type */
  public boolean isLong()
  {
    return (subType == ScalarValue.LONG);
  }

  /**
   * Returns true if this is a Date, false otherwise.
   * Typically used to distinguish a DATE from a TIMESTAMP binding.
   */
  public boolean isDate()
  {
    return (subType == ScalarValue.DATE);
  }

  @Override
  public String toString()
  {
    if (val == null) return "null"; // This is a JSON null
    if (val instanceof String) return (String)val;
    if (val instanceof BigDecimal)
      return ((BigDecimal)val).toString();
    if (val instanceof Instant)
      return ComponentTime.instantToString((Instant)val, false,
                                           (subType == ScalarValue.DATE));
    if (val instanceof byte[])
      return ByteArray.rawToHex((byte[])val);
    if (val instanceof Boolean)
      return ((Boolean)val).booleanValue() ? "true" : "false";
    // ### Impossible?
    return null;
  }

  @Override
  public int hashCode()
  {
    if (val == null) return 0;
    return val.hashCode();
  }

  @Override
  public boolean equals(Object o)
  {
    return (this.compareTo(o) == 0);
  }

  @Override
  public int compareTo(Object o)
  {
    ScalarValue v;

    // If the target is a scalar value, cast it
    if (o instanceof ScalarValue)
      v = (ScalarValue)o;
    // Otherwise if possible coerce it to a ScalarValue
    else
    {
      v = ScalarValue.from(o);
      // Otherwise this instance is deemed greater
      if (v == null) return 1;
    }

    // Special case for NULL and TRUE/FALSE
    if (this.val == v.val) return 0;

    // NULL sorts below anything else
    if (this.val == null)   return -1;
    else if (v.val == null) return  1;

    // Booleans sort just after NULL, FALSE then TRUE
    if (this.val == Boolean.FALSE)   return -1;
    else if (v.val == Boolean.FALSE) return  1;
    if (this.val == Boolean.TRUE)    return -1;
    else if (v.val == Boolean.TRUE)  return  1;

    // Numbers sort next
    if (this.val instanceof BigDecimal)
    {
      if (!(v.val instanceof BigDecimal)) return -1;
      return ((BigDecimal)val).compareTo((BigDecimal)v.val);
    }
    else if (v.val instanceof BigDecimal)
      return 1;

    // Date/time values sort next
    if (this.val instanceof Instant)
    {
      if (!(v.val instanceof Instant)) return -1;
      return ((Instant)val).compareTo((Instant)v.val);
    }
    else if (v.val instanceof Instant)
      return 1;

    // Strings are next
    if (this.val instanceof String)
    {
      if (!(v.val instanceof String)) return -1;
      return ((String)val).compareTo((String)v.val);
    }
    else if (v.val instanceof String)
      return 1;

    // Otherwise these are both binaries
    return ByteArray.compareBytes((byte[])this.val, (byte[])v.val);
  }

  public static ScalarValue dateFrom(long millis)
  {
    return ScalarValue.dateFrom(ComponentTime.millisToString(millis));
  }

  public static ScalarValue dateFrom(String isostr)
  {
    return new ScalarValue(ComponentTime.stringToInstant(isostr),
                           ScalarValue.DATE);
  }

  public static ScalarValue dateFrom(ScalarValue sval)
  {
    if (sval == null)
      return sval;
    else if (sval.isDate())
      return sval;
    else if (sval.isDateTime())
      return new ScalarValue(sval.timestampValue(), ScalarValue.DATE);
    else if (sval.isString())
      return dateFrom(sval.stringValue());
    else if (sval.isNumber())
      return dateFrom(sval.numberValue().longValue());
    return null;
  }

  public static ScalarValue timestampFrom(long millis)
  {
    return ScalarValue.timestampFrom(ComponentTime.millisToString(millis));
  }

  public static ScalarValue timestampFrom(String iso)
  {
    return new ScalarValue(ComponentTime.stringToInstant(iso));
  }

  public static ScalarValue timestampFrom(ScalarValue sval)
  {
    if (sval == null)
      return sval;
    else if (sval.isDate())
      return new ScalarValue(sval.timestampValue(), ScalarValue.TIMESTAMP);
    else if (sval.isDateTime())
      return sval;
    else if (sval.isString())
      return timestampFrom(sval.stringValue());
    else if (sval.isNumber())
      return timestampFrom(sval.numberValue().longValue());
    return null;
  }

  public static ScalarValue stringFrom(BigDecimal decval)
  {
    return new ScalarValue(decval.toString());
  }

  public static ScalarValue stringFrom(ScalarValue sval)
  {
    if (sval == null) return sval;
    if (sval.isString()) return sval;
    return new ScalarValue(sval.toString());
  }

  public static ScalarValue numberFrom(BigDecimal decval)
  {
    return new ScalarValue(decval, ScalarValue.NUMBER);
  }

  public static ScalarValue decimalFrom(BigDecimal decval)
  {
    return new ScalarValue(decval);
  }

  public static ScalarValue numberFrom(String str)
  {
    return ScalarValue.numberFrom(new BigDecimal(str));
  }

  public static ScalarValue decimalFrom(String str)
  {
    return ScalarValue.decimalFrom(new BigDecimal(str));
  }

  public static ScalarValue doubleFrom(String str)
  {
    return ScalarValue.doubleFrom(new BigDecimal(str));
  }

  public static ScalarValue doubleFrom(BigDecimal decval)
  {
    return new ScalarValue(decval, ScalarValue.DOUBLE);
  }

  public static ScalarValue doubleFrom(ScalarValue sval)
  {
    if (sval == null)
      return sval;
    else if (sval.isDouble())
      return sval;
    else if (sval.isNumber())
      return ScalarValue.doubleFrom(sval.numberValue());
    else if (sval.isString())
      return ScalarValue.doubleFrom(sval.stringValue());

    return null;
  }

  public static ScalarValue decimalFrom(ScalarValue sval)
  {
    BigDecimal bval = null;

    if (sval == null)
      return sval;
    else if (sval.isDecimalNumber())
      return sval;
    else if (sval.isNumber())
      bval = sval.numberValue();
    else if (sval.isString())
      bval = new BigDecimal(sval.stringValue());
    else if (sval.isDateTime())
      bval = new BigDecimal(((Instant)sval.val).toEpochMilli());

    if (bval != null)
      return new ScalarValue(bval);

    return null;
  }

  public static ScalarValue numberFrom(ScalarValue sval)
  {
    BigDecimal bval = null;

    if (sval == null)
      return sval;
    else if (sval.subType == ScalarValue.NUMBER)
      return sval;
    else if (sval.isNumber())
      bval = sval.numberValue();
    else if (sval.isString())
      bval = new BigDecimal(sval.stringValue());
    else if (sval.isDateTime())
      bval = new BigDecimal(((Instant)sval.val).toEpochMilli());

    if (bval != null)
      return new ScalarValue(bval, ScalarValue.NUMBER);

    return null;
  }

  public static ScalarValue binaryFrom(ScalarValue sval)
  {
    if (sval == null) return sval;
    if (sval.isString())
      return new ScalarValue(ByteArray.hexToRaw(sval.stringValue()));
    return null;
  }

  // Force the binding for non-JSON scalar types to be string or number
  public static ScalarValue simplify(ScalarValue sval)
  {
    if (sval == null)
      return sval;
    if (sval.isNumber() || sval.isString() || sval.isBoolean())
      return sval;
    return ScalarValue.stringFrom(sval);
  }

  public static ScalarValue from(Object v)
  {
    if (v == null)
      return ScalarValue.NULL_VALUE;
    if (v instanceof Boolean)
      return ((Boolean)v).booleanValue() ? ScalarValue.TRUE_VALUE :
                                           ScalarValue.FALSE_VALUE;
    if (v instanceof String)
      return new ScalarValue((String)v);
    if (v instanceof BigDecimal)
      return new ScalarValue((BigDecimal)v);
    if (v instanceof Instant)
      return new ScalarValue((Instant)v);
    if (v instanceof Date)
      return new ScalarValue((Date)v);
    if (v instanceof Integer)
      return new ScalarValue((Integer)v);
    if (v instanceof Long)
      return new ScalarValue((Long)v);
    if (v instanceof Float)
      return new ScalarValue((Float)v);
    if (v instanceof Double)
      return new ScalarValue((Double)v);
    if (v instanceof byte[])
      return new ScalarValue((byte[])v);
    return null;
  }
}
