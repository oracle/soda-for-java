/* Copyright (c) 2014, 2024, Oracle and/or its affiliates. */
/* All rights reserved.*/

/*
   DESCRIPTION
     QBE Value-type pair class to track fieldvalue and datatypes
 */

/**
 * This class is not part of the public API, and is
 * subject to change.
 *
 * Do not rely on it in your application code.
 *
 * @author Rahul Manohar Kadwe
 */

package oracle.json.parser;

import java.math.BigDecimal;

import jakarta.json.JsonBuilderFactory;
import jakarta.json.JsonValue;
import jakarta.json.JsonValue.ValueType;
import oracle.json.util.ComponentTime;

public class ValueTypePair
{
  /** Static singleton is ok since just using for constructing primitives */
  private static JsonBuilderFactory FACTORY;
  
  JsonValue value;
  
  private boolean dateFlag      = false;
  private boolean timestampFlag = false;

  ValueTypePair(JsonValue value)
  {
    this.value = value;
  }

  ValueTypePair(String value) {
    this(FACTORY.createArrayBuilder().add(value).build().get(0));
  }
  
  public ValueTypePair(BigDecimal dec) {
    this(FACTORY.createArrayBuilder().add(dec).build().get(0));
  }

  public JsonValue getValue() {
    return value;
  }
  
  void setValue(JsonValue value) {
    this.value = value;
  }
  
  public boolean isTimestamp() {
    return(timestampFlag);
  }

  public boolean isDate() {
    return(dateFlag);
  }
  
  public boolean isNumber() {
    return value.getValueType() == ValueType.NUMBER;
  }

  public boolean isBoolean() {
    return value.getValueType() == ValueType.TRUE || 
           value.getValueType() == ValueType.FALSE;
    
  }

  public boolean isString() {
    return value.getValueType() == ValueType.STRING;
  }
  
  public boolean isObject() {
    return value.getValueType() == ValueType.OBJECT;
  }
  
  public boolean isArray() {
    return value.getValueType() == ValueType.ARRAY;
  }
  
  public BigDecimal getNumberValue() {
    return isNumber() ? ((jakarta.json.JsonNumber)value).bigDecimalValue() : null;
  }
  
  public String getStringValue() {
    return isString() ? ((jakarta.json.JsonString)value).getString() : null;
  }

  public boolean getBooleanValue() {
    return JsonValue.TRUE == value;
  }

  static ValueTypePair makeTemporal(ValueTypePair bval, boolean isTimestamp)
  {
    if (bval.isNumber())
    {
      BigDecimal dval = bval.getNumberValue();
      //
      // ### This conversion assumes we want to treat numeric "dates"
      // ### as tick counts (milliseconds since 1970).
      // ### This is inconsistent with Oracle SQL's treatment of
      // ### them (as floating-point Julian days). For now we will
      // ### use this approach because Java has facilities for
      // ### that.
      //
      String tsval = ComponentTime.millisToString(dval.longValue());
      if (!isTimestamp)
      {
        int zloc = tsval.indexOf('Z');
        if (zloc > 0) tsval = tsval.substring(0, zloc);
      }
      
      bval = new ValueTypePair(tsval);
    }
    // Assumes if it's a TYPE_STRING it's in an acceptable format

    if (isTimestamp)
      bval.timestampFlag = true;
    else
      bval.dateFlag = true;

    return(bval);
  }
  
  static {
    FACTORY = jakarta.json.Json.createBuilderFactory(null);
  }


}
