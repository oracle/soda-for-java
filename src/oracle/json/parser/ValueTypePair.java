/* Copyright (c) 2014, 2015, Oracle and/or its affiliates. 
All rights reserved.*/

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

public class ValueTypePair
{
  private String stringValue;  
  private boolean booleanValue;
  private BigDecimal numberValue;

  private final int type;
  
  public static final int TYPE_NUMBER  = 1;
  public static final int TYPE_STRING  = 2;
  public static final int TYPE_BOOLEAN = 3;
  public static final int TYPE_NULL    = 4;

  ValueTypePair(int type)
  {
    this.type = type;
  }

  ValueTypePair(String value, int type)
  {
    this(type);
    this.stringValue = value;
  }

  ValueTypePair(boolean value, int type)
  {
    this(type);
    this.booleanValue = value;
  }

  ValueTypePair(BigDecimal value, int type)
  {
    this(type);
    this.numberValue = value;
  }

  public String getStringValue() 
  {
    return stringValue;
  }

  public boolean getBooleanValue()
  {
    return booleanValue;
  }

  public BigDecimal getNumberValue()
  {
    return numberValue;
  }

  public int getType()
  {
    return type;
  }

  void setBooleanValue(boolean value) 
  {
    this.booleanValue = value;
  }

  void setStringValue(String value) 
  {
    this.stringValue = value;
  }

  void setNumberValue(BigDecimal value) 
  {
    this.numberValue = value;
  }

  public static String getStringType(int type) 
  {
    String stringType;
    switch (type)
    {
      case TYPE_STRING:
        stringType = "String";
        break;
      case TYPE_BOOLEAN:
        stringType = "Boolean";
        break;
      case TYPE_NUMBER:
        stringType = "Number";
        break;
      case TYPE_NULL:
        stringType = "Null";
        break;
      default:
        stringType = "UNKNOWN";
        break;
    }
    return stringType;
  }
}
