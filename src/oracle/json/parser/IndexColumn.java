/* Copyright (c) 2014, 2015, Oracle and/or its affiliates. 
All rights reserved.*/

/*
   DESCRIPTION
    Define class that holds the structure of an index specification.
 */

/**
 * This class is not part of the public API, and is
 * subject to change.
 *
 * Do not rely on it in your application code.
 *
 *  @author  Doug McMahon
 */

package oracle.json.parser;

import java.util.List;

public class IndexColumn extends JsonPath
{
  public static final int SQLTYPE_NONE       = 0;
  public static final int SQLTYPE_CHAR       = 1;
  public static final int SQLTYPE_NUMBER     = 2;
  public static final int SQLTYPE_DATE       = 3;
  public static final int SQLTYPE_TIMESTAMP  = 4;
  
  public static final int KEY_NAME         = 1;
  public static final int KEY_UNIQUE       = 2;
  public static final int KEY_COLUMNS      = 3;
  
  public static final int MAX_CHAR_COLUMNS    = 16;
  
  public static final String ASC_ORDER  = "ASC";
  public static final String DESC_ORDER = "DESC";
  
  private int sqlType   = SQLTYPE_CHAR;
  private int maxLength = 0;
  private String order  = ASC_ORDER;
  

  public IndexColumn()
  {
    super((String)null);
  }

  public IndexColumn(String idxName, boolean u, String step)
  {
    super(step);
  }

  public IndexColumn(String idxName, boolean u, String[] steps)
  {
    super(steps);
  }

  public IndexColumn(String idxName, boolean u, List<String> steps)
  {
    super(steps);
  }

  public int getSqlType()
  {
    return this.sqlType;
  }

  public int getMaxLength() 
  {
    return this.maxLength;
  }

  public String getOrder() 
  {
    return this.order;
  }

  public String getSqlTypeName()
  {
    switch (sqlType)
    {
    case SQLTYPE_CHAR:      return("VARCHAR2");
    case SQLTYPE_NUMBER:    return("NUMBER");
    case SQLTYPE_DATE:      return("DATE");
    case SQLTYPE_TIMESTAMP: return("TIMESTAMP");
    default:
      break;
    }
    return(null);
  }

  public void setSqlType(int sql_type)
  {
    this.sqlType = sql_type;
  }  

  public void setMaxLength(int len)
  {
    this.maxLength = len;
  }

  public void setOrder(String order)
  {
    if ((order != null) &&
        ( order.equalsIgnoreCase("DESC") || order.equalsIgnoreCase("-1")))
      this.order = DESC_ORDER;
    else 
      this.order = ASC_ORDER;
  }  

  public int setSqlType(String str) 
  {
    int sqlType = SQLTYPE_NONE;
    if (str != null) 
    {
      if (str.equalsIgnoreCase("NUMBER"))
        sqlType = SQLTYPE_NUMBER;
      else if (str.equalsIgnoreCase("DATE"))
        sqlType = SQLTYPE_DATE;
      else if ((str.equalsIgnoreCase("TIMESTAMP")) ||
               (str.equalsIgnoreCase("DATETIME")))
        sqlType = SQLTYPE_TIMESTAMP;
      else if ((str.equalsIgnoreCase("STRING")) ||
               (str.equalsIgnoreCase("VARCHAR")) ||
               (str.equalsIgnoreCase("VARCHAR2")) ||
               (str.equalsIgnoreCase("NVARCHAR")) ||
               (str.equalsIgnoreCase("NVARCHAR2")))
        sqlType = SQLTYPE_CHAR;
    }
    // Set the SQL type only if a valid type was detected
    if (sqlType != SQLTYPE_NONE)
      this.sqlType = sqlType;
    // Return the detected type, which may be unknown
    return(sqlType);
  }

  public void setPath(String[] path)
  {
    this.steps = path;
  }
}
