/* Copyright (c) 2014, 2018, Oracle and/or its affiliates. 
All rights reserved.*/

/*
   DESCRIPTION
     QBE Predicate class
 */

/**
 * This class is not part of the public API, and is
 * subject to change.
 *
 * Do not rely on it in your application code.
 *
 * @author Rahul Manohar Kadwe
 * @author Doug McMahon
 * @author Max Orgiyan
 */ 

package oracle.json.parser;

public class Predicate
{

  final JsonQueryPath path;
  final String        value;
  final String        returnType;

  final String errorClause;

  public JsonQueryPath getQueryPath()
  {
    return path;
  }

  public String[] getPathSteps()
  {
    return path.getSteps();
  }

  public String getPath()
  {
    return path.toString();
  }

  public String getValue()
  {
    return value;
  }

  /* Return string is suitable for a SQL/JSON returning clause */
  public String getReturnType()
  {
    return returnType;
  }

  /* Return string is suitable for a SQL/JSON error clause */
  public String getErrorClause() {return errorClause; }

  Predicate()
  {
    this(null);
  }

  Predicate(JsonQueryPath path)
  {
    this(path, null);
  }

  Predicate(JsonQueryPath path, String value)
  {
    this(path, value, null, null);
  }

  /**
   * The value string is either:
   *   "1" or "-1" for asc/desc flag in order by predicates, or
   *   a modifier such as "double", "upper", "type", etc.
   * The return type is one of:
   *   "number", "date", "timestamp", "varchar2", or "varchar2(___)"
   * The error parameter is one of the error clause
   *   constants defined above.
   */
  Predicate(JsonQueryPath path, String value, String returnType, String errorClause)
  {
    this.path = path;
    this.value = value;
    this.returnType = returnType;
    this.errorClause = errorClause;
  }
}
