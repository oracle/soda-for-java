/* Copyright (c) 2014, 2015, Oracle and/or its affiliates. 
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
 */ 

package oracle.json.parser;

public class Predicate
{
  final JsonQueryPath path;
  final String value;

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
    this.path = path;
    this.value = value;
  }
}
