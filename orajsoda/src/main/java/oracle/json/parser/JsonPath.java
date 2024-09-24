/* Copyright (c) 2014, 2015, Oracle and/or its affiliates. 
All rights reserved.*/

/*
   DESCRIPTION
    This allows a path to be specified as a series of string member name steps.
 */

/**
 * This class is not part of the public API, and is
 * subject to change.
 *
 * Do not rely on it in your application code.
 *
 * @author  Doug McMahon
 */

package oracle.json.parser;

import java.util.List;

public class JsonPath
{
  protected String[] steps;

  public JsonPath(String step)
  {
    steps = new String[1];
    steps[0] = step;
  }

  public JsonPath(String[] steps)
  { 
    this.steps = steps;
  }

  public JsonPath(List<String> steps)
  {
    this.steps = steps.toArray(new String[steps.size()]);
  }

  public String[] getSteps()
  {
    return(steps);
  }
}
