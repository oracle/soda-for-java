/* Copyright (c) 2014, 2015, Oracle and/or its affiliates. 
All rights reserved.*/

/*
   DESCRIPTION
     QBE Query path
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

public class JsonQueryPath 
{
  private String[] steps = null;
  private String   path  = null;
  private boolean  arrayEnd = false;
  private boolean  arrayBegin = false;

  public JsonQueryPath(String path)
    throws QueryException
  {
    if (path != null)
    {
      steps = (new PathParser(path)).splitAndSQLEscape();

      if (steps == null)
        throw new QueryException(QueryMessage.EX_BAD_PATH.get(path));

      if (steps.length == 0)
        throw new QueryException(QueryMessage.EX_EMPTY_PATH.get(path));

      StringBuilder sb = new StringBuilder(path.length()+steps.length);
      for (int i = 0; i < steps.length; ++i)
      {
        // Remember if the final step is an array step
        arrayEnd = (steps[i].charAt(0) == '[');

        if (i == 0)
          // Save whether the first step is an array
          arrayBegin = arrayEnd;
        else if (!arrayEnd)
          // If it's not an array step, we need a dot separator
          sb.append(".");

        sb.append(steps[i]);
      }

      this.path = sb.toString();
    }
  }

  public JsonQueryPath()
    throws QueryException
  {
    this(null);
  }

  @Override
  public boolean equals(Object pathObj)
  {
    if (pathObj == null) return(false);

    if (this == pathObj) return(true);

    JsonQueryPath pobj = (JsonQueryPath)pathObj;

    if (path == pobj.path) return(true);

    if ((pobj.steps == null) || (steps == null))
      return(false);

    if (pobj.steps.length != steps.length) return(false);

    for (int i = 0; i < steps.length; ++i)
      if (!steps[i].equals(pobj.steps[i]))
        return(false);

    return(true);
  }

  /**
   * Returns the path as a string suitable for following $
   * For an array-leading path this will be of the form "[123].x.y"
   * For other paths, it will be of the form ".x.y"
   * If the isLeaf parameter is set to true, this will also append
   * a trailing wildcard step [*] for paths that do not end in an
   * array step.
   */
  String toQueryString(boolean isLeaf)
  {
    if ((arrayEnd) || (!isLeaf))
    {
      if (arrayBegin)
        return (path);       // Path will be of the form $[ ]...
      else
        return ("." + path); // Path needs to be $.<rest of path>
    }

    if (arrayBegin)
      return (path + "[*]");     // Path needs a trailing [*]

    return ("." + path + "[*]"); // Path is of the form $.<rest of path>[*]
  }

  public String[] getSteps()
  {
    return(steps);
  }

  @Override
  public int hashCode()
  {
    if (path == null) return(0);
    return(path.hashCode());
  }

  @Override
  public String toString()
  {
    return (path);
  }
}
