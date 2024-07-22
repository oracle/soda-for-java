/* Copyright (c) 2014, 2024, Oracle and/or its affiliates. */
/* All rights reserved.*/

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
  private boolean  expandArray = false;

  private boolean isArrayStep(String aStep)
  {
    if (aStep == null) return false;
    if (aStep.length() == 0) return false;
    return (aStep.charAt(0) == '[');
  }

  private void processSteps(int pathlen)
  {
    if (steps == null) return;

    StringBuilder sb = new StringBuilder(pathlen + steps.length);

    for (int i = 0; i < steps.length; ++i)
    {
      // Remember if the final step is an array step
      arrayEnd = isArrayStep(steps[i]);

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


  public JsonQueryPath(String[] steps)
  {
    this.steps = steps;

    this.processSteps(100);
  }

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

      this.processSteps(path.length());
    }
  }

  public JsonQueryPath()
    throws QueryException
  {
    this((String)null);
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

  void setArrayExpansion()
  {
    expandArray = true;
  }

  /**
   * Returns the path as a string suitable for following $ or @
   * For an array-leading path this will be of the form "[123].x.y"
   * For other paths, it will be of the form ".x.y"
   * If the isLeaf parameter is set to true, this will also append
   * a trailing wildcard step [*] for paths that do not end in an
   * array step.
   */
  String toQueryString(boolean isLeaf)
  {
    String adjustedPath;

    if (path == null)
      adjustedPath = "";
    else if (arrayBegin)  // Path is of the form $[ ]...
      adjustedPath = path;
    else                  // Path is of the form $.<rest of path>
      adjustedPath = "." + path;

    // Path must end with ...[*]
    if (isLeaf) adjustedPath += "[*]";

    return (adjustedPath);
  }

  String toQueryString()
  {
    return toQueryString(expandArray);
  }

  String toQueryString(String modifier) {
    return toQueryString(modifier, null);
  }
  
  String toQueryString(String modifier, String funcParam)
  {
    String qry = toQueryString();
    
    if (modifier != null) 
    {
      if (funcParam != null)
        qry = qry + "." + modifier + "(" + funcParam + ")";
      else 
        qry = qry + "." + modifier + "()";
    }

    return(qry);
  }

  /**
   * Build a path suitable for JSON_TextContains, eliminating
   * all array steps.
   */
  public void toLaxString(StringBuilder sb)
  {
    toLaxString(sb, false);
  }

  /**
   * Build a path suitable for JSON_TextContains, eliminating
   * all array steps.
   */
  public void toLaxString(StringBuilder sb, boolean isLeaf)
  {
    sb.append("$");

    for (String step : steps)
    {
      if (!isArrayStep(step))
      {
        sb.append(".");
        sb.append(step); // ### Assumes it's already SQL-escaped?
      }
    }
    if (isLeaf) sb.append("[*]");
  }

  public boolean hasArraySteps()
  {
    if (steps != null)
    {
      for (String step : steps)
        if (isArrayStep(step))
          return(true);
    }

    return(false);
  }

  /**
   * Build a path to a singleton, ignoring array steps and adding [0]
   * steps as needed. The path is appended into the supplied StringBuilder.
   * These paths are suitable for order-by clauses and index columns.
   */
  public void toSingletonString(StringBuilder sb)
  {
    toSingletonString(sb, false);
  }

  /**
   * Build a path to a singleton, ignoring array steps and optionally adding
   * [0] steps as needed. The path is appended into the supplied StringBuilder.
   * These paths are suitable for order-by clauses and index columns.
   */
  public void toSingletonString(StringBuilder sb, boolean forceSingle)
  {
    sb.append("$");

    for (String step : steps)
    {
      if (!isArrayStep(step))
      {
        sb.append(".");
        sb.append(step);    // ### Assumes it's already SQL-escaped?
        if (forceSingle)
          sb.append("[0]"); // Guarantee singleton cardinality at each step
      }
      // ### Else OK to silently ignore array steps?
    }
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
