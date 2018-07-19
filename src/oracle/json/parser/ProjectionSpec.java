/* $Header: xdk/src/java/json/src/oracle/json/parser/ProjectionSpec.java /main/10 2018/06/15 02:15:48 morgiyan Exp $ */

/* Copyright (c) 2014, 2018, Oracle and/or its affiliates. 
All rights reserved.*/

/*
   DESCRIPTION
    Parses a projection specification and holds the resulting information.

   PRIVATE CLASSES

   NOTES
   This is used to validate projection specifications.

   MODIFIED    (MM/DD/YY)
    dmcmahon    09/10/14 - Creation
 */

/**
 *  @version $Header: xdk/src/java/json/src/oracle/json/parser/ProjectionSpec.java /main/10 2018/06/15 02:15:48 morgiyan Exp $
 *  @author  dmcmahon
 *  @since   release specific (what release of product did this appear in)
 */

package oracle.json.parser;

import java.io.InputStream;
import java.io.IOException;

import java.math.BigDecimal;

import java.util.Map.Entry;
import java.util.HashMap;
import java.util.ArrayList;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonNumber;
import javax.json.JsonValue;
import javax.json.JsonException;
import javax.json.stream.JsonParsingException;


import oracle.json.util.ByteArray;
import oracle.json.util.JsonByteArray;

public class ProjectionSpec
{
  private final InputStream source;

  private boolean       is_parsed                    = false;
  private boolean       is_checked_for_array_steps   = false;
  private boolean       is_checked_for_paths_overlap = false;
  private boolean       is_validated                 = false;
  private String        rendition                    = null;

  private boolean       includeSeen                  = false;
  private boolean       excludeSeen                  = false;
  private boolean       badValueSeen                 = false;
  private boolean       arrayStepSeen                = false;
  private boolean       overlappingPathsSeen         = false;

  private ArrayList<String[]> paths = new ArrayList<String[]>();

  public ProjectionSpec(InputStream inp)
  {
    this.source = inp;
  }

  private void makeException(QueryMessage msg, Object... params)
   throws QueryException
  {
    QueryException.throwSyntaxException(msg, params);
  }

  private void close()
    throws QueryException
  {
    try
    {
      source.close();
    }
    catch (IOException e)
    {
      throw(new QueryException(QueryMessage.EX_INVALID_PROJECTION.get(), e));
    }
  }

  /**
   * Load the projection specification without parsing it.
   */
  public String getAsString()
    throws QueryException
  {
    if (rendition == null)
    {
      try
      {
        ByteArray barr = ByteArray.loadStream(source);
        rendition = barr.getString();
      }
      catch (IOException e)
      {
        throw new QueryException(QueryMessage.EX_INVALID_PROJECTION.get(), e);
      }
    }
    return(rendition);
  }

  public boolean hasIncludeRules()
  {
    return includeSeen;
  }

  public boolean hasExcludeRules()
  {
    return excludeSeen;
  }

  public boolean hasArraySteps()
  {
    if (is_checked_for_array_steps)
      return arrayStepSeen;

    for (String[] path : paths)
    {
      arrayStepSeen = hasArrayStep(path);
      if (arrayStepSeen)
      {
        is_checked_for_array_steps = true;
        return arrayStepSeen;
      }
    }

    is_checked_for_array_steps = true;
    return arrayStepSeen;
  }

  // Check for one path being a prefix of another.
  // Note: this algorithm assumes no array steps are present
  // in the paths. We expect the caller to only call this method
  // if hasArraySteps() returns false. With array steps, the
  // algorithm is not correct (for example a[0].b and a.b might
  // be an overlapping path, depending on the data).
  public boolean hasOverlappingPaths()
  {
    if (is_checked_for_paths_overlap)
      return overlappingPathsSeen;

    whileloop:
    while (paths.size() >= 2)
    {
      for (int i = 1; i < paths.size(); i++)
      {
        if (isPrefix(paths.get(0), paths.get(i)))
        {
          overlappingPathsSeen = true;
          // Since we found a case of overlapping paths (i.e. where
          // one of the paths is a prefix of another), we can now
          // break out of both the inner "for" loop, and the outter "while"
          // loop.
          break whileloop;
        }
      }

      paths.remove(0);
    }

    is_checked_for_paths_overlap = true;

    return overlappingPathsSeen;
  }

  static boolean isPrefix(String[] path1, String[] path2)
  {
    int length = (path1.length <= path2.length ) ? path1.length : path2.length;

    for (int i = 0; i < length; i++)
    {
      // If one of both of the steps are wildcards, we
      // take a pessimistic approach and assume the steps
      // could be the same (though that's not necessarily
      // true, depending on data).
      if (!(path1[i].equals(path2[i])) && !(path1[i].equals("*") || path2[i].equals("*")))
      {
        return false;
      }
    }

    return true;
  }

  boolean hasArrayStep(String[] path)
  {
    for (String step : path)
    {
      if (step.startsWith("["))
      {
        return true;
      }
    }

    return false;
  }

  /**
   * Parse the projection specification and validate it.
   */
  public boolean validate(boolean checkPaths)
    throws QueryException
  {
    if (is_validated) return(!badValueSeen);

    // Capture it as a string first
    String src = getAsString();
    QueryException ex = null;

    includeSeen = excludeSeen = badValueSeen = false;

    try
    {
      // Source was consumed so reparse it from the rendition
      DocumentLoader loader = new DocumentLoader(src);

      Object parse = loader.parse();

      if (!(parse instanceof JsonObject))
        throw new QueryException(QueryMessage.EX_INVALID_PROJECTION.get());

      JsonObject jObj = (JsonObject)parse;

      close();

      for (Entry<String, JsonValue> entry : jObj.entrySet()) 
      {
        String    entryKey = entry.getKey();
        JsonValue entryVal = entry.getValue();

        JsonValue.ValueType vtype = entryVal.getValueType();

        if (checkPaths)
        {
          PathParser pp = new PathParser(entryKey);
          String[] parr = pp.splitAndSQLEscape();

          if (parr == null)
            makeException(QueryMessage.EX_INDEX_ILLEGAL_PATH, entryKey);

          paths.add(parr);
        }

        switch (vtype)
        {
        case TRUE:
          includeSeen = true;
          break;

        case FALSE:
          excludeSeen = true;
          break;

        case STRING:
          String sval = ((JsonString)entryVal).getString();
          if (sval.equals("include"))
            includeSeen = true;
          else if (sval.equals("exclude"))
            excludeSeen = true;
          else
            badValueSeen = true;
          break;

        case NUMBER:
          BigDecimal dval = ((JsonNumber)entryVal).bigDecimalValue();
          if (dval.compareTo(BigDecimal.ZERO) == 0)
            excludeSeen = true;
          else if (dval.compareTo(BigDecimal.ONE) == 0)
            includeSeen = true;
          else
            badValueSeen = true;
          break;

        // Disallowed constructs (ARRAY, OBJECT, NULL)
        default:
          badValueSeen = true;
          break;
        }
      }
    }
    catch (IllegalArgumentException e)
    {
      throw new QueryException(QueryMessage.EX_INVALID_PROJECTION.get(), e);
    }
    catch (JsonParsingException e)
    {
      throw new QueryException(QueryMessage.EX_INVALID_PROJECTION.get(), e);
    }
    catch (JsonException e)
    {
      throw new QueryException(QueryMessage.EX_INVALID_PROJECTION.get(), e);
    }
    finally
    {
      try
      {
        // This will only really attempt to close if an exception occurs
        close();
      }
      catch (QueryException e)
      {
        // This will be thrown after the try/catch/finally block
        // but only if this isn't already finalizing an exception case.
        ex = e;
      }
    }

    if (ex != null)
      throw ex;

    is_validated = true;

    return(!badValueSeen);
  }
}
