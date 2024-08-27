/* $Header: xdk/src/java/json/orajsoda/src/main/java/oracle/json/parser/ProjectionSpec.java /st_xdk_soda1/4 2024/08/02 02:37:36 vemahaja Exp $ */

/* Copyright (c) 2014, 2024, Oracle and/or its affiliates. */
/* All rights reserved.*/

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
 *  @version $Header: xdk/src/java/json/orajsoda/src/main/java/oracle/json/parser/ProjectionSpec.java /st_xdk_soda1/4 2024/08/02 02:37:36 vemahaja Exp $
 *  @author  dmcmahon
 *  @since   release specific (what release of product did this appear in)
 */

package oracle.json.parser;

import java.io.InputStream;
import java.io.IOException;
import java.io.StringReader;

import java.math.BigDecimal;

import java.util.Map.Entry;
import java.util.HashMap;
import java.util.ArrayList;

import jakarta.json.stream.JsonParser;
import jakarta.json.stream.JsonParserFactory;
import jakarta.json.stream.JsonParser.Event;
import jakarta.json.JsonException;

import oracle.json.common.JsonFactoryProvider;

import oracle.json.util.ByteArray;
import oracle.json.util.JsonByteArray;

public class ProjectionSpec
{
  private final InputStream       source;
  private final JsonParserFactory factory;

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

  public ProjectionSpec(JsonFactoryProvider jProvider, InputStream inp)
  {
    this.factory = jProvider.getParserFactory();
    this.source  = inp;
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
  // Note: this algorithm is blind to array steps. It will consider
  // any step to be a possible array step with a wild card. For example
  // a[0].b and a.b might be an overlapping path, depending on the data.
  // ### For now, it can't look "inside" array steps to see if they
  // ### overlap. For example a[1].b and a[2].b do not overlap, but
  // ### a[1,3].b and a[3].b do overlap, etc. Explict array steps are
  // ### deemed to not overlap, while implicit array steps are deemed
  // ### to be [*] and always overlap.
  public boolean hasOverlappingPaths()
  {
    int n = paths.size();

    if (is_checked_for_paths_overlap)
      return overlappingPathsSeen;

    // O(N-squared) check of each path against all others
    for (int j = 0; j < n; ++j)
    {
      String[] path = paths.get(j);
      for (int i = j + 1; i < n; ++i)
      {
        // ### This ignores array steps
        if (ProjectionSpec.isPrefix(path, paths.get(i)))
        {
          overlappingPathsSeen = true;
          break;
        }
      }
      // Since we found a case of overlapping paths (i.e. where
      // one of the paths is a prefix of another), we can stop looking.
      if (overlappingPathsSeen) break;
    }

    is_checked_for_paths_overlap = true;

    return overlappingPathsSeen;
  }

  private static boolean isPrefix(String[] path1, String[] path2)
  {
    int i1 = 0;
    int i2 = 0;
    int length = (path1.length <= path2.length ) ? path1.length : path2.length;

    while ((i1 < path1.length) && (i2 < path2.length))
    {
      // If both steps are array steps
      if (path1[i1].startsWith("[") && path2[i2].startsWith("["))
      {
        // ### This means we can't tell if there is "overlap" in array steps.
        // ### Do we care? The SQL/JSON select engine will give an error anyway.
        if (!path1[i1].equals(path2[i2])) return false;
        // ### For now, only catch the obvious case where they are identical.
        ++i1;
        ++i2;
        continue;
      }
      // Otherwise ignore one-sided array steps. Treat the non-array-step
      // as an implicit [*], consistent with the lax semantics of the
      // projection engine.
      if (path1[i1].startsWith("["))
      {
        ++i1;
        continue;
      }
      if (path2[i2].startsWith("["))
      {
        ++i2;
        continue;
      }

      // If one or both of the steps are wildcards, we take a pessimistic
      // approach and assume the steps could be the same (though that's not
      // necessarily true, depending on data).
      if (!(path1[i1].equals(path2[i2])) &&
          !(path1[i1].equals("*") || path2[i2].equals("*")))
        return false;
      ++i1;
      ++i2;
    }

    // If we got all the way through one of the paths
    // and all its steps matched the other, it's a prefix
    return true;
  }

  private boolean hasArrayStep(String[] path)
  {
    for (String step : path)
      if (step.startsWith("["))
        return true;

    return false;
  }

  /**
   * Parse the projection specification and validate it.
   */
  public boolean validate(boolean checkPaths)
    throws QueryException
  {
    StringReader reader = null;
    JsonParser   parser = null;

    if (is_validated) return(!badValueSeen);

    // Capture it as a string first
    String src = getAsString();
    QueryException ex = null;

    includeSeen = excludeSeen = badValueSeen = false;

    // Clear the internal paths array to reuse it
    paths.clear();

    try
    {
      int depth = 0;

      // Source was consumed so reparse it from the rendition
      reader = new StringReader(src);
      parser = factory.createParser(reader);

      while (parser.hasNext())
      {
        Event ev = parser.next();

        switch (ev)
        {
        case START_OBJECT:
          if (depth > 0)
            throw new QueryException(QueryMessage.EX_INVALID_PROJECTION.get());
          ++depth;
          break;

        case END_OBJECT:
          --depth;
          break;

        case START_ARRAY:
        case END_ARRAY:
          throw new QueryException(QueryMessage.EX_INVALID_PROJECTION.get());

        case KEY_NAME:
          String entryKey = parser.getString();

          if (checkPaths)
          {
            PathParser pp = new PathParser(entryKey);
            String[] parr = pp.splitAndSQLEscape();

            if (parr == null)
              makeException(QueryMessage.EX_INDEX_ILLEGAL_PATH, entryKey);

            paths.add(parr);
          }

          break;

        case VALUE_TRUE:
          includeSeen = true;
          break;

        case VALUE_FALSE:
          excludeSeen = true;
          break;

        case VALUE_NUMBER:
          BigDecimal dval = parser.getBigDecimal();
          if (dval.compareTo(BigDecimal.ZERO) == 0)
            excludeSeen = true;
          else if (dval.compareTo(BigDecimal.ONE) == 0)
            includeSeen = true;
          else
            badValueSeen = true;
          break;

        case VALUE_STRING:
          String sval = parser.getString();
          if (sval.equals("include"))
            includeSeen = true;
          else if (sval.equals("exclude"))
            excludeSeen = true;
          else
            badValueSeen = true;
          break;

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
    catch (JsonException e)
    {
      throw new QueryException(QueryMessage.EX_INVALID_PROJECTION.get(), e);
    }
    finally
    {
      // Exceptions from closing will be thrown after the try/catch/finally
      // block but only if this isn't already finalizing an exception case.
      // This is done by setting variable ex.
      try
      {
        if (parser != null) parser.close();
        if (reader != null) reader.close();
        close();
      }
      catch (JsonException e)
      {
        ex = new QueryException(QueryMessage.EX_INVALID_PROJECTION.get(), e);
      }
      catch (QueryException e)
      {
        ex = e;
      }
    }

    if (ex != null)
      throw ex;

    is_validated = true;

    return(!badValueSeen);
  }
}
