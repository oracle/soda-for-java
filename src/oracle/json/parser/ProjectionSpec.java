/* $Header: xdk/src/java/json/src/oracle/json/parser/ProjectionSpec.java /main/4 2014/10/15 13:21:24 dmcmahon Exp $ */

/* Copyright (c) 2014, Oracle and/or its affiliates. All rights reserved.*/

/*
   DESCRIPTION
    Parses a projection specification and holds the resulting information.

   PRIVATE CLASSES

   NOTES
    This is a hack implementation that only allows for a flat list of
    columns that are presumed to be singletons. This is suitable for
    a JSON_TABLE implementation, which is all we can do without a SQL
    projection operator.

   MODIFIED    (MM/DD/YY)
    dmcmahon    09/10/14 - Creation
 */

/**
 *  @version $Header: xdk/src/java/json/src/oracle/json/parser/ProjectionSpec.java /main/4 2014/10/15 13:21:24 dmcmahon Exp $
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
import java.util.Arrays;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonNumber;
import javax.json.JsonValue;
import javax.json.JsonException;
import javax.json.stream.JsonParsingException;

import oracle.json.util.JsonByteArray;

public class ProjectionSpec
{
  private final InputStream source;

  private boolean       is_parsed = false;

  private static final IndexColumn[] NO_COLUMNS = new IndexColumn[0];

  // ### Cheesy reuse of IndexColumn but OK for now
  private IndexColumn[] columns   = NO_COLUMNS;

  // Fully quoted path strings for use in JSON_TABLE expressions
  private String[]      jtpaths   = null;

  //
  // Root of the field template used for "rendering" a JSON fragment
  //
  private final FieldStep template = new FieldStep(null);

  /*
  ** This inner class is used to represent a tree of unique fields
  */
  class FieldStep
  {
    static final int CONTAINER_STEP = -1;

    String name;
    int    columnNum = CONTAINER_STEP;

    HashMap<String, FieldStep> children = null;

    FieldStep(String name)
    {
      this.name = name;
    }

    void setColumnPosition(int pos)
    {
      if (columnNum == CONTAINER_STEP)
        columnNum = pos;
      // ### Else this is a "lost" column we cannot map. That's because
      // ### either there are dual paths to the same scalar, or the step
      // ### is mapped as both a scalar and a container on different paths.
    }

    FieldStep getChild(String name)
    {
      if (children == null) return(null);
      return(children.get(name));
    }

    void addChild(String name, FieldStep fld)
    {
      if (children == null)
      {
        children = new HashMap<String, FieldStep>();

        if (columnNum >= 0)
        {
          // This now has to be marked as a container
          this.columnNum = CONTAINER_STEP;
          // ### This has become a "lost" column
        }
      }

      children.put(name, fld);
    }
  }

  private void makeStrings()
  {
    int ncols = columns.length;
    if (ncols == 0) return;

    //
    // Make a tree of field steps, converting the path step strings from
    // the SQL escaped format to JSON field names.
    //
    for (int i = 0; i < ncols; ++i)
    {
      String[] pathSteps = columns[i].getSteps();
      String[] fieldSteps = new String[pathSteps.length];

      FieldStep parent = template;

      for (int j = 0; j < pathSteps.length; ++j)
      {
        String fieldName = PathParser.unescapeStep(pathSteps[j]);

        FieldStep child = parent.getChild(fieldName);
        if (child == null)
        {
          child = new FieldStep(fieldName);
          parent.addChild(fieldName, child);
        }
        parent = child;
      }

      parent.setColumnPosition(i);
    }

    jtpaths = new String[ncols];

    StringBuilder sb = new StringBuilder();

    for (int i = 0; i < ncols; ++i)
    {
      String[] steps;

      steps = columns[i].getSteps();

      // Build a JSON_TABLE path string
      sb.setLength(0);
      sb.append("$");

      for (int j = 0; j < steps.length; ++j)
      {
        String stp = steps[j];

        if (stp == null) continue; // ### Should never happen

        if (stp.charAt(0) != '[') // ### Silently ignore array steps?
        {
          // Separate steps with dots
          sb.append(".");
          // Append the step itself
          sb.append(stp);
          // ### Force singleton cardinality for now
          sb.append("[0]");
        }
/***
        if (stp.charAt(0) != '[')
          // Separate non-array steps with dots
          sb.append(".");
        // Append the step itself
        sb.append(stp);
        // ### Unclear if final step should get [*] or not
***/
      }

      jtpaths[i] = sb.toString();
    }
  }

  public ProjectionSpec(InputStream inp)
  {
    source = inp;
  }

  private void makeException(QueryMessage msg, Object... params)
   throws QueryException
  {
    QueryException.throwSyntaxException(msg, params);
  }

  private void close(boolean silent)
    throws QueryException
  {
    try
    {
      source.close();
    }
    catch (IOException e)
    {
      if (!silent)
        throw(new QueryException(QueryMessage.EX_INVALID_PROJECTION.get(), e));
    }
  }

  /**
   * Parse the projection specification.
   */
  public IndexColumn[] parse()
    throws QueryException
  {
    try
    {
      DocumentLoader loader = new DocumentLoader(source);

      JsonObject jObj = (JsonObject)loader.parse();

      close(false);

      ArrayList<IndexColumn> columnList = new ArrayList<IndexColumn>();

      for (Entry<String, JsonValue> entry : jObj.entrySet()) 
      {
        String    entryKey = entry.getKey();
        JsonValue entryVal = entry.getValue();

        JsonValue.ValueType vtype = entryVal.getValueType();

        String dtype = null;

        // Ignore fields set to false or null
        if ((vtype == JsonValue.ValueType.FALSE) ||
            (vtype == JsonValue.ValueType.NULL))
          continue;

        // ### Ignore structured fields (questionable)
        if ((vtype == JsonValue.ValueType.OBJECT) ||
            (vtype == JsonValue.ValueType.ARRAY))
          continue;

        // Accept 1 as equivalent to true, other numbers ignored
        if (vtype == JsonValue.ValueType.NUMBER)
        {
          BigDecimal dval = ((JsonNumber)entryVal).bigDecimalValue();
          if (dval.compareTo(BigDecimal.ONE) != 0)
            continue;
        }
        
        if (vtype == JsonValue.ValueType.STRING)
        {
          // The field is included, this may give us the data type hint
          dtype = ((JsonString)entryVal).getString();
        }

        IndexColumn idx  = new IndexColumn();

        PathParser pp = new PathParser(entryKey);
        String[] parr = pp.splitAndSQLEscape();
        if (parr == null)
          makeException(QueryMessage.EX_INDEX_ILLEGAL_PATH, entryKey);

        idx.setPath(parr);
        if (dtype != null) idx.setSqlType(dtype);

        columnList.add(idx);
      }

      int sz = columnList.size();
      if (sz > 0)
      {
        columns = new IndexColumn[sz];
        columns = columnList.toArray(columns);
      }
      // ### Should an empty column list be an exception for creation?
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
      // This will only really attempt to close if an exception occurs
      close(true);
    }

    is_parsed = true;
    makeStrings();

    return(columns);
  }

  public IndexColumn[] getColumns()
  {
    if (!is_parsed) throw new IllegalStateException("Not parsed");
    return(columns);
  }

  public int numColumns()
  {
    return(columns.length);
  }

  public String getColumnPath(int pos)
  {
    if (!is_parsed) throw new IllegalStateException("Not parsed");
    return(jtpaths[pos]);
  }

  public int getSqlType(int pos)
  {
    if (!is_parsed) throw new IllegalStateException("Not parsed");
    return(columns[pos].getSqlType());
  }

  private int projectObject(FieldStep fld,
                            String[] values,
                            JsonByteArray builder)
  {
    int totalBinds = 0;
    boolean isFirst = true;

    builder.appendOpenBrace();

    for (Entry<String, FieldStep> entry : fld.children.entrySet())
    {
      String    fldName = entry.getKey();
      FieldStep fldVal  = entry.getValue();

      if (isFirst)
        isFirst = false;
      else
        builder.appendComma();

      // Render the field name
      builder.appendValue(fldName);
      builder.appendColon();

      int pos = fldVal.columnNum;

      // If it's a container, descend into it and render any children
      if (pos == FieldStep.CONTAINER_STEP)
        totalBinds += projectObject(fldVal, values, builder);
      // Otherwise it's a leaf step so render the matching value
      else
      {
        String fieldValue = (pos < values.length) ? values[pos] : null;

        ++totalBinds;

        if (fieldValue == null)
        {
          // ### Perhaps we should omit the field instead?
          builder.append("null");
        }
        else if (getSqlType(pos) == IndexColumn.SQLTYPE_NUMBER)
        {
          try
          {
            BigDecimal numValue = new BigDecimal(fieldValue);
            builder.append(numValue.toString());
          }
          catch (NumberFormatException e)
          {
            // Render the field as a string
            builder.appendValue(fieldValue);
          }
        }
        else
        {
          // ### To-do: if the projection has a preference for a boolean
          // ### or date/time field, we need to detect that here and
          // ### attempt to render it nicely.
          builder.appendValue(fieldValue);
        }
      }
    }

    builder.appendCloseBrace();

    return(totalBinds);
  }

  /**
   * Render the values into a JSON fragment
   */
  public byte[] projectValues(String[] values)
  {
    JsonByteArray builder = new JsonByteArray();

    // Walk the template rendering the steps and any leaf values
    projectObject(template, values, builder);

    return(builder.toArray());
  }
}
