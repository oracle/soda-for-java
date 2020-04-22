/* Copyright (c) 2014, 2019, Oracle and/or its affiliates. 
All rights reserved.*/

/*
   DESCRIPTION
     QBE AND-OR tree
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

import java.io.InputStream;
import java.lang.NumberFormatException;

import java.util.*;
import java.util.Map.Entry;

import javax.json.JsonString;
import javax.json.JsonValue;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonArray;
import javax.json.JsonException;
import javax.json.stream.JsonParsingException;

import oracle.json.common.JsonFactoryProvider;

import oracle.json.parser.Evaluator.EvaluatorCode;

public class AndORTree
{
  private AndORNode                root;
  private ArrayList<ValueTypePair> valueArray;
  private ArrayList<Predicate>     orderByArray;
  private HashSet<String> keysSet;

  private String predChar = "@";

  private StringBuilder jsonExists;

  private boolean need_12_2  = false;

  private static final boolean WRAP_TEMPORAL = false;

  // Constant to match fields array under $orderby
  private static final String FIELDS_ARRAY = "$fields";
  // Constants to match $orderby flags that control
  // json_value error clause
  private static final String LAX = "$lax";
  private static final String SCALAR_REQ = "$scalarRequired";

  // Error clause constants
  public static final String ERROR_ON_ERROR = "error on error";
  public static final String ERROR_ON_ERROR_NULL_ON_EMPTY =
    "error on error null on empty";
  public static final String NULL_ON_ERROR = "null on error";

  // Private. Use createTree(...) to create AndORTrees.
  private AndORTree()
  {
    valueArray   = new ArrayList<ValueTypePair>();
    orderByArray = new ArrayList<Predicate>();
    keysSet = new HashSet<String>();
  }

  /**
   * Return the array of values to be bound to the SQL statement
   */
  public ArrayList<ValueTypePair> getValueArray()
  {
    return valueArray;
  }

  /**
   * Return an array of values for the order-by part of a SQL statement
   */
  public ArrayList<Predicate> getOrderByArray()
  {
    return orderByArray;
  }

  /**
   * Return an array of values to be bound as keys
   */
  public HashSet<String> getKeys()
  {
    return keysSet;
  }

  /**
   * Return the character to use as the leading character on a
   * predicate filter path.
   */
  String getPredChar()
  {
    return(predChar);
  }

  private void addToOrderByArray(Predicate pred)
  {
    orderByArray.add(pred);
  }

  void addToKeys(String item)
  {
    keysSet.add(item);
  }

  void checkCompatibility(EvaluatorCode code)
  {
    if (Evaluator.requires_12_2(code))
      need_12_2 = true;
  }

  String getScalarKey(JsonValue item) throws QueryException
  {
    JsonValue.ValueType valueType = item.getValueType();
    if (valueType == JsonValue.ValueType.STRING)
      return(((JsonString)item).getString());
    else if (valueType == JsonValue.ValueType.NUMBER)
      return(((JsonNumber)item).toString());

    // true/false/null, or non-scalars, cannot be used as keys
    QueryException.throwSyntaxException(QueryMessage.EX_NON_SCALAR_KEY);

    return null;
  }

  static boolean isJSONPrimitive(JsonValue item)
  {
    JsonValue.ValueType valueType = item.getValueType();
    
    if ((valueType == JsonValue.ValueType.ARRAY) ||
        (valueType == JsonValue.ValueType.OBJECT)) 
    {
      return false;     
    }
    return true;
  }

  static boolean isJSONArray(JsonValue item)
  {
    if (item.getValueType() == JsonValue.ValueType.ARRAY)
      return true;
    return false;
  }

  static boolean isJSONObject(JsonValue item)
  {
    if (item.getValueType() == JsonValue.ValueType.OBJECT)
      return true;
    return false;
  }

  static ValueTypePair createBindValue(JsonValue item, String fieldName,
                                       boolean allowLiterals)
    throws QueryException
  {
    ValueTypePair newValue = null;
    boolean       isLiteral = false;

    if (AndORTree.isJSONPrimitive(item))
    {
      if (item.getValueType() == JsonValue.ValueType.NULL)
      {
        newValue = new ValueTypePair(ValueTypePair.TYPE_NULL);
        isLiteral = true;
      }
      else if (item.getValueType() == JsonValue.ValueType.TRUE)
      {
        newValue = new ValueTypePair(true, ValueTypePair.TYPE_BOOLEAN);
        isLiteral = true;
      }
      else if (item.getValueType() == JsonValue.ValueType.FALSE)
      {
        newValue = new ValueTypePair(false, ValueTypePair.TYPE_BOOLEAN);
        isLiteral = true;
      }
      else if (item.getValueType() == JsonValue.ValueType.NUMBER)
      {
        newValue = new ValueTypePair(((JsonNumber)item).bigDecimalValue(),
                                     ValueTypePair.TYPE_NUMBER);
      }
      else if (item.getValueType() == JsonValue.ValueType.STRING)
      {
        newValue = new ValueTypePair(((JsonString) item).getString(),
                                     ValueTypePair.TYPE_STRING);
      }
    }
    else
    {
      QueryException.throwSyntaxException(QueryMessage.EX_MUST_BE_SCALAR,
                                          fieldName);
    }

    if (isLiteral && !allowLiterals)
      QueryException.throwSyntaxException(QueryMessage.EX_MUST_NOT_BE_LITERAL,
                                          fieldName);

    return(newValue);
  }

  ValueTypePair addToValueArray(JsonValue item, String fieldName)
    throws QueryException
  {
    ValueTypePair newValue = AndORTree.createBindValue(item, fieldName, true);

    if (newValue != null)
    {
      valueArray.add(newValue);
    }

    return(newValue);
  }

  ValueTypePair addToValueArray(String value)
  {
    ValueTypePair newValue =
      new ValueTypePair(value, ValueTypePair.TYPE_STRING);

    if (newValue != null)
      valueArray.add(newValue);

    return(newValue);
  }

  // These are used to mark ranges of binds with special type info
  private int                     bookmark_start = -1;
  private Evaluator.EvaluatorCode bookmarkedModifier = null;

  /*
   * Set the start of a modifier mark range
   */
  void setValBookmark(Evaluator.EvaluatorCode evc)
  {
    if (!WRAP_TEMPORAL) return;

    if (evc != null)
    {
      bookmarkedModifier = evc;
      bookmark_start = getNumVals();
    }
  }

  /*
   * End the marked range and mark all the values
   */
  void endValBookmark()
  {
    if (bookmarkedModifier != null)
    {
      boolean isDate = (bookmarkedModifier == EvaluatorCode.$date);
      boolean isTimestamp = (bookmarkedModifier == EvaluatorCode.$timestamp);
      if (isDate || isTimestamp)
      {
        for (int i = bookmark_start; i < getNumVals(); ++i)
        {
          ValueTypePair vpair = valueArray.get(i);
          vpair = ValueTypePair.makeTemporal(vpair, isTimestamp);
          valueArray.set(i, vpair);
        }
      }
      bookmarkedModifier = null;
      bookmark_start = -1;
    }
  }

  private int numBinds;

  int getNextBind()
    throws QueryException
  {
    int nvals = getNumVals();
    if (numBinds >= nvals)
    {
      QueryException.throwExecutionException(QueryMessage.RR_BIND_MISMATCH,
                                             Integer.toString(nvals),
                                             Integer.toString(numBinds + 1));
    }

    return(numBinds++);
  }

  void removeBind(int bindnum)
  {
    valueArray.remove(bindnum);
    numBinds--;
  }

  int getNumVals()
  {
    if (getValueArray() != null)
      return valueArray.size();

    return 0;
  }

  public static AndORTree createTree(JsonFactoryProvider jProvider,
                                     InputStream stream)
    throws QueryException
  {
    FilterLoader dl;

    try
    {
      dl = new FilterLoader(jProvider, stream);
    }
    catch (JsonException e)
    {
      // This can occur if the underlying parser can't detect the encoding,
      // or if there's an underlying IOException.
      throw new QueryException(e);
    }

    return createTree(dl);
  }

  private static void processArrayOrderBy(AndORTree tree, JsonArray ordby, 
                                          String errorClause)
    throws QueryException
  {
    JsonArray           fields = ordby;
    Iterator<JsonValue> fldIter = fields.iterator();

    while (fldIter.hasNext())
    {
      JsonValue fld = fldIter.next();
      checkIfValueIsJsonObject(fld, "$orderby");
      JsonObject fldobj = (JsonObject)fld;

      String  path = null;
      String  datatype = null;
      int     maxLength = 0;
      boolean ascending = true;

      for (Entry<String, JsonValue> item : fldobj.entrySet())
      {
        String    key = item.getKey();
        JsonValue val = item.getValue();

        if (key.equals("path"))
        {
          if (!(val instanceof JsonString))
            QueryException.throwSyntaxException(QueryMessage.EX_BAD_PROP_TYPE,
              "$orderby", "path", "string");

          path = ((JsonString)val).getString();
        }
        else if (key.equals("datatype"))
        {
          if (!(val instanceof JsonString))
            QueryException.throwSyntaxException(QueryMessage.EX_BAD_PROP_TYPE,
              "$orderby", "datatype", "string");

          datatype = ((JsonString)val).getString();
        }
        else if (key.equals("order"))
        {
          if (!(val instanceof JsonString))
            QueryException.throwSyntaxException(QueryMessage.EX_BAD_PROP_TYPE,
              "$orderby", "order", "string");

          String tmp = ((JsonString)val).getString();

          if (tmp.equals("asc"))
            ascending = true;
          else if (tmp.equals("desc"))
            ascending = false;
          else
            QueryException.throwSyntaxException(
              QueryMessage.EX_ORDERBY_INVALID_PROP,
              "order", tmp);
        }
        else if (key.equals("maxLength"))
        {
          if (!(val instanceof JsonNumber))
            QueryException.throwSyntaxException(QueryMessage.EX_BAD_PROP_TYPE,
              "$orderby", "maxLength", "number");

          maxLength = ((JsonNumber)val).intValue();
          if (maxLength <= 0)
            QueryException.throwSyntaxException(
              QueryMessage.EX_ORDERBY_INVALID_PROP,
              "maxLength", Integer.toString(maxLength));
        }
        else // Unknown member of field definition
        {
          QueryException.throwSyntaxException(
            QueryMessage.EX_ORDERBY_UNKNOWN_PROP, key);
        }
      }

      // Every field definition must give a path
      if (path == null)
      {
        QueryException.throwSyntaxException(
          QueryMessage.EX_ORDERBY_PATH_REQUIRED);
      }

      if (datatype != null)
      {
        if (datatype.equalsIgnoreCase("number"))
          datatype = "number";
        else if (datatype.equalsIgnoreCase("date"))
          datatype = "date";
        else if (datatype.equalsIgnoreCase("datetime"))
          datatype = "timestamp";
        else if (datatype.equalsIgnoreCase("string") ||
                 datatype.equalsIgnoreCase("varchar2") ||
                 datatype.equalsIgnoreCase("varchar"))
        {
          datatype = "varchar2";
          if (maxLength > 0) datatype += ("("+Integer.toString(maxLength)+")");
        }
        else
          QueryException.throwSyntaxException(
            QueryMessage.EX_ORDERBY_INVALID_PROP,
            "datatype", datatype);
      }

      Predicate pred = new Predicate(new JsonQueryPath(path),
        (ascending) ? "1" : "-1",
        datatype,
        errorClause);
      tree.addToOrderByArray(pred);
    }
  }

  private static AndORTree createTree(FilterLoader loader)
    throws QueryException
  {
    JsonObject jObj;

    try
    {
      jObj = (JsonObject)loader.parse();
    }
    catch (JsonParsingException e)
    {
      // The JsonParsingException can be an exception thrown by
      // us (as opposed to JSR353) from FilterLoader.
      // Convert it to a query exception if so.
      if (e.getMessage().equals(FilterLoader.MULTIPLE_ORDERBY_OPS))
        throw QueryException.getSyntaxException(QueryMessage.EX_MULTIPLE_OPS, "$orderby");
      else if (e.getMessage().equals(FilterLoader.MULTIPLE_QUERY_OPS))
        throw QueryException.getSyntaxException(QueryMessage.EX_MULTIPLE_OPS, "$query");
      else
        throw new QueryException(e);
    }
    catch (JsonException e)
    {
      throw new QueryException(e);
    }

    AndORTree tree = new AndORTree();
    AndORNode root = new AndORNode(null);
    root.setEval(Evaluator.EvaluatorCode.$and);
    root.setPredicate(new Predicate(new JsonQueryPath(), null));
    tree.root = root;

    String entryKey;

    boolean queryOperatorFound = false;
    boolean basicQBEFound = false;
    boolean envelopeFound = false;

    if (jObj != null)
    {
      for (Entry<String, JsonValue> entry : jObj.entrySet())
      {
        entryKey = entry.getKey();

        if (entryKey.equalsIgnoreCase("$project") ||
            entryKey.equalsIgnoreCase("$patch")   ||
            entryKey.equalsIgnoreCase("$merge")     )
        {
          if (basicQBEFound)
            QueryException.throwSyntaxException(
              QueryMessage.EX_ENVELOPE_WITH_OTHER_OPS);
          envelopeFound = true;

          // Ignore this because it was parsed by the upper layer
        }
        else if (entryKey.equalsIgnoreCase("$query"))
        {
          if (basicQBEFound)
            QueryException.throwSyntaxException(
              QueryMessage.EX_QUERY_WITH_OTHER_OPS);
          queryOperatorFound = envelopeFound = true;

          JsonValue value = entry.getValue();

          checkIfValueIsJsonObject(value, "$query");

          for (Entry<String, JsonValue> qryEntry :
               ((JsonObject)value).entrySet())
          {
            root.addNode(tree, qryEntry);
          }
        }
        else if (entryKey.equalsIgnoreCase("$orderby"))
        {
          if (basicQBEFound)
            QueryException.throwSyntaxException(
              QueryMessage.EX_ENVELOPE_WITH_OTHER_OPS);
          envelopeFound = true;

          JsonValue ordby = entry.getValue();

          if (ordby instanceof JsonObject)
          {
            // First attempt to process the form of order-by
            // with a fields array underneath
            JsonObject ordbyObj = (JsonObject) ordby;

            boolean scalarReq = false;
            boolean lax = false;
            JsonArray fieldsArray = null;
            String unknownKey = null;
            String keyWithNonBooleanValue = null;

            for (Map.Entry<String,JsonValue> ordbyEntry : ordbyObj.entrySet())
            {
              String ordbyEntryKey = ordbyEntry.getKey();
              if (ordbyEntryKey.equals(FIELDS_ARRAY))
              {
                JsonValue ordbyEntryVal = ordbyEntry.getValue();
                if (ordbyEntryVal.getValueType().equals(JsonValue.ValueType.ARRAY))
                {
                  fieldsArray = (JsonArray)ordbyEntryVal;
                }
              }
              else if (ordbyEntryKey.equals(SCALAR_REQ))
              {
                JsonValue ordbyEntryVal = ordbyEntry.getValue();
                if (ordbyEntryVal.getValueType().equals(JsonValue.ValueType.TRUE))
                {
                  scalarReq = true;
                }
                else if (!ordbyEntryVal.getValueType().equals(JsonValue.ValueType.FALSE))
                {
                  keyWithNonBooleanValue = ordbyEntryKey;
                }
              }
              else if (ordbyEntryKey.equals(LAX))
              {
                JsonValue ordbyEntryVal = ordbyEntry.getValue();
                if(ordbyEntryVal.getValueType().equals(JsonValue.ValueType.TRUE))
                {
                  lax = true;
                }
                else if (!ordbyEntryVal.getValueType().equals(
                          JsonValue.ValueType.FALSE))
                {
                  keyWithNonBooleanValue = ordbyEntryKey;
                }
              }
              else
              {
                unknownKey = ordbyEntryKey;
              }
            }

            // Process the order-by with a fields clause.
            if (fieldsArray != null)
            {
              // If both scalarRequired and lax flags
              // were set to true, error out
              if (scalarReq && lax)
              {
                QueryException.throwSyntaxException(QueryMessage.EX_SCALAR_AND_LAX);
              }
              else if (keyWithNonBooleanValue != null)
              {
                QueryException.throwSyntaxException(QueryMessage.EX_BAD_PROP_TYPE,
                  "$orderby", keyWithNonBooleanValue, "boolean");
              }
              else if (unknownKey != null)
              {
                QueryException.throwSyntaxException(QueryMessage.EX_ORDERBY_UNKNOWN_PROP,
                                                    unknownKey);
              }

              if (scalarReq)
              {
                processArrayOrderBy(tree, fieldsArray, ERROR_ON_ERROR);
              }
              else if (lax)
              {
                processArrayOrderBy(tree, fieldsArray, NULL_ON_ERROR);
              }
              else
                processArrayOrderBy(tree, fieldsArray, ERROR_ON_ERROR_NULL_ON_EMPTY);
            }
            // If this else is entered, this means we didn't encounter
            // the form of order-by with a field array underneath.
            // So just process it as the most basic order-by (map of
            // JSON fields/integer pairs).
            else {

              // The FilterLoader is responsible for keeping
              // order-by paths in the order they were specified,
              // and delivering them here. As a work-around
              // for customers that need control over the ordering we will
              // respect a sort based on the absolute value of the integers.
              // Thus 1, -2, 3, 4, -5 etc.

              int ipath;
              int npaths = loader.getOrderCount();
              if (npaths <= 0) continue; // Nothing to do

              String[] paths = new String[npaths];
              int[] posns = new int[npaths];

              for (ipath = 0; ipath < npaths; ++ipath) {
                String pathString = loader.getOrderPath(ipath);
                String dirString = loader.getOrderDirection(ipath);

                if (pathString == null) break; // End of array

                int ival = 0;
                // A null means one of the keys had a nonsense value
                if (dirString != null) {
                  // Otherwise try to parse it as an integer
                  try {
                    ival = Integer.parseInt(dirString);
                  } catch (NumberFormatException e) {
                    QueryException.throwSyntaxException(QueryMessage.EX_BAD_ORDERBY_PATH_VALUE, pathString, dirString);
                  }

                  // 0 value is not allowed
                  if (ival == 0) {
                    QueryException.throwSyntaxException(QueryMessage.EX_BAD_ORDERBY_PATH_VALUE, pathString, dirString);
                  }
                } else {
                  QueryException.throwSyntaxException(QueryMessage.EX_BAD_ORDERBY_PATH_VALUE2, pathString);
                }

                // Compute absolute value of the position
                int aval = (ival < 0) ? -ival : ival;

                // Linear search for insertion point
                // Ties are broken by inserting the most recent value last
                int ipos;
                for (ipos = 0; ipos < ipath; ++ipos) {
                  int bval = posns[ipos];
                  if (bval < 0) bval = -bval;
                  if (aval < bval) break; // Insertion point found
                }

                // If necessary shift values to make a slot for this entry
                // This allows clients to use 1, 2, 3, etc. to forcibly order keys
                if (ipos < ipath) {
                  System.arraycopy(paths, ipos, paths, ipos + 1, ipath - ipos);
                  System.arraycopy(posns, ipos, posns, ipos + 1, ipath - ipos);
                }

                // Insert the value in the position found
                paths[ipos] = pathString;
                posns[ipos] = ival;
              }

              // Reset path count to actual value found during iteration
              npaths = ipath;

              // Now insert the key/value pairs in the correct order
              for (ipath = 0; ipath < npaths; ++ipath) {
                Predicate pred = new Predicate(new JsonQueryPath(paths[ipath]),
                  (posns[ipath] < 0) ? "-1" : "1", null,
                  ERROR_ON_ERROR_NULL_ON_EMPTY);
                tree.addToOrderByArray(pred);
              }
            }
          }
          else if (ordby instanceof JsonArray)
          {
            // The default errorClause is "error on error null on empty"
            processArrayOrderBy(tree, (JsonArray) ordby, ERROR_ON_ERROR_NULL_ON_EMPTY);
          }
          else
          {
              QueryException.throwSyntaxException(QueryMessage.EX_BAD_OP_VALUE,
                      "$orderby");
          }

        }
        else
        {
          basicQBEFound = true;

          if (queryOperatorFound)
          {
            QueryException.throwSyntaxException(
              QueryMessage.EX_QUERY_WITH_OTHER_OPS);
          }
          if (envelopeFound)
          {
            QueryException.throwSyntaxException(
              QueryMessage.EX_ENVELOPE_WITH_OTHER_OPS);
          }

          // Otherwise pass the entire JsonElement as the QBE
          root.addNode(tree, entry);
        }
      }
    }
    return tree;
  }

  private static void checkIfValueIsJsonObject(JsonValue val, String op)
    throws QueryException
  {
    if (!(val instanceof JsonObject))
    {
      QueryException.throwSyntaxException(QueryMessage.EX_BAD_OP_VALUE, op);
    }
  }

  public boolean hasOrderBy()
  {
    return getOrderByArray().isEmpty() ? false : true;
  }

  public boolean hasKeys()
  {
    return getKeys().isEmpty() ? false : true;
  }

  public boolean hasJsonExists()
  {
    // Assumes that this method is invoked only after
    // generateJsonExists() has been called.
    // So jsonExists cannot be null here.
    if (jsonExists == null)
      throw new IllegalStateException();

    return (jsonExists.length() > 0) ? true : false;
  }

  public void generateJsonExists()
    throws QueryException
  {
    jsonExists = root.generateJsonExists(this);
  }

  public void appendJsonExists(StringBuilder output)
  {
    // Assumes that this method is invoked only if hasJsonExists()
    // returns true. So jsonExists cannot be null here.
    if (jsonExists == null)
      throw new IllegalStateException();

    output.append("'$?");
    output.append(jsonExists);
    output.append("'");

    int numBinds = getNumVals();
    if (numBinds > 0)
    {
      output.append("\npassing ");

      for (int varNum = 0; varNum < numBinds; ++varNum)
      {
        ValueTypePair vpair = valueArray.get(varNum);

        if (varNum > 0)
          output.append(", ");

        if (vpair.isTimestamp())
          // This format can consume trailing timezones including a "Z"
          // but only if it's used with TO_TIMESTAMP_TZ.
          output.append("TO_TIMESTAMP_TZ(?,'SYYYY-MM-DD\"T\"HH24:MI:SS.FFTZH:TZM')");
        else if (vpair.isDate())
          // This format includes the time component to reliably consume
          // Oracle date+time values, but should also work if the time is
          // not present in the bind variable.
          output.append("TO_DATE(?,'SYYYY-MM-DD\"T\"HH24:MI:SS')");
        else
          output.append("?");

        output.append(" as \"B");
        output.append(Integer.toString(varNum));
        output.append("\"");
      }
    }
  }

  /*
   * Append a bind variable for a SQL JSON operator,
   * with an optional format wrapper
   */
  public void appendFormattedBind(StringBuilder sb, ValueTypePair vpair,
                                  SqlJsonClause clause)
  {
    if (clause == null)
    {
      sb.append("?");
      return;
    }

    //
    // Determine whether type conversion is needed on the SQL side
    //
    boolean isDate      = clause.useDateWrapper();
    boolean isTimestamp = clause.useTimestampWrapper();

    if (vpair.getType() == ValueTypePair.TYPE_NUMBER)
    {
      //
      // For numbers that have to be coerced to date/time values, the
      // RDBMS offers Julian dates, which are numbered from 1 AD. Our
      // pseudo-standard in JSON is millisecond tick count
      // (milliseconds since 1/1/1970). So do arithmetic on the number
      // to get it to a fractional Julian date.
      // ### Ideally we'd find a way to preserve the fractional value,
      // ### but that would require conversion to seconds since midnight
      // ### and two bindings. It's too much work, so hopefully this
      // ### conversion was done in Java and we now have a string, so
      // ### that this conversion never occurs in practice.
      //
      if (isDate || isTimestamp)
      {
        sb.append("TO_DATE(((?/1000)+2440588),'J')");
        return;
      }
    }
    //
    // Use the ISO standard format mask for strings that have to be
    // converted to timestamps. This should be the common case for
    // date/time values. Note that it uses TO_TIMESTAMP_TZ because
    // otherwise trailing zones cannot be consumed (except Z).
    //
    else if (vpair.getType() == ValueTypePair.TYPE_STRING)
    {
      if (isDate)
      {
        sb.append("TO_DATE(?,'YYYY-MM-DD')");
        return;
      }
      if (isTimestamp)
      {
        sb.append("TO_TIMESTAMP_TZ(?,'SYYYY-MM-DD\"T\"HH24:MI:SS.FFTZH:TZM')");
        return;
      }
    }

    // For numbers and strings, let the default RDBMS conversion occur
    // since we don't know which direction the RDBMS will want to convert.
    // ### This is problematic if the radix character isn't forced via
    // ### ALTER SESSION.

    sb.append("?");
  }

  private ArrayList<SqlJsonClause> sjClauses = null;

  void addSqlJsonOperator(SqlJsonClause sjClause)
  {
    if (sjClauses == null)
      sjClauses = new ArrayList<SqlJsonClause>();
    sjClauses.add(sjClause);
  }

  public boolean hasSqlJsonClause()
  {
    return (sjClauses != null);
  }

  public List<SqlJsonClause> getSqlJsonOperators()
  {
    return sjClauses;
  }

  private ArrayList<SpatialClause> spatialClauses = null;

  void addSpatialOperator(SpatialClause geo)
  {
    if (spatialClauses == null)
      spatialClauses = new ArrayList<SpatialClause>();
    spatialClauses.add(geo);
  }

  public List<SpatialClause> getSpatialOperators()
  {
    return spatialClauses;
  }

  public boolean hasSpatialClause()
  {
    return (spatialClauses != null);
  }

  private ArrayList<ContainsClause> containsClauses = null;

  void addContainsClause(ContainsClause clause)
  {
    if (containsClauses == null)
      containsClauses = new ArrayList<ContainsClause>();
    containsClauses.add(clause);
  }

  public List<ContainsClause> getContainsOperators()
  {
    return containsClauses;
  }

  public boolean hasContainsClause()
  {
    return (containsClauses != null);
  }

  /**
   * Returns false if this is a QBE that uses 12.2 operators,
   * otherwise returns true.
   */
  public boolean requires_12_2()
  {
    return(need_12_2);
  }
}
