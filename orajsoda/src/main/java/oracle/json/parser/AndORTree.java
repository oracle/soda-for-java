/* Copyright (c) 2014, 2024, Oracle and/or its affiliates. */
/* All rights reserved.*/

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
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Wrapper;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import jakarta.json.JsonArray;
import jakarta.json.JsonException;
import jakarta.json.JsonNumber;
import jakarta.json.JsonObject;
import jakarta.json.JsonString;
import jakarta.json.JsonValue;
import jakarta.json.JsonValue.ValueType;
import jakarta.json.stream.JsonParsingException;
import oracle.jdbc.OraclePreparedStatement;
import oracle.jdbc.OracleTypes;
import oracle.json.sodautil.ByteArray;
import oracle.json.sodacommon.JsonFactoryProvider;
import oracle.json.parser.Evaluator.EvaluatorCode;
import oracle.sql.json.OracleJsonValue;
import oracle.sql.json.OracleJsonString;
import oracle.sql.json.OracleJsonTimestampTZ;

public class AndORTree
{
  private static final String NULL = "null";
  
  private AndORNode                root;
  private ArrayList<ValueTypePair> valueArray;
  private ArrayList<Predicate>     orderByArray;
  private HashSet<String> keysSet;
  
  private String predChar = "@";

  private StringBuilder jsonExists;

  private boolean need_12_2  = false;

  /**
   * When strictTypeMode is true:
   *  (1) Filters will use strict(type) in JSON_EXISTS
   *  (2) Binding ValueTypePair will bind objects/arrays as JSON type and map OSON
   *      extended types to corresponding SQL types bind methods see:
   *      bindTypedParameterStrict() 
   *  (3) ORDER BY will use JSON_QUERY(returning JSON)
   *      instead of JSON_VALUE
   */
  private boolean strictTypeMode = false;
  
  boolean strictTypeMatching = false;

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
  private AndORTree(boolean strictTypeMode)
  {
    this.valueArray   = new ArrayList<ValueTypePair>();
    this.orderByArray = new ArrayList<Predicate>();
    this.keysSet = new HashSet<String>();
    this.strictTypeMode = strictTypeMode;
  }

  /**
   * Return the array of values to be bound to the SQL statement
   */
  public ArrayList<ValueTypePair> getValueArray()
  {
    return valueArray;
  }

  boolean getStrictTypeMode()
  {
    return strictTypeMode;
  }

  /**
   * Check the bind values for strings exceeding a maximum number of bytes.
   * Returns true if all values are less than or equal to the number of
   * bytes (as UTF-8), false if any exceed the limit.
   */
  public void checkStringValues(int maxlen)
    throws QueryException
  {
    if (valueArray == null) return; // Nothing to do

    for (ValueTypePair vpair : valueArray)
    {
      String str = vpair.getStringValue();
      if (str == null) continue;

      // Get the character length
      int slen = str.length();

      // If it can't possibly be too big, no need to check further
      if ((slen * 3) <= maxlen)
        continue;

      // If it's less than the limit, as measured in characters
      if (slen <= maxlen)
      {
        // Convert to UTF-8 to get the actual byte length
        slen = str.getBytes(ByteArray.DEFAULT_CHARSET).length;
      }
      // Otherwise it's obviously too big

      if (slen > maxlen)
        QueryException.throwSyntaxException(QueryMessage.EX_STRING_BIND_TOO_LONG, slen, maxlen);
    }
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
    {
      JsonNumber numVal = (JsonNumber)item;
      BigDecimal decVal = numVal.bigDecimalValue().stripTrailingZeros();
      if (decVal.scale() <= 0)
        return(decVal.toBigInteger().toString());
      else
        return(decVal.toPlainString());
    }

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
      isLiteral = item.getValueType() == ValueType.NULL ||
                  item.getValueType() == ValueType.TRUE ||
                  item.getValueType() == ValueType.FALSE;
      newValue = new ValueTypePair(item);
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
    valueArray.add(newValue);
    return(newValue);
  }

  ValueTypePair addToValueArray(String value)
  {
    ValueTypePair newValue = new ValueTypePair(value);
    valueArray.add(newValue);
    return(newValue);
  }
  
  ValueTypePair addToValueArray(BigDecimal value)
  {
    ValueTypePair newValue = new ValueTypePair(value);
    valueArray.add(newValue);
    return(newValue);
  }

  // These are used to mark ranges of binds with special type info
  private int                     bookmark_start = -1;
  private Evaluator.EvaluatorCode bookmarkedModifier = null;

  ValueTypePair addToValueArray(JsonValue node)
  {
    ValueTypePair newValue = new ValueTypePair(node);
    valueArray.add(newValue);
    return(newValue);
  }

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

  /**
   * This method, unlike the ones that rely on FilterLoader, does not require that
   * the input go through JSON serialization and parsing. Data values within the
   * filter will be directly referenced by ValueTypePair. That is, ValueTypePair
   * contains a nested instance of jakarta.json.JsonValue. This nested instance,
   * if implemented by oracle.sql.json/jdbc, can be passed directly to JDBC bind
   * methods and any extended types will be preserved. For example, if the
   * JsonValue is actually OSON backed (implemented by oracle.jdbc.*) calling
   * PreparedStatement.setObject(i, JsonValue) will preserve the true SQL values
   * such as timestamp, arrays of timestamps, etc.
   */
  public static AndORTree createTree(JsonObject filter, boolean strictTypeMode, boolean isDualityView) throws QueryException 
  {
    OrderBySpecification orderBySpec = new OrderBySpecification(filter);
    return createTree(filter, orderBySpec, strictTypeMode, isDualityView);
  }

  public static AndORTree createTree(JsonFactoryProvider jProvider,
                                     InputStream stream,
				     boolean isDualityView)
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

    return AndORTree.createTree(dl, isDualityView);
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
      boolean sortByMinMax = false;

      for (Map.Entry<String, JsonValue> item : fldobj.entrySet())
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
        else if (key.equals("sortByMinMax"))
        {
          if (!JsonValue.TRUE.equals(val) && !JsonValue.FALSE.equals(val))
            QueryException.throwSyntaxException(QueryMessage.EX_BAD_PROP_TYPE,
              "$orderby", "sortByMinMax", "boolean");
          
          if (JsonValue.TRUE.equals(val))
            sortByMinMax = true;
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
        if (datatype.equalsIgnoreCase("json"))
          datatype = "json";
	else if (datatype.equalsIgnoreCase("number"))
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
        errorClause,
        sortByMinMax);
      tree.addToOrderByArray(pred);
    }
  }

  private static AndORTree createTree(FilterLoader loader, boolean isDualityView)
    throws QueryException
  {
    JsonObject jObj;
    try
    {
      JsonValue root = loader.parse();
      if (!(root instanceof JsonObject))
        throw QueryException.getSyntaxException(QueryMessage.EX_MUST_BE_OBJECT,
                                                "filter");
      jObj = (JsonObject)root;
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
    return createTree(jObj, loader.getOrderBySpec(), false, isDualityView);
  }

  private static AndORTree createTree(JsonObject jObj, OrderBySpecification orderBySpec, boolean strictTypeMode, boolean isDualityView)
      throws QueryException 
  {
    AndORTree tree = new AndORTree(strictTypeMode);
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
      for (Map.Entry<String, JsonValue> entry : jObj.entrySet())
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

          for (Map.Entry<String, JsonValue> qryEntry :
               ((JsonObject)value).entrySet())
          {
            root.addNode(tree, qryEntry, strictTypeMode);
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
              int npaths = orderBySpec.getOrderCount();
              if (npaths <= 0) continue; // Nothing to do

              String[] paths = new String[npaths];
              int[] posns = new int[npaths];

              for (ipath = 0; ipath < npaths; ++ipath) {
                String pathString = orderBySpec.getOrderPath(ipath);
                String dirString = orderBySpec.getOrderDirection(ipath);

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
                  ERROR_ON_ERROR_NULL_ON_EMPTY, false);
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
          root.addNode(tree, entry, strictTypeMode);
        }
      }
    }

    if (isDualityView)
    {
      if (tree.hasSpatialClause())
        QueryException.throwExecutionException(QueryMessage.EX_SPATIAL_NOT_SUPPORTED_ON_DUALITY);
      else if (tree.hasContainsClause())
        QueryException.throwExecutionException(QueryMessage.EX_CONTAINS_NOT_SUPPORTED_ON_DUALITY);
      else if (tree.hasSqlJsonClause())
        QueryException.throwExecutionException(QueryMessage.EX_NOT_SUPPORTED_ON_DUALITY);
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

  public void appendJsonExists(StringBuilder output, boolean isTreatAsAvailable)
  {
    appendJsonExists(output, null, 0, isTreatAsAvailable);
  }

  public int appendJsonExists(StringBuilder output, String tokenFormat, int startingTokenIndex, boolean isTreatAsAvailable)
  {
    // Assumes that this method is invoked only if hasJsonExists()
    // returns true. So jsonExists cannot be null here.
    if (jsonExists == null)
      throw new IllegalStateException();

    int tokenCount = 0;

    String token = "?";

    output.append("'$?");
    output.append(jsonExists);
    output.append("'");

    int numBinds = getNumVals();
    if (numBinds > 0)
    {
      output.append(" passing ");

      for (int varNum = 0; varNum < numBinds; ++varNum)
      {
        if (tokenFormat != null)
          token = String.format(tokenFormat, startingTokenIndex);
        ValueTypePair vpair = valueArray.get(varNum);

        if (varNum > 0)
          output.append(", ");

        if (vpair.isTimestamp())
        {
          // This format can consume trailing timezones including a "Z"
          // but only if it's used with TO_TIMESTAMP_TZ.
          output.append("TO_TIMESTAMP_TZ(");
          output.append(token);
          output.append(",'SYYYY-MM-DD\"T\"HH24:MI:SS.FFTZH:TZM')");
        }
        else if (vpair.isDate())
        {
          // This format includes the time component to reliably consume
          // Oracle date+time values, but should also work if the time is
          // not present in the bind variable.
          output.append("TO_DATE(");
          output.append(token);
          output.append("?,'SYYYY-MM-DD\"T\"HH24:MI:SS')");
        }
        else if (isTreatAsAvailable && vpair.isObject()) 
        {
          output.append("treat(");
          output.append(token);
          output.append(" as json(object))");
        }
        else if (isTreatAsAvailable && vpair.isArray()) 
        {
          output.append("treat(");
          output.append(token);
          output.append(" as json(array))");
        }
        else
          output.append(token);

        tokenCount++;

        output.append(" as \"B");
        output.append(Integer.toString(varNum));
        output.append("\"");

	startingTokenIndex++;
      }
    }

    return tokenCount;
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

    if (vpair.isNumber())
    {
      //
      // For numbers that have to be coerced to date/time values, the
      // RDBMS offers Julian dates, which are numbered from 1 AD. Our
      // pseudo-standard in JSON is the millisecond tick count
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
    else if (vpair.isString())
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

  public boolean hasFilterSpec() 
  {
    return hasJsonExists()  ||
      hasSpatialClause()  ||
      hasContainsClause() ||
      hasSqlJsonClause();
  }

  public boolean appendFilterSpec(StringBuilder sb, boolean append,
                                  String columnName, String inputFormatClause,
                                  boolean isTreatAsAvailable)
	                          throws QueryException
  {
    int length = sb.length();
    appendFilterSpec(sb, append, columnName, inputFormatClause, null, 0, isTreatAsAvailable);

    if (sb.length() == length) {
      if (append == true)
        return true;
      else
        return false;
    }
    return true;
  }

  // Not part of a public API. Needs to be public for use from REST layer.
  public int appendFilterSpec(StringBuilder sb, boolean append,
                              String columnName, String inputFormatClause,
                              String format, int startingIndex,
                              boolean isTreatAsAvailable) throws QueryException
  {
    int tokenCount = 0;

    if (format != null && !format.equals(":\"F__QV_%d\""))
      QueryException.throwExecutionException(QueryMessage.EX_UNSUPPORTED_BIND_TOKEN);

    if (startingIndex < 0)
      QueryException.throwExecutionException(QueryMessage.EX_NEG_BIND_TOKEN);

    if (hasJsonExists())
    {
      appendAnd(sb, append);
      tokenCount = appendFilterSpecJsonExists(sb, columnName, inputFormatClause, format, startingIndex, isTreatAsAvailable);
      append = true;
    }

    if (hasSpatialClause())
    {
      List<SpatialClause> spatialClauses = getSpatialOperators();
      if (spatialClauses.size() != 0)
      {
        tokenCount += appendSpatialClauses(sb, append, columnName, 
			                   spatialClauses, format,
                                           (startingIndex + tokenCount));
        append = true;
      }
    }

    if (hasContainsClause())
    {
      List<ContainsClause> containsClauses = getContainsOperators();
      if (containsClauses.size() != 0)
      {
        tokenCount += appendFullTextClauses(sb, append, columnName,
                                            containsClauses,
                                            format,
                                            (startingIndex + tokenCount));
        append = true;
      }
    }

    if (hasSqlJsonClause())
    {
      // For now, don't support SQL/JSON from REST (other than SODA REST)
      if (format != null)
        QueryException.throwExecutionException(QueryMessage.EX_SQL_JSON_UNSUPPORTED);

      append = appendSqlJsonClauses(sb, append, columnName);
    }

    return tokenCount;
  }

  private int appendFilterSpecJsonExists(StringBuilder sb, String columnName,
                                         String inputFormatClause, String tokenFormat,
                                         int startingTokenIndex, boolean isTreatAsAvailable)
  {
    int tokenCount = 0;
    sb.append("JSON_EXISTS(");
    appendColumn(sb, columnName);
    if (inputFormatClause != null) {
      sb.append(" ").append(inputFormatClause);
    }
    
    sb.append(",");
    tokenCount = appendJsonExists(sb, tokenFormat, startingTokenIndex, isTreatAsAvailable);
    if (this.strictTypeMode) {
      sb.append(" type(strict)");
    }
    sb.append(")");
    return tokenCount;
  }

  public void appendFilterSpecOrderBy(StringBuilder sb,
                                      String columnName,
                                      String inputFormatClause) throws QueryException
  {
      appendFilterSpecOrderBy(sb, columnName, false, false, inputFormatClause, null);
  }
  
  public boolean hasIDColumnRewrite(boolean hasEmbeddedID, boolean materialized)
  {
    ArrayList<Predicate> orderByArray = getOrderByArray();
    Predicate entry = null;

    if (orderByArray.size() >= 1) {

      entry = orderByArray.get(0);
      String reType = entry.getReturnType();

      // If $orderby is on _id field with datatype varchar2
      // (either explicitly specified or defaulted), and it’s
      // embedded_oid key assignment with varchar2 column,
      // that orderby can be rewritten to orderby on ID column.
      //
      // Also, if $orderby is on _id field with datatype
      // json (either explicitly specified or defaulted),
      // and it’s an embedded_oid key with raw column (which
      // triggers materialized column codepath), that orderby
      // can also be rewritten to orderby on ID column.
      //
      // Both of these rewrites are to take advantage of the
      // out-of-the-box index on ID. Strictly speaking, the
      // second one (i.e. for materialized column) might not
      // be necessary (because there are SQL side rewrites that
      // do this).
      //
      // Note: if retType is null below, then it's the default
      // case. Whether the default is varchar2 or json is
      // figured out by checking strictTypeMode (if true, then
      // it's json, otherwise it's varchar2).
      if (entry.getQueryPath().toString().equals("_id") &&
          ((hasEmbeddedID && !materialized && !strictTypeMode && ((reType == null) ||
                                                                  (reType != null && reType.equals("varchar2")))) ||
           (hasEmbeddedID && materialized && strictTypeMode && ((reType == null) ||
                                                                (reType != null && reType.equals("json"))))))
      {
        return true;
      }
    }
    return false;
  }

  public void appendFilterSpecOrderBy(StringBuilder sb,
                                      String contentColumnName,
                                      boolean hasEmbeddedID,
				      boolean materialized,
                                      String inputFormatClause,
				      String keyColumnName) throws QueryException
  {
    ArrayList<Predicate> orderByArray = getOrderByArray();
    Predicate entry = null;

    if (hasIDColumnRewrite(hasEmbeddedID, materialized))
    {
      entry = orderByArray.get(0);
      appendColumn(sb, keyColumnName);

      if (entry.getValue().equals("1"))
        sb.append(" asc");
      else
        sb.append(" desc");

      return;
    }

    for (int i = 0; i < orderByArray.size(); i++) {
      entry = orderByArray.get(i);

      if (i != 0)
        sb.append(", ");

      String returnType = entry.getReturnType();

      if (strictTypeMode && ((returnType == null) || returnType.equals("json"))) {
        sb.append("JSON_QUERY(");
      }
      else {
        sb.append("JSON_VALUE(");
      }

      appendColumn(sb, contentColumnName);
      if (inputFormatClause != null)
        sb.append(" ").append(inputFormatClause);

      JsonQueryPath qpath = entry.getQueryPath();
      if (qpath.hasArraySteps())
        throw new QueryException(QueryMessage.EX_ARRAY_STEPS_IN_PATH.get());

      sb.append(", '");
      qpath.toSingletonString(sb);
      
      if (entry.getSortByMinMaxParam() && 
          (strictTypeMode  && ((returnType == null) || returnType.equals("json")))) {
        if (entry.getValue().equals("1"))
          sb.append("[*].min()");
        else
          sb.append("[*].max()");
      }
      
      sb.append("'");

      if (returnType != null) {
        sb.append(" returning ");
        sb.append(returnType);
      }

      String errorClause = entry.getErrorClause();
      // Null on error is the json_value default, so skip it.
      if (errorClause != null && !(errorClause.equals(AndORTree.NULL_ON_ERROR))) {
        sb.append(" ");
        sb.append(errorClause);
      }

      sb.append(")");

      if (entry.getValue().equals("1"))
        sb.append(" asc");
      else
        sb.append(" desc");
      
      if (entry.getSortByMinMaxParam() && 
          (strictTypeMode  && ((returnType == null) || returnType.equals("json")))){
        if (entry.getValue().equals("1"))
          sb.append(" nulls first");
        else
          sb.append(" nulls last");
      }
    }
  }

  private boolean appendSqlJsonClauses(StringBuilder sb, boolean append, String columnName) 
  {
    if (!hasSqlJsonClause())
      return (append);

    List<SqlJsonClause> sqlJsonClauses = getSqlJsonOperators();

    if (sqlJsonClauses.size() == 0)
      return (append);

    for (SqlJsonClause clause : sqlJsonClauses) {
      appendAnd(sb, append);
      append = true;

      // Surround the expression with parens and an optional not
      sb.append(clause.isNot() ? "not(" : "(");

      // Put on the special comparator function, if any
      String compFunc = clause.getCompareFunction();
      if (compFunc != null) {
        sb.append(compFunc);
        sb.append("(");
      }

      // Put on any conversion function
      String convFunc = clause.getConversionFunction();
      if (convFunc != null) {
        sb.append(convFunc);
        sb.append("(");
      }

      // Now add the extraction function
      if (clause.isExists())
        sb.append("JSON_QUERY(");
      else
        sb.append("JSON_VALUE(");
      appendColumn(sb, columnName);
      sb.append(", '");
      clause.getPath().toSingletonString(sb);
      sb.append("'");

      String returnType = clause.getReturningType();

      if (clause.isExists())
        sb.append(" with array wrapper)");
      else {
        // Add optional RETURNING clause and close the JSON_VALUE
        if (returnType != null) {
          sb.append(" returning ");
          sb.append(returnType);
        }
        sb.append(")");
      }

      // Close the optional conversion function
      if (convFunc != null)
        sb.append(")");

      // This counter keeps track of the absolute position within the list of
      // arguments/binds (one list follows the other if both are present).
      int argPosition = 0;

      ValueTypePair vpair;

      // Add any arguments to the compare function, then close it
      for (int i = 0; i < clause.getArgCount(); ++i) {
        sb.append(",");
        vpair = clause.getValue(argPosition++);
        appendFormattedBind(sb, vpair, clause);
      }
      if (compFunc != null)
        sb.append(")");

      // Add the comparator
      String sqlCmp = clause.getComparator();
      if (sqlCmp != null) {
        sb.append(" ");
        sb.append(sqlCmp);
      }

      // Add the comparands (if any) surrounded by parentheses (if necessary)
      int nBinds = clause.getBindCount();
      if (nBinds == 1) {
        sb.append(" ");
        vpair = clause.getValue(argPosition++);
        appendFormattedBind(sb, vpair, clause);
      } else if (nBinds > 1) {
        sb.append(" (");
        for (int i = 0; i < nBinds; ++i) {
          if (i > 0)
            sb.append(",");
          vpair = clause.getValue(argPosition++);
          appendFormattedBind(sb, vpair, clause);
        }
        sb.append(")");
      }

      // Close the outer surround (which might be a not)
      sb.append(")");
    }
    return (append);
  }

  public static void appendAnd(StringBuilder sb, boolean append)
  {
    if (append)
    {
      sb.append(" and ");
    }
  }

  private void appendColumn(StringBuilder sb, String colName) 
  {
    sb.append("\"");
    sb.append(colName);
    sb.append("\"");
  }

  private int appendFullTextClauses(StringBuilder sb, boolean append, String columnName,
                                    List<ContainsClause> containsClauses,
                                    String tokenFormat, int startingTokenIndex)
  {
    String token = "?";
    int tokenCount = 0;

    for (ContainsClause clause : containsClauses) {
      appendAnd(sb, append);
      append = true;

      if (clause.isNot())
        sb.append("not(");

      // The operator
      sb.append("JSON_TextContains(");

      // The target column with extraction path
      appendColumn(sb, columnName);
      sb.append(", '");
      clause.getPath().toLaxString(sb);
      if (tokenFormat != null)
        token = String.format(tokenFormat, startingTokenIndex);
      sb.append("', ");
      sb.append(token);
      sb.append(")");

      startingTokenIndex++;
      tokenCount++;

      if (clause.isNot())
        sb.append(")");
    }

    return tokenCount;
  }

  private int appendSpatialClauses(StringBuilder sb, boolean append, String columnName,
                                   List<SpatialClause> spatialClauses,
                                   String tokenFormat, int startingTokenIndex)
  {
    int tokenCount = 0;
    String token = "?";

    for (SpatialClause clause : spatialClauses) {
      if (tokenFormat != null)
        token = String.format(tokenFormat, startingTokenIndex);

      appendAnd(sb, append);
      append = true;

      // The operator
      sb.append("(");
      sb.append(clause.getOperator());
      sb.append("(");

      // The target column (a path expression extracting a value from the rows)
      sb.append("JSON_VALUE(");
      appendColumn(sb, columnName);
      sb.append(", '");
      clause.getPath().toSingletonString(sb);
      sb.append("' returning SDO_GEOMETRY");

      String errorClause = clause.getErrorClause();
      // Null on error is the json_value default, so skip it.
      if (errorClause != null && !(errorClause.equals(AndORTree.NULL_ON_ERROR))) {
        sb.append(" ");
        sb.append(errorClause);
      }

      sb.append("),");

      // The search reference (i.e. bound from the QBE)
      // Hard-coded to "erorr on error" - we want to make
      // sure user always gives us a correct QBE (otherwise he'll get an error)
      sb.append("JSON_VALUE(");
      sb.append(token);
      startingTokenIndex++;
      tokenCount++;
      sb.append(", '$' returning SDO_GEOMETRY error on error)");

      // $near has a distance/units string which is bound
      if (clause.getDistance() != null) {
        sb.append(", ");
        if (tokenFormat != null)
          token = String.format(tokenFormat, startingTokenIndex);
        startingTokenIndex++;
        tokenCount++;
        sb.append(token);
      }

      // Close the overall operator
      if (clause.isNot())
        sb.append(") <> 'TRUE')"); // ### Not clear this is correct
      else
        sb.append(") = 'TRUE')");
    }

    return tokenCount;
  }
  
  public int bind(PreparedStatement stmt, int parameterIndex,
      BiFunction<String, ValueTypePair, Void> callback) throws SQLException 
  {
    parameterIndex = bindJsonExists(stmt, parameterIndex, callback);
    parameterIndex = bindSpatialClauses(stmt, parameterIndex, callback);
    parameterIndex = bindContainsClauses(stmt, parameterIndex, callback);
    parameterIndex = bindSqlJsonClauses(stmt, parameterIndex, callback);
    return parameterIndex;
  }
  
  private int bindJsonExists(PreparedStatement stmt, int parameterIndex,
      BiFunction<String, ValueTypePair, Void> callback) throws SQLException {
    int count = 0;
    for (ValueTypePair item : getValueArray()) {
      ++parameterIndex;
      if (callback != null) {
        String name = "B" + Integer.toString(++count);
        callback.apply(name, item);
      }
      bindTypedParam(stmt, item, parameterIndex);
    }
    return parameterIndex;
  }
  
  private int bindSqlJsonClauses(PreparedStatement stmt, int parameterIndex, 
      BiFunction<String, ValueTypePair, Void> callback) throws SQLException 
  {
    List<SqlJsonClause> sqlJsonClauses = getSqlJsonOperators();
    if (sqlJsonClauses != null)
    {
      int bindCount = 0;
      for (SqlJsonClause clause : sqlJsonClauses)
      {
        ValueTypePair varg;
        int           vpos = 0;

        for (int i = 0; i < clause.getArgCount(); ++i)
        {
          varg = clause.getValue(vpos++);
          if (callback != null) {
            String name = "JV" + (++bindCount);
            callback.apply(name, varg);
          }

          bindTypedParam(stmt, varg, ++parameterIndex);
        }

        for (int i = 0; i < clause.getBindCount(); ++i)
        {
          varg = clause.getValue(vpos++);
          if (callback != null) {
            String name = "JV" + (++bindCount);
            callback.apply(name, varg);
          }

          bindTypedParam(stmt, varg, ++parameterIndex);
        }
      }
    }
    return parameterIndex;
  }

  private int bindContainsClauses(PreparedStatement stmt, int parameterIndex, 
      BiFunction<String, ValueTypePair, Void> callback) throws SQLException 
  {
    List<ContainsClause> containsClauses = getContainsOperators();
    if (containsClauses != null)
    {
      int bindCount = 0;
      String bindName;

      for (ContainsClause clause : containsClauses)
      {
        String searchString = clause.getSearchString();

        if (searchString != null)
        {
          if (callback != null)
          {
            bindName = "TXT" + Integer.toString(++bindCount);
            callback.apply(bindName, new ValueTypePair(searchString));
          }
          stmt.setString(++parameterIndex, searchString);
        }
      }
    }
    return parameterIndex;
  }

  private int bindSpatialClauses(PreparedStatement stmt, int parameterIndex, 
      BiFunction<String, ValueTypePair, Void> callback) throws SQLException 
  {
    List<SpatialClause> spatialClauses = getSpatialOperators();
    if (spatialClauses != null)
    {
      int bindCount = 0;
      String bindName;

      for (SpatialClause clause : spatialClauses)
      {
        String spatialReference = clause.getReference();
        String spatialDistance = clause.getDistance();

        if (spatialReference != null)
        {
          if (callback != null)
          {
            bindName = "GEO" + Integer.toString(++bindCount);
            callback.apply(bindName, new ValueTypePair(spatialReference));
          }
          stmt.setString(++parameterIndex, spatialReference);

          if (spatialDistance != null)
          {
            if (callback != null)
            {
              bindName = "GEO" + Integer.toString(++bindCount);
              callback.apply(bindName, new ValueTypePair(spatialDistance));
            }
            stmt.setString(++parameterIndex, spatialDistance);
          }
        }
      }
    }
    return parameterIndex;
  }

  private void bindTypedParam(PreparedStatement stmt, ValueTypePair item, int parameterIndex) throws SQLException {
    if (this.strictTypeMode)
      bindTypedParameterStrict(stmt, item, parameterIndex);
    else 
      bindTypedParamLax(stmt, item, parameterIndex);
  }
  
  private void bindTypedParamLax(PreparedStatement stmt, ValueTypePair item, int parameterIndex) throws SQLException 
  {
    JsonValue value = item.getValue();    
    switch (value.getValueType()) {
    case NUMBER:
      stmt.setBigDecimal(parameterIndex, item.getNumberValue());
      break;
    case STRING:
      stmt.setString(parameterIndex, item.getStringValue());
      break;
    case TRUE:
    case FALSE:
      stmt.setString(parameterIndex, String.valueOf(item.getBooleanValue()));
      break;
    case NULL:
      stmt.setString(parameterIndex, NULL);
      break;
    case ARRAY:
    case OBJECT:
      throw new IllegalStateException();
    }
  }

  private void bindTypedParameterStrict(PreparedStatement stmt, ValueTypePair item, int parameterIndex) throws SQLException
  {
    JsonValue value = item.getValue();
    if (value instanceof java.sql.Wrapper) {
      Wrapper wrapper = (Wrapper)value;
      if (wrapper.isWrapperFor(OracleJsonValue.class)) {
        // Treat OSON bind values specially
        bindTypedParameterStrict(stmt, wrapper.unwrap(OracleJsonValue.class), parameterIndex);
        return;
      }
    }
    
    switch (value.getValueType()) {
    case ARRAY:
    case OBJECT:
    case FALSE:
    case TRUE:
    case NULL:
      stmt.setObject(parameterIndex, value);
      break;
    case NUMBER:
      stmt.setBigDecimal(parameterIndex, ((JsonNumber)value).bigDecimalValue());
      break;
    case STRING:
      stmt.setString(parameterIndex, ((JsonString)value).getString());
      break;
    default:
      break;
    }
  }

  public static void bindTypedParameterStrict(PreparedStatement stmt, OracleJsonValue item, int index) throws SQLException {
    OraclePreparedStatement ostmt = (OraclePreparedStatement)stmt;
    switch (item.getOracleJsonType()) {
    case ARRAY:
    case OBJECT:
    case NULL:
    case TRUE:
    case FALSE:
      // bound as JSON type (null/false/true may never since filter
      // translation doesn't create binds for them)
      stmt.setObject(index, item);
      break;
    case BINARY:
      // raw and id types bound as raw (ID type stripped here)
      ostmt.setRAW(index, item.asJsonBinary().getRAW());
      break;
    case DATE:
      ostmt.setDATE(index, item.asJsonDate().getDATE());
      break;
    case TIMESTAMP:
      ostmt.setTIMESTAMP(index, item.asJsonTimestamp().getTIMESTAMP());
      break;
    case TIMESTAMPTZ:
      // for JSON_EXISTS, we bind timestamp with timezone as timezone at UTC so that index pickup will occur
      LocalDateTime ldt = toUtcLocalDateTime(item);
      ostmt.setObject(index, ldt, OracleTypes.TIMESTAMP);
      break;
    case DECIMAL:
      // INT/LONG/DECIMAL flags lost here.  All bound as Oracle NUMBER
      ostmt.setNUMBER(index, item.asJsonDecimal().getNUMBER());
      break;
    case DOUBLE:
    case FLOAT:
      // Floating point bound as Oracle NUMBER (type fidelity lost)
      // This may need a mode to use double/float when this mode is introd 
      stmt.setBigDecimal(index, item.asJsonNumber().bigDecimalValue());
      break;
    case INTERVALDS:
      ostmt.setINTERVALDS(index, item.asJsonIntervalDS().getINTERVALDS());
      break;
    case INTERVALYM:
      ostmt.setINTERVALYM(index, item.asJsonIntervalYM().getINTERVALYM());
      break;
    case STRING:
      OracleJsonString jstr = item.asJsonString();
      String str = jstr.getString();
      // The empty string binding is lossy.
      // If this path is ever executed, we don't want to bind it as null 
      // because it will be mapped to JSON null because
      // of the ambiguity.
      if (str.isEmpty())
        ostmt.setObject(index, jstr);
      else
        ostmt.setCHAR(index, jstr.getCHAR());
      break;
    default:
      throw new IllegalStateException();
    }
  }

  private static LocalDateTime toUtcLocalDateTime(OracleJsonValue item) {
    OracleJsonTimestampTZ tstz = item.asJsonTimestampTZ();
    OffsetDateTime offsetDateTime = tstz.getOffsetDateTime();
    LocalDateTime ldt = offsetDateTime.atZoneSameInstant(ZoneOffset.UTC).toLocalDateTime();
    return ldt;
  }

  public static void bindTypedParameterStrict(PreparedStatement stmt, OracleJsonValue item, String name) throws SQLException {
    OraclePreparedStatement ostmt = (OraclePreparedStatement)stmt;
    switch (item.getOracleJsonType()) {
    case ARRAY:
    case OBJECT:
    case NULL:
    case TRUE:
    case FALSE:
      // bound as JSON type (null/false/true may never since filter
      // translation doesn't create binds for them)
      ostmt.setObjectAtName(name, item);
      break;
    case BINARY:
      // raw and id types bound as raw (ID type stripped here)
      ostmt.setRAWAtName(name, item.asJsonBinary().getRAW());
      break;
    case DATE:
      ostmt.setDATEAtName(name, item.asJsonDate().getDATE());
      break;
    case TIMESTAMP:
      ostmt.setTIMESTAMPAtName(name, item.asJsonTimestamp().getTIMESTAMP());
      break;
    case TIMESTAMPTZ:
      // for JSON_EXISTS, we bind timestamp with timezone as timezone at UTC so that index pickup will occur
      LocalDateTime ldt = toUtcLocalDateTime(item);
      ostmt.setObjectAtName(name, ldt, OracleTypes.TIMESTAMP);
      break;
    case DECIMAL:
      // INT/LONG/DECIMAL flags lost here.  All bound as Oracle NUMBER
      ostmt.setNUMBERAtName(name, item.asJsonDecimal().getNUMBER());
      break;
    case DOUBLE:
    case FLOAT:
      // Floating point bound as Oracle NUMBER (type fidelity lost)
      // This may need a mode to use double/float when this mode is introd
      ostmt.setBigDecimalAtName(name, item.asJsonNumber().bigDecimalValue());
      break;
    case INTERVALDS:
      ostmt.setINTERVALDSAtName(name, item.asJsonIntervalDS().getINTERVALDS());
      break;
    case INTERVALYM:
      ostmt.setINTERVALYMAtName(name, item.asJsonIntervalYM().getINTERVALYM());
      break;
    case STRING:
      ostmt.setCHARAtName(name, item.asJsonString().getCHAR());
      break;
    default:
      throw new IllegalStateException();
    }
  }
}
