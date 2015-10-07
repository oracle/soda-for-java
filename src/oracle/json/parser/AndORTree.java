/* Copyright (c) 2014, 2015, Oracle and/or its affiliates. 
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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map.Entry;

import javax.json.JsonString;
import javax.json.JsonValue;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonException;
import javax.json.stream.JsonParsingException;

public class AndORTree
{
  private AndORNode                root;
  private ArrayList<ValueTypePair> valueArray;
  private ArrayList<Predicate>     orderByArray;
  private HashSet<String> keysSet;

  private String predChar = "@";

  private StringBuilder jsonExists;

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

  void addToOrderByArray(Predicate pred)
  {
    orderByArray.add(pred);
  }

  void addToKeys(String item)
  {
    keysSet.add(item);
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

  boolean isJSONPrimitive(JsonValue item)
  {
    JsonValue.ValueType valueType = item.getValueType();
    
    if ((valueType == JsonValue.ValueType.ARRAY) ||
        (valueType == JsonValue.ValueType.OBJECT)) 
    {
      return false;     
    }
    return true;
  }

  boolean isJSONArray(JsonValue item)
  {
    if (item.getValueType() == JsonValue.ValueType.ARRAY)
      return true;
    return false;
  }

  boolean isJSONObject(JsonValue item)
  {
    if (item.getValueType() == JsonValue.ValueType.OBJECT)
      return true;
    return false;
  }

  ValueTypePair addToValueArray(JsonValue item, String fieldName)
    throws QueryException
  {
    ValueTypePair newValue = null;

    if (isJSONPrimitive(item))
    {
      if (item.getValueType() == JsonValue.ValueType.NULL)
      {
        newValue = new ValueTypePair(ValueTypePair.TYPE_NULL);
      }
      else if (item.getValueType() == JsonValue.ValueType.TRUE)
      {
        newValue = new ValueTypePair(true, ValueTypePair.TYPE_BOOLEAN);
      }
      else if (item.getValueType() == JsonValue.ValueType.FALSE)
      {
        newValue = new ValueTypePair(false, ValueTypePair.TYPE_BOOLEAN);
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

    if (newValue != null)
      valueArray.add(newValue);
    return(newValue);
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

  private int getNumVals()
  {
    if (getValueArray() != null)
      return valueArray.size();

    return 0;
  }

  public static AndORTree createTree(InputStream stream)
    throws QueryException
  {
    FilterLoader dl;

    try
    {
      dl = new FilterLoader(stream);
    }
    catch (JsonException e)
    {
      // This can occur if the underlying parser can't detect the encoding,
      // or if there's an underlying IOException.
      throw new QueryException(e);
    }

    return createTree(dl);
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

    if (jObj != null)
    {
      for (Entry<String, JsonValue> entry : jObj.entrySet())
      {
        entryKey = entry.getKey();

        if (entryKey.equalsIgnoreCase("$project"))
        {
          // Ignore this because it was parsed by the upper layer
        }
        else if (entryKey.equalsIgnoreCase("$query"))
        {
          queryOperatorFound = true;

          if (basicQBEFound)
          {
            QueryException.throwSyntaxException(
              QueryMessage.EX_QUERY_WITH_OTHER_OPS);
          }

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
          checkIfValueIsJsonObject(entry.getValue(), "$orderby");

          // The FilterLoader is responsible for keeping
          // order-by opaths in the order they were specified,
          // and delivering them here. As a work-around
          // for customers that need control over the ordering we will
          // respect a sort based on the absolute value of the integers.
          // Thus 1, -2, 3, 4, -5 etc.

          int ipath;
          int npaths = loader.getOrderCount();
          if (npaths <= 0) continue; // Nothing to do

          String[] paths = new String[npaths];
          int[]    posns = new int[npaths];

          for (ipath = 0; ipath < npaths; ++ipath)
          {
            String pathString = loader.getOrderPath(ipath);
            String dirString  = loader.getOrderDirection(ipath);

            if (pathString == null) break; // End of array

            int ival = 0;
            // A null means one of the keys had a nonsense value
            if (dirString != null)
            {
              // Otherwise try to parse it as an integer
              try
              {
                ival = Integer.parseInt(dirString);
              }
              catch (NumberFormatException e)
              {
                QueryException.throwSyntaxException(QueryMessage.EX_BAD_ORDERBY_PATH_VALUE, pathString, dirString);
              }

              // 0 value is not allowed
              if (ival == 0)
              {
                QueryException.throwSyntaxException(QueryMessage.EX_BAD_ORDERBY_PATH_VALUE, pathString, dirString);
              }
            }
            else
            {
              QueryException.throwSyntaxException(QueryMessage.EX_BAD_ORDERBY_PATH_VALUE2, pathString);
            }

            // Compute absolute value of the position
            int aval = (ival < 0) ? -ival : ival;

            // Linear search for insertion point
            // Ties are broken by inserting the most recent value last
            int ipos;
            for (ipos = 0; ipos < ipath; ++ipos)
            {
              int bval = posns[ipos];
              if (bval < 0) bval = -bval;
              if (aval < bval) break; // Insertion point found
            }

            // If necessary shift values to make a slot for this entry
            // This allows clients to use 1, 2, 3, etc. to forcibly order keys
            if (ipos < ipath)
            {
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
          for (ipath = 0; ipath < npaths; ++ipath)
          {
            Predicate pred = new Predicate(new JsonQueryPath(paths[ipath]),
                                           (posns[ipath] < 0) ? "-1" : "1");
            tree.addToOrderByArray(pred);
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
        if (varNum > 0)
          output.append(" , ");
        output.append(" ? as \"B");
        output.append(Integer.toString(varNum));
        output.append("\"");
      }
    }
  }
}
