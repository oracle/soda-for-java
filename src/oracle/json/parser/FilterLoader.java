/* Copyright (c) 2014, 2016, Oracle and/or its affiliates. 
All rights reserved.*/

/*
   DESCRIPTION
     Specialized DocumentLoader for QBEs.
 */

/**
 * This class is not part of the public API, and is
 * subject to change.
 *
 * Do not rely on it in your application code.
 *
 * @author Doug McMahon
 */ 

package oracle.json.parser;

import java.io.InputStream;

import java.util.ArrayList;

import java.math.BigDecimal;

import javax.json.Json;
import javax.json.JsonObjectBuilder;
import javax.json.JsonException;
import javax.json.stream.JsonParser;
import javax.json.stream.JsonParser.Event;
import javax.json.stream.JsonParsingException;

public class FilterLoader extends DocumentLoader
{

  public FilterLoader(InputStream inp)
    throws JsonException
  {
    super(inp);
  }

  private boolean parseOrderBy = false;

  // Only one $orderby is allowed.
  // False by default, and set to true after
  // the first $orderby is parsed.
  private boolean orderByParsed = false;

  // Note: this message is for an exception that will be caught
  // in SODA, and converted to a proper user exception.
  // It's not meant to be seen by end user.
  final static String MULTIPLE_ORDERBY_OPS = "SODA FOR JAVA: ENCOUNTERED MULTIPLE $orderby";

  // Only one $query is allowed.
  // False by default, and set to true after
  // the first $query is parsed.
  private boolean queryParsed = false;

  // Note: this message is for an exception that will be caught
  // in SODA, and converted to a proper user exception.
  // It's not meant to be seen by end user.
  final static String MULTIPLE_QUERY_OPS = "SODA FOR JAVA: ENCOUNTERED MULTIPLE $query";

  private ArrayList<String> orderKeys = null;
  private ArrayList<String> orderVals = null;

  private final String ORDERBY = "$orderby";
  private final String QUERY = "$query";

  int getOrderCount()
  {
    if ((orderKeys == null) || (orderVals == null)) return(0);
    return(orderKeys.size());
  }

  /**
   * Get the Nth order key path.
   * Returns null if the position is out of bounds
   */
  String getOrderPath(int pos)
  {
    if (pos < 0) return(null);
    if (orderKeys == null) return(null);
    if (pos >= orderKeys.size()) return(null);
    return(orderKeys.get(pos));
  }

  /**
   * Get the Nth order key direction (1 or -1)
   * Returns null if the value is "bad" (true/false/null)
   */
  String getOrderDirection(int pos)
  {
    if (pos < 0) return(null);
    if (orderVals == null) return(null);
    if (pos >= orderVals.size()) return(null);
    return(orderVals.get(pos));
  }

  private void appendOrderBy(int depth, String key, String val)
  {
    if (!parseOrderBy || (depth != 1)) return;

    if (orderKeys == null) orderKeys = new ArrayList<String>();
    if (orderVals == null) orderVals = new ArrayList<String>();

    orderKeys.add(key);
    orderVals.add(val);
  }

  @Override
  protected JsonObjectBuilder parseObject(int depth)
    throws JsonParsingException
  {
    JsonObjectBuilder obuilder = Json.createObjectBuilder();

    String key = null;
    String val = null;

    // Consume all events for that document
    while (parser.hasNext())
    {
      Event ev = parser.next();

      switch (ev)
      {
      case START_OBJECT:
        if (depth == 0) parseOrderBy = ORDERBY.equals(key);
        obuilder.add(key, parseObject(depth + 1));
        if (depth == 0) parseOrderBy = false;
        appendOrderBy(depth, key, null); // Signals a "bad" key
        key = null;
        break;

      case START_ARRAY:
        obuilder.add(key, parseArray(depth + 1));
        appendOrderBy(depth, key, null); // Signals a "bad" key
        key = null;
        break;

      case END_OBJECT:
        return(obuilder);

      case KEY_NAME:
        key = parser.getString();

        // If the top-level $orderby was already encountered,
        // and another top-level $orderby is seen,
        // throw an exception.
        if (orderByParsed)
        {
          // We need to throw an exception here, but are
          // limited by the overriden parseObject() method's
          // signature to throwing a JsonParsingException.
          if (key.equals(ORDERBY) && depth == 0)
             throw new JsonParsingException(MULTIPLE_ORDERBY_OPS, null);
        }
        // If the top-level $orderby hasn't been already encountered,
        // and it's seen here, set the flag signifying this
        // fact.
        else
        {
          if (key.equals(ORDERBY) && depth == 0)
            orderByParsed = true;
        }

        // If the top-level $query was already encountered,
        // and another top-level $order is seen,
        // throw an exception.
        if (queryParsed)
        {
          // We need to throw an exception here, but are
          // limited by the overriden parseObject() method's
          // signature to throwing a JsonParsingException.
          if (key.equals(QUERY) && depth == 0)
            throw new JsonParsingException(MULTIPLE_QUERY_OPS, null);
        }
        // If the top-level $orderby hasn't been already encountered,
        // and it's seen here, set the flag signifying this
        // fact.
        else
        {
          if (key.equals(QUERY) && depth == 0)
            queryParsed = true;
        }

        break;

      case VALUE_STRING:
        val = parser.getString();
        obuilder.add(key, val);
        appendOrderBy(depth, key, val);
        key = null;
        break;

      case VALUE_NUMBER:
        BigDecimal decval = parser.getBigDecimal();
        if (decval.scale() <= 0)
          obuilder.add(key, decval.toBigInteger());
        else
          obuilder.add(key, decval);
        appendOrderBy(depth, key, decval.toString());
        key = null;
        break;

      case VALUE_NULL:
        obuilder.addNull(key);
        appendOrderBy(depth, key, null); // Signals a bad key
        key = null;
        break;

      case VALUE_TRUE:
        obuilder.add(key, true);
        appendOrderBy(depth, key, null); // Signals a bad key
        key = null;
        break;

      case VALUE_FALSE:
        obuilder.add(key, false);
        appendOrderBy(depth, key, null); // Signals a bad key
        key = null;
        break;

      default:
        throw new IllegalStateException();
      }
    }

    return(obuilder);
  }
}
