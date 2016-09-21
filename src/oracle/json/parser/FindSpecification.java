/* $Header: xdk/src/java/json/src/oracle/json/parser/FindSpecification.java /main/1 2014/09/25 17:43:15 dmcmahon Exp $ */

/* Copyright (c) 2014, Oracle and/or its affiliates. All rights reserved.*/

/*
   DESCRIPTION
    Parses a query specification and holds the resulting information
    consisting of either an array of scalar keys, or a QBE projection.

   PRIVATE CLASSES

   NOTES
    Unfortunately this means a double-parse since for the QBE case there
    will be another document-like parse to create the QBE tree.

   MODIFIED    (MM/DD/YY)
    rkadwe      09/05/14 - Creation
 */

/**
 *  @version $Header: xdk/src/java/json/src/oracle/json/parser/FindSpecification.java /main/1 2014/09/25 17:43:15 dmcmahon Exp $
 *  @author  dmcmahon
 *  @since   release specific (what release of product did this appear in)
 */

package oracle.json.parser;

import java.io.InputStream;
import java.io.IOException;
import java.io.StringWriter;

import java.math.BigDecimal;

import java.util.Map.Entry;
import java.util.HashSet;
import java.util.ArrayList;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonNumber;
import javax.json.JsonValue;
import javax.json.JsonException;
import javax.json.stream.JsonGenerator;
import javax.json.stream.JsonGenerationException;
import javax.json.stream.JsonParsingException;
import javax.json.stream.JsonParser;
import javax.json.stream.JsonParser.Event;

public class FindSpecification
{
  private boolean         is_filter  = true;
  private String          projection = null;
  private HashSet<String> idList     = null;

  public FindSpecification(InputStream inp)
    throws QueryException
  {
    parse(inp);
  }

  private void makeException(QueryMessage msg, Object... params)
   throws QueryException
  {
    QueryException.throwSyntaxException(msg, params);
  }

  /**
   * Get a list of IDs from an array-like filter spec.
   */
  private void findKeys(JsonParser jParser)
    throws QueryException, JsonParsingException
  {
    ArrayList<String> arr = new ArrayList<String>();

    String badKeyType = null;

    boolean endLoop = false;

    while (jParser.hasNext())
    {
      if (endLoop)
        makeException(QueryMessage.EX_EXTRA_INPUT);

      Event ev = jParser.next();

      switch (ev)
      {
      case VALUE_STRING:
        String sval = jParser.getString();
        arr.add(sval);
        break;

      case VALUE_NUMBER:
        BigDecimal decval = jParser.getBigDecimal();
        // ### Should we check (decval.scale() <= 0) ?
        arr.add(decval.toString());
        break;

      case END_ARRAY:
        endLoop = true;
        break;

      case START_ARRAY:
        badKeyType = "array";
        break;

      case START_OBJECT:
        badKeyType = "array";
        break;

      case VALUE_TRUE:
        badKeyType = "true";
        break;

      case VALUE_FALSE:
        badKeyType = "false";
        break;

      case VALUE_NULL:
        badKeyType = "null";
        break;

      default:
        badKeyType = ev.toString();
      }

      if (badKeyType != null)
        makeException(QueryMessage.EX_INVALID_ARRAY_KEY, badKeyType);
    }

    this.idList = new HashSet<String>(arr.size());

    for (String key : arr)
      this.idList.add(key);
  }

  private void findProjection(JsonParser jParser)
    throws QueryException, JsonParsingException, JsonGenerationException
  {
    String        key       = null;
    StringWriter  strWriter = null;
    JsonGenerator generator = null;

    int level = 0;

    while (jParser.hasNext())
    {
      Event event = jParser.next();

      if (event == JsonParser.Event.KEY_NAME)
      {
        key = jParser.getString();
        if ((level == 0) && key.equals("$project"))
        {
          // This key's decendants form a projection
          strWriter = new StringWriter();
          generator = Json.createGenerator(strWriter);
        }
        continue;
      }

      // Skip unintersting events
      if (generator == null)
      {
        if ((event == JsonParser.Event.START_ARRAY) ||
            (event == JsonParser.Event.START_OBJECT))
          ++level;
        else if ((event == JsonParser.Event.END_ARRAY) ||
                 (event == JsonParser.Event.END_OBJECT))
          --level;
        key = null;
        continue;
      }

      //
      // The $project must be an object
      //
      if (event == JsonParser.Event.START_OBJECT)
      {
        if ((key == null) || (level == 0))
          generator.writeStartObject();
        else
          generator.writeStartObject(key);

        ++level;

        key = null;
        continue;
      }

      if (level == 0)
        makeException(QueryMessage.EX_INVALID_PROJECTION);

      //
      // These events are part of the $project key
      //

      if (event == JsonParser.Event.VALUE_STRING) 
      {
        if (key != null)
          generator.write(key, jParser.getString());
        else
          generator.write(jParser.getString());
      }
      else if (event == JsonParser.Event.VALUE_NUMBER)
      {
        BigDecimal decval = jParser.getBigDecimal();
        if (decval.scale() <= 0)
        {
          if (key != null)
            generator.write(key, decval.toBigInteger());
          else
            generator.write(decval.toBigInteger());
        }
        else
        {
          if (key != null)
            generator.write(key, decval);
          else
            generator.write(decval);
        }
      }
      else if (event == JsonParser.Event.VALUE_TRUE)
      {
        if (key != null)
          generator.write(key, true);
        else
          generator.write(true);
      }
      else if (event == JsonParser.Event.VALUE_FALSE)
      {
        if (key != null)
          generator.write(key, false);
        else
          generator.write(false);
      }
      else if (event == JsonParser.Event.VALUE_NULL)
      {
        if (key != null)
          generator.writeNull(key);
        else
          generator.writeNull();
      }
      else if (event == JsonParser.Event.START_ARRAY)
      {
        ++level;
        if (key == null)
          generator.writeStartArray();
        else
          generator.writeStartArray(key);
      }
      else if (event == JsonParser.Event.END_OBJECT)
      {
        --level;
        generator.writeEnd();
      }
      else if (event == JsonParser.Event.END_ARRAY)
      {
        --level;
        generator.writeEnd();  
      }

      key = null;

      // If we're back to level 0 this is the end of the projection
      if (level == 0)
      {
        generator.flush();
        generator.close();

        this.projection = strWriter.toString();
      }
    }

    // Projection not found
  }

  /**
   * Parse the query specification, throwing an exception if something's wrong.
   * Returns true if this is recognized as a QBE, false if it's recognized
   * as an array of keys.
   */
  private void parse(InputStream source)
    throws QueryException
  {
    JsonParser jParser = null;
    boolean    silent = true;

    if (source == null)
      makeException(QueryMessage.EX_SYNTAX_ERROR); // ### Need better than this

    try
    {
      jParser = Json.createParser(source);

      // Peek at the first event
      Event firstEvent = jParser.next();

      if (firstEvent == JsonParser.Event.START_OBJECT)
      {
        // See if there is a projection associated with it and if so extract it
        findProjection(jParser);
      }
      else if (firstEvent == JsonParser.Event.START_ARRAY)
      {
        // This is a list of keys, extract them
        findKeys(jParser);
        is_filter = false;
      }
      else
      {
        makeException(QueryMessage.EX_SYNTAX_ERROR); // ### Need better than this
      }

      // Allow an IO exception to be thrown on close
      silent = false;
    }
    catch (JsonException e)
    {
      throw new QueryException(QueryMessage.EX_SYNTAX_ERROR.get(), e);
    }
    finally
    {
      try
      {
        jParser.close();
        source.close();
      }
      catch (IOException e)
      {
        if (!silent)
          throw(new QueryException(QueryMessage.EX_SYNTAX_ERROR.get(), e));
      }
    }
  }

  public boolean isFilter()
  {
    return(is_filter);
  }

  public HashSet<String> getKeys()
  {
    return(idList);
  }

  public String getProjection()
  {
    return(projection);
  }
}
