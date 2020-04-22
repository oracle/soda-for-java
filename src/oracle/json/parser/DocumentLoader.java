/* Copyright (c) 2014, 2019, Oracle and/or its affiliates. 
All rights reserved.*/

/*
   DESCRIPTION
     Json document loader based on JSON Event Parser
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

import oracle.json.common.JsonFactoryProvider;

import java.nio.charset.Charset;

import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.StringReader;

import java.math.BigDecimal;

import javax.json.Json;
import javax.json.JsonStructure;
import javax.json.JsonObjectBuilder;
import javax.json.JsonArrayBuilder;
import javax.json.JsonException;
import javax.json.stream.JsonParser;
import javax.json.stream.JsonParser.Event;
import javax.json.stream.JsonParsingException;

public class DocumentLoader
{
  protected final JsonFactoryProvider jProvider;

  protected final JsonParser parser;

  public DocumentLoader(JsonFactoryProvider jProvider, byte[] data)
    throws JsonException
  {
    this(jProvider, new ByteArrayInputStream(data));
  }

  public DocumentLoader(JsonFactoryProvider jProvider, String str)
    throws JsonException
  {
    this.jProvider = jProvider;
    parser = jProvider.getParserFactory().createParser(new StringReader(str));
  }

  public DocumentLoader(JsonFactoryProvider jProvider, InputStream inp)
    throws JsonException
  {
    this.jProvider = jProvider;
    parser = jProvider.getParserFactory().createParser(inp);
  }

  public DocumentLoader(JsonFactoryProvider jProvider, InputStream inp, Charset cs)
    throws JsonException
  {
    this.jProvider = jProvider;
    InputStreamReader reader = new InputStreamReader(inp, cs);
    parser = jProvider.getParserFactory().createParser(reader);
  }

  protected JsonObjectBuilder parseObject(int depth)
    throws JsonParsingException
  {
    JsonObjectBuilder obuilder = jProvider.createObjectBuilder();

    String key = null;

    // Consume all events for that document
    while (parser.hasNext())
    {
      Event ev = parser.next();

      switch (ev)
      {
      case START_OBJECT:
        obuilder.add(key, parseObject(depth + 1));
        key = null;
        break;

      case START_ARRAY:
        obuilder.add(key, parseArray(depth + 1));
        key = null;
        break;

      case END_OBJECT:
        return(obuilder);

      case KEY_NAME:
        key = parser.getString();
        break;

      case VALUE_STRING:
        obuilder.add(key, parser.getString());
        key = null;
        break;

      case VALUE_NUMBER:
        BigDecimal decval = parser.getBigDecimal();
        if (decval.scale() <= 0)
          obuilder.add(key, decval.toBigInteger());
        else
          obuilder.add(key, decval);
        key = null;
        break;

      case VALUE_NULL:
        obuilder.addNull(key);
        key = null;
        break;

      case VALUE_TRUE:
        obuilder.add(key, true);
        key = null;
        break;

      case VALUE_FALSE:
        obuilder.add(key, false);
        key = null;
        break;

      default:
        throw new IllegalStateException();
      }
    }

    return(obuilder);
  }

  protected JsonArrayBuilder parseArray(int depth)
    throws JsonParsingException
  {
    JsonArrayBuilder abuilder = jProvider.createArrayBuilder();

    // Consume all events for that document
    while (parser.hasNext())
    {
      Event ev = parser.next();

      switch (ev)
      {
      case START_OBJECT:
        abuilder.add(parseObject(depth + 1));
        break;

      case START_ARRAY:
        abuilder.add(parseArray(depth + 1));
        break;

      case END_ARRAY:
        return(abuilder);

      case VALUE_STRING:
        abuilder.add(parser.getString());
        break;

      case VALUE_NUMBER:
        BigDecimal decval = parser.getBigDecimal();
        if (decval.scale() <= 0)
          abuilder.add(decval.toBigInteger());
        else
          abuilder.add(decval);
        break;

      case VALUE_NULL:
        abuilder.addNull();
        break;

      case VALUE_TRUE:
        abuilder.add(true);
        break;

      case VALUE_FALSE:
        abuilder.add(false);
        break;

      default:
        // Cannot happen
        throw new IllegalStateException();
      }
    }

    return(abuilder);
  }

  public JsonStructure parse()
    throws JsonParsingException, JsonException
  {
    JsonStructure result = null;

    try
    {
      while (parser.hasNext())
      {
        Event ev = parser.next();

        switch (ev)
        {
        case START_OBJECT:
          result = parseObject(0).build();
          break;

        case START_ARRAY:
          result = parseArray(0).build();
          break;

        default:
          // ### support for primitive documents?
          throw new JsonParsingException("", null);
        }
      }
    }
    finally
    {
      parser.close();
    }

    return(result);
  }
}
