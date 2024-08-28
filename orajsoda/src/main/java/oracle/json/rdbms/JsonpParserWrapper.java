/* $Header: xdk/src/java/json/src/oracle/json/rdbms/JsonpParserWrapper.java /st_xdk_soda1/1 2021/08/25 00:17:26 morgiyan Exp $ */

/* Copyright (c) 2018, 2021, Oracle and/or its affiliates. */
/*    All rights reserved.*/

/*
   MODIFIED    (MM/DD/YY)
    alerojas    02/06/19 - Change getStreamOffset to return 0 if at the begging
                           if stream
    jspiegel    12/14/18 - Creation
 */
package oracle.json.rdbms;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Wrapper;

import jakarta.json.JsonArray;
import jakarta.json.JsonException;
import jakarta.json.JsonObject;
import jakarta.json.JsonValue;
import jakarta.json.stream.JsonLocation;
import jakarta.json.stream.JsonParser;

import oracle.sql.json.OracleJsonException;
import oracle.sql.json.OracleJsonParser;
import oracle.jdbc.driver.json.binary.OsonParserImpl;

/**
 *  @version $Header: xdk/src/java/json/src/oracle/json/rdbms/JsonpParserWrapper.java /st_xdk_soda1/1 2021/08/25 00:17:26 morgiyan Exp $
 *  @author  jspiegel
 *  @since   release specific (what release of product did this appear in)
 */
public class JsonpParserWrapper implements JsonParser {

  OracleJsonParser wrapped;

  public JsonpParserWrapper(OracleJsonParser wrapped) {
    this.wrapped = wrapped;
  }

  @Override
  public void close() {
    try {
      wrapped.close();
    } catch (OracleJsonException e) {
      throw new JsonException(e.getMessage(), e);
    }
  }

  public OracleJsonParser getWrapped(){ 
    return wrapped; 
  }

  @Override
  public BigDecimal getBigDecimal() {
    return wrapped.getBigDecimal();
  }

  @Override
  public int getInt() {
    return wrapped.getInt();
  }

  @Override
  public JsonLocation getLocation() {
    JsonLocation NO_LOCATION = new JsonLocation() {
      @Override
      public long getColumnNumber() {
        return -1;
      }

      @Override
      public long getLineNumber() {
        return -1;
      }

      @Override
      public long getStreamOffset() {
        if (wrapped instanceof OsonParserImpl)
          return ((OsonParserImpl) wrapped).getStreamOffset();
        return -1;
      }
    };
    return NO_LOCATION;
  }

  @Override
  public long getLong() {
    return wrapped.getLong();
  }

  @Override
  public String getString() {
    return wrapped.getString();
  }

  @Override
  public boolean hasNext() {
    try {
      return wrapped.hasNext();
    } catch (OracleJsonException e) {
      throw new JsonException(e.getMessage(), e);
    }
  }

  @Override
  public boolean isIntegralNumber() {
    return wrapped.isIntegralNumber();
  }

  @Override
  public Event next() {

    OracleJsonParser.Event event;
    try {
      event = wrapped.next();
    } catch (OracleJsonException e) {
      throw new JsonException(e.getMessage(), e);
    }

    switch (event) {
    case END_ARRAY:
      return JsonParser.Event.END_ARRAY;
    case END_OBJECT:
      return JsonParser.Event.END_OBJECT;
    case KEY_NAME:
      return JsonParser.Event.KEY_NAME;
    case START_ARRAY:
      return JsonParser.Event.START_ARRAY;
    case START_OBJECT:
      return JsonParser.Event.START_OBJECT;
    case VALUE_DOUBLE:
    case VALUE_FLOAT:
    case VALUE_DECIMAL:
      return JsonParser.Event.VALUE_NUMBER;
    case VALUE_FALSE:
      return JsonParser.Event.VALUE_FALSE;
    case VALUE_TRUE:
      return JsonParser.Event.VALUE_TRUE;
    case VALUE_NULL:
      return JsonParser.Event.VALUE_NULL;
    case VALUE_BINARY:
    case VALUE_TIMESTAMP:
    case VALUE_DATE:
    case VALUE_INTERVALDS:
    case VALUE_INTERVALYM:
    case VALUE_STRING:
    // case VALUE_TIMESTAMPTZ: 19.x does not have timestamptz
    default:
      return JsonParser.Event.VALUE_STRING;
    }
  }

  //@Override JSONP 1.1
  public JsonValue getValue() {
    try {
      return wrapped.getValue().wrap(JsonValue.class);
    } catch (OracleJsonException e) {
      throw new JsonException(e.getMessage(), e);
    }
  }

  //@Override JSONP 1.1
  public JsonObject getObject() {
    try {
      return wrapped.getObject().wrap(JsonObject.class);
    } catch (OracleJsonException e) {
      throw new JsonException(e.getMessage(), e);
    }
  }

  //@Override JSONP 1.1
  public JsonArray getArray() {
    try {
      return wrapped.getArray().wrap(JsonArray.class);
    } catch (OracleJsonException e) {
      throw new JsonException(e.getMessage(), e);
    }
  }

  //@Override JSONP 1.1
  public void skipObject() {
    try {
      wrapped.skipObject();
    } catch (OracleJsonException e) {
      throw new JsonException(e.getMessage(), e);
    }
  }

  //@Override JSONP 1.1
  public void skipArray() {
    try {
      wrapped.skipArray();
    } catch (OracleJsonException e) {
      throw new JsonException(e.getMessage(), e);
    }
  }
}
