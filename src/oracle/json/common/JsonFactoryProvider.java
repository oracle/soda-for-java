/**
 * JsonFactoryProvider.java
 *
* Copyright (c) 2013, 2019, Oracle and/or its affiliates. All rights reserved.
 *
 * Provides construction services for JSON facilities that otherwise
 * require costly class loader operations to obtain.
 *
 * @author   Doug McMahon
 */

package oracle.json.common;

import javax.json.Json;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObjectBuilder;
import javax.json.JsonArrayBuilder;
import javax.json.JsonWriterFactory;
import javax.json.JsonReaderFactory;

import javax.json.stream.JsonGeneratorFactory;
import javax.json.stream.JsonParserFactory;

/** This class is thread safe */
public class JsonFactoryProvider
{
  private volatile JsonBuilderFactory   builderFactory   = null;
  private volatile JsonParserFactory    parserFactory    = null;
  private volatile JsonGeneratorFactory generatorFactory = null;
  private volatile JsonWriterFactory    writerFactory    = null;
  private volatile JsonReaderFactory    readerFactory    = null;
  private volatile Object               jsonFactory      = null;

  /** ### TODO remove this reflection */
  public static final String ORACLE_JSON_FACTORY   = "oracle.sql.json.OracleJsonFactory";

  public JsonFactoryProvider()
  {
  }

  public JsonParserFactory getParserFactory()
  {
    JsonParserFactory result = parserFactory;
    if (result == null) 
    {
      synchronized(this) 
      {
        result = parserFactory;
        if (result == null)
          parserFactory = result = Json.createParserFactory(null); 
      }
    }
    return result;
  }

  public JsonGeneratorFactory getGeneratorFactory()
  {
    JsonGeneratorFactory result = generatorFactory;
    if (result == null) 
    {
      synchronized(this) 
      {
        result = generatorFactory;
        if (result == null)
          generatorFactory = result = Json.createGeneratorFactory(null);
      }
    }
    return result;
  }

  public JsonWriterFactory getWriterFactory()
  {
    JsonWriterFactory result = writerFactory;
    if (result == null) 
    {
      synchronized(this) 
      {
        result = writerFactory;
        if (result == null)
          writerFactory = result = Json.createWriterFactory(null);
      }
    }
    return result;
  }
  
  public JsonReaderFactory getReaderFactory()
  {
    JsonReaderFactory result = readerFactory;
    if (result == null) 
    {
      synchronized(this) 
      {
        result = readerFactory;
        if (result == null)
          readerFactory = result = Json.createReaderFactory(null);
      }
    }
    return result;
  }
  
  public JsonBuilderFactory getBuilderFactory()
  {
    JsonBuilderFactory result = builderFactory;
    if (result == null) 
    {
      synchronized(this) 
      {
        result = builderFactory;
        if (result == null)
          builderFactory = result = Json.createBuilderFactory(null);
      }
    }
    return result;
  }

  public Object getJsonFactory()
  {
    Object result = jsonFactory;
    if (result == null)
    {
      synchronized(this)
      {
        result = jsonFactory;
        if (result == null)
        {  
           try 
           {
             Class<?> oracleJsonFactory = Class.forName(ORACLE_JSON_FACTORY);
             jsonFactory = result = oracleJsonFactory.newInstance();
           } catch (Exception e)
           {
              throw new IllegalStateException(e);
           }
        }
      }
    }
    return result;

  }

  public JsonObjectBuilder createObjectBuilder()
  {
    return getBuilderFactory().createObjectBuilder();
  }

  public JsonArrayBuilder createArrayBuilder()
  {
    return getBuilderFactory().createArrayBuilder();
  }
}
