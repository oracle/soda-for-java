/**
 * JsonFactoryProvider.java
 *
* Copyright (c) 2013, 2024, Oracle and/or its affiliates. 
 *
 * Provides construction services for JSON facilities that otherwise
 * require costly class loader operations to obtain.
 *
 * @author   Doug McMahon
 */

package oracle.json.common;

import jakarta.json.Json;
import jakarta.json.JsonBuilderFactory;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonWriterFactory;
import jakarta.json.JsonReaderFactory;
import jakarta.json.stream.JsonParser;
import jakarta.json.stream.JsonGenerator;

import jakarta.json.stream.JsonGeneratorFactory;
import jakarta.json.stream.JsonParserFactory;

import org.eclipse.parsson.JsonProviderImpl;

import jakarta.json.spi.JsonProvider;

import java.io.Writer;
import java.io.InputStream;

/** This class is thread safe */
public class JsonFactoryProvider
{
  private volatile JsonBuilderFactory   builderFactory   = null;
  private volatile JsonParserFactory    parserFactory    = null;
  private volatile JsonGeneratorFactory generatorFactory = null;
  private volatile JsonWriterFactory    writerFactory    = null;
  private volatile JsonReaderFactory    readerFactory    = null;
  private volatile JsonProvider         jsonProvider     = null;

  public JsonFactoryProvider()
  {
  }

  private JsonProvider getJsonProvider() 
  {
    JsonProvider result = jsonProvider;
    if (result == null)
    {
      synchronized (this)
      {
        result = jsonProvider;
        if (result == null)
          jsonProvider = result = new JsonProviderImpl();
      }
    }
    return result;
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
          parserFactory = result = getJsonProvider().createParserFactory(null);
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
          generatorFactory = result = getJsonProvider().createGeneratorFactory(null);
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
          writerFactory = result = getJsonProvider().createWriterFactory(null);
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
          readerFactory = result = getJsonProvider().createReaderFactory(null);
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
          builderFactory = result = getJsonProvider().createBuilderFactory(null);
      }
    }
    return result;
  }

  public JsonParser createParser(InputStream in)
  {
    return getParserFactory().createParser(in);
  }

  public JsonGenerator createGenerator(Writer writer)
  {
    return getGeneratorFactory().createGenerator(writer);
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
