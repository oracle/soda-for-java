/* $Header: xdk/src/java/json/orajsoda/src/main/java/oracle/json/sodacommon/DocumentCodecFactory.java /st_xdk_soda1/3 2024/07/16 22:56:04 vemahaja Exp $ */

/* Copyright (c) 2019, 2024, Oracle and/or its affiliates. */

/*
   DESCRIPTION
    DocumentCodecFactory produces instances of a DocumentCodec.

   PRIVATE CLASSES

   NOTES
    Implementations are expected to understand a particular binary
    image format and corresponding navigable object structure.

    This base implementation does not support a binary image format.
    The DOM model supported is the standard JsonValue/JsonStructure.

    Naming of subclasses should follow this convention:

      <Document type><Image type>CodecFactory

   Typically, the <Image type> is omitted if it's the same as the
   <Document type>.

   ### To-do: Josh to rework this to use the Service Provider pattern
   ### and a ServiceLoader. Not clear what would be loaded, probably
   ### instances of this factory class?

   MODIFIED    (MM/DD/YY)
    dmcmahon    04/16/19 - Creation
 */

/**
 *  @version $Header: xdk/src/java/json/orajsoda/src/main/java/oracle/json/sodacommon/DocumentCodecFactory.java /st_xdk_soda1/3 2024/07/16 22:56:04 vemahaja Exp $
 *  @author  dmcmahon
 *  @since   release specific (what release of product did this appear in)
 */

package oracle.json.sodacommon;

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.io.StringWriter;

import jakarta.json.stream.JsonParser;
import jakarta.json.stream.JsonGenerator;
import jakarta.json.stream.JsonParserFactory;
import jakarta.json.stream.JsonGeneratorFactory;

import jakarta.json.JsonValue;
import jakarta.json.JsonObject;
import jakarta.json.JsonNumber;
import jakarta.json.JsonString;
import jakarta.json.JsonException;
import jakarta.json.JsonReader;
import jakarta.json.JsonReaderFactory;

import oracle.json.sodacommon.JsonFactoryProvider;
import oracle.json.sodacommon.DocumentCodec;
import oracle.json.sodacommon.Message;

public class DocumentCodecFactory
{
  protected JsonFactoryProvider factoryProvider = null;

  // When this ByteArrayOutputStream is first created, and
  // segments are written into it during conversions, it expands
  // an internal byte array. When baos.toByteArray() is called,
  // this internal byte array is copied to a newly trimmed byte array.
  // By doing reset(), we can reuse the internal byte array memory on
  // subsequent allocations. From reset() Javadoc: the output stream
  // can be used again, reusing the already allocated buffer space.
  protected ByteArrayOutputStream baos = new ByteArrayOutputStream();
  // ### This is a cheesy way to reuse this object because
  // ### it ends up being shared by all DocumentCodec instances.
  // ### However, since we know they'll never be used concurrently,
  // ### for now this is OK.

  public DocumentCodecFactory()
  {
  }

  public DocumentCodecFactory(JsonFactoryProvider factoryProvider)
  {
    this.setFactoryProvider(factoryProvider);
  }

  public void setFactoryProvider(JsonFactoryProvider factoryProvider)
  {
    this.factoryProvider = factoryProvider;
  }

  public DocumentCodec getCodec()
  {
    return new DefaultDocumentCodec(factoryProvider);
  }

  private class DefaultDocumentCodec extends DocumentCodec<JsonValue>
  {
    private DefaultDocumentCodec(JsonFactoryProvider factoryProvider)
    {
      super(factoryProvider);
    }

    @Override
    protected ByteArrayOutputStream getBAOS()
    {
      if (!isDetached)
        this.baos = DocumentCodecFactory.this.baos;
      return super.getBAOS();
    }

    /**
     * Unsupported
     */
    @Override
    public void loadImage(byte[] docImage)
    {
      throw makeException(Message.EX_UNIMPLEMENTED_FORMAT);
    }
    
/*  ### remove if unused
    @Override
    public String getKey()
      throws JsonException
    {
      if (doc != null)
      {
        if (rekeyingCheck())
          throw makeException(Message.EX_REKEY_PENDING);
        return extractKey(doc);
      }

      return super.getKey();
    }
*/

    /**
     * Return a JsonValue object.
     */
    @Override
    public JsonValue getDocument()
      throws JsonException
    {
      boolean doRekeying = rekeyingCheck();

      // If we already have a document
      if (doc != null)
      {
        if (doRekeying)
        {
          // ### If it has to be rekeyed, serialize it
          // ### In principle we could rekey directly from the DOM
          jsonUnicode = getUnicode();
          doc = null;
        }
      }

      if (doc == null)
      {
        // If necessary, force a streaming rekeying to occur
        if (doRekeying)
          jsonText = getString();

        // ### To-do: update JSONP so we don't need the reader factory
        JsonReaderFactory rfactory = factoryProvider.getReaderFactory();
        JsonReader        reader   = null;

        if (jsonText != null)
        {
          StringReader sreader = new StringReader(jsonText);
          reader = rfactory.createReader(sreader);
        }
        else if (jsonUnicode != null)
        {
          reader = rfactory.createReader(new ByteArrayInputStream(jsonUnicode));
        }
        else
        {
          return null;
        }

        doc = reader.read();
        reader.close(); 
      }

      return doc;
    }

    /**
     * Unsupported
     */
    @Override
    public byte[] getImage()
      throws JsonException
    {
      throw makeException(Message.EX_UNIMPLEMENTED_FORMAT);
    }
    
    @Override
    public String getString()
      throws JsonException
    {
      boolean doRekeying = rekeyingCheck();

      if ((jsonText == null) && (jsonUnicode == null))
      {
        // If we have a document, convert it to a string
        if (doc != null)
        {
          StringWriter sw = new StringWriter();
          JsonGenerator gen = factoryProvider.getGeneratorFactory()
                                .createGenerator(sw);
          if (doRekeying)
          {
            // ### It would be more efficient to rekey the document at
            // ### this point, but for now don't bother and let the
            // ### base streaming method do that for us.
          }
          gen.write(doc);
          gen.close();
          jsonText = sw.toString();
        }
      }

      // If a document is going to be rekeyed, it's invalidated
      if (doRekeying && (doc != null)) doc = null;

      return super.getString();
    }

    @Override
    public byte[] getUnicode()
      throws JsonException
    {
      boolean doRekeying = rekeyingCheck();

      if ((jsonText == null) && (jsonUnicode == null))
      {
        // If we have a document, convert it to UTF-8
        if (doc != null)
        {
          ByteArrayOutputStream unicodeOut = getBAOS();
          JsonGenerator gen = factoryProvider.getGeneratorFactory()
                                .createGenerator(unicodeOut);
          if (doRekeying)
          {
            // ### It would be more efficient to rekey the document at
            // ### this point, but for now don't bother and let the
            // ### base streaming method do that for us.
          }
          gen.write(doc);
          gen.close();
          jsonUnicode = unicodeOut.toByteArray();
          unicodeOut.reset();
        }
      }

      // If a document is going to be rekeyed, it's invalidated
      if (doRekeying && (doc != null)) doc = null;

      return super.getUnicode();
    }

    @Override
    public Object getDocument(Class clazz)
      throws JsonException
    {
      if (clazz == JsonValue.class)
        return getDocument();
      return super.getDocument(clazz);
    }
  }
}
