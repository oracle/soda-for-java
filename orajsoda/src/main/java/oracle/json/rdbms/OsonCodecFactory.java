/* $Header: xdk/src/java/json/orajsoda/src/main/java/oracle/json/rdbms/OsonCodecFactory.java /st_xdk_soda1/5 2024/08/02 02:37:36 vemahaja Exp $ */

/* Copyright (c) 2019, 2024, Oracle and/or its affiliates. */

/*
   DESCRIPTION
    OsonCodecFactory produces instances of an OSON DocumentCodec.

   PRIVATE CLASSES

   NOTES
    The OSON codec is able to convert between JSON text and OSON binary format.

   MODIFIED    (MM/DD/YY)
    dmcmahon    04/16/19 - Creation
 */

/**
 *  @version $Header: xdk/src/java/json/orajsoda/src/main/java/oracle/json/rdbms/OsonCodecFactory.java /st_xdk_soda1/5 2024/08/02 02:37:36 vemahaja Exp $
 *  @author  dmcmahon
 *  @since   release specific (what release of product did this appear in)
 */

package oracle.json.rdbms;

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.StringReader;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import java.math.BigDecimal;

import oracle.json.common.JsonFactoryProvider;
import oracle.json.common.DocumentCodec;
import oracle.json.common.DocumentCodecFactory;
import oracle.json.common.Message;

import oracle.json.util.ByteArray;
import oracle.json.util.Pair;

import jakarta.json.JsonValue;
import jakarta.json.JsonNumber;
import jakarta.json.JsonString;
import jakarta.json.JsonException;
import jakarta.json.stream.JsonParser;
import jakarta.json.stream.JsonGenerator;

import oracle.sql.json.OracleJsonBinary;
import oracle.sql.json.OracleJsonFactory;
import oracle.sql.json.OracleJsonGenerator;
import oracle.sql.json.OracleJsonObject;
import oracle.sql.json.OracleJsonParser;
import oracle.sql.json.OracleJsonValue;
import oracle.sql.json.OracleJsonValue.OracleJsonType;

import java.sql.Wrapper;
import java.sql.SQLException;

public class OsonCodecFactory extends DocumentCodecFactory
{
  private final OracleJsonFactory osonFactory = new OracleJsonFactory();

  public OsonCodecFactory()
  {
    super();
  }

  public OsonCodecFactory(JsonFactoryProvider factoryProvider)
  {
    super(factoryProvider);
  }

  @Override
  public DocumentCodec getCodec()
  {
    // Automatically create a provider if one wasn't set
    if (factoryProvider == null)
      setFactoryProvider(new JsonFactoryProvider());

    return new OsonDocumentCodec(factoryProvider, osonFactory);
  }

  private class OsonDocumentCodec extends DocumentCodec<OracleJsonValue>
  {
    private final OracleJsonFactory osonFactory;

    OsonDocumentCodec(JsonFactoryProvider jsonFactory,
                      OracleJsonFactory osonFactory)
    {
      super(jsonFactory);
      this.osonFactory = osonFactory;
    }

    /**
     * Loads an OSON image
     */
    @Override
    public void loadImage(byte[] docImage)
    {
      reset();
      this.image = docImage;
    }

    @Override
    public String getKey(boolean needViable)
      throws JsonException
    {
      // Cannot run the request if there aren't any steps
      if (keySteps == null)
        throw makeException(Message.EX_KEY_PATH_NOT_SET);
      // Cannot get a key after requesting rekeying
      if (rekeyingCheck())
        throw makeException(Message.EX_REKEY_PENDING);

      this.needViable = needViable;

      // We require a document to extract the key here
      if (doc == null)
      {
        // ### Is it more efficient to run the streaming extraction?
        // ### Otherwise we have to materialize the entire OSON.
        if (image == null)
          return super.getKey(needViable);

        // ### We may be about to do that anyway, so this might be better.
        // ### However, if rekeying is needed, we'd want to know that first.
        doc = getDocument();
        if (doc == null) return null;
      }

      return extractKey(doc);
    }

    @Override
    public OracleJsonValue getDocument()
      throws JsonException
    {
      // If there's no document available
      if (doc == null)
      {
        // Make sure there's an OSON binary image
        if (image == null)
          image = getImage(); // This does any needed rekeying
      }

      // If rekeying is still needed, force it now
      if (rekeyingCheck())
        image = getImage();

      // If <doc> wasn't available, or was invalidated, re-create it now
      if ((doc == null) && (image != null))
        doc = osonFactory.createJsonBinaryValue(ByteBuffer.wrap(image));

      return doc;
    }

    @Override
    public byte[] getImage()
      throws JsonException
    {
      boolean doRekeying = rekeyingCheck();

      // If there's no image, we have to get it from one of the other forms
      if (image == null)
      {
        ByteArrayOutputStream osonOut = getBAOS();
        OracleJsonGenerator osonGen = osonFactory.createJsonBinaryGenerator(osonOut);

        osonOut.reset();

        // If there's a document, write it out
        if (doc != null)
        {
          osonGen.write(doc);
        } 
        else if (jsonText != null)
        {
          // Convert String to OSON
          JsonParser parser = factoryProvider.getParserFactory()
                                .createParser(new StringReader(jsonText));
          if (doRekeying)
          {
            // ### Unfortunately no code to rekey this on the fly
          }
          osonGen.writeParser(parser);
        }
        else if (jsonUnicode != null)
        {
          // Convert Unicode text to OSON
          JsonParser parser = factoryProvider.getParserFactory()
                                .createParser(new ByteArrayInputStream(jsonUnicode));
          if (doRekeying)
          {
            // ### Unfortunately no code to rekey this on the fly
          }
          JsonpGeneratorWrapper jsonpGeneratorWrapper = new JsonpGeneratorWrapper(osonGen);
          jsonpGeneratorWrapper.writeJsonParser(parser);
        }
        else if (doRekeying)
        {
          // Create an empty object document to support the rekeying
          JsonParser parser = factoryProvider.getParserFactory()
                                .createParser(new StringReader(EMPTY_OBJECT_STRING));
          osonGen.writeParser(parser);
        }
        else
        {
          rekeyingClear();
          return null;
        }

        osonGen.close();
        image = osonOut.toByteArray();

        osonOut.reset();
      }

      if (doRekeying)
        rekeyImage();

      return image;
    }

    @Override
    public String getString()
      throws JsonException
    {
      if ((jsonText == null) && (jsonUnicode == null))
        jsonUnicode = getUnicode();
      return super.getString();
    }

    @Override
    public byte[] getUnicode()
      throws JsonException
    {
      if ((jsonText == null) && (jsonUnicode == null))
      {
        // We need a document to write to Unicode
        // Run the get to force any needed rekeying
        doc = getDocument();

        if (doc == null)
          return null;

        ByteArrayOutputStream unicodeOut = getBAOS();
        OracleJsonGenerator gen = osonFactory.createJsonTextGenerator(unicodeOut);
        gen.write(doc);
        gen.close();
        jsonUnicode = unicodeOut.toByteArray();
      }
      return super.getUnicode();
    }

    @Override
    public Object getDocument(Class clazz)
      throws JsonException
    {
      if (OracleJsonValue.class.isInstance(clazz))
        return getDocument();
      else if (javax.json.JsonValue.class.isInstance(clazz))
      {
        doc = getDocument();
        if (doc != null)
          return doc.wrap(javax.json.JsonValue.class);
      }
      return super.getDocument(clazz);
    }

    private void rekeyImage()
      throws JsonException
    {
      if (image == null) return;

      if (rekeyingCheck())
      {
        if (insertKey())
        {
          // Invalidate all other forms
          doc = null;
          jsonText = null;
          jsonUnicode = null;
        }
      }
    }

    @Override
    protected void addKey(JsonGenerator generator,
                          String keyName, String keyValue)
      throws JsonException
    {
      // If running the OSON parser we can unwrap the generator
      // to access a richer type system that includes IDs.
      // ### Remove the Wrapper part now?
      if (generator instanceof Wrapper ||
          generator instanceof JsonpGeneratorWrapper)
      {
        OracleJsonGenerator osonGenerator;

        if (generator instanceof JsonpGeneratorWrapper)
          osonGenerator = ((JsonpGeneratorWrapper)generator).getWrapped();
        else
        {
          try
          {
            osonGenerator = ((Wrapper)generator).unwrap(OracleJsonGenerator.class);
          }
          catch (SQLException e)
          {
            throw new IllegalStateException(e); // infeasible
          }
        }

        // If treatment of the key as an ID is requested
        // and the key appears to be a valid hexadecimal string
        int slen = (keyValue == null) ? 0 : keyValue.length();
        if (keyIsId && ((slen & 1) == 0) && (slen > 0) && (slen < 256))
        {
          if (ByteArray.isHex(keyValue))
          {
            osonGenerator.writeKey(keyName);
            osonGenerator.writeId(ByteArray.hexToRaw(keyValue));
            return;
          }
        }
      }
      // Else it must be the textual JSON generator, key will become a string

      // Otherwise just do the default behavior
      super.addKey(generator, keyName, keyValue);
    }

    /**
     * Insert a new key into an OSON image and re-encode it
     */
    private boolean insertKey()
      throws JsonException
    {
      if ((keySteps == null) || (keySteps.length <= 0))
        throw makeException(Message.EX_KEY_PATH_EMPTY);
      if (image == null)
        throw makeException(Message.EX_NO_INPUT_DOCUMENT);
      if (image.length == 0)
        throw makeException(Message.EX_NO_INPUT_DOCUMENT);

      ByteArrayOutputStream osonOut = getBAOS();
      OracleJsonParser    osonParser =
        osonFactory.createJsonBinaryParser(ByteBuffer.wrap(image));
      OracleJsonGenerator osonGenerator =
        osonFactory.createJsonBinaryGenerator(osonOut);

      JsonParser jsonParser = new JsonpParserWrapper(osonParser);
      JsonGenerator jsonGenerator = new JsonpGeneratorWrapper(osonGenerator);

      boolean wasChanged = false;

      Pair<String, Boolean> result = parseStream(jsonParser, jsonGenerator, false);
      wasChanged = result.getSecond().booleanValue();
      jsonGenerator.close();
      jsonParser.close();

      image = osonOut.toByteArray();

      osonOut.reset();

      rekeyingClear();

      return wasChanged;
    }

    private String extractKey(OracleJsonValue doc)
      throws JsonException
    {
      if ((keySteps == null) || (keySteps.length <= 0))
        throw makeException(Message.EX_KEY_PATH_EMPTY);

      OracleJsonValue v = doc;
      if (v.getOracleJsonType() != OracleJsonType.OBJECT)
        throw makeException(Message.EX_DOCUMENT_NOT_OBJECT);

      OracleJsonObject obj = v.asJsonObject();
      // Jump to the last object in path
      for (int i = 0; i < (keySteps.length - 1); ++i)
      {
        v = obj.get(keySteps[i]);
        if (v == null) return null;
        if (v.getOracleJsonType() != OracleJsonType.OBJECT)
          return null;

        obj = v.asJsonObject();
      }

      OracleJsonValue val = obj.get(keySteps[keySteps.length - 1]);

      if (val == null) return null;

      switch (val.getOracleJsonType())
      {
      case STRING:
        return val.asJsonString().getString();
      case DOUBLE: {
        double d = val.asJsonDouble().doubleValue();
        if (!Double.isInfinite(d) && !Double.isNaN(d))
        {
          BigDecimal bd = BigDecimal.valueOf(d);
          if (bd.stripTrailingZeros().scale() <= 0)
            return bd.toBigInteger().toString();
          // Else we don't allow keys with fraction values in SODA
        }
        break;
      }
      case FLOAT: {
        float f = val.asJsonFloat().floatValue();
        if (!Float.isInfinite(f) && !Float.isNaN(f))
        {
          BigDecimal bd = BigDecimal.valueOf(f);
          if (bd.stripTrailingZeros().scale() <= 0)
            return bd.toBigInteger().toString();
          // Else we don't allow keys with fraction values in SODA
        }
        break;
      }
      case DECIMAL: 
        return bigDecimalToKey(val.asJsonDecimal().bigDecimalValue());
      case BINARY: {
        OracleJsonBinary b = val.asJsonBinary();
        if (b.isId())
          return b.getString(); // This will be lowercase hexadecimal
        // Other binaries that aren't IDs aren't considered viable keys
        break;
      }
      // None of these are considered viable keys for SODA
      case NULL:
      case TRUE:
      case FALSE:
      case OBJECT:
      case ARRAY:
      default:
        break;
      }

      if (needViable)
        throw makeException(Message.EX_KEY_MUST_BE_STRING);

      return null;
    }
  }
}
