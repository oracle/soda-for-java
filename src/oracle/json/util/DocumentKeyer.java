/* $Header: xdk/src/java/json/src/oracle/json/common/DocumentKeyer.java /main/2 2017/11/04 18:02:53 dmcmahon Exp $ */

/* Copyright (c) 2017, Oracle and/or its affiliates. All rights reserved.*/

/*
   DESCRIPTION
    Class for extracting or inserting string keys by path into JSON documents

   PRIVATE CLASSES

   NOTES
    This can be reused to process multiple documents by setting up the
    input and then running the required operation.

    ### To-do: extend to support multiple keys in parallel?

   MODIFIED    (MM/DD/YY)
    dmcmahon    02/26/17 - Creation
 */

/**
 *  @version $Header: xdk/src/java/json/src/oracle/json/common/DocumentKeyer.java /main/2 2017/11/04 18:02:53 dmcmahon Exp $
 *  @author  dmcmahon
 *  @since   release specific (what release of product did this appear in)
 */

package oracle.json.common;

import java.io.StringWriter;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.lang.IllegalArgumentException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.logging.Logger;

import java.math.BigDecimal;

import javax.json.Json;
import javax.json.JsonException;
import javax.json.stream.JsonGenerator;
import javax.json.stream.JsonGenerationException;
import javax.json.stream.JsonParser;
import javax.json.stream.JsonParser.Event;
import javax.json.stream.JsonParsingException;

import oracle.json.common.Configuration;
import oracle.json.common.Message;

public class DocumentKeyer
{
  protected static final Logger log =
    Logger.getLogger(DocumentKeyer.class.getName());

  private final String[]    keySteps;

  private InputStream jsonStream = null;

  private StringWriter  strWriter = null;
  private JsonGenerator generator = null;
  private JsonParser    jParser   = null;

  private String  key      = null;
  private int     level    = 0;
  private int     arrLevel = 0;
  private int     stepNum  = 0;
  private boolean foundKey = false;
  private boolean wroteKey = false;

  // Use this flag to disable overwritting a key with a different value
  private static boolean DISALLOW_KEY_OVERWRITE = true;

  /*
   * Create a document key finder/inserter around the array of key steps.
   * The array must have at least one step. The steps must all be field
   * name steps (no array steps).
   */
  DocumentKeyer(String[] keySteps)
  {
    this.keySteps = keySteps;
  }

  public void setInput(InputStream inp)
  {
    this.jsonStream = inp;
  }

  public void setInput(byte[] buf)
  {
    setInput(new ByteArrayInputStream(buf));
  }

  public void setInput(String str)
  {
    setInput(str.getBytes(Configuration.DEFAULT_CHARSET));
  }

  private JsonException makeException(Message msg, Object ... params)
  {
    return(new JsonException(msg.get(params)));
  }

  private void serializeItem(Object val)
  {
    if (generator != null)
    {
      if (val == null)
      {
        if (key != null) generator.writeNull(key);
        else             generator.writeNull();
      }
      else if (val instanceof Boolean)
      {
        boolean boolval = ((Boolean)val).booleanValue();

        if (key != null) generator.write(key, boolval);
        else             generator.write(boolval);
      }
      else if (val instanceof BigDecimal)
      {
        BigDecimal decval = (BigDecimal)val;

        if (decval.scale() <= 0)
        {
          if (key != null) generator.write(key, decval.toBigInteger());
          else             generator.write(decval.toBigInteger());
        }
        else
        {
          if (key != null) generator.write(key, decval);
          else             generator.write(decval);
        }
      }
      else if (val instanceof String)
      {
        String strval = (String)val;
    
        if (key != null) generator.write(key, strval);
        else             generator.write(strval);
      }
      else if (val instanceof JsonParser.Event)
      {
        JsonParser.Event ev = (JsonParser.Event)val;
        if (ev == JsonParser.Event.START_ARRAY)
        {
          if (key == null) generator.writeStartArray();
          else             generator.writeStartArray(key);
        }
        else if (ev == JsonParser.Event.START_OBJECT)
        {
          if (key == null)   generator.writeStartObject();
          else               generator.writeStartObject(key);
        }
        else if ((ev == JsonParser.Event.END_OBJECT) ||
                 (ev == JsonParser.Event.END_ARRAY))
        {
          generator.writeEnd();
        }
      }
      else
      {
        String strval = val.toString();
    
        if (key != null) generator.write(key, strval);
        else             generator.write(strval);
      }
    }

    key = null;
  }

  private void serializeItem(String tempKey, Object val)
  {
    this.key = tempKey;
    serializeItem(val);
    this.key = null;
  }

  /**
   * Inject enough object steps to insert a new key value
   */
  private void createKey(String newKey)
  {
    int i;
    int n = keySteps.length - 1;

    // Insert enough object steps to add the key
    for (i = stepNum; i < n; ++i)
      serializeItem(keySteps[i], JsonParser.Event.START_OBJECT);
    // Add the key/value
    serializeItem(keySteps[i], newKey);
    // Close the object steps
    for (i = stepNum; i < n; ++i)
      serializeItem(null, JsonParser.Event.END_OBJECT);
  }

  /**
   * Process the next event. If possible, extract the old key value
   * or insert the new key value. Returns null if nothing was changed,
   * or the old key value if it's found. If the key had to be inserted,
   * returns a pointer to newKey.
   */
  private String processEvent(JsonParser.Event event, String newKey)
    throws JsonGenerationException
  {
    String oldKey = null;

    if (event == JsonParser.Event.KEY_NAME)
    {
      // Remember the key
      key = jParser.getString();

      if (arrLevel == 0)
        if (level == (stepNum + 1))
          if (keySteps.length > stepNum)
            if (key.equals(keySteps[stepNum]))
              if (++stepNum == keySteps.length)
                foundKey = true;
    }
    else if (event == JsonParser.Event.VALUE_STRING) 
    {
      String fldVal = jParser.getString();

      if (foundKey)
      {
        oldKey = fldVal;

        // If requested, replace the key with a new value
        if (newKey != null) fldVal = newKey;

        foundKey = false;
        wroteKey = true;
        --stepNum;

        if (DISALLOW_KEY_OVERWRITE && (newKey != null))
          if (!oldKey.equals(newKey))
            throw makeException(Message.EX_KEY_MISMATCH);
      }

      serializeItem(fldVal);
    }
    else if (event == JsonParser.Event.VALUE_NUMBER)
    {
      BigDecimal bval = jParser.getBigDecimal();

      if (!foundKey)
        serializeItem(bval);
      else
      {
        // If requested, replace the key with a new value
        if (newKey != null)
          serializeItem(newKey);
        else
          serializeItem(bval);

        // Convert the number to a string
        if (bval.scale() > 0)
          oldKey = bval.toString();
        else
          oldKey = bval.toBigInteger().toString();

        foundKey = false;
        wroteKey = true;
        --stepNum;

        if (DISALLOW_KEY_OVERWRITE && (newKey != null))
        {
          boolean keysMatch = false;

          try
          {
            BigDecimal nval = new BigDecimal(newKey); 
            keysMatch = bval.equals(nval);
          }
          catch (NumberFormatException e)
          {
            keysMatch = false;
          }

          if (!keysMatch)
            throw makeException(Message.EX_KEY_MISMATCH);
        }
      }
    }
    else if (event == JsonParser.Event.VALUE_TRUE)
    {
      if (foundKey)
        throw makeException(Message.EX_KEY_MUST_BE_STRING);

      serializeItem(Boolean.TRUE);
    }
    else if (event == JsonParser.Event.VALUE_FALSE)
    {
      if (foundKey)
        throw makeException(Message.EX_KEY_MUST_BE_STRING);

      serializeItem(Boolean.FALSE);
    }
    else if (event == JsonParser.Event.VALUE_NULL)
    {
      if (foundKey)
        throw makeException(Message.EX_KEY_MUST_BE_STRING);

      serializeItem(null);
    }
    else if (event == JsonParser.Event.START_ARRAY)
    {
      if (foundKey)
        throw makeException(Message.EX_KEY_MUST_BE_STRING);

      ++level;
      ++arrLevel;
      serializeItem(event);
    }
    else if (event == JsonParser.Event.START_OBJECT)
    {
      if (foundKey)
        throw makeException(Message.EX_KEY_MUST_BE_STRING);

      ++level;
      serializeItem(event);
    }
    else if (event == JsonParser.Event.END_OBJECT)
    {
      --level;
      if (level == stepNum)
      {
        if (!wroteKey && (newKey != null))
        {
          // This means the key was not found, so we need to inject it
          createKey(newKey);
          wroteKey = true;
          oldKey = newKey;
        }
        if (stepNum > 0) --stepNum;
      }

      serializeItem(event);
    }
    else if (event == JsonParser.Event.END_ARRAY)
    {
      --level;
      --arrLevel;
      serializeItem(event);
    }

    return(oldKey);
  }

  private void clearParse()
  {
    key      = null;
    level    = 0;
    arrLevel = 0;
    stepNum  = 0;
    foundKey = false;
    wroteKey = false;
  }

  private String parseStream(String newKey, boolean earlyExit)
    throws JsonException
  {
    clearParse();

    if (jsonStream == null)
      throw makeException(Message.EX_NO_INPUT_DOCUMENT);

    JsonParser jParser = Json.createParser(jsonStream);
    boolean    firstEvent = true;
    String     oldKey = null;

    if (keySteps.length <= 0)
      throw makeException(Message.EX_KEY_PATH_EMPTY);

    while (jParser.hasNext())
    {
      Event ev = jParser.next();

      if (firstEvent)
      {
        firstEvent = false;
        if (ev != JsonParser.Event.START_OBJECT)
          throw makeException(Message.EX_DOCUMENT_NOT_OBJECT);
      }

      String insertedKey = processEvent(ev, newKey);

      // If a key was found in the stream, or inserted
      if (insertedKey != null)
      {
        // If we've already found or inserted the key,
        // this is a duplicate position
        if (oldKey != null)
        {
          throw makeException(Message.EX_DUPLICATE_KEY);
        }
        else
        {
          oldKey = insertedKey;
          if (earlyExit) break;
        }
      }
    }

    if (!earlyExit && (level != 0))
      throw makeException(Message.EX_DOCUMENT_NOT_CLOSED);

    jParser.close();

    return(oldKey);
  }

  /**
   * Extract the desired key as a string from the document.
   * Early-terminates the parse as soon as a matching key is found.
   * Note that the key is returned "as-is" so it may need canonicalization
   * before being used (i.e. uppercasing of hex values, canonical width
   * via 0-padding, etc.)
   */
  public String extractKey()
    throws JsonException
  {
    return(extractKey(false));
  }

  /**
   * Extract the desired key as a string from the document.
   * If validate is set to true, fully parses the document to make sure
   * the key isn't duplicated elsewhere.
   * Note that the key is returned "as-is" so it may need canonicalization
   * before being used (i.e. uppercasing of hex values, canonical width
   * via 0-padding, etc.)
   */
  public String extractKey(boolean validate)
    throws JsonException
  {
    generator = null;

    return(parseStream(null, !validate));
  }

  /**
   * Insert the new key at the head of the document.
   * Returns a String representing the new document with the key inserted.
   * Throws an exception if the document is a JSON array and the insertion
   * cannot be performed.
   */
  public String insertKey(String newKey)
    throws JsonException
  {
    // Create a writer/generator pair for the re-serialization
    strWriter = new StringWriter();
    generator = Json.createGenerator(strWriter);

    String oldKey = parseStream(newKey, false);
    // If this fails to return a pointer to the key, the insertion failed
    // (That in theory cannot happen.)

    generator.flush();
    generator.close();

    // Now return the generated output as a string
    String result = strWriter.toString();

    generator = null;
    strWriter = null;

    return(result);
  }
}
