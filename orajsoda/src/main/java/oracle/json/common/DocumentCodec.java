/* $Header: xdk/src/java/json/orajsoda/src/main/java/oracle/json/common/DocumentCodec.java /st_xdk_soda1/6 2024/08/02 02:37:36 vemahaja Exp $ */

/* Copyright (c) 2019, 2024, Oracle and/or its affiliates. */

/*
   DESCRIPTION
    DocumentCodec is a stateful object that translates between a binary document
    format of some kind and JSON text. There are four forms of a document
    supported by each codec:

      - Json text as a String
      - Json text in Unicode UTF-8 form
      - A Document Object Model instance
      - A serialized binary form

    The typical life cycle of a codec usage is as follows:

      - Load the codec with a document in one of the supported forms
      - Extract the current value of the key, if any
      - Optionally request modification of an intrinsic document key
      - Get the document back in one of the supported forms.

    A codec is reset every time a load operation is performed. Any requested
    key modifications are automatically performed whenever any get operation
    is performed, and this then clears them.

    Codecs may share resources with the factory that produced them.
    They're fine to use as long as a single thread controls the factory.
    If a codec has to be used independently across threads, it needs to
    be detached from its factory using that method.

    Codecs can extract an existing key from a document non-destructively,
    but the cost can vary greatly. In DOM form, the key is relatively
    cheap to extract via navigation. In binary image form, the key may or
    may not be cheap to extract, depending on whether the format is
    navigable. In textual forms, extraction requires a streaming pass,
    which is cheap only if the key appears early in the serialization,
    and only if full validation isn't also required.

    The other common theme in codec operations is rekeying. Rekeying is
    costly because it implies a reserialization of the document in the
    event of a change. Codecs attempt to minimize this cost by deferring
    the rekeying until a get is performed. If the get is for an alternate
    format, the rekeying is done during the conversion to the new format.
    A typical situation is rekeying a binary JSON format during conversion
    to a textual format, or the inverse of this. Rekeying requests are
    as follows:

      - None:          the document is converted as-is
      - Remove Only:   any existing key is stripped out during conversion
      - Key Assurance: a key is inserted only if an existing key is not found
      - Key Insertion: a new key is inserted and any existing key is discarded

    Some forms of the document will strongly prefer or even demand that the
    key appear as the first field in a document; this is typically for read
    performance reasons. Textual formats will prefer this, though it's not
    required. The DOM format and navigable binary formats won't care about
    key ordering. This poses a problem for the Key Assurance process when the
    target is not navigable, so it should be avoided whenever possible in
    such cases.

    Unfortunately avoidance may have unwanted side-effects. The general
    avoidance strategy is to use the existing key, if known, and request
    a Key Insertion instead of a Key Assurance. Unfortunately, this means
    the key will be inserted as a string. If the document format(s) support
    a richer type system than the standard JSON types, it's possible that
    this process will strip away a key represented in another scalar form,
    such as a UUID; even for standard JSON, this will strip away a key that
    was originally a number and yield it back as a string.

   PRIVATE CLASSES

   NOTES
    Also provides the ability to add, change, or remove a key from the
    document during translation. Rekeying is driven by the following:

      removeKey    newKey   behavior
      =====================================================
      false        null     no changes
      true         null     remove any existing key
      false        <id>     inject if not present
      true         <id>     remove, then inject

    Also provides a getter to look at any existing key in the document.
    The use of rekeying operations requires that the caller set the
    key path. This setting is not cleared on reuse of the codec instance.

    Most codecs should throw JsonException to signal errors, but some
    may use standard Java runtime exceptions to signal errors as follows:

      UnsupportedOperationException
        Thrown if a method or request is unimplemented.
        Example: loadImage/getImage on the default codec.
      IllegalStateException
        Thrown when a combination of methods is invalid.
        Example: setNewKey when setKeyPath hasn't been done.
      NoSuchElementException
        Thrown for data that cannot be processed.
        Example: using a BsonSymbol as a document key.
      IllegalArgumentException
        Thrown wrapped on parser/generator errors.
        Example: invalid JSON text resulting in a parse error.

   MODIFIED    (MM/DD/YY)
    dmcmahon    04/16/19 - Creation
 */

/**
 *  @version $Header: xdk/src/java/json/orajsoda/src/main/java/oracle/json/common/DocumentCodec.java /st_xdk_soda1/6 2024/08/02 02:37:36 vemahaja Exp $
 *  @author  dmcmahon
 *  @since   release specific (what release of product did this appear in)
 */

package oracle.json.common;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import java.io.StringReader;
import java.io.StringWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import java.math.BigDecimal;

import java.lang.reflect.Method; // ### Temporary

import jakarta.json.stream.JsonParser;
import jakarta.json.stream.JsonGenerator;
import jakarta.json.stream.JsonParserFactory;
import jakarta.json.stream.JsonGeneratorFactory;
import jakarta.json.stream.JsonGenerationException;

import jakarta.json.JsonValue;
import jakarta.json.JsonObject;
import jakarta.json.JsonValue;
import jakarta.json.JsonNumber;
import jakarta.json.JsonString;
import jakarta.json.JsonException;
import jakarta.json.JsonReader;
import jakarta.json.JsonReaderFactory;

import oracle.json.common.Message;
import oracle.json.common.JsonFactoryProvider;

import oracle.json.util.Pair;

import oracle.json.rdbms.JsonpParserWrapper;
import oracle.json.rdbms.JsonpGeneratorWrapper;

import oracle.sql.json.OracleJsonParser;
import oracle.sql.json.OracleJsonGenerator;

public abstract class DocumentCodec<T>
{
  protected static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

  protected static final String EMPTY_OBJECT_STRING = "{}";

  protected T      doc         = null;
  protected String jsonText    = null;
  protected byte[] jsonUnicode = null;
  protected byte[] image       = null;

  protected String[] keySteps = null;
  protected boolean  validate = false;

  protected String  newKey     = null;
  protected boolean removeKey  = false;
  protected boolean keyIsId    = false;
  protected boolean mustMatch  = false;
  protected boolean needViable = false;

  protected final JsonFactoryProvider factoryProvider;

  protected DocumentCodec(JsonFactoryProvider factoryProvider)
  {
    this.factoryProvider = factoryProvider;
  }

  protected boolean isDetached = false;
  protected ByteArrayOutputStream baos = null;

  //Signals whether to generate _id field as specified in eJSON,
  //i.e. _id : {"$oid" : "..."}
  private boolean eJSONId = false;

  /**
   * Make this object independent of shared resources from its factory.
   * In particular, the captive ByteArrayOutputStream is no longer
   * linked to the factory (and thereby shared with other codecs produced
   * by the factory).
   */
  public void detachFactory()
  {
    if (!isDetached)
    {
      // Break any dependency on the parent factory's shared resources
      isDetached = true;
      baos = null;
    }
  }

  /**
   * Getter for the captive ByteArrayOutputStream for this codec.
   * Used internally. This reuses the same object repeatedly. The
   * The stream is reset by this routine, ready for reuse. Note that
   * it may be shared with other codecs produced by the same factory.
   * The idea is that it's only needed transiently, and each codec
   * can have it's own document state otherwise. Using a captive
   * byte array allows it to be reused on many rows for a bulk operation.
   */
  protected ByteArrayOutputStream getBAOS()
  {
    if (baos == null)
    {
      // Since this codec is getting a private BAOS, it's automatically detached
      isDetached = true;
      baos = new ByteArrayOutputStream();
    }
    baos.reset();
    return baos;
  }

  /**
   * Internal full reset of the codec. Done implicitly by all load
   * methods, ensuring that only the loaded form is present.
   */
  protected void reset()
  {
    doc         = null;
    jsonText    = null;
    jsonUnicode = null;
    image       = null;

    rekeyingClear();
  }

  /**
   * Load a document into the codec. The document is typically in
   * a navigable form such as a Map or List.
   */
  public void loadDocument(T doc)
  {
    reset();
    this.doc = doc;
  }

  /**
   * Load a document into the codec from a serialized image. A codec
   * that doesn't have a binary format may throw an exception for this method.
   */
  abstract public void loadImage(byte[] docImage);

  /**
   * Load a document into the code from JSON text.
   */
  public void loadString(String docJson)
  {
    reset();
    jsonText = docJson;
  }

  /**
   * Load a document into the codec from JSON Unicode text.
   */
  public void loadUnicode(byte[] docUnicode)
  {
    reset();
    jsonUnicode = docUnicode;
  }

  /**
   * Set the path to a unique key for the document,
   * as a series of step strings. May be null to disable.
   * This automatically clears any rekeying settings.
   */
  public void setKeyPath(String[] keySteps)
  {
    if (keySteps != null)
      if (keySteps.length == 0)
        keySteps = null;
    this.keySteps = keySteps;
    rekeyingClear();
  }

  /**
   * If set to true, this requests a full validation of the JSON
   * during key extraction. Otherwise, the parse will terminate
   * as soon as the key is found.
   */
  public void setValidation(boolean validate)
  {
    this.validate = validate;
  }

  /**
   * Get the value for the key as a string.
   * Returns null if the key is not present in the document.
   * Also returns null if the key is present but considered
   * unviable for use by SODA (e.g. a JSON null/true/value literal,
   * or a nested container).
   * This cannot be called unless the key path is set.
   * This cannot be called if a rekeying has been requested.
   */
/* ### remove if unusued
  public String getKey()
    throws JsonException
  {
    return getKey(false);
  }
*/

  /**
   * Get the value for the key as a string. Returns null if the key is
   * not present, or is unviable for use by SODA. If needViable is true,
   * this will throw an exception if an unviable key is discovered.
   */
  public String getKey(boolean needViable)
    throws JsonException
  {
    this.needViable = needViable;

    // Cannot run the request if there aren't any steps
    if (keySteps == null)
      throw makeException(Message.EX_KEY_PATH_NOT_SET);
    // Cannot get a key after requesting rekeying
    if (rekeyingCheck())
      throw makeException(Message.EX_REKEY_PENDING);

    return extractKey(validate);
  }

  /**
   * If true, remove the existing key from the document.
   * If the key is not present in the document, does nothing.
   * Use removeKey = true together with setNewKey() if you
   * want to force a new key to be used in the document.
   */
  public void setRemoveKey(boolean removeKey)
    throws JsonException
  {
    if (keySteps == null)
      throw makeException(Message.EX_KEY_PATH_NOT_SET);
    this.removeKey = removeKey;
  }

  public void generateEJSONId(boolean val)
  {
     this.eJSONId = val;
  }
  /**
   * Request that the document key set be to the specified string.
   * To be sure of removing an existing key, combine this with removeKey()
   * otherwise setNewKey will only operate if a key is not already found.
   */
  public void setNewKey(String key)
    throws JsonException
  {
    setNewKey(key, false);
  }

  /**
   * Request that the document key be set to the specified string.
   * If isIdentifier = true, requests that the string be treated as
   * an internal identifier for the document format. This will typically
   * be either an ObjectId or UUID. For textual JSON isIdentifier has
   * no effect, and keys are stored "as-is" as strings. For binary
   * formats, the conversion to identifier will depend on the length
   * of the key string.
   */
  public void setNewKey(String key, boolean isIdentifier)
    throws JsonException
  {
    setNewKey(key, isIdentifier, false);
  }

  /**
   * Set a new key for a document, optionally as an identifier.
   * If mustMatch = true, requires that if an old key is present in
   * the document, it must be a match to the new key. This allows a
   * caller to ensure a key value is present, but without forcibly
   * replacing an existing key with a different value. An exception
   * is thrown if the keys don't match.
   */
  public void setNewKey(String key, boolean isIdentifier, boolean mustMatch)
  {
    if (keySteps == null)
      throw makeException(Message.EX_KEY_PATH_NOT_SET);
    this.newKey = key;
    this.keyIsId = isIdentifier;
    this.mustMatch = mustMatch;
  }

  /**
   * Return the document as a navigable (DOM-like) object.
   * For some codecs this may return null, though typically codecs
   * will offer something resembling the Java Map or List constructs.
   */
  abstract public T getDocument() throws JsonException;

  /**
   * Return the document as a serialized binary image. A codec
   * that doesn't have a binary format will throw an exception.
   */
  abstract public byte[] getImage() throws JsonException;

  /**
   * Return the document as JSON text.
   * This may require a lossy translation of a document that has
   * extended semantics in DOM or binary image form.
   */
  public String getString()
    throws JsonException
  {
    if (rekeyingCheck())
      insertKey();

    if (jsonText == null)
      if (jsonUnicode != null)
        jsonText = new String(jsonUnicode, DEFAULT_CHARSET);
    return jsonText;
  }

  /**
   * Return the document as JSON text in Unicode UTF-8 format.
   * This may require a lossy translation of a document that has
   * extended semantics in DOM or binary image form.
   */
  public byte[] getUnicode()
    throws JsonException
  {
    if (rekeyingCheck())
      insertKey();

    if (jsonUnicode == null)
      if (jsonText != null)
        jsonUnicode = jsonText.getBytes(DEFAULT_CHARSET);
    return jsonUnicode;
  }

  // ### To-do for Josh
  public Object getDocument(Class clazz)
    throws JsonException
  {
    throw makeException(Message.EX_UNSUPPORTED_DOC_TYPE, clazz.getName());
  }

  /**
   * Internal for sub-classes; clears the rekeying state, except for
   * the key steps. This is typically done after a rekeying operation.
   */
  protected void rekeyingClear()
  {
    newKey     = null;
    removeKey  = false;
    keyIsId    = false;
    mustMatch  = false;
    needViable = false;
    validate   = false;
  }

  /**
   * Internal for sub-classes, check if a rekeying request is pending.
   */
  protected boolean rekeyingCheck()
  {
    return ((newKey != null) || removeKey);
  }

  // ### To-do: eJSON?

  protected JsonException makeException(Message msg, Object ... params)
  {
    return(new JsonException(msg.get(params)));
  }

  /**
   * Internal utility
   * Check if a numeric value "matches" a string.
   * ### Note that fractional numeric values are not allowed as SODA
   * ### keys, so these will never be considered to match.
   */
  protected boolean checkNumericKey(String newKey, BigDecimal bval)
  {
    boolean keysMatch = false;

    // We don't allow keys with fraction values in SODA
    if (bval.stripTrailingZeros().scale() > 0)
      return false;
    // ### Should we disallow negative number keys too?

    try
    {
      BigDecimal nval = new BigDecimal(newKey);
      keysMatch = bval.equals(nval);
    }
    catch (NumberFormatException e)
    {
      keysMatch = false;
    }

    return keysMatch;
  }

  /**
   * Convert a numeric key to the simplest string form.
   * Typically, this is to a simple integer.
   */
  static public String bigDecimalToKey(BigDecimal bval) 
  {
    if (bval.stripTrailingZeros().scale() <= 0)
      return bval.toBigInteger().toString();
    else // ### Perhaps this case should be disallowed as an error?
      return bval.toString();
  }

  /*************************************************************************
   * Key extraction/injection (originally in DocumentKeyer)                *
   *************************************************************************/

  /**
   * Sets up a streaming parse starting from the root of a document.
   * The document must be available in Unicode or String form. If
   * necessary, an empty object will be automatically created for a
   * key insertion request.
   * This function runs for both streaming extractions and streaming
   * insertions. In the insertion case, a generator is passed as the
   * target for the output. For insertions, newKey would typically be set
   * to the desired new key to be injected. The earlyExit flag is
   * ignored for insertions. For extractions, earlyExit = false forces
   * the full parse to be run to completion to validate the document
   * and to ensure that a duplicate key isn't present. Typically,
   * extractions will want to pass earlyExit = true to get the best
   * performance.
   */
  private String parseRoot(JsonGenerator generator, boolean earlyExit)
    throws JsonException
  {
    JsonParserFactory pfactory = factoryProvider.getParserFactory();
    JsonParser        parser   = null;

    if (jsonUnicode != null)
      parser = pfactory.createParser(new ByteArrayInputStream(jsonUnicode));
    else if (jsonText != null)
      parser = pfactory.createParser(new StringReader(jsonText));
    else if (generator != null) // auto-create document when rekeying
      parser = pfactory.createParser(new StringReader(EMPTY_OBJECT_STRING));
    else // extractKey on empty document
      throw makeException(Message.EX_NO_INPUT_DOCUMENT);

    String  oldKey     = null;
    boolean wasChanged = false;

    Pair<String, Boolean> result = parseStream(parser, generator, earlyExit);
    oldKey     = result.getFirst();
    wasChanged = result.getSecond().booleanValue();
    parser.close();

    return(oldKey);
  }

  /**
   * Extract the desired key as a string from the document.
   * If validate is set to true, fully parses the document to make sure
   * the key isn't duplicated elsewhere. Otherwise, early-terminates the
   * parse as soon as a matching key is found.
   * Note that the key is returned "as-is" so it may need canonicalization
   * before being used (i.e. uppercasing of hex values, canonical width
   * via 0-padding, etc.)
   */
  private String extractKey(boolean validate)
    throws JsonException
  {
    return parseRoot(null, !validate);
  }

  /**
   * Insert the new key.
   * Throws an exception if the document is a JSON array and the insertion
   * cannot be performed.
   */
  private void insertKey()
    throws JsonException
  {
    // Create a writer/generator pair for the re-serialization
    StringWriter strWriter = new StringWriter();
    JsonGenerator generator = factoryProvider.getGeneratorFactory()
                                             .createGenerator(strWriter);

    String oldKey = parseRoot(generator, false);
    // Throws an exception if insertion failed

    generator.flush();
    generator.close();

    // Now return the generated output as a string
    this.jsonText = strWriter.toString();
    this.jsonUnicode = null;
    rekeyingClear();
  }

  /**
   * Write a key to the document.
   * This may be overridden in sub-classes to support binary identifiers.
   */
  protected void addKey(JsonGenerator generator,
                        String keyName, String keyValue)
    throws JsonException
  {
    if (eJSONId)
      generator.writeStartObject(keyName).write("$oid", keyValue).writeEnd();
    else
      generator.write(keyName, keyValue);
  }

  /**
   * Run a streaming extract/insert on a document, re-encoding if
   * requested to the target generator. earlyExit is meaningful only
   * for extractions (i.e. when generator = null).
   */
  protected Pair<String, Boolean> parseStream(JsonParser parser,
                                              JsonGenerator generator,
                                              boolean earlyExit)
    throws JsonException
  {
    if ((keySteps == null) || (keySteps.length <= 0))
      throw makeException(Message.EX_KEY_PATH_EMPTY);

    boolean firstEvent = true;
    boolean wasChanged = false;
    boolean keyWritten = false;

    // Make the key the first field if it's top-level
    // and any old key needs to be removed anyway.
    // (The BSON format requires this, so rekeying of BSONs
    // should be done with removeKey == true.)
    boolean firstField = ((keySteps.length == 1) &&
                          removeKey && (newKey != null));

    int     oDepth      = -1;    // Object depth level (0-based)
    int     aDepth      = -1;    // Array depth level (0-based)
    int     currentStep = 0;     // Position along the search path
    boolean stopSearch  = false; // Abandon search for key
    boolean onPath      = false; // Last step on path but not end of path
    String  oldKey      = null;  // Key replaced (or written out)
    String  fldKey      = null;  // Key for the next field (null for an array)

    // If not rekeying or searching for a key, no special handling of root
    if (!rekeyingCheck() && (generator != null))
      firstEvent = false;

    // Can't early-exit from generation
    if (generator != null) earlyExit = false;

    while (parser.hasNext())
    {
      JsonParser.Event ev = parser.next();

      // Special handling of the first event
      // We need to make sure this is an object, not an array or scalar
      if (firstEvent)
      {
        if (ev != JsonParser.Event.START_OBJECT)
          throw makeException(Message.EX_DOCUMENT_NOT_OBJECT);
        firstEvent = false;

        if (firstField)
        {
          ++oDepth;
          if (generator != null)
          {
            generator.writeStartObject();
            addKey(generator, keySteps[0], newKey);
            oldKey = newKey;
            wasChanged = true;
            keyWritten = true;
            // If found later, key must be removed/skipped
          }
          continue;
        }
      }

      switch (ev)
      {
      case START_OBJECT:
        // onPath gets set to true immediately after we have matched a
        // KEY event that matches some intermediate key step.  This is
        // to detect the case where a primitive value occurs for that
        // intermediate key value.  At this point we are starting a
        // new object so we know that a primtive is not at currentStep.
        onPath = false;
        ++oDepth;
        if (generator != null)
        {
          if (fldKey != null)
            generator.writeStartObject(fldKey);
          else
            generator.writeStartObject();
          fldKey = null;
        }
        break;

      case START_ARRAY:
        ++aDepth;
        if (onPath)
        {
          // Found something unexpected while still on path
          // Set this flag to abandon the search
          stopSearch = true;
          onPath     = false;
        }
        if (generator != null)
        {
          if (fldKey != null)
            generator.writeStartArray(fldKey);
          else
            generator.writeStartArray();
          fldKey = null;
        }
        break;

      case END_ARRAY:
        --aDepth;
        if (generator != null)
          generator.writeEnd();
        break;

      case END_OBJECT:
        --oDepth;
        // If not under an array
        if (aDepth == -1)
        {
          // If we still haven't found the key, and we're either back at the
          // root object, or currently on an object level that matches the path
          if (!stopSearch && ((oDepth == -1) || (currentStep > 0)))
          {
            if ((newKey != null) && !keyWritten)
            {
              if (generator != null)
              {
                // Inject enough new object steps to reach the key level
                for (int i = currentStep; i < (keySteps.length - 1); ++i)
                  generator.writeStartObject(keySteps[i]);

                addKey(generator, keySteps[keySteps.length - 1], newKey);

                // Close the injected object steps
                for (int i = currentStep; i < (keySteps.length - 1); ++i)
                  generator.writeEnd();

                wasChanged = true;
                keyWritten = true;
              }
              oldKey = newKey;
            }

            stopSearch = true;
          }
          // If this was a path step, decrement the step depth
          if (currentStep > 0) --currentStep;
        }
        if (generator != null)
          generator.writeEnd();
        break;

      case KEY_NAME:
        
        fldKey = parser.getString();
	// If we're not under an array and we're at an object nesting depth
	// that matches the current step, this could be a matching step
        if ((aDepth == -1) && (oDepth == currentStep))
        {
          // If this key string matches the next path step
          if (fldKey.equals(keySteps[currentStep]))
          {
            // If we'd previously found the key, this is a duplicate field.
            // Textual JSON allows them, unfortunately; here, it's an error.
            if (stopSearch)
              throw makeException(Message.EX_KEY_DUPLICATE_STEP, fldKey);

            // Otherwise, this is the next step on the path
            ++currentStep;
            // If this is the last step, then this might be the key
            if (currentStep == keySteps.length) 
            {
              ev = parser.next();

              BigDecimal bval = null;
              switch (ev)
              {
               case VALUE_STRING:
                oldKey = parser.getString();
                //
                // ### There's trouble here with ID-type keys. Binaries mis-report
                // ### as type STRING, so one problem is that this won't disallow
                // ### the non-ID type binaries, as we should. Another issue is
                // ### that the string comparison here is case-sensitive, whereas
                // ### for an ID it should ignore case doing hex comparison.
                //
                if (mustMatch && (newKey != null))
                  if (!oldKey.equals(newKey))
                    throw makeException(Message.EX_KEY_MISMATCH);
                break;

               case VALUE_NUMBER:
                bval = parser.getBigDecimal();
                if (mustMatch && (newKey != null))
                  if (!checkNumericKey(newKey, bval))
                    throw makeException(Message.EX_KEY_MISMATCH);
                oldKey = bigDecimalToKey(bval);
                break;

               default:
                // ### Unsupported type of key
                if (needViable || (mustMatch && (newKey != null)))
                  throw makeException(Message.EX_KEY_MISMATCH);
                break;
              }


              if (generator != null)
              {
                // If we're not removing the old key, we need to rewrite it
                if (!removeKey)
                {
                  generator.writeKey(fldKey);
                  writeCurrentPrimitive(generator, parser, ev);
                  keyWritten = true;
                  fldKey = null;
                }
                // Otherwise if there's a new key to inject, do it now
                else if ((newKey != null) && !wasChanged)
                {
                  addKey(generator, fldKey, newKey);
                  wasChanged = true;
                  keyWritten = true;
                  fldKey = null;
                }
                // Otherwise we're just removing the old key
              }

              --currentStep;     // Closed the last step
              stopSearch = true; // Found/emitted/skipped the key
              continue;
            }

            // This flag is set to indicate that the most
            // recent step was on the path, but didn't end it
            onPath = true;
          }
        }

        // This is not the trailing key step so emit it
        // (fldKey is set to a non-null value)
        break;

      default:
        // Emitting a primitive scalar value
        if (onPath)
        {
          // Scalar found at intermediate path step, so we can't ever match
          // the whole path. Set this flag to abandon the search.
          stopSearch = true;
          onPath     = false;
        }

        if (generator != null)
        {
          if (fldKey != null) {
            generator.writeKey(fldKey);
            writeCurrentPrimitive(generator, parser, ev);
          } else {
            writeCurrentPrimitive(generator, parser, ev);
	  }
          fldKey = null;
        }
      }

      if (stopSearch && earlyExit) break;
    }

    if ((!earlyExit) && ((oDepth != -1) || (aDepth != -1)))
      throw makeException(Message.EX_DOCUMENT_NOT_CLOSED);

    return new Pair<String, Boolean>(oldKey, new Boolean(wasChanged));
  }

  /** Transfers current primitive event from parser at ev to generator */
  private void writeCurrentPrimitive(JsonGenerator generator, JsonParser parser, JsonParser.Event ev) {
    if (!(generator instanceof JsonpGeneratorWrapper) || !(parser instanceof JsonpParserWrapper)) {
      switch(ev) {
      case VALUE_FALSE:
        generator.write(false);
        break;
      case VALUE_TRUE:
        generator.write(true);
        break;
      case VALUE_NULL:
        generator.writeNull();
        break;
      case VALUE_NUMBER:
        generator.write(parser.getBigDecimal());
        break;
      case VALUE_STRING:
        generator.write(parser.getString());
        break;
      default:
        throw new UnsupportedOperationException(); // todo
      }
    } else {
      // oson on both sides
      JsonpGeneratorWrapper gWrapper = (JsonpGeneratorWrapper)generator;
      JsonpParserWrapper pWrapper = (JsonpParserWrapper)parser;
      OracleJsonGenerator oGenerator = gWrapper.getWrapped();
      OracleJsonParser oParser = pWrapper.getWrapped();
      oGenerator.write(oParser.getValue());
    }
  }

  /*************************************************************************
   * Key extraction using DOM navigation                                   *
   *************************************************************************/

  /**
   * Extract a key from the navigable form of the document.
   * Sub-classes will typically not use this method, but will have
   * a similar method for the particular document format.
   */
/* ### remove if unused
  protected String extractKey(JsonValue doc)
    throws JsonException
  {
    if ((keySteps == null) || (keySteps.length <= 0))
      throw makeException(Message.EX_KEY_PATH_EMPTY);

    JsonValue v = doc;
    if (v.getValueType() != JsonValue.ValueType.OBJECT)
      throw makeException(Message.EX_DOCUMENT_NOT_OBJECT);

    JsonObject obj = (JsonObject)v;
    // jump to the last object in path
    for (int i = 0; i < (keySteps.length - 1); ++i) 
    {
      v = obj.get(keySteps[i]);
      if (v == null) return null;
      if (v.getValueType() != JsonValue.ValueType.OBJECT)
        return null;

      obj = (JsonObject)v;
    }

    JsonValue val = obj.get(keySteps[keySteps.length - 1]);
    if (val == null) return null;

    switch (val.getValueType())
    {
    case NUMBER:
      return bigDecimalToKey(((JsonNumber)val).bigDecimalValue());

    case STRING:
      return ((JsonString)val).getString();

    default:
      throw makeException(Message.EX_KEY_MUST_BE_STRING);
    }
  }
 */
}
