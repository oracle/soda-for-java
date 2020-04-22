/* Copyright (c) 2014, 2020, Oracle and/or its affiliates. 
All rights reserved.*/

/*
   DESCRIPTION
    This class is used to specify RDBMS-specific options for a collection.
    It's a core object for managing the metadata associated with a collection.
 */

/**
 *  @author  Josh Spiegel
 *  @author  Doug McMahon
 *  @author  Max Orgiyan
 */

package oracle.soda.rdbms.impl;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Locale;
import java.util.Map;

import javax.json.JsonException;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonStructure;
import javax.json.JsonValue;
import javax.json.stream.JsonParsingException;

import oracle.json.common.JsonFactoryProvider;
import oracle.json.parser.DocumentLoader;
import oracle.json.util.ByteArray;
import oracle.json.util.JsonByteArray;
import oracle.soda.OracleDocument;
import oracle.soda.OracleException;
import oracle.soda.rdbms.OracleRDBMSMetadataBuilder;

/**
 * An immutable collection descriptor
 */
public class CollectionDescriptor
{
  /** 
   * Internal flag for testing purposes only.
   * ### This can be removed once ADB OSON storage default is enabled
   */
  private static final boolean FORCE_BINARY = Boolean.getBoolean("oracle.soda.rdbms.impl.CollectionDescriptor.FORCE_BINARY");

  //
  // Parameters used internal to the package
  //

  static final byte STRING_KEY  = 1; // Key is a VARCHAR2(255)
  static final byte NCHAR_KEY   = 2; // Key is an NVARCHAR2(255)
  static final byte INTEGER_KEY = 3; // Key is a long integer NUMBER
  static final byte RAW_KEY     = 4; // Key is a uid RAW(16)

  static final byte CHAR_CONTENT  = 1; // Content is VARCHAR2(32k)
  static final byte RAW_CONTENT   = 2; // Content is RAW(32k)
  static final byte NCHAR_CONTENT = 3; // Content is an NVARCHAR2(32k)
  static final byte BLOB_CONTENT  = 4; // Content is a BLOB
  static final byte CLOB_CONTENT  = 5; // Content is a CLOB
  static final byte NCLOB_CONTENT = 6; // Content is an NCLOB

  // LOB compress level
  static final byte LOB_COMPRESS_NONE   = 0;
  static final byte LOB_COMPRESS_HIGH   = 1; 
  static final byte LOB_COMPRESS_MEDIUM = 2; 
  static final byte LOB_COMPRESS_LOW    = 3;

  // LOB encryption algorithms
  static final byte LOB_ENCRYPT_NONE     = 0;
  static final byte LOB_ENCRYPT_3DES168  = 1; 
  static final byte LOB_ENCRYPT_AES128   = 2; 
  static final byte LOB_ENCRYPT_AES192   = 3;
  static final byte LOB_ENCRYPT_AES256   = 4;

  static final byte KEY_ASSIGN_CLIENT   = 1; // Client-assigned
  static final byte KEY_ASSIGN_UUID     = 2; // From Java UUID
  static final byte KEY_ASSIGN_GUID     = 3; // From SYS_OP_GUID
  static final byte KEY_ASSIGN_SEQUENCE = 4; // From DB sequence
  static final byte KEY_ASSIGN_IDENTITY = 5; // From DB identity column

  static final byte VERSION_NONE       = 0; // Application generated
  static final byte VERSION_TIMESTAMP  = 1; // Precise timestamp
  static final byte VERSION_SEQUENTIAL = 2; // Integer version number
  static final byte VERSION_UUID       = 3; // Generate UUID stamp
  static final byte VERSION_SHA256     = 4; // Generate SHA-256 checksum
  static final byte VERSION_MD5        = 5; // Generate MD-5 checksum
  
  static final byte DBOBJECT_TABLE     = 0;
  static final byte DBOBJECT_VIEW      = 1;
  static final byte DBOBJECT_PACKAGE   = 2;
  
  static final byte VALIDATION_LAX      = 0;
  static final byte VALIDATION_STANDARD = 1; // Known as 'strict' mode in 
                                             // JSON SQL. 
  static final byte VALIDATION_STRICT   = 2; // Known as 'strict with
                                             // unique keys' mode in JSON SQL.

  private static final int VAR_KEY_TYPE_DEFAULT_LENGTH      = 255;

  // Default length for VARCHAR2 content (in bytes)
  // ### DBMS_SODA_ADMIN needs to be enhanced to default
  // this to 32767 bytes when extended datatypes are on.
  private static final int DEFAULT_VARCHAR2_CONTENT_LENGTH  = 4000;

  // Default length for RAW content (in bytes)
  // ### DBMS_SODA_ADMIN needs to be enhanced to default
  // this to 32767 bytes when extended datatypes are on.
  private static final int DEFAULT_RAW_CONTENT_LENGTH       = 2000;

  // Default length for NVARCHAR2 content (in characters).
  // Oracle limit is 4000 bytes, which translates to 2000
  // characters because we assume AL16UTF16 national character set.
  // ### DBMS_SODA_ADMIN needs to be enhanced to default
  // this to 16383 bytes when extended datatypes are on.
  private static final int DEFAULT_NVARCHAR2_CONTENT_LENGTH = 2000;

  //
  //  Constants for initial values.  
  //
  private static final String  DEFAULT_CONTENT_NAME    = "JSON_DOCUMENT";
  private static final byte    DEFAULT_LOB_ENCRYPT     = LOB_ENCRYPT_NONE;
  private static final byte    DEFAULT_VERSION         = VERSION_NONE;
  private static final byte    DEFAULT_LOB_COMPRESS    = LOB_COMPRESS_NONE;
  private static final byte    DEFAULT_CONTENT         = BLOB_CONTENT;
  private static final String  DEFAULT_KEY_NAME        = "ID";
  private static final int     DEFAULT_KEY_LENGTH      = 0;
  private static final byte    DEFAULT_KEY_TYPE        = STRING_KEY;
  private static final byte    DEFAULT_CONTENT_LENGTH  = 0; 
  private static final boolean DEFAULT_LOB_CACHE       = true;
  private static final byte    DEFAULT_KEY_ASSIGN      = KEY_ASSIGN_UUID;
  private static final boolean DEFAULT_WRITABLE        = true;
  private static final boolean DEFAULT_PRE_PARSED      = false;
  private static final byte    DEFAULT_VALIDATION_MODE = VALIDATION_STANDARD;
  private static final byte    DEFAULT_DBOBJECT        = DBOBJECT_TABLE;
  
  static final int MAX_KEY_LENGTH = 255;

  //
  // The members are visible within the package for convenience
  // 

  final String   uriName;
  final String   dbObjectName;
  final byte     dbObjectType;
  final String   dbSchema;
  final boolean  writable;
  final String   jsonFormat;
  final byte     validationMode;
  final String   timeIndex;
  final int      keyAssignmentMethod;
  final String   keySequenceName;
  final int      versioningMethod;
  final String   keyColumnName;
  final int      keyDataType;
  final int      keyLength;
  final String   contentColumnName;
  final int      contentDataType;
  final int      contentLength;
  final int      contentLobCompress;
  final int      contentLobEncrypt;
  final boolean  contentLobCache;
  final String   timestampColumnName;
  final String   versionColumnName;
  final String   doctypeColumnName;
  final String   creationColumnName;

  /**
   * Return a SQL identifier derived from the string, case-preserving
   */
  static String stringToIdentifier(String jsonName)
  {
    return(CollectionDescriptor.stringToIdentifier(jsonName, true));
  }

  /**
   * Return a SQL identifier derived from the string,
   * optionally either case-preserving or upper-casing
   */
  static String stringToIdentifier(String jsonName, boolean preserveCase)
  {
    String identifierName = null;
    if (jsonName != null)
    {
      char[] data = jsonName.toCharArray();
      if (data.length == 0) return("");

      boolean hasUpper = false;
      boolean hasLower = false;
      boolean toUpper = !preserveCase;

      // Examine all characters of data
      for (int i = 0; i < data.length; ++i)
      {
        char ch = data[i];
        if (ch == '"')
          data[i] = '_'; // Replace double quotes with underscores
        else if (ch < ' ')
          data[i] = '_'; // Replace control characters with underscores
        else if ((ch >= 'A') && (ch <= 'Z'))
          hasUpper = true;
        else if ((ch >= 'a') && (ch <= 'z'))
          hasLower = true;
        // If the string contains non-alphanumeric characters
        // that aren't allowed by SQL, we shouldn't uppercase it
        else if (((ch < '0') || (ch > '9')) &&
                 ((ch != '_') && (ch != '$') && (ch != '#')))
          toUpper = false;
      }

      // If the string is mixed case, we shouldn't uppercase it
      if (toUpper && hasUpper && hasLower)
        toUpper = false;
      // If the string doesn't start with an alphabetic character
      // we shouldn't upper case it
      if (toUpper)
      {
        char ch = data[0];
        if (((ch < 'A') || (ch > 'Z')) && ((ch < 'a') || (ch > 'z')))
          toUpper = false;
      }

      identifierName = new String(data);

      if (toUpper) identifierName = identifierName.toUpperCase(Locale.US);
    }
    return(identifierName);
  }

  public static CollectionDescriptor createDefault(String name)
  {
    try 
    {
      return new Builder().buildDescriptor(name);
    }
    catch (OracleException e)
    {
      throw new IllegalStateException(e);
    }
  }
  
  private static boolean isLobType(int contentDataType)
  {
    return contentDataType == BLOB_CONTENT ||
           contentDataType == CLOB_CONTENT ||
           contentDataType == NCLOB_CONTENT;
  }
  
  private static boolean keyTypeSupportsLength(int keyType)
  {
    return keyType == STRING_KEY ||
           keyType == NCHAR_KEY;
  }
  
  private static String contentTypeToString(int contentDataType)
  {
    switch (contentDataType)
    {
    case CHAR_CONTENT:  
      return "VARCHAR2";
    case RAW_CONTENT:   
      return "RAW";
    case NCHAR_CONTENT: 
      return "NVARCHAR2";
    case BLOB_CONTENT:  
      return "BLOB";
    case CLOB_CONTENT:  
      return "CLOB";
    case NCLOB_CONTENT:
      return "NCLOB";
    default:
      throw new IllegalStateException();
    }
  }

  /**
   * Use CollectionDescriptor.Builder to create an instance.
   */
  private CollectionDescriptor(String uriName,
                       String  dbSchema,
                       String  dbObjectName,
                       byte    dbObjectType,
                       String  keyColumnName,
                       byte    keyColumnType,
                       int     keyColumnLength,
                       String  contentColumnName,
                       byte    contentColumnType,
                       int     contentColumnLength,
                       byte    contentLobCompress,
                       boolean contentLobCache,
                       byte    contentLobEncrypt, 
                       String  doctypeColumnName,
                       String  creationColumnName,
                       String  timestampColumnName,
                       String  versionColumnName,
                       byte    versioningMethod,
                       byte    keyAssignmentMethod,
                       String  keySequenceName,
                       boolean writable,
                       String  jsonFormat,
                       byte    validationMode,
                       String  timeIndex)
  {
    this.uriName = uriName;
    this.dbSchema = dbSchema;
    this.dbObjectName = dbObjectName;
    this.dbObjectType = dbObjectType;
    this.keyColumnName = keyColumnName;
    this.keyLength = keyColumnLength;
    this.keyDataType = keyColumnType;
    this.contentColumnName = contentColumnName;
    this.contentDataType = contentColumnType;
    this.contentLength = contentColumnLength;
    this.contentLobCompress = contentLobCompress;
    this.contentLobEncrypt = contentLobEncrypt;
    this.contentLobCache = contentLobCache;
    this.doctypeColumnName = doctypeColumnName;
    this.creationColumnName = creationColumnName;
    this.timestampColumnName = timestampColumnName;
    this.timeIndex = timeIndex;
    this.versionColumnName = versionColumnName;
    this.versioningMethod = versioningMethod;
    this.keySequenceName = keySequenceName;
    this.keyAssignmentMethod = keyAssignmentMethod;
    this.writable = writable;
    this.jsonFormat = jsonFormat;
    this.validationMode = validationMode;
  }

  public String getName()
  {
    return(uriName);
  }

  String getKeyAssignmentMethod()
  {
    switch (keyAssignmentMethod)
    {
    case KEY_ASSIGN_CLIENT:
      return "CLIENT";
    case KEY_ASSIGN_UUID:
      return "UUID";
    case KEY_ASSIGN_GUID:
      return "GUID";
    case KEY_ASSIGN_SEQUENCE:
      return "SEQUENCE";
    case KEY_ASSIGN_IDENTITY:
      return "IDENTITY";
    default:
      throw new IllegalStateException();
    }
  }

  String getVersioningMethod()
  {
    switch (versioningMethod)
    {
    case VERSION_TIMESTAMP:
      return "TIMESTAMP";
    case VERSION_SEQUENTIAL:
      return "SEQUENTIAL";
    case VERSION_UUID: 
      return "UUID";
    case VERSION_SHA256: 
      return "SHA256";
    case VERSION_MD5: 
      return "MD5";
    case VERSION_NONE: 
      return "NONE";
    default:
      throw new IllegalStateException();
    }
  }

  String getKeyDataType()
  { 
    switch (keyDataType)
    {
    case INTEGER_KEY: 
      return "NUMBER";
    case RAW_KEY: 
      return "RAW";
    case NCHAR_KEY:
      return "NVARCHAR2";
    case STRING_KEY:
      return "VARCHAR2";
    default:
      throw new IllegalStateException();
    }
  }

  String getContentDataType()
  {
    return contentTypeToString(contentDataType);
  }

  String getValidationMode()
  {
    switch (validationMode)
    {
    case VALIDATION_LAX:
      return "LAX";
    case VALIDATION_STANDARD:
      return "STANDARD";
    case VALIDATION_STRICT:
      return "STRICT";
    default:
      throw new IllegalStateException();
    }
  }

  String getLobCompression()
  {
    switch (contentLobCompress)
    {
    case LOB_COMPRESS_HIGH:
      return "HIGH";
    case LOB_COMPRESS_MEDIUM: 
      return "MEDIUM";
    case LOB_COMPRESS_LOW:
      return "LOW";
    case LOB_COMPRESS_NONE: 
      return "NONE";
    default:
      throw new IllegalStateException();
    }
  }
  
  String getLobEncryption()
  {
    switch (contentLobEncrypt)
    {
    case LOB_ENCRYPT_3DES168:
      return "3DES168";
    case LOB_ENCRYPT_AES128:
      return "AES128";
    case LOB_ENCRYPT_AES192:
      return "AES192";
    case LOB_ENCRYPT_AES256:
      return "AES256";
    case LOB_ENCRYPT_NONE: 
      return "NONE";
    default:
      throw new IllegalStateException();
    }
  }

  /**
   *
   * Returns true if running any jsonFormat (e.g. "oson")
   */
  boolean hasBinaryFormat()
  {
    return (jsonFormat != null || FORCE_BINARY);
  }

  /**
   * Compare two strings, even if one or both are null.
   */
  private boolean compareStrings(String s1, String s2)
  {
    if (s1 == s2) return(true);
    if (s1 == null) return(false);
    if (s2 == null) return(false);
    return(s1.equals(s2));
  }

  boolean matches(CollectionDescriptor desc)
  {
    if (this == desc)
      return(true);
    if (this.versioningMethod != desc.versioningMethod)
      return(false);
    if (this.keyAssignmentMethod != desc.keyAssignmentMethod)
      return(false);
    if (this.keyDataType != desc.keyDataType)
      return(false);
    if (this.contentDataType != desc.contentDataType)
      return(false);
    if (this.contentLobCache != desc.contentLobCache)
      return(false);
    if (this.contentLobEncrypt != desc.contentLobEncrypt)
      return(false);
    if (this.contentLobCompress != desc.contentLobCompress)
      return(false);
    if (this.keyLength != desc.keyLength)
      return(false);
    if (this.contentLength != desc.contentLength)
      return(false);
    if (this.writable != desc.writable)
      return(false);
    if (!compareStrings(this.jsonFormat, desc.jsonFormat))
      return(false);
    if (this.dbObjectType != desc.dbObjectType)
      return(false);
    if (this.validationMode != desc.validationMode)
      return(false);
    if (!compareStrings(this.timeIndex, desc.timeIndex))
      return(false);
    if (!compareStrings(this.uriName, desc.uriName))
      return(false);
    if (!compareStrings(this.dbSchema, desc.dbSchema))
      return(false);
    if (!compareStrings(this.dbObjectName, desc.dbObjectName))
      return(false);
    if (!compareStrings(this.keyColumnName, desc.keyColumnName))
      return(false);
    if (!compareStrings(this.contentColumnName, desc.contentColumnName))
      return(false);
    if (!compareStrings(this.keySequenceName, desc.keySequenceName))
      return(false);
    if (!compareStrings(this.timestampColumnName, desc.timestampColumnName))
      return(false);
    if (!compareStrings(this.versionColumnName, desc.versionColumnName))
      return(false);
    if (!compareStrings(this.doctypeColumnName, desc.doctypeColumnName))
      return(false);
    if (!compareStrings(this.creationColumnName, desc.creationColumnName))
      return(false);
    return(true);
  }

  /**
   * Returns a serialization of the descriptor as a JSON document
   */
  public String getDescription()
  {
    JsonByteArray builder = new JsonByteArray();

    builder.appendOpenBrace();

    if (dbSchema != null)
    {
      builder.appendValue("schemaName");
      builder.appendColon();
      builder.appendValue(dbSchema);
      builder.appendComma();
    }

    if (dbObjectName != null)
    {
      switch (dbObjectType)
      {
      case DBOBJECT_PACKAGE: 
        builder.appendValue("packageName");
        break;
      case DBOBJECT_VIEW: 
        builder.appendValue("viewName");
        break;
      case DBOBJECT_TABLE: 
        builder.appendValue("tableName");
        break;
      default:
        throw new IllegalStateException(); 
      }
      builder.appendColon();
      builder.appendValue(dbObjectName);
      builder.appendComma();
    }

    builder.appendValue("keyColumn");
    builder.appendColon();
    builder.appendOpenBrace();
    builder.appendValue("name");
    builder.appendColon();
    builder.appendValue(keyColumnName);
    builder.appendComma();
    builder.appendValue("sqlType");
    builder.appendColon();
    builder.appendValue(getKeyDataType());
    if (keyLength > 0)
    {
      builder.appendComma();
      builder.appendValue("maxLength");
      builder.appendColon();
      builder.append(Integer.toString(keyLength));
    }
    builder.appendComma();

    if (keySequenceName != null)
    {
      builder.appendValue("sequenceName");
      builder.appendColon();
      builder.appendValue(keySequenceName);
      builder.appendComma();
    }
    builder.appendValue("assignmentMethod");
    builder.appendColon();
    builder.appendValue(getKeyAssignmentMethod());

    builder.appendCloseBrace();

    builder.appendComma();

    builder.appendValue("contentColumn");
    builder.appendColon();
    builder.appendOpenBrace();
    builder.appendValue("name");
    builder.appendColon();
    builder.appendValue(contentColumnName);
    builder.appendComma();
    builder.appendValue("sqlType");
    builder.appendColon();
    builder.appendValue(getContentDataType());
    if (contentLength > 0)
    {
      builder.appendComma();
      builder.appendValue("maxLength");
      builder.appendColon();
      builder.append(Integer.toString(contentLength));
    }
    else if (jsonFormat == null)
    {
      builder.appendComma();
      builder.appendValue("compress");
      builder.appendColon();
      builder.appendValue(getLobCompression());
    
      builder.appendComma();
      builder.appendValue("cache");
      builder.appendColon();
      builder.append((contentLobCache) ? "true" : "false");
    
      builder.appendComma();
      builder.appendValue("encrypt");
      builder.appendColon();
      builder.appendValue(getLobEncryption());
    }

    if (jsonFormat == null)
    {
      builder.appendComma();
      builder.appendValue("validation");
      builder.appendColon();
      builder.appendValue(getValidationMode());
    }

    if (jsonFormat != null)
    {
      builder.appendComma();
      builder.appendValue("jsonFormat");
      builder.appendColon();
      builder.appendValue(jsonFormat);
    }

    builder.appendCloseBrace();

    if (versionColumnName != null)
    {
      builder.appendComma();
      builder.appendValue("versionColumn");
      builder.appendColon();
      builder.appendOpenBrace();
      builder.appendValue("name");
      builder.appendColon();
      builder.appendValue(versionColumnName);
      builder.appendComma();

      // This information is inferred and output only
      builder.appendValue("type");
      builder.appendColon();
      switch (versioningMethod)
      {
      case CollectionDescriptor.VERSION_SEQUENTIAL:
      case CollectionDescriptor.VERSION_TIMESTAMP:
        builder.appendValue("Integer");
        break;
      default:
        builder.appendValue("String");
        break;
      }
      builder.appendComma();

      builder.appendValue("method");
      builder.appendColon();
      builder.appendValue(getVersioningMethod());
      builder.appendCloseBrace();
    }

    if (timestampColumnName != null)
    {
      builder.appendComma();
      builder.appendValue("lastModifiedColumn");
      builder.appendColon();
      builder.appendOpenBrace();
      builder.appendValue("name");
      builder.appendColon();
      builder.appendValue(timestampColumnName);
      if (timeIndex != null)
      {
        builder.appendComma();
        builder.appendValue("index");
        builder.appendColon();
        builder.appendValue(timeIndex);
      }
      builder.appendCloseBrace();
    }

    if (creationColumnName != null)
    {
      builder.appendComma();
      builder.appendValue("creationTimeColumn");
      builder.appendColon();
      builder.appendOpenBrace();
      builder.appendValue("name");
      builder.appendColon();
      builder.appendValue(creationColumnName);
      builder.appendCloseBrace();
    }

    if (doctypeColumnName != null)
    {
      builder.appendComma();
      builder.appendValue("mediaTypeColumn");
      builder.appendColon();
      builder.appendOpenBrace();
      builder.appendValue("name");
      builder.appendColon();
      builder.appendValue(doctypeColumnName);
      builder.appendCloseBrace();
    }

    builder.appendComma();
    builder.appendValue("readOnly");
    builder.appendColon();
    builder.append((writable) ? "false" : "true");

    builder.appendCloseBrace();

    return(new String(builder.getArray(), 0, builder.getLength(),
                      ByteArray.DEFAULT_CHARSET));
  }

  /**
   * Parse a JSON collection descriptor and return a matching
   * properties object. If a null stream is given, returns
   * the "standard" properties.
   */
  // Note: this can throw run-time exceptions.
  public static Builder jsonToBuilder(InputStream inp)
          throws OracleException
  {
    // ### Prop up any existing usage of this old interface
    return jsonToBuilder(new JsonFactoryProvider(), inp);
  }

  static Builder jsonToBuilder(JsonFactoryProvider jProvider, InputStream inp)
      throws OracleException
  {    
    if (inp == null) 
      return createStandardBuilder();

    Builder builder = new Builder();
    DocumentLoader loader;
    JsonObject jDocument;

    try
    {
      loader = new DocumentLoader(jProvider, inp);
    }
    catch (JsonException e)
    {
      Throwable cause = e.getCause();

      if (cause instanceof IOException)
      {
        throw SODAUtils.makeException(
          SODAMessage.EX_METADATA_DOC_IO_EXCEPTION, cause);
      }
      else
      {
        throw SODAUtils.makeException(
          SODAMessage.EX_METADATA_DOC_INVALID_JSON_CANT_DETERMINE_ENCODING, e);
      }
    }

    try
    {
      JsonStructure structure = loader.parse();
      if (JsonValue.ValueType.OBJECT != structure.getValueType())
        throw SODAUtils.makeException(SODAMessage.EX_COL_SPEC_NOT_OBJECT);

      jDocument = (JsonObject)structure;
    }
    catch (JsonParsingException e)
    {
      throw SODAUtils.makeException(SODAMessage.EX_METADATA_DOC_INVALID_JSON, e);
    }
    catch (JsonException e)
    {
      Throwable cause = e.getCause();
      if (cause instanceof IOException)
      {
        throw SODAUtils.makeException(SODAMessage.EX_METADATA_DOC_IO_EXCEPTION,
                                      cause);
      }
      // Shouldn't occur: According to the JSR353 Javadoc, a JsonException
      // thrown here will have an IOException as the cause.
      else
      {
        throw SODAUtils.makeException(SODAMessage.EX_METADATA_DOC_INVALID_JSON,
                                      e);
      }
    }

    for (Map.Entry<String, JsonValue> entry : jDocument.entrySet()) 
    {
      String entryKey = entry.getKey();
      
      if (entryKey.equalsIgnoreCase("schemaName"))
      {
         builder.schemaName(entryToString(entry));
      }
      else if (entryKey.equalsIgnoreCase("tableName"))
      {
         builder.tableName(entryToString(entry));
      }
      else if (entryKey.equalsIgnoreCase("viewName"))
      {
         builder.viewName(entryToString(entry));
      }
      else if (entryKey.equalsIgnoreCase("packageName"))
      {
         builder.packageName(entryToString(entry));
      }
      else if (entryKey.equalsIgnoreCase("contentColumn"))
      {
        for (Map.Entry<String, JsonValue> subentry : entryToObject(entry).entrySet())
        {
          String fieldName = subentry.getKey();
          if (fieldName.equalsIgnoreCase("name"))
          {
            builder.contentColumnName(entryToString(subentry));
          }
          else if (fieldName.equalsIgnoreCase("sqlType"))
          {
            builder.contentColumnType(entryToString(subentry));
          }
          else if (fieldName.equalsIgnoreCase("maxLength"))
          {
            builder.contentColumnMaxLength(entryToInt(subentry));
          }
          else if (fieldName.equalsIgnoreCase("validation"))
          {
            builder.contentColumnValidation(entryToString(subentry));
          }
          else if (fieldName.equalsIgnoreCase("compress"))
          {
            builder.contentColumnCompress(entryToString(subentry));
          }
          else if (fieldName.equalsIgnoreCase("cache"))
          {
            builder.contentColumnCache(entryToBoolean(subentry));
          }
          else if (fieldName.equalsIgnoreCase("encrypt"))
          {
            builder.contentColumnEncrypt(entryToString(subentry));
          }
          else if (fieldName.equalsIgnoreCase("jsonFormat"))
          {
            builder.jsonFormat(entryToString(subentry));
          }
          else
            throw SODAUtils.makeException(SODAMessage.EX_COL_SPEC_NOT_EXPECTED, fieldName);
        }
      }
      else if (entryKey.equalsIgnoreCase("keyColumn"))
      {
        for (Map.Entry<String, JsonValue> subentry : entryToObject(entry).entrySet())
        {
          String fieldName = subentry.getKey();
          if (fieldName.equalsIgnoreCase("name"))
          {
            builder.keyColumnName(entryToString(subentry));
          }
          else if (fieldName.equalsIgnoreCase("sqlType"))
          {
            builder.keyColumnType(entryToString(subentry));
          }
          else if (fieldName.equalsIgnoreCase("maxLength"))
          {
            builder.keyColumnMaxLength(entryToInt(subentry));
          }
          else if (fieldName.equalsIgnoreCase("sequenceName"))
          {
            builder.keyColumnSequenceName(entryToString(subentry));
          }
          else if (fieldName.equalsIgnoreCase("assignmentMethod"))
          {
            builder.keyColumnAssignmentMethod(entryToString(subentry));
          }
          else
            throw SODAUtils.makeException(SODAMessage.EX_COL_SPEC_NOT_EXPECTED, fieldName);
        }
      }
      else if (entryKey.equalsIgnoreCase("creationTimeColumn"))
      { 
        for (Map.Entry<String, JsonValue> subentry : entryToObject(entry).entrySet())
        {
          String fieldName = subentry.getKey();
          if (fieldName.equalsIgnoreCase("name"))
          {
            builder.creationTimeColumnName(entryToString(subentry));
          }
          else
            throw SODAUtils.makeException(SODAMessage.EX_COL_SPEC_NOT_EXPECTED, fieldName);
        }
      }
      else if (entryKey.equalsIgnoreCase("lastModifiedColumn"))
      {
        for (Map.Entry<String, JsonValue> subentry : entryToObject(entry).entrySet())
        {
          String fieldName = subentry.getKey();
          if (fieldName.equalsIgnoreCase("name"))
          {
            builder.lastModifiedColumnName(entryToString(subentry));
          }
          else if (fieldName.equalsIgnoreCase("index"))
          {
            builder.lastModifiedColumnIndex(entryToString(subentry));
          }
          else
            throw SODAUtils.makeException(SODAMessage.EX_COL_SPEC_NOT_EXPECTED, fieldName);
        }
      }
      else if (entryKey.equalsIgnoreCase("versionColumn"))
      {
        for (Map.Entry<String, JsonValue> subentry : entryToObject(entry).entrySet())
        {
          String fieldName = subentry.getKey();
          if (fieldName.equalsIgnoreCase("name"))
          {
            builder.versionColumnName(entryToString(subentry));
          }
          else if (fieldName.equalsIgnoreCase("method"))
          {
            builder.versionColumnMethod(entryToString(subentry));
          }
          else if (fieldName.equalsIgnoreCase("type")) 
          {
            // inferred value that is only added for readability
          }
          else
            throw SODAUtils.makeException(SODAMessage.EX_COL_SPEC_NOT_EXPECTED, fieldName);          
        }
      }
      else if (entryKey.equalsIgnoreCase("mediaTypeColumn"))
      {
        for (Map.Entry<String, JsonValue> subentry : entryToObject(entry).entrySet())
        {
          String fieldName = subentry.getKey();
          if (fieldName.equalsIgnoreCase("name"))
          {
            builder.mediaTypeColumnName(entryToString(subentry));
          }
          else
            throw SODAUtils.makeException(SODAMessage.EX_COL_SPEC_NOT_EXPECTED, fieldName);          
        }
      }
      else if (entryKey.equalsIgnoreCase("readOnly"))
      {
        JsonValue value = entry.getValue();
        if (value.getValueType() == JsonValue.ValueType.TRUE)
          builder.readOnly(true);
        else if (value.getValueType() == JsonValue.ValueType.FALSE) 
          builder.readOnly(false);
        else if (value.getValueType() == JsonValue.ValueType.STRING) 
        {
          // legacy? can we remove this case?
          String readWrite = ((JsonString)value).getString();
          if ("READWRITE".equalsIgnoreCase(readWrite)) 
            builder.readOnly(false);
          else if ("READONLY".equalsIgnoreCase(readWrite))
            builder.readOnly(true);
          else
            throw SODAUtils.makeException(SODAMessage.EX_UNEXPECTED_IN_COL_SPEC, entryKey);
        }
        else 
          throw SODAUtils.makeException(SODAMessage.EX_UNEXPECTED_IN_COL_SPEC, entryKey);
      }
      else 
        throw SODAUtils.makeException(SODAMessage.EX_COL_SPEC_NOT_EXPECTED, entryKey);
    }
    return(builder);
  }
  
  private static String entryToString(Map.Entry<String, JsonValue> entry) throws OracleException 
  {
    JsonValue value = entry.getValue();
    if (value.getValueType() == JsonValue.ValueType.STRING)
      return ((JsonString)value).getString();
    else 
      throw SODAUtils.makeException(SODAMessage.EX_UNEXPECTED_IN_COL_SPEC, entry.getKey());
  }
  
  private static int entryToInt(Map.Entry<String, JsonValue> entry) throws OracleException 
  {
    JsonValue value = entry.getValue();
    if (value.getValueType() == JsonValue.ValueType.NUMBER)
      return ((JsonNumber)value).intValue();
    else
      throw SODAUtils.makeException(SODAMessage.EX_UNEXPECTED_IN_COL_SPEC, entry.getKey());    
  }
  
  private static boolean entryToBoolean(Map.Entry<String, JsonValue> entry) throws OracleException 
  {
    JsonValue value = entry.getValue();
    if (value.getValueType() == JsonValue.ValueType.TRUE)
      return true;
    else if (value.getValueType() == JsonValue.ValueType.FALSE) 
      return false;
    else
      throw SODAUtils.makeException(SODAMessage.EX_UNEXPECTED_IN_COL_SPEC, entry.getKey());    
  }
  
  private static JsonObject entryToObject(Map.Entry<String, JsonValue> entry) throws OracleException 
  {
    JsonValue value = entry.getValue();
    if (value.getValueType() != JsonValue.ValueType.OBJECT) 
      throw SODAUtils.makeException(SODAMessage.EX_UNEXPECTED_IN_COL_SPEC, entry.getKey());
    
    return (JsonObject)value;
  }

  /**
   * Get properties given a JSON descriptor
   */
  public static Builder jsonToBuilder(String jsonDescriptor) 
      throws OracleException
  {
    byte[] data = jsonDescriptor.getBytes(ByteArray.DEFAULT_CHARSET);
    ByteArrayInputStream inp = new ByteArrayInputStream(data);
    return(CollectionDescriptor.jsonToBuilder(inp));
  }

  static Builder jsonToBuilder(JsonFactoryProvider jProvider,
                               String jsonDescriptor)
          throws OracleException
  {
    byte[] data = jsonDescriptor.getBytes(ByteArray.DEFAULT_CHARSET);
    ByteArrayInputStream inp = new ByteArrayInputStream(data);
    return(CollectionDescriptor.jsonToBuilder(jProvider, inp));
  }

  public String toString()
  {
    return(getName());
  }
  
  /**
   * Standard settings are used by:
   * (1) SODA as the defaults
   * (2) REST as the settings when no collection metadata is specified
   */
  public static Builder createStandardBuilder() {
    return new Builder().
      creationTimeColumnName("CREATED_ON").
      lastModifiedColumnName("LAST_MODIFIED").
      versionColumnName("VERSION").
      versionColumnMethod(VERSION_SHA256).
      contentColumnCache(true);
  }

  public static final class Builder implements OracleRDBMSMetadataBuilder 
  { 
    private String  dbSchema;
    private String  dbObjectName;
    private byte    dbObjectType          = DEFAULT_DBOBJECT; 
    private String  keyColumnName         = DEFAULT_KEY_NAME;
    private byte    keyColumnType         = DEFAULT_KEY_TYPE;
    private int     keyColumnLength       = DEFAULT_KEY_LENGTH;
    private String  contentColumnName     = DEFAULT_CONTENT_NAME;
    private byte    contentColumnType     = DEFAULT_CONTENT;
    private int     contentColumnLength   = DEFAULT_CONTENT_LENGTH;
    private byte    contentLobCompress    = DEFAULT_LOB_COMPRESS; 
    private boolean contentLobCache       = DEFAULT_LOB_CACHE;
    private byte    contentLobEncrypt     = DEFAULT_LOB_ENCRYPT;
    private String  doctypeColumnName;
    private String  creationColumnName;
    private String  timestampColumnName;
    private String  versionColumnName;
    private byte    versioningMethod      = DEFAULT_VERSION;
    private byte    keyAssignmentMethod   = DEFAULT_KEY_ASSIGN;
    private String  keySequenceName;
    private boolean writable              = DEFAULT_WRITABLE;
    private String  jsonFormat            = null;
    private byte    validationMode        = DEFAULT_VALIDATION_MODE;
    private String  timeIndex;
    
    @Override
    public OracleDocument build() throws OracleException 
    {
      CollectionDescriptor result = buildDescriptor(null);
      String json = result.getDescription();
      byte[] bytes;
      try 
      {
        bytes = json.getBytes(ByteArray.DEFAULT_ENCODING);
      } 
      catch (UnsupportedEncodingException e) 
      {
        // The default is UTF-8 and the JVM must support it.
        throw new IllegalStateException(e);
      }
      return new OracleDocumentImpl(bytes);
    }

    /** Not part of public API */
    public CollectionDescriptor buildDescriptor(String uriName)
      throws OracleException
    {
      return buildDescriptor(uriName, null);
    }

    CollectionDescriptor buildDescriptor(String uriName,
                                         String defaultDbSchema)
                                         throws OracleException
    {
      validate();

      int keyLength = effectiveKeyLength();      
      int contentLength = effectiveContentLength();
      
      String dbObjectName = this.dbObjectName;
      byte dbObjectType = this.dbObjectType;
      if (dbObjectName == null)
      {
        dbObjectName = stringToIdentifier(uriName, false); // Case converting
        dbObjectType = DBOBJECT_TABLE;
      }

      return new CollectionDescriptor(uriName,
                           (dbSchema == null) ? defaultDbSchema : dbSchema,
                           dbObjectName,
                           dbObjectType,
                           keyColumnName,
                           keyColumnType,
                           keyLength,
                           contentColumnName,
                           contentColumnType,
                           contentLength,
                           contentLobCompress,
                           contentLobCache, 
                           contentLobEncrypt, 
                           doctypeColumnName,
                           creationColumnName,
                           timestampColumnName,
                           versionColumnName,
                           versioningMethod,
                           keyAssignmentMethod,
                           keySequenceName,
                           writable,
                           jsonFormat,
                           validationMode, 
                           timeIndex);
    }

    /** 
     * If the key column supports length and the length is unspecified, use the default.
     * 
     * ### A value of 0 (unspecified) can be left alone in the descriptor and let 
     *     DBMS_SODA_ADMIN determine the effective length
     */
    private int effectiveKeyLength() 
    {
      if (keyTypeSupportsLength(keyColumnType) && keyColumnLength == 0)
        return VAR_KEY_TYPE_DEFAULT_LENGTH;
      
      // ### it should be a validation error if keyColumnLength > MAX_KEY_LENGTH
      if (keyTypeSupportsLength(keyColumnType) && keyColumnLength > MAX_KEY_LENGTH)
        return VAR_KEY_TYPE_DEFAULT_LENGTH;

      return keyColumnLength;
    }
    
    /** 
     * If the content column supports length and the length is unspecified, use the default.
     *
     * ### A value of 0 (unspecified) can be left alone in the descriptor and let 
     *     DBMS_SODA_ADMIN determine the effective length
     */
    private int effectiveContentLength() 
    {
      if (!isLobType(contentColumnType) && contentColumnLength == 0)
      {
        switch (contentColumnType)
        {
        case (CHAR_CONTENT) :
          return DEFAULT_VARCHAR2_CONTENT_LENGTH;
        case (NCHAR_CONTENT) :
          return DEFAULT_NVARCHAR2_CONTENT_LENGTH;
        case (RAW_CONTENT) :
          return DEFAULT_RAW_CONTENT_LENGTH;
        default :
          throw new IllegalStateException();
        }
      }

      return contentColumnLength;
    }

    private void validate() throws OracleException 
    {
      // content column type is LOB and max length specified
      if (contentColumnLength > 0 && isLobType(contentColumnType))
        throw SODAUtils.makeException(SODAMessage.EX_MAX_LEN_LOB_TYPE);
      
      // content column type does not support a maximum length
      if (keyColumnLength > 0 && !keyTypeSupportsLength(keyColumnType))
        throw SODAUtils.makeException(SODAMessage.EX_KEY_TYPE_BAD_LENGTH);
      
      // When using GUID or UUID to assign character type key values,
      // the key column length must be 32 or greater
      if ((keyColumnLength > 0) && (keyColumnLength < 32) &&
          ((keyAssignmentMethod == KEY_ASSIGN_GUID) ||
           (keyAssignmentMethod == KEY_ASSIGN_UUID)) &&
          ((keyColumnType == STRING_KEY) || (keyColumnType == NCHAR_KEY)))
        throw SODAUtils.makeException(SODAMessage.EX_KEY_TYPE_BAD_LENGTH2);
      
      // SecureFile LOB settings used with non-LOB type
      if (!isLobType(contentColumnType) && 
          (
            contentLobCompress != LOB_COMPRESS_NONE ||
            contentLobEncrypt != LOB_ENCRYPT_NONE ||
            contentLobCache != false
          )
        )
      {
        throw SODAUtils.makeException(SODAMessage.EX_SECURE_FILE_NOT_LOB, 
                                      contentTypeToString(contentColumnType));
      }

      // If the assignment method is "SEQUENCE",
      // a key column sequence name must be specified. 
      if (keySequenceName == null && keyAssignmentMethod == KEY_ASSIGN_SEQUENCE)
        throw SODAUtils.makeException(SODAMessage.EX_KEY_COL_SEQ);

      // Sets the last-modified index but not the column name
      if (timeIndex != null && timestampColumnName == null)
        throw SODAUtils.makeException(SODAMessage.EX_LAST_MODIFIED_COL);

      // A version method was specified
      // but the version column name is unspecified.
      if (versionColumnName == null && versioningMethod != VERSION_NONE)
        throw SODAUtils.makeException(SODAMessage.EX_VERSION_METHOD);
      
      if (doctypeColumnName != null && contentColumnType != BLOB_CONTENT)
        throw SODAUtils.makeException(SODAMessage.EX_MEDIA_TYPE_COLUMN_NOT_SUP);
    }

    @Override
    public Builder schemaName(String schemaName) 
    {
      this.dbSchema = stringToIdentifier(schemaName);
      return this;
    }

    @Override
    public Builder tableName(String tableName) 
    {
      this.dbObjectName = stringToIdentifier(tableName);
      this.dbObjectType = DBOBJECT_TABLE;
      return this;
    }

    @Override
    public Builder viewName(String viewName) 
    {
      this.dbObjectName = stringToIdentifier(viewName);
      this.dbObjectType = DBOBJECT_VIEW;
      return this;
    }

    public Builder packageName(String packageName)
    {
      this.dbObjectName = stringToIdentifier(packageName);
      this.dbObjectType = DBOBJECT_PACKAGE;
      return this;
    }
    
    /** internal */
    public Builder dbObject(String name, String type)
    {
      if ("PACKAGE".equalsIgnoreCase(type))
        this.dbObjectType = DBOBJECT_PACKAGE;
      else if ("VIEW".equalsIgnoreCase(type))
        this.dbObjectType = DBOBJECT_VIEW;
      else
        this.dbObjectType = DBOBJECT_TABLE;

      this.dbObjectName = stringToIdentifier(name);
      return this;
    }

    @Override
    public Builder contentColumnName(String name) 
    {
      this.contentColumnName = stringToIdentifier(name);
      return this;
    }

    @Override
    public Builder contentColumnType(String sqlType) throws OracleException 
    {
      byte type;
      if (sqlType == null)
        type = DEFAULT_CONTENT;
      else if (sqlType.equalsIgnoreCase("char"))
        type = CHAR_CONTENT;
      else if (sqlType.equalsIgnoreCase("varchar"))
        type = CHAR_CONTENT;
      else if (sqlType.equalsIgnoreCase("varchar2"))
        type = CHAR_CONTENT;
      /* Raw, and n-types are not currently supported
      else if (sqlType.equalsIgnoreCase("raw"))
        type = RAW_CONTENT;
      else if (sqlType.equalsIgnoreCase("nchar"))
        type = NCHAR_CONTENT;
      else if (sqlType.equalsIgnoreCase("nvarchar"))
        type = NCHAR_CONTENT;
      else if (sqlType.equalsIgnoreCase("nvarchar2"))
        type = NCHAR_CONTENT;
      else if (sqlType.equalsIgnoreCase("nclob"))
        type = NCLOB_CONTENT;
      */
      else if (sqlType.equalsIgnoreCase("blob"))
        type = BLOB_CONTENT;
      else if (sqlType.equalsIgnoreCase("clob"))
        type = CLOB_CONTENT;
      else
        throw SODAUtils.makeException(SODAMessage.EX_INVALID_ARG_VALUE, sqlType);

      this.contentColumnType = type;
      // Reset dependent fields
      if (isLobType(contentColumnType))
      {
        this.contentColumnLength = 0;
        this.contentLobCache = true;
      }
      else
      {
        this.contentLobCompress = LOB_COMPRESS_NONE;
        this.contentLobCache = false;
        this.contentLobEncrypt = LOB_ENCRYPT_NONE;
      }
      return this;
    }

    @Override
    public Builder contentColumnMaxLength(int maxLength) throws OracleException 
    {
      if (maxLength < 0) 
        throw SODAUtils.makeException(SODAMessage.EX_INVALID_ARG_VALUE, maxLength);

      this.contentColumnLength = maxLength;
      return this;
    }

    @Override
    public Builder contentColumnValidation(String mode) throws OracleException 
    {
      byte type;
      if (mode == null)
        type = DEFAULT_VALIDATION_MODE;
      else if (mode.equalsIgnoreCase("strict"))
        type = VALIDATION_STRICT;
      else if (mode.equalsIgnoreCase("standard"))
        type = VALIDATION_STANDARD;
      else if (mode.equalsIgnoreCase("lax"))
        type = VALIDATION_LAX;
      else 
        throw SODAUtils.makeException(SODAMessage.EX_INVALID_ARG_VALUE, mode);
      this.validationMode = type;
      return this;
    }

    @Override
    public Builder contentColumnCompress(String compress) throws OracleException 
    {
      byte type;
      if (compress == null)
        type = DEFAULT_LOB_COMPRESS;
      else if (compress.equalsIgnoreCase("high"))
        type = LOB_COMPRESS_HIGH;
      else if (compress.equalsIgnoreCase("medium"))
        type = LOB_COMPRESS_MEDIUM;
      else if (compress.equalsIgnoreCase("low"))
        type = LOB_COMPRESS_LOW;
      else if (compress.equalsIgnoreCase("none"))   
        type = LOB_COMPRESS_NONE;
      else
        throw SODAUtils.makeException(SODAMessage.EX_INVALID_ARG_VALUE, compress);
      this.contentLobCompress = type;
      return this;
    }

    @Override
    public Builder contentColumnCache(boolean cache) 
    {
      this.contentLobCache = cache;
      return this;
    }

    @Override
    public Builder contentColumnEncrypt(String encrypt) throws OracleException 
    {
      byte type;
      if (encrypt == null)
        type = DEFAULT_LOB_ENCRYPT;
      else if (encrypt.equalsIgnoreCase("3DES168"))
        type = LOB_ENCRYPT_3DES168;
      else if (encrypt.equalsIgnoreCase("AES128"))
        type = LOB_ENCRYPT_AES128;
      else if (encrypt.equalsIgnoreCase("AES192"))
        type = LOB_ENCRYPT_AES192;
      else if (encrypt.equalsIgnoreCase("AES256"))
        type = LOB_ENCRYPT_AES256;
      else if (encrypt.equalsIgnoreCase("NONE"))   
        type = LOB_ENCRYPT_NONE;
      else 
        throw SODAUtils.makeException(SODAMessage.EX_INVALID_ARG_VALUE, encrypt);
      this.contentLobEncrypt = type;
      return this;
    }

    @Override
    public Builder keyColumnName(String name) 
    {
      if (name == null)
      { 
        this.keyColumnName = CollectionDescriptor.DEFAULT_KEY_NAME;
      }
      else
      {
        this.keyColumnName = stringToIdentifier(name);
      }
      return this;
    }

    @Override
    public Builder keyColumnType(String ktype) throws OracleException 
    {
      byte type;
      if (ktype == null)
        type = DEFAULT_KEY_TYPE;
      else if (ktype.equalsIgnoreCase("string"))
        type = STRING_KEY;
      else if (ktype.equalsIgnoreCase("char"))
        type = STRING_KEY;
      else if (ktype.equalsIgnoreCase("varchar"))         
        type = STRING_KEY;
      else if (ktype.equalsIgnoreCase("varchar2"))
        type = STRING_KEY;
      else if (ktype.equalsIgnoreCase("nchar"))
        type = NCHAR_KEY;
      else if (ktype.equalsIgnoreCase("nvarchar"))
        type = NCHAR_KEY;
      else if (ktype.equalsIgnoreCase("nvarchar2"))
        type = NCHAR_KEY;
      else if (ktype.equalsIgnoreCase("integer"))
        type = INTEGER_KEY;
      else if (ktype.equalsIgnoreCase("number"))
        type = INTEGER_KEY;
      else if (ktype.equalsIgnoreCase("raw"))
        type = RAW_KEY;
      else if (ktype.equalsIgnoreCase("guid"))
        type = RAW_KEY;
      else if (ktype.equalsIgnoreCase("uuid"))
        type = RAW_KEY;
      else if (ktype.equalsIgnoreCase("uid"))   
        type = RAW_KEY;      
      else
        throw SODAUtils.makeException(SODAMessage.EX_INVALID_ARG_VALUE, ktype);
      
      this.keyColumnType = type;
      // Reset dependent fields
      if (keyColumnType == INTEGER_KEY || keyColumnType == RAW_KEY) 
      {
        this.keyColumnLength = 0;
      }
      return this;
    }

    @Override
    public Builder keyColumnMaxLength(int maxLength) throws OracleException 
    {
      if (maxLength < 0)
      {
        throw SODAUtils.makeException(SODAMessage.EX_INVALID_ARG_VALUE, maxLength);
      }
      this.keyColumnLength = maxLength;
      return this;
    }

    @Override
    public Builder keyColumnAssignmentMethod(String method)
            throws OracleException 
    {
      byte type;
      if (method == null)
        type = DEFAULT_KEY_ASSIGN;
      else if (method.equalsIgnoreCase("sequence"))
        type = KEY_ASSIGN_SEQUENCE;
      else if (method.equalsIgnoreCase("guid")) 
        type = KEY_ASSIGN_GUID;
      else if (method.equalsIgnoreCase("uuid"))
        type = KEY_ASSIGN_UUID;
      else if (method.equalsIgnoreCase("client"))
        type = KEY_ASSIGN_CLIENT;
      else if (method.equalsIgnoreCase("identity"))
        type = KEY_ASSIGN_IDENTITY;
      else 
        throw SODAUtils.makeException(SODAMessage.EX_INVALID_ARG_VALUE, method);
      
      // Reset dependent fields
      this.keyAssignmentMethod = type;
      if (keyAssignmentMethod != KEY_ASSIGN_SEQUENCE) 
      {
        keySequenceName = null;
      }
      return this;
    }

    @Override
    public Builder keyColumnSequenceName(String sequenceName) 
    {
      // Reset dependent fields
      if (sequenceName != null)
      {
        this.keyAssignmentMethod = KEY_ASSIGN_SEQUENCE;
      }
      this.keySequenceName = stringToIdentifier(sequenceName);
      return this;
    }

    @Override
    public Builder creationTimeColumnName(String name) 
    {
      this.creationColumnName = stringToIdentifier(name);
      return this;
    }

    @Override
    public Builder lastModifiedColumnName(String name) 
    {
      // Reset dependent fields
      if (name == null) 
      {
        this.timeIndex = null;
      }
      this.timestampColumnName = stringToIdentifier(name);
      return this;
    }

    @Override
    public Builder lastModifiedColumnIndex(String index) 
    {
      this.timeIndex = stringToIdentifier(index);
      return this;
    }

    @Override
    public Builder versionColumnName(String name) 
    {
      // Reset dependent fields
      if (name == null) 
      {
        this.versioningMethod = VERSION_NONE;
      }
      this.versionColumnName = stringToIdentifier(name);
      return this;
    }

    @Override
    public Builder versionColumnMethod(String method) 
            throws OracleException 
    {
      byte type;
      if (method == null)                             
        type = DEFAULT_VERSION;
      else if (method.equalsIgnoreCase("timestamp"))
        type = VERSION_TIMESTAMP;
      else if (method.equalsIgnoreCase("sequential"))
        type = VERSION_SEQUENTIAL;
      else if (method.equalsIgnoreCase("uuid"))
        type = VERSION_UUID;
      else if (method.equalsIgnoreCase("SHA256"))
        type = VERSION_SHA256;
      else if (method.equalsIgnoreCase("MD5"))
        type = VERSION_MD5;
      else if (method.equalsIgnoreCase("NONE"))
        type = VERSION_NONE;
      else 
        throw SODAUtils.makeException(SODAMessage.EX_INVALID_ARG_VALUE, method);
      return versionColumnMethod(type);
    }

    protected Builder versionColumnMethod(byte versioningMethod) {
      this.versioningMethod = versioningMethod;
      return this;
    }
    
    @Override
    public Builder mediaTypeColumnName(String name) 
    {
      this.doctypeColumnName = stringToIdentifier(name);
      return this;
    }
  
    @Override
    public Builder readOnly(boolean readOnly) 
    {
      this.writable = !readOnly;
      return this;
    }
    
    // ###
    public Builder jsonFormat(String format)
    {
      this.jsonFormat = format;
      return this;
    }
    
    @Override
    public Builder removeOptionalColumns() 
    {
      return this
        .creationTimeColumnName(null)
        .lastModifiedColumnName(null)
        .versionColumnName(null)
        .mediaTypeColumnName(null);
    }
    
  }
}
