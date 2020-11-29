/* Copyright (c) 2014, 2020, Oracle and/or its affiliates. */
/* All rights reserved.*/

/*
   DESCRIPTION
    This is the RDBMS-specific implementation of OracleDatabase.
 */

/**
 *  This class is not part of the public API, and is
 *  subject to change.
 *
 *  Do not rely on it in your application code.
 *
 *  @author  Doug McMahon
 *  @author  Max Orgiyan
 */

package oracle.soda.rdbms.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.sql.Array;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

import javax.json.JsonException;
import javax.json.JsonValue;
import javax.json.stream.JsonGenerator;
import javax.json.stream.JsonParser;
import javax.json.stream.JsonParserFactory;

import oracle.jdbc.OracleCallableStatement;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.OracleTypes;
import oracle.json.common.JsonFactoryProvider;
import oracle.json.common.MetricsCollector;
import oracle.json.logging.OracleLog;
import oracle.json.util.ByteArray;
import oracle.json.util.ComponentTime;
import oracle.json.util.HashFuncs;
import oracle.soda.OracleCollection;
import oracle.soda.OracleDatabase;
import oracle.soda.OracleDatabaseAdmin;
import oracle.soda.OracleDocument;
import oracle.soda.OracleDropResult;
import oracle.soda.OracleException;
import oracle.soda.rdbms.impl.CollectionDescriptor.Builder;
import oracle.soda.rdbms.impl.cache.DescriptorCache;

public class OracleDatabaseImpl implements OracleDatabase
{
  /** Magic as a byte array for Oracle Binary JSON */
  private static final byte[] MAGIC_BYTES = new byte[] { -1, 74, 90 };
  
  static ArrayList<String> EMPTY_LIST = new ArrayList<String>();

  //
  // Maximum size for a string binding (in bytes as UTF-8)
  //
  static final int MAX_STRING_BIND_LENGTH = 4000;

  //
  // Oracle internal character-set IDs for Unicode character sets
  //
  private static final short DBCS_AL32UTF8 = 873;
  private static final short DBCS_UTF8     = 871;

  private static final String SELECT_GUID =
    "select SYS_GUID() from SYS.DUAL";

  private static final String SELECT_GUID_BATCH =
    "declare\n"+
    "  type VCTAB is table of varchar2(255) index by binary_integer;\n"+
    "  N number;\n"+
    "  X varchar2(255);\n"+
    "  K XDB.DBMS_SODA_ADMIN.VCNTAB;\n"+
    "begin\n"+
    "  N := ?;\n"+
    "  K := XDB.DBMS_SODA_ADMIN.VCNTAB();\n"+
    "  K.extend(N);\n"+
    "  for I in 1..N loop\n"+
    "    select rawtohex(SYS_GUID()) into X from SYS.DUAL;\n"+
    "    K(I) := X;\n"+
    "  end loop;\n"+
    "  ? := K;\n"+
    "end;";

  private static final String SELECT_SCN =
    "begin\n  DBMS_SODA_ADMIN.GET_SCN(?);\nend;";

  private static final String COLLECTION_CREATION_MODE = "DDL";
  
  private static final Logger log =
    Logger.getLogger(OracleDatabaseImpl.class.getName());

  private final OracleConnection conn;

  // Shared cache of CollectionDescriptor objects
  private final DescriptorCache sharedDescriptorCache;

  // Local cache of CollectionDescriptor objects
  private final HashMap<String, CollectionDescriptor> localDescriptorCache;

  // Local cache of OracleCollection objects
  private final HashMap<String, OracleCollectionImpl> localCollectionCache;

  private MetricsCollector metrics;

  // Maximum column lengths
  private int rawMaxLength = -1;       // Unknown (default 2000)
  private int nvarcharMaxLength = -1;  // Unknown (default 2000)
  private int varcharMaxLength = -1;   // Unknown (default 4000)
  private boolean dbIsUnicode = false; // Unknown (default false)

  private boolean metadataTableExists = true;

  private OracleDatabaseAdmin admin;

  // ### Work-around for JDBC bug with the SQL "returning" clause
  // The internal bug is 14496772. The failure occurs if colons are
  // present in a "returning" expression. The bug is fixed,
  // but leaving th workaround code in just in case for now.
  static final boolean JDBC_WORKAROUND = false;

  // Length of the JSON collection metadata descriptor (in bytes),
  // as stored in the DB and returned by DBMS_SODA_ADMIN
  private static final int DESC_LENGTH = 4000;

  // Length of the creation timestamp (in bytes)
  // as returned by DBMS_SODA_ADMIN
  private static final int CREATION_TIMESTAMP_LENGTH = 255;

  // Length of DDL SQL as returned by GET_SQL_TEXT
  private static final int DDL_SQL_LENGTH = 32500;

  private boolean avoidTxnManagement;

  private SODAUtils.SQLSyntaxLevel sqlSyntaxLevel = SODAUtils.SQLSyntaxLevel.SQL_SYNTAX_UNKNOWN;

  /**
   * Not part of a public API.
   *
   * Used internally by the REST component.
   *
   * Construct a database implementation for the specified connection.
   * Provide a metrics collector and a collection metadata descriptor
   * cache.
   */
  public OracleDatabaseImpl(OracleConnection conn,
                            DescriptorCache descriptorCache,
                            MetricsCollector metrics,
                            boolean localCaching,
                            boolean avoidTxnManagement)
  {
    this.conn = conn;
    this.sharedDescriptorCache = descriptorCache;
    this.metrics = metrics;
    this.avoidTxnManagement = avoidTxnManagement;

    //
    // This is a back-handed method of getting the DB character set
    //
    try
    {
      short dbcsid = conn.getStructAttrCsId();
      if ((dbcsid == DBCS_AL32UTF8) || (dbcsid == DBCS_UTF8))
        dbIsUnicode = true;
    }
    catch (SQLException e)
    {
      if (OracleLog.isLoggingEnabled())
        log.warning(e.toString());
    }

/***
    // ### This may or may not be necessary
    setDateTimeNumFormats();
***/

    if (localCaching)
    {
      localDescriptorCache = new HashMap<String, CollectionDescriptor>();
      localCollectionCache = new HashMap<String, OracleCollectionImpl>();
    }
    else
    {
      localDescriptorCache = null;
      localCollectionCache = null;
    }
  }

  /**
   * Not part of a public API.
   * Used internally.
   */
  public OracleDatabaseImpl(OracleConnection conn,
                            DescriptorCache descriptorCache,
                            MetricsCollector metrics,
                            boolean localCaching)
  {
    this(conn, descriptorCache, metrics, localCaching, false);
  }

  /**
   * Not part of a public API.
   * Used internally.
   */
  public OracleDatabaseImpl(OracleConnection conn,
                            DescriptorCache descriptorCache,
                            MetricsCollector metrics)
  {
    this(conn, descriptorCache, metrics, true, false);
  }

  // oracle.sql.json classes which may or may not be available for binary conversions
  
  private static final Class<?> JSON_FACT_CLASS;
  private static final Method JSON_FACT_CREATE_BINARY_VALUE;
  private static final Method JSON_FACT_CREATE_TEXT_VALUE;
  private static final Method JSON_FACT_CREATE_JSON_TEXT_GENERATOR;
  private static final Method JSON_FACT_CREATE_BINARY_PARSER; 
  private static final Method JSON_FACT_CREATE_BINARY_GENERATOR; 
  private static final Method JSON_FACT_CREATE_TEXT_PARSER;
  protected static final Class<?> JSON_VALUE_CLASS;
  private static final Class<?> JSON_GEN_CLASS;
  protected static final Class<?> JSON_PARSE_CLASS;
  private static final Class<?> JSON_DATUM_CLASS;
  private static final Method JSON_SHARE_BYTES;
  
  protected static byte[] binaryToText(byte[] binary, Object jsonFactory) throws OracleException 
  {
    if (binary == null)
    {
      return null;
    }
    if (binary.length == 0)
    {
      return new byte[0]; 
    }
    Object binaryParser = createBinaryParser(binary, jsonFactory);
    try 
    {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      Object textGenerator = JSON_FACT_CREATE_JSON_TEXT_GENERATOR.invoke(jsonFactory, baos);
      writeParserToGenerator(binaryParser, textGenerator, true);
      return baos.toByteArray();
    } 
    catch (Exception e)
    {
      throw SODAUtils.makeException(SODAMessage.EX_FROM_BINARY_CONVERSION_ERROR, e);
    }
  }

  protected static Object createBinaryParser(byte[] oson, Object jsonFactory) throws OracleException
  {
    try 
    {
      return JSON_FACT_CREATE_BINARY_PARSER.invoke(jsonFactory, ByteBuffer.wrap(oson));
    }
    catch (Exception e) 
    {
      throw SODAUtils.makeException(SODAMessage.EX_FROM_BINARY_CONVERSION_ERROR, e);
    }
  }

  protected static Object createTextParser(byte[] oson, Object jsonFactory) throws OracleException
  {
    try 
    {
      return JSON_FACT_CREATE_TEXT_PARSER.invoke(jsonFactory, new ByteArrayInputStream(oson));
    }
    catch (Exception e) 
    {
      throw SODAUtils.makeException(SODAMessage.EX_FROM_BINARY_CONVERSION_ERROR, e);
    }
  }
  
  protected static JsonValue binaryToJsonValue(byte[] binary, Object jsonFactory) throws OracleException
  {
    JsonParser parser = binaryToJsonParser(binary, jsonFactory);
    parser.next();
    JsonValue value = parser.getValue();
    parser.close();
    return value;
  }
  
  protected static Object binaryToOracleJsonValue(byte[] binary, Object jsonFactory) throws OracleException
  {
    try 
    {
      return JSON_FACT_CREATE_BINARY_VALUE.invoke(jsonFactory, ByteBuffer.wrap(binary));
    }
    catch (Exception e) 
    {
      throw SODAUtils.makeException(SODAMessage.EX_FROM_BINARY_CONVERSION_ERROR, e);
    }
  }
  
  protected static Object textToOracleJsonValue(byte[] text, Object jsonFactory) throws OracleException
  {
    try 
    {
      return JSON_FACT_CREATE_TEXT_VALUE.invoke(jsonFactory, new ByteArrayInputStream(text));
    }
    catch (Exception e) 
    {
      throw SODAUtils.makeException(SODAMessage.EX_FROM_BINARY_CONVERSION_ERROR, e);
    }
  }
  
  protected static JsonParser binaryToJsonParser(byte[] binary, Object jsonFactory) throws OracleException 
  {
    Object binaryParser = createBinaryParser(binary, jsonFactory);
    try 
    {
      Method wrap = JSON_PARSE_CLASS.getMethod("wrap", Class.class);
      return (JsonParser)wrap.invoke(binaryParser, JsonParser.class);
    }
    catch (Exception e) 
    {
      throw SODAUtils.makeException(SODAMessage.EX_FROM_BINARY_CONVERSION_ERROR, e);
    }
  }
  
  public byte[] textToBinary(byte[] data) throws OracleException 
  {
    if (data == null)
      return null;
    
    ByteArrayOutputStream osonOut = new ByteArrayOutputStream();
    Object binaryGen = createBinaryGenerator(osonOut);
    JsonParserFactory parserFactory = jProvider.getParserFactory();
    JsonParser parser = parserFactory.createParser(new ByteArrayInputStream(data));
    writeParserToGenerator(parser, binaryGen, false);
    return osonOut.toByteArray();
  }
    
  public byte[] textToBinary(Reader data) throws OracleException 
  {
    if (data == null)
      return null;

    ByteArrayOutputStream osonOut = new ByteArrayOutputStream();
    Object binaryGen = createBinaryGenerator(osonOut);
    JsonParserFactory parserFactory = jProvider.getParserFactory();
    JsonParser parser = parserFactory.createParser(data);
    writeParserToGenerator(parser, binaryGen, false);
    return osonOut.toByteArray();
  }
  
  private byte[] parserToBinary(Object parser) throws OracleException 
  {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Object binaryGen = createBinaryGenerator(baos);
    writeParserToGenerator(parser, binaryGen, false);
    return baos.toByteArray();
  }
  
  private byte[] jsonValueToBinary(JsonValue value) throws OracleException 
  {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Object binaryGen = createBinaryGenerator(baos);
    writeValueToGenerator(value, binaryGen);
    return baos.toByteArray();
  }

  private byte[] oracleJsonValueToBinary(Object value) throws OracleException 
  {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Closeable binaryGen = createBinaryGenerator(baos);
    writeOracleValueToGenerator(value, binaryGen);
    return baos.toByteArray();
  }
  
  private Closeable createBinaryGenerator(ByteArrayOutputStream osonOut) throws OracleException 
  {
    try 
    {
      return (Closeable)JSON_FACT_CREATE_BINARY_GENERATOR.invoke(getJsonFactory(), osonOut); 
    }
    catch (Exception e) 
    {
      throw SODAUtils.makeException(SODAMessage.EX_TO_BINARY_CONVERSION_ERROR);
    }
  }
  
  private static void writeParserToGenerator(Object parser, Object generator, boolean fromBinary) throws OracleException 
  {
    try 
    {
      Method writeParser = JSON_GEN_CLASS.getMethod("writeParser", Object.class);
      writeParser.invoke(generator, parser);
      ((Closeable)generator).close();
    } 
    catch (Exception e) 
    {
      if (fromBinary)
        throw SODAUtils.makeException(SODAMessage.EX_FROM_BINARY_CONVERSION_ERROR, e);
      else
        throw SODAUtils.makeException(SODAMessage.EX_TO_BINARY_CONVERSION_ERROR, e);
    }
  }

  private static void writeValueToGenerator(JsonValue value, Object generator) throws OracleException 
  {
    try 
    {
      JsonGenerator gen = wrapGenerator(generator);
      gen.write(value);
      gen.close();
    } 
    catch (JsonException e) 
    {
      throw SODAUtils.makeException(SODAMessage.EX_FROM_BINARY_CONVERSION_ERROR, e);
    }
  }
  
  private static void writeOracleValueToGenerator(Object value, Closeable generator) throws OracleException 
  {
    try 
    {
      Method method = JSON_GEN_CLASS.getMethod("write", JSON_VALUE_CLASS);
      method.invoke(generator, value);
      generator.close();
    } 
    catch (Exception e) 
    {
      throw SODAUtils.makeException(SODAMessage.EX_FROM_BINARY_CONVERSION_ERROR, e);
    }
  }

  private static JsonGenerator wrapGenerator(Object generator) throws OracleException 
  {
    try 
    {
      Method wrap = JSON_GEN_CLASS.getMethod("wrap", Class.class);
      return (JsonGenerator)wrap.invoke(generator, JsonGenerator.class);
    } 
    catch (Exception e) 
    {
      throw SODAUtils.makeException(SODAMessage.EX_FROM_BINARY_CONVERSION_ERROR, e);
    }
  }
  
  private void putDescriptorIntoCaches(CollectionDescriptor desc)
  {
    String collectionName = desc.getName();

    if (localDescriptorCache != null)
    {
      CollectionDescriptor existingDesc = localDescriptorCache.put(collectionName, desc);
      if (OracleLog.isLoggingEnabled())
      {
        if (existingDesc !=null)
          log.fine(collectionName+" descriptor was already present in local cache, replaced it with a new descriptor");
        else
          log.fine("Put "+collectionName+" descriptor into local cache");
      }
    }

    if (sharedDescriptorCache != null)
    {
      CollectionDescriptor existingDesc = sharedDescriptorCache.put(desc);
      if (OracleLog.isLoggingEnabled())
      {
        if (existingDesc !=null)
          log.fine(collectionName+" descriptor was already present in shared cache, replaced it with a new descriptor");
        else
          log.fine("Put "+collectionName+" descriptor into local cache");
      }
    }
  }

  private CollectionDescriptor getDescriptorFromCaches(String collectionName)
  {
    CollectionDescriptor result = null;

    if (localDescriptorCache != null)
    {
      result = localDescriptorCache.get(collectionName);
      if (result != null)
      {
        if (OracleLog.isLoggingEnabled())
          log.fine("Got "+collectionName+" descriptor from local cache");
        
        if (sharedDescriptorCache != null)
        {
          sharedDescriptorCache.putIfAbsent(result);
        }
      }
    }

    if (result == null && sharedDescriptorCache != null)
    {
      result = sharedDescriptorCache.get(collectionName);
      if (result != null)
      {
        if (OracleLog.isLoggingEnabled())
          log.fine("Got "+collectionName+" descriptor from shared cache");

        // Since the descriptor can get evicted from
        // from the shared cache, store it in the local cache.
        // The local cache doesn't ever evict descriptors.
        if (localDescriptorCache != null)
        {
          localDescriptorCache.put(collectionName, result);
        }
      }

    }

    if (result == null && ((localCollectionCache != null) ||
                           (sharedDescriptorCache != null)))
    {
      if (OracleLog.isLoggingEnabled())
        log.fine("Descriptor not found in caches");
    }

    return result;
  }

  private void removeCollectionFromCaches(String collectionName) {

    if (sharedDescriptorCache != null)
    {
      sharedDescriptorCache.remove(collectionName);
    }

    if (localDescriptorCache != null)
    {
      localDescriptorCache.remove(collectionName);
    }

    if (localCollectionCache != null)
    {
      localCollectionCache.remove(collectionName);
    }
  }

  void setMetricsCollector(MetricsCollector metrics)
  {
    this.metrics = metrics;
  }

  private ArrayList<CollectionDescriptor> loadCollections(Integer limit,
                                                          int offset)
    throws OracleException
  {
    if (!metadataTableExists) return(null);

    return(callListCollections(null, limit, offset));
  }

  private ArrayList<CollectionDescriptor> loadCollections(Integer limit,
                                                          String startName)
    throws OracleException
  {
    if (!metadataTableExists) return(null);

    return(callListCollections(startName, limit, 0));
  }

    /**
     * Create a collection with the specified name.
     * This will use default options for the creation.
     * If the collection already exists, it's returned.
     */
    private OracleCollection createCollection(String collectionName)
            throws OracleException
    {
      try
      {
        if (sqlSyntaxLevel == SODAUtils.SQLSyntaxLevel.SQL_SYNTAX_UNKNOWN)
          sqlSyntaxLevel = SODAUtils.getDatabaseVersion(conn);
      }
      catch (SQLException se)
      {
        if (OracleLog.isLoggingEnabled())
          log.severe(se.getMessage());

        throw new OracleException(se);
      }

      if (SODAUtils.sqlSyntaxBelow_19(sqlSyntaxLevel))
        return(createCollection(collectionName, (CollectionDescriptor) null));
      else
        return createCollection(collectionName, (String) null);
    }

    /**
     * Create a collection with the specified name and options.
     * @throws OracleException 
     */
    private OracleCollection createCollection(String collectionName,
                                              CollectionDescriptor options) 
      throws OracleException
    {
      return createCollection(collectionName, options, COLLECTION_CREATION_MODE);
    }

    /**
     * Create a collection with the specified name and options.
     *
     * This method is part of the new createCollection code path. It relies on
     * server side to create the metadata (as opposed to the original code path,
     * which relied on the client). This code path is in effect if running
     * against DB >= 19.The old code path is retain to support older databases (below 18),
     * that do not do proper collection defaulting on the server side.
     *
     * ### The old codepath also runs for 18 DB, although we could enable the new code
     * path for that release, because proper defaulting should be done on the database
     * side starting with 18.
     */
    private OracleCollection createCollection(String collectionName,
                                              String options)
          throws OracleException
    {
      return createCollection(collectionName, options, COLLECTION_CREATION_MODE);
    }
    
    /**
     * Not part of a public API.
     *
     * Used internally by REST. REST uses the "NEW" collectionCreateMode
     * which doesn't support mapped collections and raises an error if
     * the dbObject already exists.
     */
    public OracleCollection createCollection(String collectionName,
                                             CollectionDescriptor options,
                                             String collectionCreateMode)
      throws OracleException
    {
      OracleCollection result = openCollection(collectionName, options);
      if (result != null)
      {
        return(result);
      }

      // Create a default descriptor if necessary
      if (options == null)
      {
        options = CollectionDescriptor.createStandardBuilder().buildDescriptor(collectionName);
      }

      options = callCreatePLSQL(collectionName, options.getDescription(), collectionCreateMode);

      // Now load the metadata from the database and return it
      return(openCollection(collectionName, options));
    }

  /**
   * Not part of a public API.
   *
   * In addition to serving SODA Java, this method is used
   * internally by REST. REST uses the "NEW" collectionCreateMode
   * which doesn't support mapped collections and raises an error if
   * the dbObject already exists.
   *
   * This method is part of the new createCollection code path. It relies on
   * server side to create the metadata (as opposed to the original code path,
   * which relied on the client). This code path is in effect if running
   * against DB >= 19.The old code path is retain to support older databases (below 18),
   * that do not do proper collectin defaulting on the server side.
   *
   * ### The old codepath also runs for 18 DB, although we could enable the new code
   * path for that release, because proper defaultng should be done on the database
   * side starting with 18.
   */
  public OracleCollection createCollection(String collectionName,
                                           String options,
                                           String collectionCreateMode)
    throws OracleException
  {

    OracleCollectionImpl coll = null;

    if (options == null) {
      OracleCollection result = openCollectionFromCaches(collectionName, null);
      if (result != null)
        return (result);
    }

    CollectionDescriptor descOptions = callCreatePLSQL(collectionName,
                                                       options,
                                                       collectionCreateMode);

    if (descOptions != null) {
      putDescriptorIntoCaches(descOptions);

      coll = new TableCollectionImpl(this, collectionName, descOptions);

      if (localCollectionCache != null) {
        localCollectionCache.put(collectionName, coll);
      }

      if (coll != null) {
        coll.setAvoidTxnManagement(avoidTxnManagement);
      }
    }
    return coll;
  }

    /**
     * Open a collection with the specified name. If the collection
     * doesn't exist, returns null.
     */
    public OracleCollection openCollection(String collectionName)
            throws OracleException
    {
        return(openCollection(collectionName, null));
    }

    /**
     * Open a collection with the specified name and metadata.
     */
    private OracleCollection openCollection(String collectionName,
                                            CollectionDescriptor options)
      throws OracleException
    {
        if (collectionName == null)
        {
          throw SODAUtils.makeException(SODAMessage.EX_ARG_CANNOT_BE_NULL,
                                        "collectionName");
        }

        // See if this collection has already been opened/created
        OracleCollectionImpl coll = null;

        if (localCollectionCache != null)
        {
          coll = localCollectionCache.get(collectionName);
        }

        if (coll != null)
        {
          if (options != null && !coll.matches(options))
          {
            throw SODAUtils.makeException(SODAMessage.EX_MISMATCHED_DESCRIPTORS);
          }

          coll.setAvoidTxnManagement(avoidTxnManagement);
          return(coll);
        }

        // If not, see if the metadata is available
        CollectionDescriptor desc = getDescriptorFromCaches(collectionName);

        // If not, attempt to load it from the database
        if (desc == null)
        {
          desc = loadCollection(collectionName);
        }

        boolean createFlag = false;

        // If found, create a new collection object and cache it
        if (desc != null)
        {
          if (options != null && !desc.matches(options))
          {
            throw SODAUtils.makeException(SODAMessage.EX_MISMATCHED_DESCRIPTORS);
          }

          createFlag = true;
        }
        // Otherwise it's not found

        if (createFlag)
        {
          if (desc.dbObjectType == CollectionDescriptor.DBOBJECT_PACKAGE)
          {
/***
            coll = new PlsqlCollectionImpl(this, collectionName, desc);
***/
            throw new IllegalStateException();
          }
          else // TableCollectionImpl is used for views and tables
          {
            coll = new TableCollectionImpl(this, collectionName, desc);
          }

          if (localCollectionCache != null)
          {
            localCollectionCache.put(collectionName, coll);
          }
        }

        if (coll != null)
        {
          coll.setAvoidTxnManagement(avoidTxnManagement);
        }
        return(coll);
  }

  /**
   * Open a collection with the specified name and metadata, using caches.
   */
  private OracleCollection openCollectionFromCaches(String collectionName,
                                                    CollectionDescriptor options)
          throws OracleException
  {
    if (collectionName == null)
    {
      throw SODAUtils.makeException(SODAMessage.EX_ARG_CANNOT_BE_NULL,
              "collectionName");
    }

    // See if this collection has already been opened/created
    OracleCollectionImpl coll = null;

    if (localCollectionCache != null)
    {
      coll = localCollectionCache.get(collectionName);
    }

    if (coll != null)
    {
      if (options != null && !coll.matches(options))
      {
        throw SODAUtils.makeException(SODAMessage.EX_MISMATCHED_DESCRIPTORS);
      }

      coll.setAvoidTxnManagement(avoidTxnManagement);
      return(coll);
    }

    // If not, see if the metadata is available
    CollectionDescriptor desc = getDescriptorFromCaches(collectionName);

    boolean createFlag = false;

    // If found, create a new collection object and cache it
    if (desc != null)
    {
      if (options != null && !desc.matches(options))
      {
        throw SODAUtils.makeException(SODAMessage.EX_MISMATCHED_DESCRIPTORS);
      }

      createFlag = true;
    }
    // Otherwise it's not found

    if (createFlag)
    {
      if (desc.dbObjectType == CollectionDescriptor.DBOBJECT_PACKAGE)
      {
/***
 coll = new PlsqlCollectionImpl(this, collectionName, desc);
 ***/
        throw new IllegalStateException();
      }
      else // TableCollectionImpl is used for views and tables
      {
        coll = new TableCollectionImpl(this, collectionName, desc);
      }

      if (localCollectionCache != null)
      {
        localCollectionCache.put(collectionName, coll);
      }
    }

    if (coll != null)
    {
      coll.setAvoidTxnManagement(avoidTxnManagement);
    }
    return(coll);
  }

  private OracleCollection createCollection(String collectionName,
                                            OracleDocument collectionMetadata)
    throws OracleException
  {
    if (collectionMetadata == null)
    {
      return createCollection(collectionName);
    }

    try
    {
      if (sqlSyntaxLevel == SODAUtils.SQLSyntaxLevel.SQL_SYNTAX_UNKNOWN)
        sqlSyntaxLevel = SODAUtils.getDatabaseVersion(conn);
    }
    catch (SQLException se)
    {
      if (OracleLog.isLoggingEnabled())
        log.severe(se.getMessage());

      throw new OracleException(se);
    }

    if (SODAUtils.sqlSyntaxBelow_19(sqlSyntaxLevel)) {
      CollectionDescriptor descriptor =
              createCollectionDescriptor(collectionName, collectionMetadata);

      return createCollection(collectionName, descriptor);
    }
    else {
      String meta = collectionMetadata.getContentAsString();

      if (meta == null || meta.isEmpty())
      {
        throw SODAUtils.makeException(SODAMessage.EX_METADATA_DOC_HAS_NO_CONTENT);
      }

      if (collectionName == null)
      {
         throw SODAUtils.makeException(SODAMessage.EX_ARG_CANNOT_BE_NULL,
                                       "collectionName");
      }

      return createCollection(collectionName, meta);
    }
  }

  private CollectionDescriptor createCollectionDescriptor(String name,
                                                          OracleDocument metadata)
    throws OracleException
  {
    String meta = metadata.getContentAsString();

    if (meta == null || meta.isEmpty())
    {
      throw SODAUtils.makeException(SODAMessage.EX_METADATA_DOC_HAS_NO_CONTENT);
    }

    Builder builder = CollectionDescriptor.jsonToBuilder(jProvider, meta);

    String defaultSchema = null;

    try
    {
      defaultSchema = conn.getCurrentSchema();
    }
    catch (SQLException e)
    {
      // gulp
      if (OracleLog.isLoggingEnabled())
        log.severe(e.getMessage());
    }

    return builder.buildDescriptor(name, defaultSchema);
  }

  /**
   * Drop collection with the specified name.
   */
  void dropCollection(String collectionName) throws OracleException
  {
    if (collectionName == null)
    {
      throw SODAUtils.makeException(SODAMessage.EX_ARG_CANNOT_BE_NULL,
                                    "collectionName");
    }

    callDropPLSQL(collectionName);

    removeCollectionFromCaches(collectionName);
  }

  /**
   * Get a list of the names of all collections in the database.
   */
  private List<String> getCollectionNames()
    throws OracleException
  {
    return(getCollectionNames(null, 0));
  }

  /**
   * Get a list of the names of collections in the database with a
   * limit on the number returned.
   *
   * This method implies that the list is ordered.
   */
  private List<String> getCollectionNames(int limit)
    throws OracleException
  {
    return(getCollectionNames(limit, 0));
  }

  /**
   * Get a list of the names of collections in the database with a
   * limit on the number returned, starting at a specific offset in
   * the list.
   *
   * This method implies that the list is ordered.
   */
  private List<String> getCollectionNames(int limit, int skip)
    throws OracleException
  {
    if (limit < 1)
    {
      throw SODAUtils.makeException(SODAMessage.EX_ARG_MUST_BE_POSITIVE,
                                    "limit");
    }

    if (skip < 0)
    {
      throw SODAUtils.makeException(SODAMessage.EX_ARG_MUST_BE_NON_NEGATIVE,
                                    "skip");
    }

    return getCollectionNames(Integer.valueOf(limit), skip);
  }

  private List<String> getCollectionNames(Integer limit, int skip)
    throws OracleException
  {

    ArrayList<CollectionDescriptor> arr = loadCollections(limit, skip);
    int sz = arr.size();
    ArrayList<String> results = EMPTY_LIST;
    if (sz > 0)
    {
      results = new ArrayList<String>(sz);
      for (CollectionDescriptor desc : arr)
        results.add(desc.uriName);
    }
    return(results);
  }

  /**
   * Get a list of the names of collections in the database with a
   * limit on the number returned, starting at the first name greater
   * than startName. If a null or empty string is passed as startName,
   * it's equivalent to starting at the first collection in sequence.
   *
   * This method implies that the list is ordered.
   */
  private List<String> getCollectionNames(int limit, String startName)
    throws OracleException
  {
    if (limit < 1)
    {
      throw SODAUtils.makeException(SODAMessage.EX_ARG_MUST_BE_POSITIVE,
                                    "limit");
    }

    if (startName == null)
    {
      throw SODAUtils.makeException(SODAMessage.EX_ARG_CANNOT_BE_NULL,
                                    "startName");
    }

    ArrayList<CollectionDescriptor> arr = loadCollections(limit, startName);
    int sz = arr.size();
    ArrayList<String> results = EMPTY_LIST;
    if (sz > 0)
    {
      results = new ArrayList<String>(sz);
      for (CollectionDescriptor desc : arr)
        results.add(desc.uriName);
    }
    return(results);
  }

  /**
   * Create new document from InputStream methods
   */

  // Not part of a public API.
  public OracleDocument createDocumentFromStream(InputStream content)
  {
    // ### Currently swallows up IOException.
    //     Revisit if stream-based createDocument methods ever become public.
    return(new OracleDocumentImpl(null, null, null, content, null));
  }

  // Not part of a public API.
  public OracleDocument createDocumentFromStream(String key,
                                                 InputStream content)
  {
    // ### Currently swallows up IOException.
    //     Revisit if stream-based createDocument methods ever become public.
    return(new OracleDocumentImpl(key, null, null, content, null));
  }

  // Not part of a public API.
  public OracleDocument createDocumentFromStream(String key,
                                                 InputStream content,
                                                 String contentType)
  {
    return(new OracleDocumentImpl(key, null, null, content, contentType));
  }

  /**
   * Create new document from String methods
   */

  public OracleDocument createDocumentFromString(String content)
  {
    return(new OracleDocumentImpl(null, content));
  }

  public OracleDocument createDocumentFromString(String key,
                                                 String content)
  {
    return(new OracleDocumentImpl(key, content));
  }

  public OracleDocument createDocumentFromString(String key,
                                                 String content,
                                                 String contentType)
  {
    return(new OracleDocumentImpl(key, null, null, content, contentType));
  }

  /**
   * Create a new document from byte[] methods
   */

  public OracleDocument createDocumentFromByteArray(byte[] content)
  {
    return(new OracleDocumentImpl(null, content));
  }

  public OracleDocument createDocumentFromByteArray(String key,
                                                    byte[] content)
  {
    return(new OracleDocumentImpl(key, content));
  }

  public OracleDocument createDocumentFromByteArray(String key,
                                                    byte[] content,
                                                    String contentType)
  {
    return(new OracleDocumentImpl(key, null, null, content, contentType));
  }

  MetricsCollector getMetrics()
  {
    return(metrics);
  }

  public OracleConnection getConnection()
  {
    return(conn);
  }

  /**
   * Internal database timekeeper. This fetches a SYSTIMESTAMP from
   * the database and keeps a cached value for it. Repeated requests
   * will attempt to avoid SQL round-trips by comparing clock differences
   * locally and providing an estimate in lieu of the actual DB time.
   * When a maximum elapsed time is reached for the possibly stale time
   * estimate, a refresh SQL is done. To cope with possible clock skew,
   * the time will only advance, so if the SQL clock is behind the
   * current estimate, the current estimate is simply advanced one tick.
   */

  // Refresh database time after 100 milliseconds (in nanoseconds)
  private static final long TIME_REFRESH_INTERVAL = 100000000L;

  // Note: no time zone on the format, string is consumed/parsed internally
  private static String SELECT_DB_TIMESTAMP =
    "select to_char(sys_extract_utc(SYSTIMESTAMP)," +
                   "'YYYY-MM-DD\"T\"HH24:MI:SS.FF') from SYS.DUAL";

  // Current time in component-long format
  private long   currentTimestamp = 0L;

  // Most recent refresh (in local nanoseconds)
  private long    updateNanos = 0L;

  /**
   * Get the current timestamp from the database;
   * internally this may simply adjust the mirrored time using the local
   * clock to avoid a round-trip, until the elapsed local time exceeds
   * a threshold (which should still be sub-second).
   */
  long getDatabaseTime()
    throws SQLException
  {
    return(getDatabaseTime(false));
  }

  long getDatabaseTime(boolean forceFlag)
    throws SQLException
  {
    long currentNanos = System.nanoTime();
    long deltaNanos;

    if (updateNanos == 0L)
      deltaNanos = -1L;
    else
      deltaNanos = currentNanos - updateNanos;

    // If too much time has elapsed or the local clock rolled over
    if (forceFlag || (deltaNanos > TIME_REFRESH_INTERVAL) || (deltaNanos < 0L))
    {
      // Refresh with a real SQL operation
      refreshDatabaseTime(currentNanos);
    }
    // Otherwise adjust the time by the elapsed time on the local clock
    else
    {
      // Increment the timestamp by the elapsed time
      long elapsedmicros = deltaNanos / 1000L;
      if (elapsedmicros <= 0L)
        elapsedmicros = 1L;
      long newTimestamp = ComponentTime.increment(currentTimestamp,
                                                  elapsedmicros);
      // If that can't be done without a rollover, refresh from SQL
      if (newTimestamp < 0L)
        refreshDatabaseTime(currentNanos);
      else
        currentTimestamp = newTimestamp;
    }

    return(currentTimestamp);
  }

  /**
   * This sets the database time from a string (presumably from a SQL
   * statement). The clock will only be moved forward; if clock skew
   * is detected, the clock will advance by one tick.
   */
  private void setDatabaseTime(long currentNanos, String strtime)
  {
    if (strtime.length() > 26)
      strtime = strtime.substring(0, 26);

    long newTime = ComponentTime.stringToStamp(strtime);

    // If the time stamp from the database is lagging we must have clock skew
    if (newTime <= currentTimestamp)
    {
      // Force the current time ahead one tick
      newTime = ComponentTime.plus(currentTimestamp, +1L);
    }

    // Mark the local update time as precisely as possible
    updateNanos = currentNanos;

    // Update the timestamp
    currentTimestamp = newTime;
  }

  private void refreshDatabaseTime(long currentNanos)
    throws SQLException
  {
    PreparedStatement stmt = null;
    ResultSet         rows = null;

    String sqltext = SELECT_DB_TIMESTAMP;

    try
    {
      metrics.startTiming();

      stmt = conn.prepareStatement(sqltext);
      stmt.setFetchSize(1);
      rows = stmt.executeQuery();

      if (rows.next())
        setDatabaseTime(currentNanos, rows.getString(1));

      // These calls to close the objects can throw exceptions
      rows.close();
      rows = null;
      stmt.close();
      stmt = null;

      metrics.recordTimestampRead();
    }
    finally
    {
      // Exceptions aren't thrown inside a finally block
      // These close operations are just to clean up from an exception
      for (String message : SODAUtils.closeCursor(stmt, rows))
      {
        if (OracleLog.isLoggingEnabled())
          log.severe(message);
      }
    }
  }

  long getDatabaseScn()
    throws OracleException
  {
    CallableStatement stmt = null;
    String sqltext = SELECT_SCN;
    long   scn = -1L;

    try
    {
      metrics.startTiming();

      stmt = conn.prepareCall(sqltext);
      stmt.registerOutParameter(1, Types.NUMERIC);

      stmt.execute();

      scn = stmt.getLong(1);

      stmt.close();
      stmt = null;

      metrics.recordReads(1, 1);
    }
    catch (SQLException e)
    {
      throw SODAUtils.makeExceptionWithSQLText(e, sqltext);
    }
    finally
    {
      for (String message : SODAUtils.closeCursor(stmt, null))
      {
        if (OracleLog.isLoggingEnabled())
          log.severe(message);
      }
    }

    return(scn);
  }

  /**
   * This gets a timestamp in the database native time zone.
   * This appears to be the only valid way to express a flashback query.
   */
  public String getAsOfTimestamp()
    throws OracleException
  {
    String result = null;

    try
    {
      long tstamp = getDatabaseTime(true);
      result = ComponentTime.stampToString(tstamp);
    }
    catch (SQLException e)
    {
      throw SODAUtils.makeExceptionWithSQLText(e, SELECT_DB_TIMESTAMP);
    }

    return(result);
  }

  public long getAsOfScn()
    throws OracleException
  {
    return(getDatabaseScn());
  }

  /*
   * Internal GUID cache
   * This keeps a small block of GUIDs assigned by the database
   * in a memory cache. When the cache is exhausted, a new set of
   * GUIDs is fetched from the database in a single round trip.
   */

  private static final int GUID_BATCH_SIZE = 100;
  private final String[] guidCache = new String[GUID_BATCH_SIZE];
  private       int      guidCachePos = GUID_BATCH_SIZE;

  String nextGuid()
    throws OracleException
  {
    if (guidCachePos >= guidCache.length)
      fetchGuids();
    return(guidCache[guidCachePos++]);
  }

  private void fetchGuids()
    throws OracleException
  {
    CallableStatement stmt = null;
    String sqltext = SELECT_GUID_BATCH;

    int count = GUID_BATCH_SIZE;

    try
    {
      metrics.startTiming();

      stmt = conn.prepareCall(sqltext);
      stmt.setInt(1, count);
      stmt.registerOutParameter(2, Types.ARRAY, "XDB.DBMS_SODA_ADMIN.VCNTAB");

      stmt.execute();

      Array parray = stmt.getArray(2);
      String[] vcarr = (String[])parray.getArray();

      count = vcarr.length;
      if (count > 0)
      {
        guidCachePos -= count;
        for (int i = 0; i < count; ++i)
          guidCache[guidCachePos + i] = vcarr[i];
      }

      stmt.close();
      stmt = null;

      metrics.recordGUIDS();
    }
    catch (SQLException e)
    {
      if (OracleLog.isLoggingEnabled())
        log.severe(e.getMessage());

      throw SODAUtils.makeExceptionWithSQLText(e, sqltext);
    }
    finally
    {
      for (String message : SODAUtils.closeCursor(stmt, null))
      {
        if (OracleLog.isLoggingEnabled())
          log.severe(message);
      }
    }
  }

  /**
   * Fetch a single GUID from the database
   * This is used as a back-up for UUID generation.
   */
  private byte[] fetchGuid()
  {
    PreparedStatement stmt = null;
    ResultSet         rows = null;
    String            sqltext = SELECT_GUID;

    byte[] data = null;

    try
    {
      metrics.startTiming();

      stmt = conn.prepareStatement(sqltext);

      rows = stmt.executeQuery();
      if (rows.next())
      {
        data = rows.getBytes(1);
      }

      rows.close();
      rows = null;
      stmt.close();
      stmt = null;

      metrics.recordGUIDS();

    }
    catch (SQLException e)
    {
      if (OracleLog.isLoggingEnabled())
        log.severe(e.getMessage());
    }
    finally
    {
      for (String message : SODAUtils.closeCursor(stmt, rows))
      {
        if (OracleLog.isLoggingEnabled())
          log.severe(message);
      }
    }

    return(data);
  }

  /**
   * Generate a UUID key string
   */
  String generateKey() throws OracleException
  {
    byte[] data = null;

    // ### The following fails inside Oracle RDBMS internal
    // driver.
    try
    {
      // Get a UUID-based key (this should be most efficient)
      data = HashFuncs.getRandom();
    }
    catch (Exception e)
    {
      if (OracleLog.isLoggingEnabled())
        log.warning(e.toString());
    }
    // ### If the above method failed, select SYS_GUID
    if (data == null)
    {
      data = fetchGuid();
    }
    // ### If fetching SYS_GUID failed as well, give up.
    if (data == null)
    {
      throw SODAUtils.makeException(SODAMessage.EX_UNABLE_TO_CREATE_UUID);
    }

    return(ByteArray.rawToHex(data));
  }

  public OracleDatabaseAdmin admin()
  {
    if (admin == null)
    {
      admin = new OracleDatabaseAdministrationImpl();
    }
    return admin;
  }

  /**
   * Internal call to PL/SQL DDL drop.
   */
  private void callDropPLSQL(String collectionName) throws OracleException
  {
    OracleCallableStatement stmt = null;
    String sqltext = "begin\n"+
                     " DBMS_SODA_ADMIN.DROP_COLLECTION(P_URI_NAME => ?);\n"+
                     "end;";

    try
    {
      metrics.startTiming();

      stmt = (OracleCallableStatement)conn.prepareCall(sqltext);

      stmt.setNString(1, collectionName);

      stmt.execute();
      if (OracleLog.isLoggingEnabled())
        log.info("Dropped collection "+collectionName);

      stmt.close();
      stmt = null;

      metrics.recordDDL();
    }
    catch (SQLException e)
    {
      if (OracleLog.isLoggingEnabled())
        log.severe(e.toString());

      boolean commitNeeded = false;

      // Workaround for the cryptic "ORA-00054: resource busy and acquire with
      // NOWAIT specified or timeout expired" exception. This exception occurs
      // in 12.1.0.2  when a collection with uncommitted writes is attempted to
      // be dropped. We wrap ORA-00054 in an OracleException with a message 
      // telling the user to commit. In 12.2, DBMS_SODA_ADMIN has been modified 
      // to output a custom 40626 exception instead of the ORA-00054. For 
      // consistency, we wrap it in the same OracleException.
      try
      {
        if (sqlSyntaxLevel == SODAUtils.SQLSyntaxLevel.SQL_SYNTAX_UNKNOWN)
          sqlSyntaxLevel = SODAUtils.getDatabaseVersion(conn);
      }
      catch (SQLException se)
      {
        if (OracleLog.isLoggingEnabled())
          log.severe(se.getMessage());

        throw new OracleException(se);
      }

      if (!SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel))
      {
        if (e.getErrorCode() == 40626)
        {
          commitNeeded = true;
        }
      }
      else if (e.getErrorCode() == 54)
      {
          commitNeeded = true;
      }

      if (commitNeeded)
        throw SODAUtils.makeExceptionWithSQLText(SODAMessage.EX_COMMIT_MIGHT_BE_NEEDED,
                                                 e,
                                                 sqltext);

      // ### Error when collection doesn't exist,
      // don't throw an exception in this case
      // (this error was added in PLSQL API in 12.2.0.2,
      // for new SODA PLSQL and C implementations,
      // but to preserve the original SODA Java behavior we
      // don't throw it in Java).
      if (e.getErrorCode() != 40671)
        throw SODAUtils.makeExceptionWithSQLText(e, sqltext);
    }
    finally
    {
      for (String message : SODAUtils.closeCursor(stmt, null))
      {
        if (OracleLog.isLoggingEnabled())
          log.severe(message);
      }
    }
  }

  private CollectionDescriptor callCreatePLSQL(String collectionName,
                                               String options,
                                               String collectionCreateMode)
          throws OracleException
  {
    CallableStatement stmt = null;
    String sqltext = "begin\n"                                   +
            "  DBMS_SODA_ADMIN.CREATE_COLLECTION(\n"    +
            "                   P_URI_NAME    => ?,\n"  +
            "                   P_CREATE_MODE => ?,\n"  +
            "                   P_DESCRIPTOR  => ?,\n"  +
            "                   P_CREATE_TIME => ?);\n" +
            "end;";

    String jsonDescriptor = options;
    if (OracleLog.isLoggingEnabled())
      log.info("Create collection:\n" + jsonDescriptor);
    CollectionDescriptor newDescriptor = null;

    String sqlddl = null;

    try
    {
      metrics.startTiming();

      stmt = conn.prepareCall(sqltext);

      stmt.setNString(1, collectionName);
      stmt.setString(2, collectionCreateMode);
      stmt.setString(3, jsonDescriptor);

      stmt.registerOutParameter(3, Types.VARCHAR, DESC_LENGTH);
      // Creation time (unused)
      stmt.registerOutParameter(4, Types.VARCHAR, CREATION_TIMESTAMP_LENGTH);

      stmt.execute();
      if (OracleLog.isLoggingEnabled())
        log.info("Created collection "+collectionName);

      String createTime = stmt.getString(4);

      jsonDescriptor = stmt.getString(3);

      stmt.close();
      stmt = null;

      metrics.recordDDL();

      //### Do we need to re-build the descriptor on client?
      if (createTime != null)
      {
        Builder builder = CollectionDescriptor.jsonToBuilder(jProvider,
                jsonDescriptor);
        newDescriptor = builder.buildDescriptor(collectionName);
      }

      // See if we can get the DDL operation actually run by PL/SQL
      sqlddl = callGetDDL();
      if (sqlddl != null && OracleLog.isLoggingEnabled())
        log.info(sqlddl);
    }
    catch (SQLException e)
    {
      if (OracleLog.isLoggingEnabled())
        log.severe(e.toString());

      sqlddl = callGetDDL();
      if (sqlddl != null)
        sqltext = sqlddl;

      if (e.getErrorCode() == 40669)
        throw SODAUtils.makeExceptionWithSQLText(SODAMessage.EX_MISMATCHED_DESCRIPTORS, 
                                                 e, sqltext);
      else if (e.getErrorCode() == 40675)
        throw SODAUtils.makeExceptionWithSQLText(SODAMessage.EX_METADATA_DOC_INVALID_JSON, 
                                                   e, sqltext);
      else
        throw SODAUtils.makeExceptionWithSQLText(e, sqltext);
    }
    finally
    {
      for (String message : SODAUtils.closeCursor(stmt, null))
      {
        if (OracleLog.isLoggingEnabled())
          log.severe(message);
      }
    }

    return(newDescriptor);
  }

  private String callGetDDL()
  {
    String sqlddl = null;
    CallableStatement stmt = null;
    String sqltext = "begin\n  DBMS_SODA_ADMIN.GET_SQL_TEXT(?);\nend;";

    try
    {
      metrics.startTiming();

      stmt = conn.prepareCall(sqltext);

      // SQL text (returned)
      stmt.registerOutParameter(1, Types.VARCHAR, DDL_SQL_LENGTH);

      stmt.execute();

      sqlddl = stmt.getString(1);

      stmt.close();
      stmt = null;

      metrics.recordCall();
    }
    catch (SQLException e)
    { 
      if (OracleLog.isLoggingEnabled())
        log.severe(e.toString());
      // ### For now ignore the exception
      // ### It's possible we're calling an older version of the
      // ### PL/SQL interface that lacks the new API.
    }
    finally
    {
      for (String message : SODAUtils.closeCursor(stmt, null))
      {
        if (OracleLog.isLoggingEnabled())
          log.severe(message);
      }
    }

    return(sqlddl);
  }

  /**
   * Load the metadata for a collection from the table.
   */
  private CollectionDescriptor loadCollection(String collectionName)
    throws OracleException
  {
    if (!metadataTableExists)
    {
      return null;
    }

    CallableStatement stmt = null;

    String sqltext = "begin\n"+
                     "  DBMS_SODA_ADMIN.DESCRIBE_COLLECTION(\n"  +
                     "                   P_URI_NAME   => ?,\n"   +
                     "                   P_DESCRIPTOR => ?);\n"  +
                     "end;";

    CollectionDescriptor desc = null;

    try
    {
      metrics.startTiming();

      stmt = conn.prepareCall(sqltext);

      stmt.setNString(1, collectionName);

      stmt.registerOutParameter(2, Types.VARCHAR, DESC_LENGTH);

      stmt.execute();

      String jsonDescriptor = stmt.getString(2);

      if (jsonDescriptor != null)
      {
        Builder builder = CollectionDescriptor.jsonToBuilder(jProvider,
                                                             jsonDescriptor);
        desc = builder.buildDescriptor(collectionName); 
        putDescriptorIntoCaches(desc);
      }
      else
      {
        removeCollectionFromCaches(collectionName);
      }

      stmt.close();
      stmt = null;

      metrics.recordCall();
    }
    catch (SQLException e)
    {
      if (OracleLog.isLoggingEnabled())
        log.severe(e.toString());
      throw SODAUtils.makeExceptionWithSQLText(e, sqltext);
    }
    finally
    {
      for (String message : SODAUtils.closeCursor(stmt, null))
      {
        if (OracleLog.isLoggingEnabled())
          log.severe(message);
      }
    }

    return(desc);
  }

  /**
   * Internal call to PL/SQL metadata bulk load.
   */
  private ArrayList<CollectionDescriptor> callListCollections(String startName,
                                                              Integer limit,
                                                              int offset)
    throws OracleException
  {
    CallableStatement stmt = null;
    ResultSet         rows = null;
    String sqltext = "begin\n"                                  +
                     "  DBMS_SODA_ADMIN.LIST_COLLECTIONS(\n"   +
                     "                   P_START_NAME => ?,\n"  +
                     "                   P_RESULTS    => ?);\n" +
                     "end;";

    boolean resultFull = false;

    ArrayList<CollectionDescriptor> results =
      new ArrayList<CollectionDescriptor>();

    try
    {
      int rowCount = 0;

      metrics.startTiming();

      stmt = conn.prepareCall(sqltext);

      if (startName == null)
        stmt.setNull(1, Types.VARCHAR);
      else
        stmt.setNString(1, startName);

      // ### OracleTypes.CURSOR == -10
      // ### Types.REF_CURSOR   == 2012
      // ### The Oracle driver has bug 28387681, won't take REF_CURSOR?
      stmt.registerOutParameter(2, OracleTypes.CURSOR);

      stmt.execute();

      rows = ((OracleCallableStatement)stmt).getCursor(2);

      if (OracleLog.isLoggingEnabled())
        log.fine("Loaded collections");

      while (rows.next())
      {
        // Get the name of the next collection
        String uriName        = rows.getNString(1);
        String jsonDescriptor = rows.getString(2);

        //### Do we need to re-build the descriptor on client?
        Builder builder = CollectionDescriptor.jsonToBuilder(jProvider,
                                                             jsonDescriptor);
        CollectionDescriptor desc = builder.buildDescriptor(uriName);
        putDescriptorIntoCaches(desc);

        if ((rowCount >= offset) && (!resultFull))
          results.add(desc);

        ++rowCount;

        // Stop filling results after limit rows are retrieved.
        // If null is passed in for limit, that means unlimited
        // number of results.
        if (limit != null && limit > 0)
        {
          if (rowCount >= (offset + limit))
          {
            if (rowCount > SODAConstants.BATCH_FETCH_SIZE)
              break;
            resultFull = true;
          }
        }
      }

      rows.close();
      rows = null;

      stmt.close();
      stmt = null;

      metrics.recordCall();
    }
    catch (SQLException e)
    {
      if (OracleLog.isLoggingEnabled())
        log.severe(e.toString());
      throw SODAUtils.makeExceptionWithSQLText(e, sqltext);
    }
    finally
    {
      for (String message : SODAUtils.closeCursor(stmt, rows))
      {
        if (OracleLog.isLoggingEnabled())
          log.severe(message);
      }
    }

    return(results);
  }

  /**
   * Add the standard formatting for convering a bind string to a DB
   * timestamp (this must never have a time zone).
   */
  static void addToTimestamp(String operation, StringBuilder sb)
  {
    if (operation != null) sb.append(operation);
    // ### Is this OK? What about to_timestamp_tz() for that trailing zone?
    // ### But then it would have to be surrounded with sys_extract_utc()?
    sb.append("to_timestamp(?,'SYYYY-MM-DD\"T\"HH24:MI:SS.FFTZH:TZM')");
  }
  
  /**
   * Returns TRUE if the database is Unicode-based
   */
  boolean isUnicode()
  {
    return dbIsUnicode;
  }

  int getMaxRawLength()
  {
    return rawMaxLength;
  }

  int getMaxVarcharLength()
  {
    return varcharMaxLength;
  }

  int getMaxNvarcharLength()
  {
    return nvarcharMaxLength;
  }

  /**
   * Interrogate database for maximum non-LOB column length limits
   */
  // ### This code is no longer used but may be restored in future
  private void getMaxLengths()
  {
    if (!metadataTableExists) return;

    CallableStatement stmt = null;
    //
    // ### Some of the ugly logic here could be avoided if we offer
    // ### an alternative version of GET_PARAMETERS that takes the
    // ### VCNTAB style arguments.
    //
    String sqltext = "declare\n"                                +
                     "  KARR XDB.DBMS_SODA_ADMIN.VCNTAB;\n"     +
                     "  VARR XDB.DBMS_SODA_ADMIN.VCNTAB;\n"     +
                     "  KEYS XDB.DBMS_SODA_ADMIN.VCTAB;\n"      +
                     "  VALS XDB.DBMS_SODA_ADMIN.VCTAB;\n"      +
                     "begin\n"                                  +
                     "  DBMS_SODA_ADMIN.GET_PARAMETERS(\n"      +
                     "                  P_KEY   => KEYS,\n"     +
                     "                  P_VALUE => VALS);\n"    +
                     "  KARR := XDB.DBMS_SODA_ADMIN.VCNTAB();\n"+
                     "  VARR := XDB.DBMS_SODA_ADMIN.VCNTAB();\n"+
                     "  KARR.extend(KEYS.count);\n"             +
                     "  VARR.extend(VALS.count);\n"             +
                     "  for I in 1..KEYS.count loop\n"          +
                     "    KARR(I) := KEYS(I);\n"                +
                     "  end loop;\n"                            +
                     "  for I in 1..VALS.count loop\n"          +
                     "    VARR(I) := VALS(I);\n"                +
                     "  end loop;\n"                            +
                     "  ? := KARR;\n"                           +
                     "  ? := VARR;\n"                           +
                     "end;";

    try
    {
      metrics.startTiming();

      stmt = conn.prepareCall(sqltext);

      // Register two table-of-strings parameters as outputs
      stmt.registerOutParameter(1, Types.ARRAY, "XDB.DBMS_SODA_ADMIN.VCNTAB");
      stmt.registerOutParameter(2, Types.ARRAY, "XDB.DBMS_SODA_ADMIN.VCNTAB");

      stmt.execute();

      String[] pkeys = (String[])stmt.getArray(1).getArray();
      String[] pvals = (String[])stmt.getArray(2).getArray();

      int nparams = pkeys.length;
      if (nparams > pvals.length) nparams = pvals.length;
      for (int i = 0; i < nparams; ++i)
      {
        String pkey = pkeys[i];
        String pval = pvals[i];

        try
        {
          if (pkey.equalsIgnoreCase("VARCHAR2_MAX"))
            varcharMaxLength = Integer.parseInt(pval);
          else if (pkey.equalsIgnoreCase("NVARCHAR2_MAX"))
            nvarcharMaxLength = Integer.parseInt(pval);
          else if (pkey.equalsIgnoreCase("RAW_MAX"))
            rawMaxLength = Integer.parseInt(pval);
          else if (pkey.equalsIgnoreCase("DB_IS_UNICODE"))
          {
            if (!dbIsUnicode)
              dbIsUnicode = pval.equalsIgnoreCase("true");
          }
          // ### Else unrecognized parameter, ignored for now
        }
        catch (NumberFormatException e)
        {
          // ### Just ignore non-numeric junk responses
          if (OracleLog.isLoggingEnabled())
            log.warning(e.toString());
        }
      }

      stmt.close();
      stmt = null;

      metrics.recordCall();
    }
    catch (SQLException e)
    {
      // Can't throw SQL exceptions to the constructor
      // So max lengths remain unknown
      if (OracleLog.isLoggingEnabled())
        log.severe(e.toString());

      // TODO: exception ignored?
    }
    finally
    {
      for (String message : SODAUtils.closeCursor(stmt, null))
      {
        if (OracleLog.isLoggingEnabled())
          log.severe(message);
      }
    }
  }

  /**
   * Drop collections
   */
  public List<OracleDropResult> dropCollections(boolean force)
    throws OracleException
  {
    if (!metadataTableExists)
      return new ArrayList<OracleDropResult>();

    List<OracleDropResult> res = new ArrayList<OracleDropResult>();

    CallableStatement stmt = null;
    String sqltext = "begin\n"                  +
      "  DBMS_SODA_ADMIN.DROP_COLLECTIONS(\n"   +
      "                  P_COLLECTIONS => ?,\n" +
      "                  P_ERRORS => ?,\n"      + 
      "                  P_FORCE => ?);\n"      +
      "end;";

    try
    {

      metrics.startTiming();

      stmt = conn.prepareCall(sqltext);

      stmt.registerOutParameter(1, Types.ARRAY, "XDB.DBMS_SODA_ADMIN.NVCNTAB");
      stmt.registerOutParameter(2, Types.ARRAY, "XDB.DBMS_SODA_ADMIN.VCNTAB");

      if (force)
        stmt.setString(3, "true");
      else
        stmt.setString(3, "false");

      stmt.execute();

      Array cnames = stmt.getArray(1);
      Array errors = stmt.getArray(2);

      final String[] cnameStrs  = (String[])cnames.getArray();
      final String[] errorStrs  = (String[])errors.getArray();

      stmt.close();
      stmt = null;

      metrics.recordDDL();

      for (int i=0; i < cnameStrs.length; i++) {
        // Copy to final index so that we can
        // pass it into the anonymous OracleDropResult()
        // class. Anon class can only access final
        // local vars.
        final int index = i;

        res.add(new OracleDropResult() {

          String collName = cnameStrs[index];
          String error = errorStrs[index];

          public String getName()
          {
            return collName;
          }
 
          public String getError()
          {
            return error;
          }
        });
      }

      if (sharedDescriptorCache != null)
      {
        String name;
        try {
            name = OracleDatabaseImpl.getSchemaName(conn);
        }
        catch (SQLException e) {
            throw new OracleException(e);
        }

        if (name == null)
            throw SODAUtils.makeException(SODAMessage.EX_SCHEMA_NAME_IS_NULL);

        sharedDescriptorCache.remove(name);
      }

      if (localDescriptorCache != null)
      {
        localDescriptorCache.clear();
      }
 
      if (localCollectionCache != null)
      {
        localCollectionCache.clear();
      }

      return res;
    }
    catch (SQLException e)
    {
      if (OracleLog.isLoggingEnabled())
        log.severe(e.toString());

      throw SODAUtils.makeExceptionWithSQLText(e, sqltext);
    }
    finally
    {
      for (String message : SODAUtils.closeCursor(stmt, null))
      {
        if (OracleLog.isLoggingEnabled())
          log.severe(message);
      }
    }
  }

  /**
   * Force the NLS DATE and TIMESTAMP formats to be ISO-friendly
   * Force numbers to use . for the radix
   */
  private void setDateTimeNumFormats()
  {
    PreparedStatement stmt = null;

    String sqltext;

    try
    {
      sqltext = "alter session set NLS_DATE_FORMAT='YYYY-MM-DD\"T\"HH24:MI:SS'";
      stmt = conn.prepareStatement(sqltext);
      stmt.execute();
      stmt.close();

      sqltext = "alter session set NLS_TIMESTAMP_FORMAT='YYYY-MM-DD\"T\"HH24:MI:SS.FF'";
      stmt = conn.prepareStatement(sqltext);
      stmt.execute();
      stmt.close();

      sqltext = "alter session set NLS_NUMERIC_CHARACTERS='.,'";
      stmt = conn.prepareStatement(sqltext);
      stmt.execute();
      stmt.close();

      stmt = null;
    }
    catch (SQLException e)
    {
      // Any exception means it doesn't work (and the old $ should be used)
      // or it failed during the close() operations (but the @ sign works).
      if (OracleLog.isLoggingEnabled())
        log.info(e.toString());
    }
    finally
    {
      // Exceptions aren't thrown inside a finally block
      // These close operations are just to clean up from an exception
      for (String message : SODAUtils.closeCursor(stmt, null))
      {
        if (OracleLog.isLoggingEnabled())
          log.severe(message);
      }
    }
  }

  /**
   * Override the internal one if a global one is available
   */
  public void setJsonFactoryProvider(JsonFactoryProvider jProvider)
  {
    this.jProvider = jProvider;
  }

  public JsonFactoryProvider getJsonFactoryProvider()
  {
    return jProvider;
  }

  /** ### TODO remove this reflection */
  private Object jsonFactory = null;

  
  /*
  ** Reusable factory objects
  */
  private JsonFactoryProvider  jProvider = new JsonFactoryProvider();
  
  /**
   * Not part of the public API
   */
  public Object getJsonFactory() throws OracleException
  {
    if (JSON_FACT_CLASS == null) {
      throw SODAUtils.makeException(SODAMessage.EX_TO_BINARY_CONVERSION_ERROR);
    }
    
    Object result = jsonFactory;
    if (result == null)
    {
      try 
      {
        jsonFactory = result = JSON_FACT_CLASS.newInstance();
      }
      catch (Exception e)
      {
        throw new IllegalStateException(e);
      }
    }
    return result;
  }
  
  public static boolean isOracleJsonAvailable() {
    return JSON_FACT_CLASS != null;
  }
  

  /**
   * OracleDatabaseAdministrationImpl
   *
   * This is RDBMS-specific implementation of OracleDatabaseAdmin
   */
  private class OracleDatabaseAdministrationImpl implements OracleDatabaseAdmin
  {
    /**
     * Create a collection with the specified name.
     * This will use default options for the creation.
     * If the collection already exists, it's returned.
     */
    public OracleCollection createCollection(String collectionName)
      throws OracleException
    {
      return OracleDatabaseImpl.this.createCollection(collectionName);
    }

    public OracleCollection createCollection(String collectionName,
                                             OracleDocument collectionMetadata)
            throws OracleException
    {
      return OracleDatabaseImpl.this.createCollection(collectionName,
                                                      collectionMetadata);
    }

    /**
     * Get a list of the names of all collections in the database.
     */
    public List<String> getCollectionNames()
      throws OracleException
    {
      return OracleDatabaseImpl.this.getCollectionNames();
    }

    /**
     * Get a list of the names of collections in the database with a
     * limit on the number returned.
     *
     * This method implies that the list is ordered.
     */
    public List<String> getCollectionNames(int limit)
      throws OracleException
    {
      return OracleDatabaseImpl.this.getCollectionNames(limit);
    }

    /**
     * Get a list of the names of collections in the database with a
     * limit on the number returned, starting at a specific offset in
     * the list.
     *
     * This method implies that the list is ordered.
     */
    public List<String> getCollectionNames(int limit, int skip)
      throws OracleException
    {
      return OracleDatabaseImpl.this.getCollectionNames(limit, skip);
    }

    /**
     * Get a list of the names of collections in the database with a
     * limit on the number returned, starting at the first name greater
     * than startName. If a null or empty string is passed as startName,
     * it's equivalent to starting at the first collection in sequence.
     *
     * This method implies that the list is ordered.
     */
    public List<String> getCollectionNames(int limit, String startName)
      throws OracleException
    {
      return OracleDatabaseImpl.this.getCollectionNames(limit, startName);
    }

    public OracleConnection getConnection()
    {
      return OracleDatabaseImpl.this.getConnection();
    }

    public List<OracleDropResult> dropCollections(boolean force)
      throws OracleException
    {
      return OracleDatabaseImpl.this.dropCollections(force);
    }
  }

  // Not part of the public interface
  public void cancelOperation()
  {
    OracleDatabaseImpl.cancelOperation(conn);
  }

  private static final String SELECT_SCHEMA_NAME =
    "select SYS_CONTEXT('USERENV','CURRENT_SCHEMA') from SYS.DUAL";
 
  /**
   * Get the current database schema name from the SQL environment.
   * This is the same thing that the getSchema() method does under
   * the covers. However, getSchema() has the ability to piggyback
   * this information on operations that switch the schema, avoiding
   * round-trips in cases where the schema is not being switched.
   * So if possible, the getSchema() method is used when available.
   *
   * Note that this is not the same as the DatabaseMetadata
   * getUserName() method, which returns the current value of USER and
   * is similar to SYS_CONTEXT('USERENV','CURRENT_USER').
   */
  // Not part of the public interface
  public static String getSchemaName(Connection conn)
    throws SQLException
  {
    PreparedStatement stmt = null;
    ResultSet         rows = null;
    SQLException      ex   = null;

    String schemaName = null;

    try
    {
      // If this connection has a getSchemaName method, attempt it
      if (GET_SCHEMA_METHOD != null)
        schemaName = (String)GET_SCHEMA_METHOD.invoke(conn);
    }
    catch (Exception e)
    {
      if (OracleLog.isLoggingEnabled())
        log.warning(e.getMessage());
    }

    if (schemaName != null)
      return schemaName;

    // Select it from the environment
    try
    {
      stmt = conn.prepareStatement(SELECT_SCHEMA_NAME);
      rows = stmt.executeQuery();
      stmt.setFetchSize(1);
      if (rows.next())
        schemaName = rows.getString(1);
    }
    catch (SQLException e)
    {
      ex = e;
      schemaName = null;
    }
    finally
    {
      for (String message : SODAUtils.closeCursor(stmt, null))
      {
        if (OracleLog.isLoggingEnabled())
          log.severe(message);
      }
    }

    //
    // Attempt to get a schema name using a standard technique.
    // This will only run if the above select statement fails.
    // In practice, this means only for non-Oracle database servers.
    //
    if (schemaName == null)
    {
      DatabaseMetaData dbmd = conn.getMetaData();
      if (dbmd != null)
        schemaName = dbmd.getUserName();
    }

    // Throw the exception if we still can't get a schema name
    if (schemaName == null)
      if (ex != null)
        throw ex;

    return schemaName;
  }

  private static final Method CANCEL_METHOD;
  private static final Method GET_SCHEMA_METHOD;

  // Not part of the public interface
  public static void cancelOperation(Connection conn)
  {
    try
    {
      // If this connection has a cancel method, attempt it
      if (CANCEL_METHOD != null)
        CANCEL_METHOD.invoke(conn);
    }
    catch (Exception e)
    {
      if (OracleLog.isLoggingEnabled())
        log.warning(e.getMessage());
    }
  }

  /***************************************************************************
  ** Methods that are available on Oracle statements, but are not generic
  */
  
  private static final Method PREFETCH_METHOD;

  private static final Integer LOB_PREFETCH_SIZE =
                   new Integer(SODAConstants.LOB_PREFETCH_SIZE);

  /**
   * Set the size limit to prefetch LOB data, useful for array bindings.
   * If unsupported by the driver, has no effect.
   */
  void setLobPrefetchSize(Statement stmt)
  {
    try
    {
      // If possible, set the LOB prefetch size
      if (PREFETCH_METHOD != null)
        PREFETCH_METHOD.invoke(stmt, LOB_PREFETCH_SIZE);
    }
    catch (Exception e)
    {
      if (OracleLog.isLoggingEnabled())
        log.fine(e.getMessage());
    }
  }

  //
  // Oracle driver methods for enhanced performance with LOBs.
  // These are faster than the standard setBytes/setClob methods,
  // so use them if the driver offers them.
  //
  private static final Method SET_BLOB_BYTES;
  private static final Method SET_CLOB_STRING;

  /**
   * Set byte array value for BLOB binding.
   * Returns true if successful, false if the method does not exit.
   */
  boolean setBytesForBlob(Statement stmt, int pos, byte[] data)
    throws SQLException
  {
    try
    {
      if (SET_BLOB_BYTES != null)
      {
        SET_BLOB_BYTES.invoke(stmt, new Integer(pos), data);
        return true;
      }
    }
    catch (Exception e)
    {
      if (e instanceof SQLException)
        throw (SQLException)e;
      if (OracleLog.isLoggingEnabled())
        log.fine(e.getMessage());
    }

    return false;
  }

  /**
   * Set String value for CLOB binding.
   * Return true if successful, false if the method is not available.
   */
  boolean setStringForClob(Statement stmt, int pos, String str)
    throws SQLException
  {
    try
    {
      if (SET_CLOB_STRING != null)
      {
        SET_CLOB_STRING.invoke(stmt, new Integer(pos), str);
        return true;
      }
    }
    catch (Exception e)
    {
      if (e instanceof SQLException)
        throw (SQLException)e;
      if (OracleLog.isLoggingEnabled())
        log.fine(e.getMessage());
    }

    return false;
  }

  //
  // ### These are used because of bug 29645450. Oracle doesn't give
  // ### us a good way to get DML return results except by using some
  // ### proprietary methods. The standard getResultSet() method of a
  // ### PreparedStatement won't return the RETURNING results. A work-around
  // ### is to use a CallableStatement and registerOutParameter(), but
  // ### this required wrapping the statement in an anonymous block to
  // ### turn it into a call. It works, and only takes one round trip,
  // ### but it changes the nature of the call and adds some PL/SQL overhead.
  //
  private static final Method REGISTER_RETURN_PARAMETER;
  private static final Method GET_RETURN_RESULT_SET;

  private static final Integer RETURN_VARCHAR = new Integer(Types.VARCHAR);
  
  private static final Integer RETURN_TIMESTAMP = new Integer(Types.TIMESTAMP);

  /**
   * Register a return parameter of type VARCHAR
   * Return true if successful, false if the method is not available.
   */
  boolean registerReturnString(Statement stmt, int pos)
    throws SQLException
  {
    try
    {
      if (REGISTER_RETURN_PARAMETER != null)
      {
        REGISTER_RETURN_PARAMETER.invoke(stmt, new Integer(pos), RETURN_VARCHAR);
        return true;
      }
    }
    catch (Exception e)
    {
      if (e instanceof SQLException)
        throw (SQLException)e;
      if (OracleLog.isLoggingEnabled())
        log.fine(e.getMessage());
    }

    return false;
  }

  boolean registerReturnTimestamp(Statement stmt, int pos)
      throws SQLException
    {
      try
      {
        if (REGISTER_RETURN_PARAMETER != null)
        {
          REGISTER_RETURN_PARAMETER.invoke(stmt, new Integer(pos), RETURN_TIMESTAMP);
          return true;
        }
      }
      catch (Exception e)
      {
        if (e instanceof SQLException)
          throw (SQLException)e;
        if (OracleLog.isLoggingEnabled())
          log.fine(e.getMessage());
      }

      return false;
    }
  
  /**
   * Get the results of a DML returning clause in a SQL insert/update.
   */
  ResultSet getReturnResultSet(Statement stmt)
    throws SQLException
  {
    try
    {
      if (GET_RETURN_RESULT_SET != null)
      {
        return (ResultSet)GET_RETURN_RESULT_SET.invoke(stmt);
      }
    }
    catch (Exception e)
    {
      if (e instanceof SQLException)
        throw (SQLException)e;
      if (OracleLog.isLoggingEnabled())
        log.fine(e.getMessage());
    }

    return null;
  }

  public static final Class<? extends Connection> ORACLE_CONNECTION;

  public boolean hasOracleConnection()
  {
    return OracleDatabaseImpl.isOracleConnection(conn);
  }

  public static boolean isOracleConnection(Connection conn)
  {
    if (ORACLE_CONNECTION == null)
      return false;
    return (ORACLE_CONNECTION.isInstance(conn));
  }

  void setBytesForJson(PreparedStatement stmt, int pos, byte[] data)
    throws SQLException
  {
    Object d = null;
/***
    // ### Should we use an OracleJsonDatum instance for the binding?
    if (JSON_DATUM_CLASS != null)
    {
      try
      {
        d = JSON_DATUM_CLASS.newInstance();
      }
      catch (Exception e)
      {
        if (OracleLog.isLoggingEnabled())
          log.fine(e.getMessage());
      }
    }
 ***/
    if (d == null) d = data;
    // ### OracleTypes.JSON == 2016, prop up older JDBC drivers
    stmt.setObject(pos, d, 2016);
  }

  static byte[] getBytesForJson(ResultSet rows, int pos)
    throws OracleException
  {
    try
    {
      if (JSON_SHARE_BYTES != null)
      {
        Object d = rows.getObject(pos, JSON_DATUM_CLASS);
        if (d != null)
          return (byte[])JSON_SHARE_BYTES.invoke(d);
      }
    }
    catch (Exception e)
    {
      if (e instanceof SQLException)
        throw new OracleException((SQLException)e);
      if (OracleLog.isLoggingEnabled())
        log.fine(e.getMessage());
      throw new OracleException(e);
    }

    return null;
  }
  
  @Override
  public OracleDocument createDocumentFrom(Object content) throws OracleException 
  {
    return createDocumentFrom(null, content);
  }

  @Override
  public OracleDocument createDocumentFrom(String key, Object content) throws OracleException 
  {
    if (!isOracleJsonAvailable())
      throw SODAUtils.makeException(SODAMessage.EX_JSON_FACTORY_MISSING_IN_JDBC);

    byte[] oson = convertToOson(content);
    OracleDocumentImpl doc = new OracleDocumentImpl(key, null, null, oson);
    doc.setBinary();
    doc.setJsonFactory(getJsonFactory());
    return doc;
  }

  private byte[] convertToOson(Object obj) throws OracleException 
  {
    if (obj instanceof InputStream) 
      return convertByteArrayToOson(readStream((InputStream)obj));
    else if (obj instanceof byte[])
      return convertByteArrayToOson((byte[])obj);
    else if (obj instanceof CharSequence)
      return textToBinary(new StringReader(((CharSequence)obj).toString()));
    else if (obj instanceof Reader)
      return textToBinary((Reader)obj);
    else if (obj instanceof JsonValue)
      return jsonValueToBinary((JsonValue)obj);
    else if (obj instanceof JsonParser)
      return parserToBinary(obj);
    else if (JSON_VALUE_CLASS.isInstance(obj))
      return oracleJsonValueToBinary(obj);
    else if (JSON_PARSE_CLASS.isInstance(obj)) 
      return parserToBinary(obj);
    else
      throw SODAUtils.makeException(SODAMessage.EX_INVALID_TYPE_MAPPING, 
          obj == null ? null : obj.getClass());
  }

  /** 
   * Input byte array may be either OSON or JSON text encoding 
   */
  private byte[] convertByteArrayToOson(byte[] bytes) throws OracleException 
  {
    return isOsonArray(bytes) ? bytes : textToBinary(bytes);
  }

  /**
   * Returns true if the input array contains oson bytes, false otherwise.
   */
  private static boolean isOsonArray(byte[] src) 
  {
    return src.length > MAGIC_BYTES.length 
        && src[0] == MAGIC_BYTES[0]
        && src[1] == MAGIC_BYTES[1]
        && src[2] == MAGIC_BYTES[2];
  } 
  
  private byte[] readStream(InputStream is) throws OracleException 
  {
    try 
    {
      final byte[] buffer = new byte[8 * 1024];
      int n;
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      while ((n = is.read(buffer)) != -1) 
      {
        baos.write(buffer, 0, n);
      }
      // don't close the InputStream
      return baos.toByteArray();
    } 
    catch (IOException ioEx) 
    {
      throw new OracleException(ioEx);
    } 
  }
  
  public static String localDateTimeToString(LocalDateTime ldt) 
  {
    StringBuilder result = new StringBuilder(27);
    appendInt(result, ldt.getYear(), 4);
    result.append("-");
    appendInt(result, ldt.getMonthValue(), 2);
    result.append("-");
    appendInt(result, ldt.getDayOfMonth(), 2);
    result.append("T");
    appendInt(result, ldt.getHour(), 2);
    result.append(":");
    appendInt(result, ldt.getMinute(), 2);
    result.append(":");
    appendInt(result, ldt.getSecond(), 2);
    result.append(".");
    appendInt(result, ldt.getNano(), 9);
    result.append("Z");
    return result.toString();
  }
  
  /**
   * @param n The number to append
   * @param i The width (zero padded)
   */
  private static void appendInt(StringBuilder result, int n, int i) {
    int tmp = n;
    while (tmp > 0) {
      tmp /= 10;
      i--;
    }
    while (i > 0) {
      result.append('0');
      i--;
    }
    if (n != 0) {
      result.append(n);
    }
  }
  
  public static String getTimestamp(ResultSet rs, int col) throws SQLException {
    LocalDateTime ldt = rs.getObject(col, LocalDateTime.class);
    return OracleDatabaseImpl.localDateTimeToString(ldt);
  }
  
  public static String getTimestamp(CallableStatement rs, int col) throws SQLException {
    LocalDateTime ldt = rs.getObject(col, LocalDateTime.class);
    return OracleDatabaseImpl.localDateTimeToString(ldt);
  }
  
  //
  // Get handles to Oracle-specific extensions
  //
  static
  {
    Method m;

    Class<? extends Connection> connectionClass = null;

    try
    {
      connectionClass = Class.forName("oracle.jdbc.OracleConnection")
                             .asSubclass(Connection.class);
    }
    catch (ClassNotFoundException e)
    {
      connectionClass = null;
    }

    ORACLE_CONNECTION = connectionClass;

    // Get the cancel method if available
    try
    {
      if (connectionClass == null)
        m = null;
      else
        m = connectionClass.getMethod("cancel");
    }
    catch (NoSuchMethodException e)
    {
      m = null;
    }
    CANCEL_METHOD = m;

    // Get the getCurrentSchema method if available
    try
    {
      if (connectionClass == null)
        m = null;
      else
        m = connectionClass.getMethod("getCurrentSchema");
    }
    catch (NoSuchMethodException e)
    {
      m = null;
    }
    GET_SCHEMA_METHOD = m;

    Class<? extends PreparedStatement> statementClass = null;

    try
    {
      statementClass = Class.forName("oracle.jdbc.OraclePreparedStatement")
                            .asSubclass(PreparedStatement.class);
    }
    catch (ClassNotFoundException e)
    {
      statementClass = null;
    }

    // Get statement methods if available
    try
    {
      if (statementClass == null)
        m = null;
      else
        m = statementClass.getMethod("setLobPrefetchSize", Integer.TYPE);
    }
    catch (NoSuchMethodException e)
    {
      m = null;
    }
    PREFETCH_METHOD = m;

    try
    {
      if (statementClass == null)
        m = null;
      else
        m = statementClass.getMethod("setBytesForBlob",
                                     Integer.TYPE, byte[].class);
    }
    catch (NoSuchMethodException e)
    {
      m = null;
    }
    SET_BLOB_BYTES = m;

    try
    {
      if (statementClass == null)
        m = null;
      else
        m = statementClass.getMethod("setStringForClob",
                                     Integer.TYPE, String.class);
    }
    catch (NoSuchMethodException e)
    {
      m = null;
    }
    SET_CLOB_STRING = m;

    try
    {
      if (statementClass == null)
        m = null;
      else
        m = statementClass.getMethod("registerReturnParameter",
                                     Integer.TYPE, Integer.TYPE);
    }
    catch (NoSuchMethodException e)
    {
      m = null;
    }
    REGISTER_RETURN_PARAMETER = m;

    try
    {
      if (statementClass == null)
        m = null;
      else
        m = statementClass.getMethod("getReturnResultSet");
    }
    catch (NoSuchMethodException e)
    {
      m = null;
    }
    GET_RETURN_RESULT_SET = m;

    Class<?> factoryClass, parserClass, generatorClass, datumClass, valueClass;
    Method createJsonBinaryValue, createJsonTextValue, createJsonBinaryParser, 
     createJsonTextParser, createJsonTextGenerator, createJsonBinaryGenerator;
    try 
    {
      valueClass = Class.forName("oracle.sql.json.OracleJsonValue");
      factoryClass = Class.forName("oracle.sql.json.OracleJsonFactory");
      parserClass = Class.forName("oracle.sql.json.OracleJsonParser");
      generatorClass = Class.forName("oracle.sql.json.OracleJsonGenerator");
      datumClass = Class.forName("oracle.sql.json.OracleJsonDatum");
      m = datumClass.getMethod("shareBytes");
      createJsonBinaryValue = factoryClass.getMethod("createJsonBinaryValue", ByteBuffer.class);
      createJsonBinaryParser = factoryClass.getMethod("createJsonBinaryParser", ByteBuffer.class);
      createJsonTextGenerator = factoryClass.getMethod("createJsonTextGenerator", OutputStream.class);
      createJsonBinaryGenerator = factoryClass.getMethod("createJsonBinaryGenerator", OutputStream.class);
    } 
    catch (Exception e) 
    {
      factoryClass = null;
      generatorClass = null;
      parserClass = null;
      datumClass = null;
      valueClass = null;
      createJsonBinaryValue = null;
      createJsonBinaryParser = null;
      createJsonBinaryGenerator = null;
      createJsonTextGenerator = null;
      
      m = null;
    }
    JSON_FACT_CLASS = factoryClass;
    JSON_VALUE_CLASS = valueClass;
    JSON_PARSE_CLASS = parserClass;
    JSON_GEN_CLASS = generatorClass;
    JSON_DATUM_CLASS = datumClass;
    JSON_SHARE_BYTES = m;
    JSON_FACT_CREATE_BINARY_VALUE = createJsonBinaryValue;
    JSON_FACT_CREATE_BINARY_PARSER = createJsonBinaryParser;
    JSON_FACT_CREATE_JSON_TEXT_GENERATOR = createJsonTextGenerator;
    JSON_FACT_CREATE_BINARY_GENERATOR = createJsonBinaryGenerator;
    
    try {
      createJsonTextParser = factoryClass.getMethod("createJsonTextParser", InputStream.class);
      createJsonTextValue = factoryClass.getMethod("createJsonTextValue", InputStream.class);
    } catch (Exception e) { 
      createJsonTextParser = null;
      createJsonTextValue = null;
    }
    JSON_FACT_CREATE_TEXT_PARSER = createJsonTextParser;
    JSON_FACT_CREATE_TEXT_VALUE = createJsonTextValue;
  }

}
