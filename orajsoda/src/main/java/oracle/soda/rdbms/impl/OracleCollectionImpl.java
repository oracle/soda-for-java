/* Copyright (c) 2014, 2024, Oracle and/or its affiliates.*/
/* All rights reserved.*/

/*
   DESCRIPTION
    This is the RDBMS-specific implementation of OracleCollection.
 */

/**
 *  This class is not part of the public API, and is
 *  subject to change.
 *
 *  Do not rely on it in your application code.
 *
 *  @author  Doug McMahon
 *  @author  Max Orgiyan
 *  @author  Rahul Kadwe
 */

package oracle.soda.rdbms.impl;

import java.io.ByteArrayInputStream;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.CharacterCodingException;

import java.math.BigDecimal;

import java.security.NoSuchAlgorithmException;

import java.sql.ResultSet;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Clob;
import java.sql.Types;
import java.sql.Array;
import java.sql.DatabaseMetaData;

import oracle.jdbc.OracleConnection;

import oracle.json.logging.OracleLog;
import oracle.json.parser.JsonPath;
import oracle.json.parser.IndexColumn;
import oracle.json.parser.JsonQueryPath;

import java.util.logging.Logger;
import java.util.ArrayList;
import java.util.List;

import oracle.json.common.MetricsCollector;
import oracle.json.common.DocumentCodec;

import oracle.json.util.ByteArray;
import oracle.json.util.JsonByteArray;
import oracle.json.util.Pair;
import oracle.json.parser.PathParser;
import oracle.json.parser.IndexSpecification;
import oracle.json.parser.QueryException;

import oracle.soda.OracleException;
import oracle.soda.OracleDocument;
import oracle.soda.OracleCollection;
import oracle.soda.OracleCollectionAdmin;
import oracle.soda.OracleOperationBuilder;

import jakarta.json.JsonException;

public abstract class OracleCollectionImpl implements OracleCollection
{
  protected static final boolean NO_JSON_TRANSFORM = 
    Boolean.getBoolean("oracle.soda.noJsonTransform");
          
  protected static final Logger log =
    Logger.getLogger(OracleCollectionImpl.class.getName());

  static ArrayList<OracleDocument> EMPTY_LIST = new ArrayList<OracleDocument>();

  static byte[] EMPTY_DATA = new byte[0];

  private static final int ORA_SQL_DATAGUIDE_NOT_EXISTS = 40582;

  protected final String collectionName;
  protected final OracleConnection conn;
  protected final OracleDatabaseImpl db;
  protected final MetricsCollector metrics;
  protected final CollectionDescriptor options;

  private OracleCollectionAdministrationImpl admin;

  private final static int ORA_SQL_OBJECT_EXISTS = 955;
  private final static int ORA_SQL_OBJECT_NOT_EXISTS = 942;
  private final static int ORA_SQL_INDEX_NOT_EXISTS = 1418;

  protected StringBuilder sb = new StringBuilder(SODAConstants.SQL_STATEMENT_SIZE);

  // This is a work-around for the problem that inside the RDBMS
  // the SQL "returning" clause simply isn't supported.
  // This also triggers avoidance of a 32k limit on setBytes/setString.
  protected boolean internalDriver = false;
  public    boolean oracleDriver   = false;

  // ### This is enables use of CallableStatement for RETURNING clauses.
  public    boolean useCallableReturns = false;

  private SODAUtils.SQLSyntaxLevel sqlSyntaxLevel = SODAUtils.SQLSyntaxLevel.SQL_SYNTAX_UNKNOWN;

  boolean avoidTxnManagement = false;

  OracleCollectionImpl(OracleDatabaseImpl db, String name)
  {
    this(db, name, CollectionDescriptor.createDefault(name));
  }

  OracleCollectionImpl(OracleDatabaseImpl db, String name,
                       CollectionDescriptor options)
  {
    this.db = db;
    this.collectionName = name;
    this.options = options;
    this.metrics = db.getMetrics();
    this.conn = db.getConnection();

    setAvoid();
  }

  //### Not part of a public API. Made public for internal use.
  public boolean getStrictMode() throws OracleException
  {
    sqlSyntaxLevel = SODAUtils.getSQLSyntaxLevel(conn, sqlSyntaxLevel);

    if (!SODAUtils.sqlSyntaxBelow_23(sqlSyntaxLevel) && options.hasJsonType()) {
      return true;
    }

    return false;
  }

  //### Not part of a public API. Made public for internal use.
  public boolean isTreatAsAvailable() throws OracleException
  {
    sqlSyntaxLevel = SODAUtils.getSQLSyntaxLevel(conn, sqlSyntaxLevel);

    if (!SODAUtils.sqlSyntaxBelow_23(sqlSyntaxLevel) && !SODAUtils.sqlSyntax_23_2(sqlSyntaxLevel)) {
      return true;
    }

    return false;
  }

  void checkJDBCVersion() throws OracleException
  {
    checkJDBCVersion(false);
  }

  void checkJDBCVersion(boolean isInsertAndGet) throws OracleException
  {
    boolean hasBinaryFormat = options.hasBinaryFormat();
    boolean hasJSONType = options.hasJsonType();
    if (hasBinaryFormat || hasJSONType)
    {
      try
      {
        DatabaseMetaData dbmd = conn.getMetaData();
        int major = dbmd.getDriverMajorVersion();
        int minor = dbmd.getDriverMinorVersion();

        if (hasBinaryFormat && ((major < 19) || ((major == 19) && (minor < 7))))
          throw SODAUtils.makeException(SODAMessage.EX_JDBC_JAR_HAS_NO_OSON_SUPPORT);
        else if (hasJSONType) 
        {
	  if (major < 21)
            throw SODAUtils.makeException(SODAMessage.EX_JDBC_JAR_HAS_NO_JSON_TYPE_SUPPORT);
          if (isInsertAndGet && (major < 23) && (options.hasMaterializedEmbeddedID() && !options.isDualityView()))
            throw SODAUtils.makeException(SODAMessage.EX_INSERT_AND_GET_REQUIRES_23C_JDBC);
	}
      }
      catch (SQLException e)
      {
        throw new OracleException(e);
      }
    }
  }

  public void setAvoid()
  {
    if (System.getProperty("oracle.jserver.version") != null)
    {
      internalDriver = true;
      if (OracleLog.isLoggingEnabled())
        log.fine("Avoid returning clauses for internal connections");
    }
    if (db.hasOracleConnection())
    {
      oracleDriver = true;
    }
    else
    {
      // ### Switch this off to disable use of callable statements
      // ### as the alternative when the Oracle driver isn't present.
      // ### If this is disabled, then OracleOperationBuildImpl and
      // ### TableCollectionImpl need modifications to avoid using
      // ### the RETURNING clauses in situations where the Oracle
      // ### driver is not used, e.g.
      // ###   if (!oracleDriver && !useCallableReturns)
      // ###     ...use a two-round-trip strategy...
      // ### TO-DO for Max to implement that.
      useCallableReturns = true;
    }
  }

  void setAvoidTxnManagement(boolean avoidTxnManagement) 
  {
    this.avoidTxnManagement = avoidTxnManagement;
  }
 
  /**
   * Get the collection name.
   */
  private String getName()
  {
    return (collectionName);
  }

  /**
   * Returns true if the collection is writable, false if it's read-only.
   */
  protected boolean isReadOnly()
  {
    return (!options.writable);
  }

  /**
   * Returns true if the collection uses ObjectId key assignment, false
   * otherwise. The client is allowed to assign keys. Otherwise, an
   * ObjectId key is generated.
   * 
   * Not part of a public API.
   * ### Remove public later
   */
  public boolean usesObjectIdKeys()
  {
    return (options.keyAssignmentMethod == CollectionDescriptor.KEY_ASSIGN_EMBEDDED_OID);
  }

  /**
   * Returns true if the collection can store non-JSON data.
   * 
   * For an RDBMS-based collection, this means it must have a content type
   * column and the content column must be based on a BLOB.
   */
  private boolean isHeterogeneous()
  {
    return ((options.doctypeColumnName != null) &&
            (options.contentDataType == CollectionDescriptor.BLOB_CONTENT));
  }

  /**
   * Returns true if the collection is backed by a JSON duality view.
   * Not part of a public API.
   */
  public boolean isDualityViewBased()
  {
    return options.isDualityView();
  }

  /**
   * Returns name of the table or view backing the collection
   */
  private String getDBObjectName()
  {
    return options.dbObjectName;
  }

  /**
   * Returns name of the schema that owns the table or view 
   * backing the collection
   */
  private String getDBObjectSchemaName()
  {
    return options.dbSchema;
  }

  /**
   * Returns true if the collection allows client-assigned keys.
   * Not part of a public API.
   */
  public boolean hasClientAssignedKeys()
  {
    return (options.keyAssignmentMethod ==
            CollectionDescriptor.KEY_ASSIGN_CLIENT);
  }

  /**
   * Returns true if the collection has the key in the document,
   * either extrated or inserted.
   * Not part of a public API.
   */
  protected boolean hasDocumentFieldKey()
  {
    return (options.keyAssignmentMethod == CollectionDescriptor.KEY_ASSIGN_EMBEDDED_OID);
  }

  protected boolean hasExtrinsicServerKey()
  {
    return !hasDocumentFieldKey() && !hasClientAssignedKeys();
  }

  /**
   * Returns true if the collection is backed by a binary payload column.
   * Not part of a public API.
   */
  public boolean isBinary()
  {
    return ((options.contentDataType == CollectionDescriptor.BLOB_CONTENT) ||
            (options.contentDataType == CollectionDescriptor.RAW_CONTENT));
  }

  /**
   * This returns true if versioning method creates a payload based digest.
   * Not part of a public API.
   */
  public boolean payloadBasedVersioning()
  {
    return ((options.versioningMethod == CollectionDescriptor.VERSION_MD5) ||
            (options.versioningMethod == CollectionDescriptor.VERSION_SHA256) ||
            ((options.versioningMethod == CollectionDescriptor.VERSION_NONE) && (!options.isNative())));
  }

  boolean matches(CollectionDescriptor desc)
  {
    return (this.options.matches(desc));
  }

  /**
   * Drop this collection.
   */
  private void drop() throws OracleException
  {
    db.dropCollection(collectionName);
  }

  /**
   * Drop this collection.
   */
  private void drop(boolean purge, boolean dropMappedTable) throws OracleException
  {
    db.dropCollection(collectionName, purge, dropMappedTable);
  }

  void writeCheck(String operator) throws OracleException
  {
    if (isReadOnly())
    {
      if (OracleLog.isLoggingEnabled())
        log.warning("Write to " + options.uriName + " not allowed");

      throw SODAUtils.makeException(SODAMessage.EX_READ_ONLY,
                                    options.uriName,
                                    operator);

    }
  }

  protected String computeVersion(byte[] data) throws OracleException
  {
    if (data == null)
      data = EMPTY_DATA;

    String version = null;

    metrics.startTiming();
    
    switch (options.versioningMethod)
    {
    case CollectionDescriptor.VERSION_MD5:
      try
      {
        byte[] md5 = db.hasher.MD5(data);

        if (md5 != null)
          version = ByteArray.rawToHex(md5);
        else
        {
          OracleException oe = SODAUtils.makeException(SODAMessage.EX_MD5_NOT_SUPPORTED);
          if (OracleLog.isLoggingEnabled())
            log.warning(oe.getMessage());
          throw oe;
        }
      }
      catch (NoSuchAlgorithmException e)
      {
        OracleException oe = SODAUtils.makeException(SODAMessage.EX_MD5_NOT_SUPPORTED, e);
        if (OracleLog.isLoggingEnabled())
          log.warning(oe.getMessage());
        throw oe;
      }

      break;

    case CollectionDescriptor.VERSION_SHA256:
      try
      {
        byte[] sha = db.hasher.SHA256(data);

        if (sha != null)
          version = ByteArray.rawToHex(sha);
        else
        {
          OracleException oe = SODAUtils.makeException(SODAMessage.EX_SHA256_NOT_SUPPORTED);
          if (OracleLog.isLoggingEnabled())
            log.warning(oe.getMessage());
          throw oe;
        }
      }
      catch (NoSuchAlgorithmException e)
      {
        OracleException oe = SODAUtils.makeException(SODAMessage.EX_SHA256_NOT_SUPPORTED, e);
        if (OracleLog.isLoggingEnabled())
          log.warning(oe.getMessage());
        throw oe;
      }

      break;
    }

    metrics.recordChecksum();

    return (version);
  }

  // Convert a hex string to a decimal number
  // ### Could this conversion be more efficient?
  protected String uidToDecimal(String hexstr)
  {
    byte[] raw = ByteArray.hexToRaw(hexstr);
    BigDecimal x = BigDecimal.ZERO;
    BigDecimal shift1byte = new BigDecimal(256L);
    for (int i = 0; i < raw.length; ++i)
      x = x.multiply(shift1byte).add(new BigDecimal((int) raw[i] & 0xFF));
    return (x.toPlainString());
  }

  private boolean isInteger(String key)
  {
    if (key == null) return(false);
    if (key.length() == 0) return(false);
    char arr[] = key.toCharArray();
    for (int i = 0; i < arr.length; ++i)
      if ("0123456789".indexOf(arr[i]) < 0)
        return(false);
    return(true);
  }

  private String zeroStrip(String key)
  {
    if (key == null) return(null);
    int klen = key.length();
    if (klen == 0) return(key);
    int pos;
    for (pos = 0; pos < klen; ++pos)
      if (key.charAt(pos) != '0')
        break;
    if (pos == 0) return(key);
    if (pos == klen) return("0");
    return(key.substring(pos));
  }

  protected String canonicalKey(String key) throws OracleException
  {
    // Note: ObjectId-style keys are 24-byte hexadecimal strings (12-byte raw values)
    // We also have to tolerate arbitrary client-assigned keys though, so we can't
    // just check that the key is a 24 byte hex string.

    // For integer key columns, the assignment method is irrelevant,
    // the key string must be a valid integer.
    if (options.keyDataType == CollectionDescriptor.INTEGER_KEY)
    {
      key = zeroStrip(key);
      if (!isInteger(key))
        throw SODAUtils.makeException(SODAMessage.EX_INVALID_KEY,
                                      key,
                                      options.getKeyDataType());
    }
    // If the assignment method is GUID/UUID, ensure that the key
    // is a 32-character uppercased hexadecimal string.
    // This includes INSERT keys because they are UUIDs
    else if ((options.keyAssignmentMethod == CollectionDescriptor.KEY_ASSIGN_GUID) ||
             (options.keyAssignmentMethod == CollectionDescriptor.KEY_ASSIGN_UUID))
    {
      int len = key.length();
      int xlen = 32;
      if (len < xlen)
      {
        String zeros = "00000000000000000000000000000000";
        key = zeros.substring(0, xlen - len) + key;
      }
      else if (len > xlen)
      {
        throw SODAUtils.makeException(SODAMessage.EX_INVALID_KEY,
                                      key,
                                      options.getKeyAssignmentMethod());
      }

      if (!ByteArray.isHex(key))
        throw SODAUtils.makeException(SODAMessage.EX_INVALID_KEY,
                                      key,
                                      options.getKeyAssignmentMethod());
    }
    // For RAW key columns, make sure the string is safe to pass to
    // a SQL HexToRaw conversion.
    else if (options.keyDataType == CollectionDescriptor.RAW_KEY)
    {
      if (!ByteArray.isHex(key))
        throw SODAUtils.makeException(SODAMessage.EX_INVALID_KEY,
                                      key,
                                      options.getKeyDataType());
    }
    // For sequence-assigned keys (includes the IDENTITY column case)
    else if ((options.keyAssignmentMethod ==
              CollectionDescriptor.KEY_ASSIGN_SEQUENCE) ||
             (options.keyAssignmentMethod ==
              CollectionDescriptor.KEY_ASSIGN_IDENTITY))
    {
      key = zeroStrip(key);
      if (!isInteger(key))
        throw SODAUtils.makeException(SODAMessage.EX_INVALID_KEY,
                                      key,
                                      "INTEGER");
    }
    return (key);
  }

  private DocumentCodec osonCodec = null;

  /**
   * Create an OSON/JSON factory that can be shared by all cursors and
   * documents created by this collection implementation.
   */
  DocumentCodec getCodec()
  {
    if (osonCodec == null)
      osonCodec = db.getCodecFactory().getCodec();
    return osonCodec;
  }

  protected byte[] convertToBinary(byte[] data) throws OracleException
  {
    byte[] binary = null;

    if ((data != null) && (data.length != 0))
    {
      DocumentCodec cvt = getCodec();

      try
      {
        cvt.loadUnicode(data);
        binary = cvt.getImage();
      }
      // ### To catch OracleJsonGenerationException and JsonException
      catch (RuntimeException e)
      {
        throw SODAUtils.makeException(SODAMessage.EX_TO_BINARY_CONVERSION_ERROR, e);
      }
    }

    return binary;
  }

  private String[] dockeySteps = null;

  private BigDecimal readJsonStringAsNumber(String decString) throws OracleException
  {
    try
    {
      return new BigDecimal(decString);
    }
    catch (NumberFormatException e)
    {
      throw SODAUtils.makeException(SODAMessage.EX_STRING_NOT_A_NUMBER, decString);
    }
  }

  // Extract _id key value from eJSON
  private String extractKey(byte[] content) throws OracleException
  {
    String dockey = null;

    oracle.sql.json.OracleJsonFactory factory = new oracle.sql.json.OracleJsonFactory();
    ByteArrayInputStream bstream = new ByteArrayInputStream(content);
    oracle.sql.json.OracleJsonParser parser = factory.createJsonTextParser(bstream);
    int depth = 0;
    boolean firstEventSeen = false;

    while (parser.hasNext()) {
      oracle.sql.json.OracleJsonParser.Event e = parser.next();

      if (!firstEventSeen) {
        if (e != oracle.sql.json.OracleJsonParser.Event.START_OBJECT) {
          throw SODAUtils.makeException(SODAMessage.EX_EJSON_MUST_BE_AN_OBJECT);
        }
        firstEventSeen = true;
      }

      if ((e == oracle.sql.json.OracleJsonParser.Event.START_OBJECT) ||
          (e == oracle.sql.json.OracleJsonParser.Event.START_ARRAY)) {
        depth++;
      }
      else if ((e == oracle.sql.json.OracleJsonParser.Event.END_OBJECT) ||
               (e == oracle.sql.json.OracleJsonParser.Event.END_ARRAY)) {
        depth--;
      }

      if ((e == oracle.sql.json.OracleJsonParser.Event.KEY_NAME) && parser.getString().equals("_id")) {
        e = parser.next();

        if (e == oracle.sql.json.OracleJsonParser.Event.START_OBJECT) {
          e = parser.next();

          if (e == oracle.sql.json.OracleJsonParser.Event.KEY_NAME) {
            String key = parser.getString();

            switch (key) {
              case "$numberDecimal":
                e = parser.next();
                switch (e) {
                  case VALUE_STRING:
                    //### is going thru createDecimal and then back to bigDecimalValue()
                    //necessary? Can BigDecimal be used directly?
                    try {
                      oracle.sql.json.OracleJsonDecimal dec = factory.createDecimal(readJsonStringAsNumber(parser.getString()));
                      dockey = DocumentCodec.bigDecimalToKey(dec.bigDecimalValue());
                    } catch (Exception ex) {
                      throw SODAUtils.makeException(SODAMessage.EX_INVALID_EJSON_VALUE, "$numberDecimal");
                    }
                    break;
                  case VALUE_DECIMAL:
                  case VALUE_FLOAT:
                  case VALUE_DOUBLE:
                    //### is going thru createDecimal and then back to bigDecimalValue()
                    //necessary? Can BigDecimal be used directly?
                    dockey = DocumentCodec.bigDecimalToKey(factory.createDecimal(parser.getBigDecimal()).bigDecimalValue());
                    break;
                  default:
                    throw SODAUtils.makeException(SODAMessage.EX_INVALID_EJSON_VALUE, "$numberDecimal");
                }
                break;
              case "$numberLong":
                e = parser.next();
                if ((e == oracle.sql.json.OracleJsonParser.Event.VALUE_DECIMAL) && parser.isIntegralNumber()) {
                  BigInteger i = parser.getBigInteger();
                  try {
                    //### Is the conversion to big decimal from long correct?

                    //### is going thru createDecimal and then back to bigDecimalValue()
                    //necessary? Can BigDecimal be used directly?
                    dockey = DocumentCodec.bigDecimalToKey(factory.createDecimal(i.longValueExact()).bigDecimalValue());
                  } catch (Exception ex) {
                    throw SODAUtils.makeException(SODAMessage.EX_INVALID_EJSON_VALUE, "$numberLong");
                  }
                } else if (e == oracle.sql.json.OracleJsonParser.Event.VALUE_STRING) {
                  try {
                    BigDecimal bd = readJsonStringAsNumber(parser.getString());
                    //### is going thru createDecimal and then back to bigDecimalValue()
                    //necessary? Can BigDecimal be used directly?
                    dockey = DocumentCodec.bigDecimalToKey(factory.createDecimal(bd.longValueExact()).bigDecimalValue());
                  } catch (Exception ex) {
                    throw SODAUtils.makeException(SODAMessage.EX_INVALID_EJSON_VALUE, "$numberLong");
                  }
                } else
                  throw SODAUtils.makeException(SODAMessage.EX_INVALID_EJSON_VALUE, "$numberLong");
                break;
              case "$numberInt":
                e = parser.next();
                if ((e == oracle.sql.json.OracleJsonParser.Event.VALUE_DECIMAL) && parser.isIntegralNumber()) {
                  BigInteger i = parser.getBigInteger();
                  try {
                    //### Is the conversion to big decimal from long correct?

                    //### is going thru createDecimal and then back to bigDecimalValue()
                    //necessary? Can BigDecimal be used directly?
                    dockey = DocumentCodec.bigDecimalToKey(factory.createDecimal(i.intValueExact()).bigDecimalValue());
                  } catch (Exception ex) {
                    throw SODAUtils.makeException(SODAMessage.EX_INVALID_EJSON_VALUE, "$numberInt");
                  }
                } else if (e == oracle.sql.json.OracleJsonParser.Event.VALUE_STRING) {
                  try {
                    BigDecimal bd = readJsonStringAsNumber(parser.getString());
                    //### is going thru createDecimal and then back to bigDecimalValue()
                    //necessary? Can BigDecimal be used directly?
                    dockey = DocumentCodec.bigDecimalToKey(factory.createDecimal(bd.intValueExact()).bigDecimalValue());
                  } catch (Exception ex) {
                    throw SODAUtils.makeException(SODAMessage.EX_INVALID_EJSON_VALUE, "$numberInt");
                  }
                } else
                  throw SODAUtils.makeException(SODAMessage.EX_INVALID_EJSON_VALUE, "$numberInt");
                break;
              case "$numberDouble":
                e = parser.next();
                double d;
                switch (e) {
                  case VALUE_DECIMAL:
                  case VALUE_FLOAT:
                  case VALUE_DOUBLE:
                    //### is the createDouble and then back using doubleValue() necessary?
                    //Can the double be used directly?
                    d = factory.createDouble(parser.getDouble()).doubleValue();
                    if (!Double.isInfinite(d) && !Double.isNaN(d)) {
                      BigDecimal bd = BigDecimal.valueOf(d);
                      if (bd.stripTrailingZeros().scale() <= 0)
                        dockey = bd.toBigInteger().toString();
                        // Else we don't allow keys with fraction values in SODA
                        // ### should we allow this?
                      else
                        throw SODAUtils.makeException(SODAMessage.EX_INVALID_EJSON_VALUE, "$numberDouble");
                    } else
                      throw SODAUtils.makeException(SODAMessage.EX_INVALID_EJSON_VALUE, "$numberDouble");
                    break;
                  case VALUE_STRING:
                    try {
                      //### is the createDouble and then back using doubleValue() necessary?
                      //Can the double be used directly?
                      d = factory.createDouble(Double.parseDouble(parser.getString())).doubleValue();
                    } catch (Exception ex) {
                      throw SODAUtils.makeException(SODAMessage.EX_INVALID_EJSON_VALUE, "$numberDouble");
                    }
                    // ### same as above, refactor
                    if (!Double.isInfinite(d) && !Double.isNaN(d)) {
                      BigDecimal bd = BigDecimal.valueOf(d);
                      if (bd.stripTrailingZeros().scale() <= 0)
                        dockey = bd.toBigInteger().toString();
                        // Else we don't allow keys with fraction values in SODA
                        // ### should we allow this?
                      else
                        throw SODAUtils.makeException(SODAMessage.EX_INVALID_EJSON_VALUE, "$numberDouble");
                    } else {
                      throw SODAUtils.makeException(SODAMessage.EX_INVALID_EJSON_VALUE, "$numberDouble");
                    }
                    break;
                  default:
                    throw SODAUtils.makeException(SODAMessage.EX_INVALID_EJSON_VALUE, "$numberDouble");
                }
                break;
              case "$numberFloat":
                e = parser.next();
                float f;
                switch (e) {
                  case VALUE_DECIMAL:
                  case VALUE_FLOAT:
                  case VALUE_DOUBLE:
                    //### is the createFloat and then back using floatValue() necessary?
                    //Can the float be used directly?
                    f = factory.createFloat(parser.getFloat()).floatValue();
                    if (!Float.isInfinite(f) && !Float.isNaN(f)) {
                      BigDecimal bd = BigDecimal.valueOf(f);
                      if (bd.stripTrailingZeros().scale() <= 0)
                        dockey = bd.toBigInteger().toString();
                        // Else we don't allow keys with fraction values in SODA
                        // ### should we allow this?
                      else
                        throw SODAUtils.makeException(SODAMessage.EX_INVALID_EJSON_VALUE, "$numberFloat");
                    } else
                      throw SODAUtils.makeException(SODAMessage.EX_INVALID_EJSON_VALUE, "$numberFloat");
                    break;
                  case VALUE_STRING:
                    try {
                      //### is the createFloat and then back using floatValue() necessary?
                      //Can the float be used directly?
                      f = factory.createFloat(Float.parseFloat(parser.getString())).floatValue();
                      // ### Same as above, refactor
                      if (!Float.isInfinite(f) && !Float.isNaN(f)) {
                        BigDecimal bd = BigDecimal.valueOf(f);
                        if (bd.stripTrailingZeros().scale() <= 0)
                          dockey = bd.toBigInteger().toString();
                        // Else we don't allow keys with fraction values in SODA
                        // ### should we allow this?
                      } else
                        throw SODAUtils.makeException(SODAMessage.EX_INVALID_EJSON_VALUE, "$numberFloat");
                    } catch (Exception ex) {
                      throw SODAUtils.makeException(SODAMessage.EX_INVALID_EJSON_VALUE, "$numberFloat");
                    }
                    break;
                  default:
                    throw SODAUtils.makeException(SODAMessage.EX_INVALID_EJSON_VALUE, "$numberFloat");
                }
                break;
              case "$oid":
                e = parser.next();
                if (e == oracle.sql.json.OracleJsonParser.Event.VALUE_STRING) {
                  String s = parser.getString();
                  if ((ByteArray.isHex(s)) && (s.length() == 24)) 
                  {
                    //### is the a createBinary that allows us to mark it as id?
                    oracle.sql.json.OracleJsonBinary b = new oracle.jdbc.driver.json.tree.OracleJsonBinaryImpl(ByteArray.hexToRaw(s), true);
                    dockey = b.getString();
                  }
                  else
                    throw SODAUtils.makeException(SODAMessage.EX_INVALID_EJSON_VALUE, "$oid");
                } else
                  throw SODAUtils.makeException(SODAMessage.EX_INVALID_EJSON_VALUE, "$oid");
                break;
              default:
                throw SODAUtils.makeException(SODAMessage.EX_INVALID_EJSON_KEY, parser.getString());
            }

            if (parser.hasNext())
            {
              e = parser.next();
              if (e != oracle.sql.json.OracleJsonParser.Event.END_OBJECT) {
                if (e == oracle.sql.json.OracleJsonParser.Event.KEY_NAME)
                  throw SODAUtils.makeException(SODAMessage.EX_INVALID_EJSON_KEY, parser.getString());
                // Cannot really be anything other than a KEY_NAME, so this "else" case
                // should never run.
                else
                  throw SODAUtils.makeException(SODAMessage.EX_INVALID_EJSON_FOR_ID);
              }
            }
          }
          else
            throw SODAUtils.makeException(SODAMessage.EX_INVALID_EJSON_FOR_ID);
        }
        // _id : "someString" | someNumber
        else if ((e == oracle.sql.json.OracleJsonParser.Event.VALUE_STRING) ||
                 (e == oracle.sql.json.OracleJsonParser.Event.VALUE_DOUBLE) ||
                 (e == oracle.sql.json.OracleJsonParser.Event.VALUE_FLOAT) ||
                 (e == oracle.sql.json.OracleJsonParser.Event.VALUE_DECIMAL) ||
                 (e == oracle.sql.json.OracleJsonParser.Event.VALUE_BINARY)) {

          // Code copied from OsonDocumentCodec.extractKey(...)
          // ### refactor into a common method later,to avoid this
          // code duplication
          oracle.sql.json.OracleJsonValue val = parser.getValue();
          switch (val.getOracleJsonType())
          {
            case STRING:
              dockey = val.asJsonString().getString();
              break;
            case DOUBLE:
              double d = val.asJsonDouble().doubleValue();
              if (!Double.isInfinite(d) && !Double.isNaN(d))
              {
                BigDecimal bd = BigDecimal.valueOf(d);
                if (bd.stripTrailingZeros().scale() <= 0)
                  dockey = bd.toBigInteger().toString();
                // Else we don't allow keys with fraction values in SODA
              }
              break;
            case FLOAT:
              float f = val.asJsonFloat().floatValue();
              if (!Float.isInfinite(f) && !Float.isNaN(f))
              {
                BigDecimal bd = BigDecimal.valueOf(f);
                if (bd.stripTrailingZeros().scale() <= 0)
                  dockey = bd.toBigInteger().toString();
                // Else we don't allow keys with fraction values in SODA
              }
              break;
            case DECIMAL:
              return DocumentCodec.bigDecimalToKey(val.asJsonDecimal().bigDecimalValue());
            case BINARY:
              oracle.sql.json.OracleJsonBinary b = val.asJsonBinary();
              if (b.isId())
                dockey = b.getString(); // This will be lowercase hexadecimal
              // Other binaries that aren't IDs aren't considered viable keys
              break;
            default:
              break;
          }
          if (dockey == null)
            throw SODAUtils.makeException(SODAMessage.EX_INVALID_EJSON_FOR_ID);
        } else {
          throw SODAUtils.makeException(SODAMessage.EX_INVALID_EJSON_FOR_ID);
        }
      }
      if (depth != 0)
        continue;
    }
    parser.close();

    return dockey;
  }
  
  
  String[] initializeDocumentKeySteps() throws OracleException {
    String[] dockeySteps = null;
    // First-time initialization of the document key steps
    if (options.keyColumnPath == null)
      throw SODAUtils.makeException(SODAMessage.EX_INVALID_PATH, "<null>");
    try {
      // ### Should be an error if keyColumnPath is null
      PathParser pparser = new PathParser(options.keyColumnPath);
      dockeySteps = pparser.splitSteps(false);
      // ### Should check for and disallow array steps
    } catch (QueryException e) {
      throw SODAUtils.makeException(SODAMessage.EX_INVALID_PATH, options.keyColumnPath);
    }
    return dockeySteps;
  }
  
  public String extractKeyForEmbeddedIdEJSONCollections(OracleDocument document, 
                                                        String dockey,
                                                        boolean eJSON)
  throws OracleException {
    
    if (OracleDocumentImpl.isBinary(document) && eJSON)
      throw SODAUtils.makeException(SODAMessage.EX_EJSON_CANNOT_BE_USED_WITH_BINARY_DOC);

    if ((options.keyAssignmentMethod == CollectionDescriptor.KEY_ASSIGN_EMBEDDED_OID) && eJSON)
    {
      if (!OracleDatabaseImpl.isOracleJsonAvailable())
        throw SODAUtils.makeException(SODAMessage.EX_JDBC_196_REQUIRED);

      // ### Temporary processing of eJSON. Can be removed once JDBC support eJSON to OSON
      // conversion. Then the latter can be entirely done in the JDBC layer, and the key
      // would be extraced using the codecs *after* the conversion to OSON takes place.
      //
      // extractKey function parses the JSON in exactly the same way as the upcoming JDBC
      // eJSON parser. Then the key is extracted using the same logic as in the codec (extractKey(...)
      // method in OsonDocumentCodec).
      dockey = extractKey(document.getContentAsByteArray());
    }
    
    return dockey;
  }
  
  public String extractKeyForEmbeddedIdCollections(DocumentCodec keyProcessor, 
                                                                 OracleDocument document, 
                                                                 boolean eJSON,
                                                                 String key) 
  throws OracleException {
    
    String dockey = key;
    
    // If there's no key in the document,
    // see if a key can/should be extracted from the document
    if ((dockey == null))
    {
      // eJSON key extraction is already taken care of by extractKey above
      if (options.keyAssignmentMethod == CollectionDescriptor.KEY_ASSIGN_EMBEDDED_OID && !eJSON)
      {
        if (OracleDocumentImpl.isBinary(document)) 
        {
          OracleDocumentImpl odocument = (OracleDocumentImpl)document;
          byte[] oson = odocument.getBinaryContentAsByteArray();
          try
          {
            keyProcessor.loadImage(oson);
            keyProcessor.setValidation(false);
            dockey = keyProcessor.getKey(true);
          }
          catch (JsonException e)
          {
            throw SODAUtils.makeException(SODAMessage.EX_PATH_EXTRACT_FAILED, e,
                                          options.keyColumnPath);
          }
        }
        else
        {
          // Get the content as a byte array. If the document was backed by
          // a stream, this will force it to be materialized. That's necessary
          // to ensure that the document can be re-read later (when sending it
          // to the database).
          byte[] data = document.getContentAsByteArray();

          try
          {
            keyProcessor.loadUnicode(data);
            keyProcessor.setValidation(false);
            dockey = keyProcessor.getKey(true);
          }
          catch (JsonException e)
          {
            throw SODAUtils.makeException(SODAMessage.EX_PATH_EXTRACT_FAILED, e,
                                          options.keyColumnPath);
          }
        }
      }
      
      // If we found a key, validate it and then use it
      if (dockey != null)
        return canonicalKey(dockey);
    }
    
    return null;
  }
  
  /**
   * Get the key provided for a document.
   * For client-assigned keys, this is simply the key string associated
   * with the document.
   * For extracted keys, including ObjectId, the key is pulled from the
   * document using the specified path.
   * For inserted keys, including ObjectId, a key is generated locally and
   * then inserted.
   * If an extraction or insertion was necessary, the document content may
   * have been read, buffered, and swapped out.
   */
  protected Pair<String, Object> getDocumentKey(OracleDocument document, boolean generate, boolean eJSON)
    throws OracleException
  {
    byte[] data = null;

    if (document == null) return null;

    // Get the document's key
    String dockey = document.getKey();
    
    if (dockey != null && !db.allowDocumentKey() && options.hasEmbeddedID())
      throw SODAUtils.makeException(SODAMessage.EX_KEY_SET_WITH_EMBEDDED_OID);
    
    Pair<String, Object> docKeyAndOsonPayload = new Pair<String, Object>(dockey, null);

    // If it's a server-assigned key method just return
    if (hasExtrinsicServerKey())
      return docKeyAndOsonPayload;
    // Client-assigned keys are not inserted/extracted so just return
    if (options.keyAssignmentMethod == CollectionDescriptor.KEY_ASSIGN_CLIENT)
      return docKeyAndOsonPayload;

    // If the assignment method isn't OID, ignore the document key;
    // we will be forced to extract it, or generate and insert it.
    //
    // ### It's not clear this is correct. If the caller provided a key,
    // ### should we trust it? What if we're running  OID,
    // ### should we make sure it's injected into the document?
    //
    // ### Even for non-eJSON cases, we can avoid retrieving the key with getKey()
    // from the doc.
    if ((options.keyAssignmentMethod != CollectionDescriptor.KEY_ASSIGN_EMBEDDED_OID) ||
        ((options.keyAssignmentMethod == CollectionDescriptor.KEY_ASSIGN_EMBEDDED_OID) && eJSON))
    {
      dockey = null;
    }

    dockey = extractKeyForEmbeddedIdEJSONCollections(document, dockey ,eJSON);

    // Run it through the canonicalizer to make sure it's valid
    // ### Ignore it if it's not valid - is that OK, or an error?
    if (dockey != null)
    {
      try
      {
        dockey = canonicalKey(dockey);
        docKeyAndOsonPayload = new Pair<String, Object>(dockey, null);
      }
      catch (OracleException e)
      {
        // If not, continue as if the client didn't provide a key
        dockey = null;
      }
    }

    // ### For now, for the ObjectId case only, we'll just accept a
    // ### client-provided key (if any), bypassing the costly key processor.
    if (options.keyAssignmentMethod == CollectionDescriptor.KEY_ASSIGN_EMBEDDED_OID)
      if (dockey != null)
      {
        // ### We should probably ensure that the key is inserted.
        // ### Unfortunately the insert code will error out if we do
        // ### that, and it's also costly to run even if we fix that.
        return docKeyAndOsonPayload;
      }


    DocumentCodec keyProcessor = getCodec();
    if (dockeySteps == null)
      dockeySteps = initializeDocumentKeySteps();
    keyProcessor.setKeyPath(dockeySteps);
    
    String extractedKey = extractKeyForEmbeddedIdCollections(keyProcessor, document, eJSON, dockey);
    
    if (extractedKey != null)
      return new Pair<String, Object>(canonicalKey(extractedKey), null);
    

    // See if the key needs to be generated for auto-insertion
    if (generate && (dockey == null))
    {
      if (options.keyAssignmentMethod ==
               CollectionDescriptor.KEY_ASSIGN_EMBEDDED_OID)
      {
        dockey = db.nextObjectId();
      }
      // The generated keys don't need to be checked for canonical form
    }
    // ### This won't override a key gotten from the document - is that OK?
    // ### If not, the code above needs to set it to null to force the
    // ### generation to occur.

    // If there's a key available, try inserting it
    if (dockey != null)
    {
      if (options.keyAssignmentMethod ==
           CollectionDescriptor.KEY_ASSIGN_EMBEDDED_OID)
      {
        // BSON documents need the key to be the first field in sequence.
        // To make sure this happens, set the removeKey flag to true
        // so that the injector knows its safe to put the key in blindly
        // without waiting to see if an old key exists (we already know
        // it doesn't because we looked for it earlier). OSON doesn't
        // care about this setting since fields are unordered.
        boolean removeKey = true;
        // ### For now, require the key to match any existing key?
        boolean mustMatch = true;

        if (OracleDocumentImpl.isBinary(document))
        {
          OracleDocumentImpl odocument = (OracleDocumentImpl)document;
          byte[] oson = odocument.getBinaryContentAsByteArray();

          try
          {
            keyProcessor.loadImage(oson);
            keyProcessor.setNewKey(dockey, removeKey, mustMatch);
            keyProcessor.setRemoveKey(true);
            byte[] newOson = keyProcessor.getImage();
            if (newOson != null)
              docKeyAndOsonPayload = new Pair<String, Object>(dockey, newOson);
          }
          catch (JsonException e)
          {
            throw SODAUtils.makeException(SODAMessage.EX_PATH_INSERT_FAILED, e,
                                          options.keyColumnPath);
          }
        }
        else
        {
          // If we haven't yet read out the data, do it now
          if (data == null)
            data = document.getContentAsByteArray();

          // Now insert the key, creating a new content string
          try
          {
            keyProcessor.loadUnicode(data);
            keyProcessor.setNewKey(dockey, removeKey, mustMatch);
            keyProcessor.setRemoveKey(true);
            if ((options.hasBinaryFormat() || options.hasJsonType()) && !eJSON) {
              // input is text document, id can still be inserted
              // using an extended type
              byte[] newOson = keyProcessor.getImage();
              if (newOson != null) {
                docKeyAndOsonPayload = new Pair<String, Object>(dockey, newOson);
              }
            } else {
              if (eJSON)
                keyProcessor.generateEJSONId(true);
              String newContent = keyProcessor.getString();
              // If it worked, change the document content to the new string
              if (newContent != null)
                docKeyAndOsonPayload = new Pair<String, Object>(dockey, newContent);
            }
          }
          catch (JsonException e)
          {
            throw SODAUtils.makeException(SODAMessage.EX_PATH_INSERT_FAILED, e,
                                          options.keyColumnPath);
          }
        }
      }
    }

    return docKeyAndOsonPayload;
  }
  
  /**
   * Convert a byte array to a string after autodetecting the character set.
   */
  static String stringFromBytes(byte[] data, boolean checked)
    throws OracleException
  {
    if (data == null) return("");
    if (data.length == 0) return("");

    // ### This gets the character set for the JSON data,
    //     but it would be best to avoid this in cases
    //     where the data originated from a JSON parse.
    Charset datacs = JsonByteArray.getJsonCharset(data);

    // Unchecked conversion is faster.
    // In cases where bad bytes are present, switching them to
    // Unicode replacement characters seems reasonable since they're
    // going to undergo a potentially lossy character set conversion
    // to SQL anyway.
    //
    if (!checked)
      return new String(data, datacs);

    // Otherwise the bytes need to be validated
    try
    {
      return ByteArray.bytesToString(data, datacs);
    }
    catch (CharacterCodingException e)
    {
      throw new OracleException(e);
    }
  }

  /**
   * Convert a byte array to a string after autodetecting the character set.
   */
  static String stringFromBytes(byte[] data)
    throws OracleException
  {
    return(OracleCollectionImpl.stringFromBytes(data, false));
  }

  /**
   * Return a single object matching a key.
   */
  public OracleDocument findOne(String key)
          throws OracleException
  {
    return find().key(key).getOne();
  }

  /**
   * Finds a document fragment.
   * Not part of a public API.
   */
  abstract public OracleDocumentFragmentImpl findFragment(String key, long offset, int length)
    throws OracleException;

  /**
   * Return an OracleOperationBuilder for all objects in the collection.
   */
  public OracleOperationBuilder find()
  {
    return new OracleOperationBuilderImpl(this, conn);
  }

  private void truncate()
    throws OracleException
  {
    writeCheck("truncate");

    // Truncate not supported on views (or PLSQL collections for now)
    if (options.dbObjectType != CollectionDescriptor.DBOBJECT_TABLE)
      throw SODAUtils.makeException(SODAMessage.EX_TRUNCATE_NOT_SUPP,
                                    options.uriName);

    sb.setLength(0);
    sb.append("truncate table \"");
    sb.append(options.dbObjectName);
    sb.append("\"");
    String sqltext = sb.toString();

    PreparedStatement stmt = null;

    try
    {
      metrics.startTiming();

      stmt = conn.prepareStatement(sqltext);

      stmt.execute();
      if (OracleLog.isLoggingEnabled())
        log.fine("Truncated collection "+collectionName);

      stmt.close();
      stmt = null;

      // Commit unnecessary for TRUNCATE TABLE

      metrics.recordDDL();
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
  }

  /**
   * Not part of a public API
   */
  @Override
  public String toString()
  {
    return(options.toString());
  }

  public OracleCollectionAdmin admin()
  {
    if (admin == null)
    {
      admin = new OracleCollectionAdministrationImpl();
    }
    return admin;
  }

  /**
   * Not part of a public API
   */
  public CollectionDescriptor getOptions()
  {
    return options;
  }

  MetricsCollector getMetrics()
  {
    return metrics;
  }

  OracleDatabaseImpl getDatabase()
  {
    return db;
  }
  
  /**
   * Build string ["schema".]"table"
   */
  void appendTable(StringBuilder sb)
  {
    sb.append("\"");
    if (options.dbSchema != null)
    {
      sb.append(options.dbSchema);
      sb.append("\".\"");
    }
    sb.append(options.dbObjectName);
    sb.append("\"");
  }
  
  /**
   * Append a SQL format clause if the content column is binary
   */
  public String getInputFormatClause() 
  {
    if (options.hasBinaryFormat())
      return "format oson";
    // Append the format clause for binary types
    else if ((options.contentDataType == CollectionDescriptor.BLOB_CONTENT) ||
             (options.contentDataType == CollectionDescriptor.RAW_CONTENT))
      return "format json";
    else
      return null;
  }

  private String buildMultiValueIndexDDL(String indexName, boolean unique, JsonPath[] columns) throws OracleException {

    if (columns.length > 1)
    {
      throw SODAUtils.makeException(SODAMessage.EX_ONLY_SINGLE_PATH_SUPPORTED);
    }

    sb.setLength(0);

    sb.append("create ");
    if (unique)
      sb.append("unique");
    sb.append(" multivalue index ");
    appendSanitizedName(sb, indexName);
    sb.append(" on ");
    appendTable(sb);
    sb.append(" (JSON_TABLE(\"");
    sb.append(options.contentColumnName);
    sb.append("\",\'");
    for (JsonPath column : columns)
    {
      String[] steps = column.getSteps();
      JsonQueryPath jqp = new JsonQueryPath(steps);
      jqp.toSingletonString(sb);
      if (jqp.hasArraySteps())
      {
        throw SODAUtils.makeException(SODAMessage.EX_ARRAY_STEPS_IN_PATH);
      }
    }
    sb.append("[*]\' error on error present on empty null on mismatch columns ( GENERATED_COLUMN any ora_rawcompare path '$')))");

    return (sb.toString());
  }

  private String buildSpatialIndexDDL(String indexName, JsonQueryPath spatial,
                                      boolean scalarRequired, boolean lax)
  {
    sb.setLength(0);

    sb.append("create index ");
    appendSanitizedName(sb, indexName);
    sb.append(" on ");
    appendTable(sb);
    sb.append(" (JSON_VALUE(\"");
    sb.append(options.contentColumnName);
    sb.append("\", '");
    spatial.toSingletonString(sb);
    sb.append("' returning SDO_GEOMETRY");

    if (!lax)
    {
      if (scalarRequired)
        sb.append(" ERROR ON ERROR))");
      else
        sb.append(" ERROR ON ERROR NULL ON EMPTY))");
    }
    else
    {
      sb.append("))");
    }

    sb.append(" indextype is MDSYS.SPATIAL_INDEX");

    // ### Left for reference since 1000 is the default
    // ### Other parameters may be of interest here
    // sb.append(" parameters('SDO_DML_BATCH_SIZE=1000')");

    // ### Should we have this on by default?
    sb.append(" parallel 8");

    return (sb.toString());
  }

  private String buildCTXIndexDDL(String indexName, String language,
                                  boolean is121TextIndexWithLang)
          throws OracleException
  { 
    if (!is121TextIndexWithLang)
    {
      sb.setLength(0);
      sb.append("create index ");
      appendSanitizedName(sb, indexName);
      sb.append(" on ");
      appendTable(sb);
      sb.append(" (\"");
      sb.append(options.contentColumnName);
      sb.append("\") ");
      sb.append("indextype is CTXSYS.CONTEXT");
      sb.append(" parameters('section group CTXSYS.JSON_SECTION_GROUP sync (on commit)");
      sb.append("')");

      // ### Should we specify these hard coded parameters?
      //sb.append(" memory 100M");
      //sb.append("')");
      //sb.append(" parallel 8");
    }
    // ###
    // 12.1 Text Index with language support. Uses
    // slow auto-lexer and broken on update to 12.2.
    // Do not use in production!!!
    else
    {
      String lexerName = null;

      try {
        lexerName = IndexSpecification.get121Lexer(language);
      } catch (QueryException e) {
        throw SODAUtils.makeException(SODAMessage.EX_INVALID_INDEX_CREATE, e);
      }

      sb.setLength(0);

      sb.append("create index ");
      appendSanitizedName(sb, indexName);
      sb.append(" on ");
      appendTable(sb);
      sb.append(" (\"");
      sb.append(options.contentColumnName);
      sb.append("\") ");
      sb.append("indextype is CTXSYS.CONTEXT");

      sb.append(" parameters('");     // BEGIN PARAMETERS

      sb.append("section group CTXSYS.JSON_SECTION_GROUP");

      // Append the lexer for this language
      if (lexerName != null) {
        sb.append(" lexer ");
        sb.append(lexerName);
      }

      sb.append(" stoplist CTXSYS.EMPTY_STOPLIST");
      sb.append(" sync (on commit)");
      sb.append(" memory 100M"); // ### Hard-coded!

      sb.append("')");                // END PARAMETERS

      // ### Should we have this and "memory 100M" on by default?
      sb.append(" parallel 8");
    }

    return (sb.toString());
  }

  private String buildDGIndexDDL(String indexName,
                                 String language,
                                 String search_on,
                                 String dataguide)
    throws OracleException
  {
    sb.setLength(0);
    sqlSyntaxLevel = SODAUtils.getSQLSyntaxLevel(conn, sqlSyntaxLevel);

    String lexerName = null;

    try
    {
      // English is the default, and will be picked when language is
      // not specified.
      if (language != null) {
        lexerName = IndexSpecification.getLexer(language);
      }
    }
    catch (QueryException e)
    {
      throw SODAUtils.makeException(SODAMessage.EX_INVALID_INDEX_CREATE, e);
    }

    sb.append("create search index ");
    appendSanitizedName(sb, indexName);
    sb.append(" on ");
    appendTable(sb);
    sb.append(" (\"");
    sb.append(options.contentColumnName);
    sb.append("\") ");
    sb.append("for json");

    if (SODAUtils.sqlSyntaxBelow_23(sqlSyntaxLevel))
    {
      // Default data guide type
      if (search_on == null)
        search_on = "text_value";

      // By default, dataguide is "on"
      if (dataguide == null)
        dataguide = "on";

      sb.append(" parameters('");     // BEGIN PARAMETERS

      sb.append("sync (on commit)");
      sb.append(" search_on ");
      sb.append(search_on.toLowerCase());
      sb.append(" dataguide ");
      sb.append(dataguide.toLowerCase());

      // If language parameter is omitted,
      // it defaults to English.
      if (lexerName != null)
      {
        sb.append(" language ");
        sb.append(lexerName);
      }

      sb.append("')");                // END PARAMETERS
    }
    else
    {
      if ((search_on == null || search_on.equals("")) && (dataguide == null || dataguide.equals("")))
      {
        return (sb.toString());
      }

      sb.append(" parameters('");

      if (search_on != null)
      {
        sb.append(" search_on ");
        sb.append(search_on.toLowerCase());
      }
      if (dataguide != null)
      {
        sb.append(" dataguide ");
        sb.append(dataguide.toLowerCase());
      }

      sb.append("')");
    }

    return (sb.toString());
  }

  /**
   * Build the drop DDL for an index
   */
  private String dropIndexDDL(String indexName, boolean forceFlag)
  {
    sb.setLength(0);
    sb.append("drop index ");
    appendSanitizedName(sb, indexName);
    if (forceFlag) sb.append(" force");
    return(sb.toString());
  }

  /**
   * Build the DDL for an index
   */
  private String buildIndexDDL(String indexName,
                               boolean unique,
                               boolean scalarRequired,
                               boolean lax,
                               JsonPath[] columns,
                               boolean indexNulls)
          throws OracleException
  {
    sb.setLength(0);
    sb.append("create ");
    if (unique)
      sb.append("unique ");
    sb.append("index ");
    appendSanitizedName(sb, indexName);
    sb.append(" on ");
    appendTable(sb);
    sb.append(" (");
    
    boolean first = true;
    int numCharCols = 0;
    int numCharColsLen = 0;
    for (JsonPath column : columns)
    {
      String sqlTypeName = null;
      String sqlTypeNameDefault = null;
      int    sqlType     = IndexColumn.SQLTYPE_NONE;
      int    maxLength   = 0;
      String sqlOrder    = null;

      if (column instanceof IndexColumn)
      {
        sqlTypeName = ((IndexColumn)column).getSqlTypeName();
        sqlTypeNameDefault = ((IndexColumn)column).getSqlTypeNameDefault();
        sqlType = ((IndexColumn)column).getSqlType();
        maxLength = ((IndexColumn)column).getMaxLength();
        sqlOrder = ((IndexColumn)column).getOrder();
      }

      String[] steps = column.getSteps();
      if (first)
        first = false;
      else
        sb.append(", ");

      sb.append("JSON_VALUE(\"");
      sb.append(options.contentColumnName);
      sb.append("\"");
      String clause = getInputFormatClause();
      if (clause != null)
        sb.append(" ").append(clause);
      
      sb.append(",\'");

      // Use JsonQueryPath to centralize the singleton string builder
      JsonQueryPath jqp = new JsonQueryPath(steps);
      jqp.toSingletonString(sb);

      if (jqp.hasArraySteps())
      {
        throw SODAUtils.makeException(SODAMessage.EX_ARRAY_STEPS_IN_PATH);
      }

      sb.append("\'");

      // return type not explicitly specified

      if (getStrictMode() && sqlTypeName == null)
      {
        sb.append(" returning any scalar");
      }
      else
      {
        sb.append(" returning ");
        if (sqlTypeName != null && sqlTypeName.equalsIgnoreCase("any_scalar"))
        {
          if (!getStrictMode())
            throw SODAUtils.makeException(SODAMessage.EX_23DB_AND_JSON_TYPE_REQUIRED_FOR_INDEX);
          else
            sb.append("any scalar");
        }
        else
        {
          if (sqlTypeName == null)
            sqlTypeName = sqlTypeNameDefault;

          sb.append(sqlTypeName);
          if (sqlType == (IndexColumn.SQLTYPE_CHAR) || sqlTypeName.equals(sqlTypeNameDefault))
          {
            if (numCharCols + 1 > IndexColumn.MAX_CHAR_COLUMNS)
              throw SODAUtils.makeException(SODAMessage.EX_TOO_MANY_COLUMNS, Integer.toString(numCharCols));

            if (maxLength > 0)
            {
              sb.append("(");
              sb.append(maxLength);
              sb.append(")");
              numCharColsLen += maxLength;
            }
            else
            {
              sb.append("(\uFFFF)"); // Marker character to be filled in later
              ++numCharCols;
            }
          }
        }
      }

      if (!lax) 
      {
        if (scalarRequired)
          sb.append(" ERROR ON ERROR");
        else
          sb.append(" ERROR ON ERROR NULL ON EMPTY");
      }

      sb.append(") ");

      if (sqlOrder != null)
        sb.append(sqlOrder);
    }

    if (indexNulls)
      sb.append(",1)");
    else
      sb.append(")");

    String str = sb.toString();

    if (numCharCols > 0)
    {
      int defaultSize = 0;

      // Compute the maximum length by removing all fixed sized values
      // from a budget of 2000 characters and dividing the remaining space.
      if (numCharColsLen < 2000)
        defaultSize = (2000 - numCharColsLen)/numCharCols;

      // If the budget is exhausted hard-wire the minimum of 255
      if (defaultSize < 255)
        defaultSize = 255;

      // Now see what the grand total is
      numCharColsLen += (defaultSize * numCharColsLen);
      if (numCharColsLen > 4000)
      {
        // This is trouble because the total size is probably too large
        if (OracleLog.isLoggingEnabled())
          log.warning("Total size of index "+indexName+" is "+numCharColsLen);
        // ### For now do nothing but log the issue
        // ### Possibly we should reduce the default one more time?
      }

      str = str.replaceAll("\uFFFF", Integer.toString(defaultSize));
    } 

    return(str);
  }

  private void callCreateIndexPLSQL(String indexName, OracleDocument indexSpec)
          throws OracleException
  {
    CallableStatement stmt = null;
    String sqltext = "begin\n"                         +
            "  DBMS_SODA_ADMIN.CREATE_INDEX(\n"        +
            "                   P_URI_NAME   => ?,\n"  +
            "                   P_INDEX_SPEC => ?);\n" +
            "end;";

    try
    {
      metrics.startTiming();

      stmt = conn.prepareCall(sqltext);

      stmt.setNString(1, collectionName);
      stmt.setString(2, indexSpec.getContentAsString());

      stmt.execute();
      if (OracleLog.isLoggingEnabled())
        log.info("Created index "+indexName+" on collection "+collectionName);

      stmt.close();
      stmt = null;

      metrics.recordCall();
    }
    catch (SQLException e)
    {
      if (OracleLog.isLoggingEnabled())
        log.warning(e.toString());

      if (e.getErrorCode() == ORA_SQL_OBJECT_EXISTS)
      {
        throw SODAUtils.makeException(SODAMessage.EX_INDEX_ALREADY_EXISTS);
      }
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

  private void checkAllowedTextIndexContentAndKeyTypes() throws OracleException {
    // Text indexing cannot currently support the National Character Set.
    // This might or might not be supported in the future. We throw
    // the error here as opposed to relying on the SQL, because
    // it will create an invalid index object.
    if ((options.contentDataType == CollectionDescriptor.NCHAR_CONTENT) ||
        (options.contentDataType == CollectionDescriptor.NCLOB_CONTENT))
    {
      throw SODAUtils.makeException(SODAMessage.EX_UNSUPPORTED_INDEX_CREATE,
        options.getContentDataType());
    }
    // Text indexing doesn't support encrypted columns. We throw
    // the error here as opposed to relying on the SQL, because
    // the SQL incorrectly doesn't throw the error (bug 20202126).
    // Even if the error is thrown once the bug is fixed, the SQL will
    // likely create an invalid index object.
    else if (options.contentLobEncrypt != CollectionDescriptor.LOB_ENCRYPT_NONE)
    {
      throw SODAUtils.makeException(SODAMessage.EX_UNSUPPORTED_ENCRYPTED_INDEX_CREATE);
    }
    // Text indexing cannot currently support National Character Set
    // primary key. This seems like a legacy issue, and will hopefully
    // be fixed in the future. Bug number: 20116846. We throw the
    // error here as opposed to relying on the SQL, because it
    // will create an invalid index object.
    else if (options.keyDataType == CollectionDescriptor.NCHAR_KEY)
    {
      throw SODAUtils.makeException(SODAMessage.EX_UNSUPPORTED_INDEX_CREATE2);
    }
  }

  private void appendSanitizedName(StringBuilder builder, String name) {
    // stringToIdentifier will replace any double quotes in the string
    // with underscores. And then we surround the resulting name with
    // double quotes on both sides. Thus SQL injection via name is not
    // possible.
    builder.append("\"");
    builder.append(CollectionDescriptor.stringToIdentifier(name));
    builder.append("\"");
  }

  /**
   * Create an index
   */
  // ### 
  //
  // 1) If index with provided name exists already,check
  // that it has an equivalent specification (otherwise throw an
  // error). Current behavior simply ignores the error if
  // an index with the same name exists already.
  //
  // 2) Consider alternative (or additional) functionality, where
  // the unique name is automatically generated.
  private void createIndex(String indexName, 
                           boolean unique,
                           boolean scalarRequired,
                           boolean lax,
                           boolean indexNulls,
                           JsonPath[] columns,
                           JsonQueryPath spatial,
                           String language,
                           String search_on,
                           String dataguide,
                           boolean is121TextIndexWithLang,
                           BigDecimal ttl,
                           boolean multiValue,
                           OracleDocument indexSpec)
          throws OracleException
  {
    PreparedStatement stmt = null;

    if (indexName == null)
      throw SODAUtils.makeException(SODAMessage.EX_ARG_CANNOT_BE_NULL,
                                    "indexName");

    String sqltext = null;

    if (spatial != null)
    {
      if (isHeterogeneous())
      {
        throw SODAUtils.makeException(SODAMessage.EX_NO_SPATIAL_INDEX_ON_HETERO_COLLECTIONS);
      }

      if (spatial.hasArraySteps())
      {
        throw SODAUtils.makeException(SODAMessage.EX_ARRAY_STEPS_IN_PATH);
      }

      sqltext = buildSpatialIndexDDL(indexName, spatial, scalarRequired, lax);
    }
    else if (multiValue) {

      sqltext = buildMultiValueIndexDDL(indexName, unique, columns);
    }
    else if (columns == null || columns.length == 0)
    {
      checkAllowedTextIndexContentAndKeyTypes();

      if (isHeterogeneous())
      {
        throw SODAUtils.makeException(SODAMessage.EX_NO_TEXT_INDEX_ON_HETERO_COLLECTIONS);
      }

      sqlSyntaxLevel = SODAUtils.getSQLSyntaxLevel(conn, sqlSyntaxLevel);

      // Field cross-validation is performed in IndexSpecification class,
      // after parsing. However, IndexSpecification class is not aware of db release
      // info. Perform additional validation here based on db release
      // info.
      if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel))
      {
        if (search_on != null)
          throw SODAUtils.makeException((SODAMessage.EX_INVALID_PARAM_121_INDEX), "search_on");

        if (dataguide != null)
          throw SODAUtils.makeException((SODAMessage.EX_INVALID_PARAM_121_INDEX), "dataguide");

        if (language != null && !is121TextIndexWithLang)
          throw SODAUtils.makeException(SODAMessage.EX_INVALID_PARAM_121_INDEX, "language");
      }

      // Old text index with languages, which requires a special textIndex121WithLang
      // flag to be set to true in the index spec, is not supported on 12.2 and above.
      // Note that even on 12.1.0.2, it's not to be used for production!!!
      // (see IndexSpecification.java for more info).
      if (is121TextIndexWithLang && !SODAUtils.sqlSyntaxBelow_12_2((sqlSyntaxLevel)))
        throw SODAUtils.makeException(SODAMessage.EX_TEXT_INDEX_WITH_LANG_NOT_SUPPORTED);

      if (is121TextIndexWithLang || SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel))
      {
        sqltext = buildCTXIndexDDL(indexName, language, is121TextIndexWithLang);
      }
      else
      {
        sqltext = buildDGIndexDDL(indexName, language, search_on, dataguide);
      }

    }
    else
    {
      if (isHeterogeneous())
      {
        throw SODAUtils.makeException(SODAMessage.EX_NO_FUNC_INDEX_ON_HETERO_COLLECTIONS);
      }

      sqlSyntaxLevel = SODAUtils.getSQLSyntaxLevel(conn, sqlSyntaxLevel);

      if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel) && !scalarRequired && !lax)
      {
        throw SODAUtils.makeException(SODAMessage.EX_NULL_ON_EMPTY_NOT_SUPPORTED );
      }

      if (ttl != null) {
        if (SODAUtils.sqlSyntaxBelow_19(sqlSyntaxLevel)) {
            throw SODAUtils.makeException(SODAMessage.EX_TTL_INDEX_NOT_SUPPORTED);
        }

        callCreateIndexPLSQL(indexName, indexSpec);
        return;
      }

      else {
        sqltext = buildIndexDDL(indexName, unique, scalarRequired, lax, columns, indexNulls);
      }
    }

    try
    {
      metrics.startTiming();

      if (OracleLog.isLoggingEnabled())
        log.info("Index DDL: "+sqltext);

      stmt = conn.prepareStatement(sqltext);

      stmt.execute();

      if (OracleLog.isLoggingEnabled())
        log.info("Created index "+indexName);

      stmt.close();
      stmt = null;

      metrics.recordDDL();
    }
    catch (SQLException e)
    {
      if (OracleLog.isLoggingEnabled())
        log.severe(e.toString() + "\n" + sqltext);

      if (e.getErrorCode() == ORA_SQL_OBJECT_EXISTS)
      {
        throw SODAUtils.makeException(SODAMessage.EX_INDEX_ALREADY_EXISTS, indexName);
      }
      else
      {
        throw SODAUtils.makeExceptionWithSQLText(e, sqltext);
      }
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
   * Drop the named index
   */
  private void dropIndex(String indexName, boolean force) throws OracleException
  {
    PreparedStatement stmt = null;

    if (indexName == null)
      throw SODAUtils.makeException(SODAMessage.EX_ARG_CANNOT_BE_NULL,
                                    "indexName");

    String sqltext = dropIndexDDL(indexName, force);

    try
    {
      metrics.startTiming();

      stmt = conn.prepareStatement(sqltext);

      stmt.execute();
      if (OracleLog.isLoggingEnabled())
        log.info("Dropped index "+indexName);
      stmt.close();
      stmt = null;

      metrics.recordDDL();
    }
    catch (SQLException e)
    {
      int errcode = e.getErrorCode();
      if ((errcode == ORA_SQL_OBJECT_NOT_EXISTS) ||
          (errcode == ORA_SQL_INDEX_NOT_EXISTS))
      {
        // Index doesn't exist - ignore the error.
        if (OracleLog.isLoggingEnabled())
          log.warning(e.toString());
      }
      else
      {
        if (OracleLog.isLoggingEnabled())
          log.severe(e.toString() + "\n" + sqltext);
        throw SODAUtils.makeExceptionWithSQLText(e, sqltext);
      }
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
   * Drop the named index
   * ### TODO Refactor (combine with above dropIndex)
   */
  public void dropIndexWithError(String indexName, boolean force, boolean errorOnMissing) throws OracleException
  {
    PreparedStatement stmt = null;

    if (indexName == null)
      throw SODAUtils.makeException(SODAMessage.EX_ARG_CANNOT_BE_NULL,
                                    "indexName");

    String sqltext = dropIndexDDL(indexName, force);

    try
    {
      metrics.startTiming();

      stmt = conn.prepareStatement(sqltext);

      stmt.execute();
      if (OracleLog.isLoggingEnabled())
        log.info("Dropped index "+indexName);
      stmt.close();
      stmt = null;

      metrics.recordDDL();
    }
    catch (SQLException e)
    {
      int errcode = e.getErrorCode();
      if (((errcode == ORA_SQL_OBJECT_NOT_EXISTS) ||
           (errcode == ORA_SQL_INDEX_NOT_EXISTS)) &&
          !errorOnMissing)		       
      {
        // Index doesn't exist - ignore the error.
        if (OracleLog.isLoggingEnabled())
          log.warning(e.toString());
      }
      else
      {
        if (OracleLog.isLoggingEnabled())
          log.severe(e.toString() + "\n" + sqltext);
        throw SODAUtils.makeExceptionWithSQLText(e, sqltext);
      }
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
   * Not part of a public API. Used internally by the REST layer.
   */
  public void dropIndex(OracleDocument indexSpecification)
    throws OracleException
  {
    String idxName = null;

    if (indexSpecification == null)
      throw SODAUtils.makeException(SODAMessage.EX_ARG_CANNOT_BE_NULL,
                                    "indexSpecification");

    IndexSpecification ispec =
      new IndexSpecification(db.getJsonFactoryProvider(),
              ((OracleDocumentImpl) indexSpecification).getContentAsStream());

    try
    {
      idxName = ispec.parse();
    }
    catch (QueryException e)
    {
      if (OracleLog.isLoggingEnabled())
        log.warning(e.toString());
      throw SODAUtils.makeException(SODAMessage.EX_INVALID_INDEX_DROP, e);
    }

    OracleCollectionImpl.this.dropIndex(idxName, ispec.force());
  }
  
  private OracleDocument getDataGuide() throws OracleException
  {
    OracleDocument    doc  = null;
    PreparedStatement stmt = null;
    ResultSet         rows = null;

    String sqltext = "select DBMS_JSON.GET_INDEX_DATAGUIDE(?,?,?,?) from SYS.DUAL";

    try
    {
      metrics.startTiming();

      stmt = conn.prepareStatement(sqltext);

      // get_index_dataguide requires double quotes
      // inside the name string literal, in order to 
      // interpret the table or column name literally. 
      // Otherwise, it's interpreted as an unquoted identifier,
      // e.g. might get upper-cased.

      String quotedName = options.dbObjectName;

      // ### Due to bug 23509094, quotes don't work, so for now attempt
      // ### to avoid them for uppercase alphanumeric ASCII strings.
      if (!quotedName.matches("^[_#$A-Z][_#$A-Z\\d]*$"))
        quotedName = "\"" + quotedName + "\"";

      stmt.setString(1, quotedName);

      quotedName = options.contentColumnName;

      // ### Due to bug 23509094, quotes don't work, so for now attempt
      // ### to avoid them for uppercase alphanumeric ASCII strings.
      if (!quotedName.matches("^[_#$A-Z][_#$A-Z\\d]*$"))
        quotedName = "\"" + quotedName + "\"";

      stmt.setString(2, quotedName);

      stmt.setInt(3, 1); // JSON Schema (1) versus Relational (2)
      stmt.setInt(4, 0); // Not the pretty-printed mode (which is 1)

      rows = stmt.executeQuery();

      if (rows.next())
      {
        // We get it as a CLOB because it's a temp LOB and we want to free it
        Clob tmp = rows.getClob(1);
        String tmpStr = tmp.getSubString(1L, (int)tmp.length());

        doc = new OracleDocumentImpl(tmpStr);

        tmp.free();
      }

      rows.close();
      rows = null;
      stmt.close();
      stmt = null;

      metrics.recordCall();
    }
    catch (SQLException e)
    {
      int errcode = e.getErrorCode();
      if (errcode == ORA_SQL_DATAGUIDE_NOT_EXISTS)
      {
        // No DataGuide available, null return is OK
      }
      else
      {
        if (OracleLog.isLoggingEnabled())
          log.severe(e.toString());
        throw SODAUtils.makeExceptionWithSQLText(e, sqltext);
      }
    }
    finally
    {
      for (String message : SODAUtils.closeCursor(stmt, rows))
      {
        if (OracleLog.isLoggingEnabled())
          log.severe(message);
      }
    }

    return(doc);
  }

  /***************************************************************************
   * ### SHOULD BE ON THE ADMIN CLASS?
   ***************************************************************************/

  private static final String GET_INDEX_INFO =
    "begin\n"                                       +
   "  DBMS_SODA_ADMIN.LIST_INDEXES(?, NULL, ?);\n" +
    "end;\n";

  public List<OracleDocument> listIndexes()
    throws OracleException
  {
    CallableStatement stmt    = null;
    String            sqltext = GET_INDEX_INFO;

    ArrayList<OracleDocument> results = new ArrayList<OracleDocument>();

    try
    {
      metrics.startTiming();

      stmt = conn.prepareCall(sqltext);
      stmt.setNString(1, options.uriName);
      stmt.registerOutParameter(2, Types.ARRAY, "XDB.DBMS_SODA_ADMIN.VCNTAB");

      stmt.execute();

      Array descarr = stmt.getArray(2);
      String[] descriptions  = (String[])descarr.getArray();

      for (String jsonText : descriptions)
        results.add(new OracleDocumentImpl(jsonText));

      stmt.close();
      stmt = null;

      metrics.recordCall();
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

    return results;
  }

  /**
   * OracleCollectionAdministrationImpl
   *
   * This is the RDBMS-specific implementation of OracleCollectionAdmin
   */
  private class OracleCollectionAdministrationImpl implements
    OracleCollectionAdmin
  {
    public String getName()
    {
      return OracleCollectionImpl.this.getName();
    }

    public void drop() throws OracleException
    {
      OracleCollectionImpl.this.drop();
    }

    public void drop(boolean purge, boolean dropMappedTable) throws OracleException
    {
      OracleCollectionImpl.this.drop(purge, dropMappedTable);
    }

    public void truncate() throws OracleException
    {
      OracleCollectionImpl.this.truncate();
    }

    public boolean isHeterogeneous()
    {
      return OracleCollectionImpl.this.isHeterogeneous();
    }

    public String getDBObjectName()
    {
      return OracleCollectionImpl.this.getDBObjectName();
    }

    public String getDBObjectSchemaName()
    {
      return OracleCollectionImpl.this.getDBObjectSchemaName();
    }

    public boolean isReadOnly()
    {
      return OracleCollectionImpl.this.isReadOnly();
    }

    public boolean usesObjectIdKeys()
    {
      return OracleCollectionImpl.this.usesObjectIdKeys();
    }

    /**
     * Returns a JSON description of the collection.
     *
     * @return                         JSON description of the collection
     */
    public OracleDocument getMetadata()
    {
      OracleDocument result = new OracleDocumentImpl(options.getDescription());
      return(result);
    }

    /**
     * Returns a JSON schema for documents in the collection.
     *
     * @return       JSON schema for the collection, or null if unknown
     */
    public OracleDocument getDataGuide() throws OracleException
    {
      OracleDocument result = OracleCollectionImpl.this.getDataGuide();
      return(result);
    }

    public void createJsonSearchIndex(String indexName) throws OracleException
    {
      OracleCollectionImpl.this.createIndex(indexName, false, false, false,
                                            false, null, null, null, null,
                                            null, false, null, false, null);
    }

    public void createIndex(OracleDocument indexSpecification)
      throws OracleException
    {
      String idxName = null;

      if (indexSpecification == null)
        throw SODAUtils.makeException(SODAMessage.EX_ARG_CANNOT_BE_NULL,
                                      "indexSpecification");
 
      IndexSpecification ispec =
      new IndexSpecification(db.getJsonFactoryProvider(),
               ((OracleDocumentImpl) indexSpecification).getContentAsStream());

      try
      {
        idxName = ispec.parse();
      }
      catch (QueryException e)
      {
        if (OracleLog.isLoggingEnabled())
          log.warning(e.toString());
        throw SODAUtils.makeException(SODAMessage.EX_INVALID_INDEX_CREATE, e);
      }

      OracleCollectionImpl.this.createIndex(idxName,
        ispec.isUnique(),
        ispec.isScalarRequired(),
        ispec.isLax(),
        ispec.indexNulls(),
        ispec.getColumns(),
        ispec.getSpatialPath(),
        ispec.getLanguage(),
        ispec.getSearchOn(),
        ispec.getDataGuide(),
        ispec.is121TextIndexWithLang(),
        ispec.getTTL(),
        ispec.getMultiValue(),
        indexSpecification);
    }

    public void dropIndex(String indexName)
      throws OracleException
    {
      OracleCollectionImpl.this.dropIndex(indexName, false);
    }

    public void dropIndex(String indexName, boolean force)
            throws OracleException
    {
      OracleCollectionImpl.this.dropIndex(indexName, force);
    }
  }
}
