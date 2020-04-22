/* Copyright (c) 2014, 2020, Oracle and/or its affiliates. 
All rights reserved.*/

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

import java.math.BigDecimal;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.security.NoSuchAlgorithmException;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Logger;

import oracle.json.common.MetricsCollector;
import oracle.json.logging.OracleLog;
import oracle.json.parser.IndexColumn;
import oracle.json.parser.IndexSpecification;
import oracle.json.parser.JsonPath;
import oracle.json.parser.JsonQueryPath;
import oracle.json.parser.QueryException;
import oracle.json.util.ByteArray;
import oracle.json.util.HashFuncs;
import oracle.json.util.JsonByteArray;
import oracle.soda.OracleCollection;
import oracle.soda.OracleCollectionAdmin;
import oracle.soda.OracleDocument;
import oracle.soda.OracleException;
import oracle.soda.OracleOperationBuilder;

public abstract class OracleCollectionImpl implements OracleCollection
{
  protected static final Logger log =
    Logger.getLogger(OracleCollectionImpl.class.getName());

  static ArrayList<OracleDocument> EMPTY_LIST = new ArrayList<OracleDocument>();

  static byte[] EMPTY_DATA = new byte[0];

  private static final int ORA_SQL_DATAGUIDE_NOT_EXISTS = 40582;

  protected final String collectionName;
  protected final Connection conn;
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
   * Returns true if the collection allows client-assigned keys.
   * Not part of a public API.
   */
  public boolean hasClientAssignedKeys()
  {
    return (options.keyAssignmentMethod ==
            CollectionDescriptor.KEY_ASSIGN_CLIENT);
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
            (options.versioningMethod == CollectionDescriptor.VERSION_NONE));
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
        byte[] md5 = HashFuncs.MD5(data);

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
        byte[] sha = HashFuncs.SHA256(data);

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

  protected byte[] convertToBinary(byte[] data) throws OracleException
  {
    byte[] binary = null;

    if ((data != null) && (data.length != 0))
      binary = db.textToBinary(data);

    return binary;
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
  void addFormat(StringBuilder sb)
  {
    if (options.hasBinaryFormat())
      sb.append(" format oson");
    // Append the format clause for binary types
    else if ((options.contentDataType == CollectionDescriptor.BLOB_CONTENT) ||
             (options.contentDataType == CollectionDescriptor.RAW_CONTENT))
      sb.append(" format json");
  }

  private String buildSpatialIndexDDL(String indexName, JsonQueryPath spatial,
                                      boolean scalarRequired, boolean lax)
  {
    sb.setLength(0);

    sb.append("create index \"");
    sb.append(indexName);
    sb.append("\" on ");
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
      sb.append("create index \"");
      sb.append(indexName);
      sb.append("\" on ");
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

      sb.append("create index \"");
      sb.append(indexName);
      sb.append("\" on ");
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

    // Default data guide type
    if (search_on == null)
      search_on = "text_value";

    // By default, dataguide is "on"
    if (dataguide == null)
      dataguide = "on";

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

    sb.append("create search index \"");
    sb.append(indexName);
    sb.append("\" on ");
    appendTable(sb);
    sb.append(" (\"");
    sb.append(options.contentColumnName);
    sb.append("\") ");
    sb.append("for json");

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

    return (sb.toString());
  }

  /**
   * Build the drop DDL for an index
   */
  private String dropIndexDDL(String indexName, boolean forceFlag)
  {
    sb.setLength(0);
    sb.append("drop index \"");
    // Assumed to a valid identifier (already passed
    // through CollectionDescriptor.stringToIdentifier())
    sb.append(indexName);
    sb.append("\"");
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
    sb.append("index \"");
    // Assumed to a valid identifier (already passed
    // through CollectionDescriptor.stringToIdentifier())
    sb.append(indexName);
    sb.append("\" on ");
    appendTable(sb);
    sb.append(" (");
    
    boolean first = true;
    int numCharCols = 0;
    int numCharColsLen = 0;
    for (JsonPath column : columns)
    {
      String sqlTypeName = null;
      int    sqlType     = IndexColumn.SQLTYPE_NONE;
      int    maxLength   = 0;
      String sqlOrder    = null;

      if (column instanceof IndexColumn)
      {
        sqlTypeName = ((IndexColumn)column).getSqlTypeName();
        sqlType = ((IndexColumn)column).getSqlType();
        maxLength = ((IndexColumn)column).getMaxLength();
        sqlOrder = ((IndexColumn)column).getOrder();
      }

      String[] steps = column.getSteps();
      if (first)
        first = false;
      else
        sb.append(", ");

      // Signals that we shouldn't bother with a RETURNING clause
      boolean renderReturning = false;

      if (sqlTypeName != null)
        renderReturning = true;

      sb.append("JSON_VALUE(\"");
      sb.append(options.contentColumnName);
      sb.append("\"");
      addFormat(sb);
      sb.append(",\'");

      // Use JsonQueryPath to centralize the singleton string builder
      JsonQueryPath jqp = new JsonQueryPath(steps);
      jqp.toSingletonString(sb);

      if (jqp.hasArraySteps())
      {
        throw SODAUtils.makeException(SODAMessage.EX_ARRAY_STEPS_IN_PATH);
      }

      sb.append("\'");

      if (renderReturning)
      {
        sb.append(" returning ");
        sb.append(sqlTypeName);
        if (sqlType == (IndexColumn.SQLTYPE_CHAR))
        {
          if (numCharCols + 1 > IndexColumn.MAX_CHAR_COLUMNS)
            throw SODAUtils.makeException(SODAMessage.EX_TOO_MANY_COLUMNS,
                      Integer.toString(numCharCols));
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

  /**
   * Create an index
   */
  // ### Consider alternative (or additional) functionality, where
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
                           boolean is121TextIndexWithLang)
          throws OracleException
  {
    PreparedStatement stmt = null;

    if (indexName == null)
      throw SODAUtils.makeException(SODAMessage.EX_ARG_CANNOT_BE_NULL,
                                    "indexName");

    indexName = CollectionDescriptor.stringToIdentifier(indexName);

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

      sqltext = buildIndexDDL(indexName, unique, scalarRequired, lax, columns, indexNulls);
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
        log.warning(e.toString());

      if (e.getErrorCode() == ORA_SQL_OBJECT_EXISTS)
      {
        throw SODAUtils.makeException(SODAMessage.EX_INDEX_ALREADY_EXISTS);
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

    indexName = CollectionDescriptor.stringToIdentifier(indexName);

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
          log.warning(e.toString());
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
      // inside the table name string literal, in order to 
      // interpret the table name literally. Otherwise,
      // it's interpreted as an unquoted identifier,
      // e.g. might get upper-cased.
      String quotedName = options.dbObjectName;

      // ### Due to bug 23509094, quotes don't work, so for now attempt
      // ### to avoid them for uppercase alphanumeric ASCII strings.
      if (!quotedName.matches("^[_#$A-Z][_#$A-Z\\d]*$"))
        quotedName = "\"" + quotedName + "\"";

      stmt.setString(1, quotedName);
      stmt.setString(2, options.contentColumnName);
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
          log.warning(e.toString());
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

    public void truncate() throws OracleException
    {
      OracleCollectionImpl.this.truncate();
    }

    public boolean isHeterogeneous()
    {
      return OracleCollectionImpl.this.isHeterogeneous();
    }

    public boolean isReadOnly()
    {
      return OracleCollectionImpl.this.isReadOnly();
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
                                            null, false);
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
        ispec.is121TextIndexWithLang());
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
