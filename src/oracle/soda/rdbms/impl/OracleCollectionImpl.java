/* Copyright (c) 2014, 2015, Oracle and/or its affiliates. 
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

import java.nio.charset.Charset;
import java.nio.charset.CharacterCodingException;

import java.math.BigDecimal;

import java.security.NoSuchAlgorithmException;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.ArrayList;

import oracle.json.logging.OracleLog;
import oracle.json.parser.JsonPath;
import oracle.json.parser.IndexColumn;

import java.util.logging.Logger;

import oracle.jdbc.OracleConnection;

import oracle.json.common.MetricsCollector;

import oracle.json.util.HashFuncs;
import oracle.json.util.ByteArray;
import oracle.json.util.JsonByteArray;

import oracle.json.parser.IndexSpecification;
import oracle.json.parser.QueryException;

import oracle.soda.OracleException;
import oracle.soda.OracleDocument;
import oracle.soda.OracleCollection;
import oracle.soda.OracleCollectionAdmin;
import oracle.soda.OracleOperationBuilder;

public abstract class OracleCollectionImpl implements OracleCollection
{
  protected static final Logger log =
    Logger.getLogger(OracleCollectionImpl.class.getName());

  static ArrayList<OracleDocument> EMPTY_LIST = new ArrayList<OracleDocument>();

  static byte[] EMPTY_DATA = new byte[0];

  protected final String collectionName;
  protected final OracleConnection conn;
  protected final OracleDatabaseImpl db;
  protected final MetricsCollector metrics;
  protected final CollectionDescriptor options;

  private OracleCollectionAdministrationImpl admin;

  protected StringBuilder sb = new StringBuilder(SODAConstants.SQL_STATEMENT_SIZE);

  private final static int ORA_SQL_OBJECT_EXISTS = 955;
  private final static int ORA_SQL_OBJECT_NOT_EXISTS = 942;
  private final static int ORA_SQL_INDEX_NOT_EXISTS = 1418;

  // This is a work-around for the problem that inside the RDBMS
  // the SQL "returning" clause simply isn't supported.
  // This also triggers avoidance of a 32k limit on setBytes/setString.
  protected boolean internalDriver = false;

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

  // ### Inefficiently convert a hex string to a decimal number
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
        key = zeros.substring(1, xlen - len) + key;
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
    // For sequence-assigned keys
    else if (options.keyAssignmentMethod ==
            CollectionDescriptor.KEY_ASSIGN_SEQUENCE)
    {
      key = zeroStrip(key);
      if (!isInteger(key))
        throw SODAUtils.makeException(SODAMessage.EX_INVALID_KEY,
                                      key,
                                      "INTEGER");
    }
    return (key);
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

  CollectionDescriptor getOptions()
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
  private void addFormat(StringBuilder sb)
  { 
    //Append the format clause for binary types
    if ((options.contentDataType == CollectionDescriptor.BLOB_CONTENT) ||
        (options.contentDataType == CollectionDescriptor.RAW_CONTENT))
      sb.append(" format json");
  }

  private String buildCTXIndexDDL(String indexName, String language)
          throws OracleException
  { 
    sb.setLength(0);

    sb.append("create index \"");
    sb.append(indexName);
    sb.append("\" on ");
    appendTable(sb);
    sb.append(" (\"");
    sb.append(options.contentColumnName);
    sb.append("\") ");
    sb.append("indextype is ctxsys.context parameters(");

    sb.append("'section group CTXSYS.JSON_SECTION_GROUP ");

    // Append the lexer for this language
    try
    {
      sb.append("lexer ");
      sb.append(IndexSpecification.getLexer(language));
      sb.append(" ");
    }
    catch (QueryException e)
    {
      throw SODAUtils.makeException(SODAMessage.EX_INVALID_INDEX_CREATE, e);
    }

    sb.append("stoplist CTXSYS.EMPTY_STOPLIST ");

    sb.append("sync (on commit) ");
    sb.append("memory 100M");
    sb.append("') parallel 8");
    return (sb.toString());
  }

  /**
   * Build the drop DDL for an index
   */
  private String dropIndexDDL(String indexName)
  {
    sb.setLength(0);
    sb.append("drop index \"");
    // Assumed to a valid identifier (already passed
    // through CollectionDescriptor.stringToIdentifier())
    sb.append(indexName);
    sb.append("\"");
    return(sb.toString());
  }

  /**
   * Build the DDL for an index
   */
  private String buildIndexDDL(String indexName,
                               boolean unique,
                               boolean singleton,
                               JsonPath[] columns)
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
      sb.append(",\'$");
      for (String step : steps)
      {
        if (step.charAt(0) != '[') // Silently ignore array steps.
        {
          sb.append(".");
          sb.append(step); // Assumes necessary escaping/double-quoting
                           // has been done
          sb.append("[0]");
        }
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

      if (singleton)
        sb.append(" ERROR ON ERROR");

      sb.append(") ");

      if (sqlOrder != null)
        sb.append(sqlOrder);
    }
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

  /**
   * Create a concatenated index on an array of paths.
   * The RDBMS implementation accepts an IndexColumn[] array as well.
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
                           boolean singleton,
                           JsonPath[] columns, 
                           String language)
          throws OracleException
  {
    PreparedStatement stmt = null;

    if (indexName == null)
      throw SODAUtils.makeException(SODAMessage.EX_ARG_CANNOT_BE_NULL,
                                    "indexName");

    indexName = CollectionDescriptor.stringToIdentifier(indexName);

    String sqltext = null;

    if (columns == null || columns.length == 0)
    {

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

      sqltext = buildCTXIndexDDL(indexName, language);
    }
    else
    {
      sqltext = buildIndexDDL(indexName, unique, singleton, columns);
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
      if (e.getErrorCode() == ORA_SQL_OBJECT_EXISTS)
      {
        // Index already exists - ignore the error.
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
   * Drop the named index
   */
  private void dropIndex(String indexName) throws OracleException
  {
    PreparedStatement stmt = null;

    if (indexName == null)
      throw SODAUtils.makeException(SODAMessage.EX_ARG_CANNOT_BE_NULL,
                                    "indexName");

    indexName = CollectionDescriptor.stringToIdentifier(indexName);

    String sqltext = dropIndexDDL(indexName);

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
      new IndexSpecification(((OracleDocumentImpl) indexSpecification).getContentAsStream());

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

    OracleCollectionImpl.this.dropIndex(idxName);
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

    public void indexAll(String indexName) throws OracleException
    {
      OracleCollectionImpl.this.createIndex(indexName, false, false, null, null);
    }

    public void indexAll(String indexName, String language) 
      throws OracleException
    {
      OracleCollectionImpl.this.createIndex(indexName, false, false, null, language);
    }

    public void createIndex(OracleDocument indexSpecification)
      throws OracleException
    {
      String idxName = null;
       
      if (indexSpecification == null)
        throw SODAUtils.makeException(SODAMessage.EX_ARG_CANNOT_BE_NULL,
                                      "indexSpecification");
 
      IndexSpecification ispec =
        new IndexSpecification(((OracleDocumentImpl) indexSpecification).getContentAsStream());

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
                                            ispec.isSingleton(),
                                            ispec.getColumns(), 
                                            ispec.getLanguage());
    }

    public void dropIndex(String indexName)
      throws OracleException
    {
      OracleCollectionImpl.this.dropIndex(indexName);
    }
  }
}
