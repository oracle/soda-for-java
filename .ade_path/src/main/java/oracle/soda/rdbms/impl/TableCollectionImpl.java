/* Copyright (c) 2014, 2024, Oracle and/or its affiliates.*/
/* All rights reserved.*/

/*
   DESCRIPTION
    This is the RDBMS-specific implementation of OracleCollection
    for collections based on a table or view.
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
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.BatchUpdateException;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import java.time.Instant;

import oracle.json.sodacommon.DocumentCodec;
import oracle.json.sodacommon.LobInputStream;
import oracle.json.logging.OracleLog;
import oracle.json.sodautil.ByteArray;
import oracle.json.sodautil.ComponentTime;
import oracle.json.sodautil.LimitedInputStream;
import oracle.json.sodautil.Pair;
import oracle.soda.OracleBatchException;
import oracle.soda.OracleDocument;
import oracle.soda.OracleException;

public class TableCollectionImpl extends OracleCollectionImpl
{
  private static final int    MAX_RANGE_TRANSFER = 1024*1024; // 1Mbyte
  private static final int    MIN_RANGE_TRANSFER = 4*1024;    // 4Kbyte

  // JDBC batching batch size
  private static final int    BATCH_MAX_SIZE = 1000;

  TableCollectionImpl(OracleDatabaseImpl db, String name)
  {
    super(db, name);
  }

  TableCollectionImpl(OracleDatabaseImpl db,
                      String name,
                      CollectionDescriptor options)
  {
    super(db, name, options);
  }

  private String buildSelectForUpsert(String hint)
  {
    sb.setLength(0);

    boolean append = false;

    sb.append("select ");
    //### Do we need to append hint here?
    if (hint != null)
      sb.append("/*+ " + hint + " */ ");


    if (options.hasMaterializedEmbeddedID())
    {
      sb.append("\"");
      sb.append(options.keyColumnName);
      sb.append("\"");

      append = true;
    }

    if (options.creationColumnName != null)
    {
      if (append)
      {
        sb.append(", \"");
      }
      else
      {
        sb.append("\"");
      }

      sb.append(options.creationColumnName);
      sb.append('\"');

      append = true;
    }

    if (returnVersion())
    {
      if (append)
      {
        sb.append(", \"");
      }
      else
      {
        sb.append("\"");
      }

      sb.append(options.versionColumnName);
      sb.append("\"");
    }

    addFrom(sb);

    addWhereKey(sb, false, options.hasMaterializedEmbeddedID());

    return(sb.toString());
  }

  boolean returnVersion()
  {
    if (options.versionColumnName != null &&
        (options.versioningMethod == CollectionDescriptor.VERSION_NONE ||
         options.versioningMethod == CollectionDescriptor.VERSION_SEQUENTIAL))
    {
      return true;
    }

    return false;
  }

  private String buildQuery()
  {
    sb.setLength(0);

    sb.append("select ");

    // Key is always returned as a string
    switch (options.keyDataType)
    {
    case CollectionDescriptor.INTEGER_KEY:
      sb.append("to_char(\"");
      sb.append(options.keyColumnName);
      sb.append("\")");
      break;
    case CollectionDescriptor.RAW_KEY:
      sb.append("rawtohex(\"");
      sb.append(options.keyColumnName);
      sb.append("\")");
      break;
    case CollectionDescriptor.STRING_KEY:
    case CollectionDescriptor.NCHAR_KEY:
      sb.append("\"");
      sb.append(options.keyColumnName);
      sb.append("\"");
      break;
    }

    if (options.doctypeColumnName != null)
    {
      sb.append(",\"");
      sb.append(options.doctypeColumnName);
      sb.append("\"");
    }

    sb.append(",\"");
    sb.append(options.contentColumnName);
    sb.append("\"");

    if (options.timestampColumnName != null)
    {
      sb.append(",\"");
      sb.append(options.timestampColumnName);
      sb.append('\"');
    }

    if (options.creationColumnName != null)
    {
      sb.append(",\"");
      sb.append(options.creationColumnName);
      sb.append('\"');
    }

    if (options.versionColumnName != null)
    {
      sb.append(",\"");
      sb.append(options.versionColumnName);
      sb.append("\"");
    }

    addFrom(sb);

    // Add bind variable for single-key select (as a string)
    addWhereKey(sb, false, false);

    return(sb.toString());
  }

  private void addFrom(StringBuilder sb)
  {
    sb.append(" from \"");
    sb.append(options.dbObjectName);
    sb.append("\"");
  }

  private boolean returnInsertedTime()
  {
     return ((options.timestampColumnName != null) ||
             (options.creationColumnName != null));
  }

  private boolean returnInsertedKey()
  {
    return ((options.keyAssignmentMethod ==
             CollectionDescriptor.KEY_ASSIGN_GUID) ||
            (options.keyAssignmentMethod ==
             CollectionDescriptor.KEY_ASSIGN_SERVER) ||
            (options.keySequenceName != null) ||
            (options.hasMaterializedEmbeddedID()));
  }

  private boolean returnInsertedVersion()
  {
    return ((options.versionColumnName != null) &&
            (options.versioningMethod == CollectionDescriptor.VERSION_NONE));
  }

  private boolean insertHasReturnClause(boolean disableReturning)
  {
    return (!disableReturning && (returnInsertedKey() ||
                                  returnInsertedTime() ||
                                  returnInsertedVersion()));
  }

  static void addInto(StringBuilder sb, int count)
  {
    if (count > 0)
    {
      sb.append(" into ?");

      for (int i = 1; i < count; i++)
      {
        sb.append(", ?");
      }
    }
  }

  static void addComma(StringBuilder sb, int count)
  {
    if (count > 0)
    {
      sb.append(", ");
    }
  }

  private void addJsonTransformForID(StringBuilder sb)
  {
    // Injecting as eJSON for now, because we want the value to be marked as ID
    // in OSON (as opposed to just a raw buffer). Once RDBMS "as ID" oson construction
    // is available, it can replace the eJSON here.
    sb.append("json_transform(?, set '$._id' = " );
    sb.append(" oson(" );
    sb.append(" '{\"$rawid\": \"' || " );
    sb.append("      ltrim(" );
    sb.append("        to_char(" );
    sb.append("          mod(" );
    sb.append("            floor(" );
    sb.append("              (to_date(to_char(sys_extract_utc(systimestamp), 'YYYY-MM-DD\"T\"HH24:MI:SS')," );
    sb.append("                       'YYYY-MM-DD\"T\"HH24:MI:SS') - to_date('1970-01-01','YYYY-MM-DD'))*24*60*60)," );
    sb.append("            power(2,32))," );
    sb.append("         'xxxxxxxx'), ' ') || " );
    sb.append("      lower(" );
    sb.append("        substr(rawtohex(sys_guid()),32-5) ||" );
    sb.append("        substr(rawtohex(sys_guid()),13,4) ||" );
    sb.append("        substr(rawtohex(sys_guid()),7,6))" );
    sb.append(" || '\"}'" );
    sb.append(" extended) format oson ignore on existing)" );
  }

  /**
   * Build one of two variants of INSERT to the collection.
   * The base version may have a RETURNING clause for server-generated
   * GUID or sequence keys, and/or the optional Last-Modified timestamp.
   * The batch version disables the RETURNING clause, obliging the
   * caller to supply those values from the server.
   */
  private String buildInsert(boolean disableReturning, String hint, Boolean eJSON)
  {
    sb.setLength(0);

    // Make this a callable statement for non-Oracle drivers
    if (insertHasReturnClause(disableReturning) && useCallableReturns)
      sb.append("begin\n");

    sb.append("insert");
    if (hint != null)
      sb.append(" /*+ " + hint + " */");
    sb.append(" into ");
    appendTable(sb);
    sb.append(" (\"");
    if ((options.keyAssignmentMethod != CollectionDescriptor.KEY_ASSIGN_SERVER) &&
	     !options.hasMaterializedEmbeddedID()) {
      sb.append(options.keyColumnName);
      sb.append("\",\"");
    }
    if (options.doctypeColumnName != null)
    {
      sb.append(options.doctypeColumnName);
      sb.append("\",\"");
    }
    sb.append(options.contentColumnName);
    sb.append("\"");

    if (options.timestampColumnName != null)
    {
      sb.append(",\"");
      sb.append(options.timestampColumnName);
      sb.append("\"");
    }
    
    if (options.creationColumnName != null)
    {
      sb.append(",\"");
      sb.append(options.creationColumnName);
      sb.append("\"");
    }

    // Bind version column only if versioning method specified
    if ((options.versionColumnName != null) &&
        (options.versioningMethod) != CollectionDescriptor.VERSION_NONE)
    {
      sb.append(",\"");
      sb.append(options.versionColumnName);
      sb.append("\"");
    }

    sb.append(") values (");

    // Assign from a named server sequence
    if ((options.keySequenceName != null) && (!disableReturning))
    {
      switch (options.keyDataType)
      {
      case CollectionDescriptor.INTEGER_KEY:
        sb.append("\"");
        sb.append(options.keySequenceName);
        sb.append("\".NEXTVAL");
        break;
      case CollectionDescriptor.RAW_KEY:
        sb.append("hextoraw(substr(to_char(\"");
        sb.append(options.keySequenceName);
        sb.append("\".NEXTVAL,'0XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'),3))");
        break;
      case CollectionDescriptor.STRING_KEY:
      case CollectionDescriptor.NCHAR_KEY:
      default:
        sb.append("to_char(\"");
        sb.append(options.keySequenceName);
        sb.append("\".NEXTVAL)");
        break;
      }
    }
    // Assign a GUID on the server
    else if ((options.keyAssignmentMethod ==
              CollectionDescriptor.KEY_ASSIGN_GUID) && (!disableReturning))
    {
      switch (options.keyDataType)
      {
      case CollectionDescriptor.INTEGER_KEY:
        sb.append("to_number(");
        sb.append("rawtohex(SYS_GUID()),");
        sb.append("'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')");
        break;
      case CollectionDescriptor.RAW_KEY:
        sb.append("SYS_GUID()");
        break;
      case CollectionDescriptor.STRING_KEY:
      case CollectionDescriptor.NCHAR_KEY:
      default:
        sb.append("rawtohex(SYS_GUID())");
        break;
      }
    }
    // Assignment is from the client, or from middle-tier UUID
    else if ((options.keyAssignmentMethod != CollectionDescriptor.KEY_ASSIGN_SERVER) &&
             !(options.hasMaterializedEmbeddedID()))
    {
      addKey(sb);
    }

    if (options.doctypeColumnName != null)
    {
      // content type
      sb.append(",?");
    }

    // Content column
    if (eJSON)
    {
      if (options.hasBinaryFormat())
         sb.append(",OSON(? EXTENDED)");
      else if (options.hasJsonType())
         sb.append(",JSON(? EXTENDED)");
      //### TODO BOV
    }
    else
    {
      if (options.isDualityView())
        sb.append("?");
      else if (options.hasMaterializedEmbeddedID())
        if (OracleCollectionImpl.NO_JSON_TRANSFORM || options.isNative())
          sb.append("?");
        else
          addJsonTransformForID(sb);
      else
        sb.append(",?");
    }

    // Timestamp is always generated on the server
    if (options.timestampColumnName != null)
    {
      if (disableReturning)
        OracleDatabaseImpl.addToTimestamp(",", sb);
      else
        sb.append(",sys_extract_utc(SYSTIMESTAMP)");
    }

    // Timestamp is always generated on the server
    if (options.creationColumnName != null)
    {
      if (disableReturning)
        OracleDatabaseImpl.addToTimestamp(",", sb);
      else
        sb.append(",sys_extract_utc(SYSTIMESTAMP)");
    }

    // Version is always supplied (even if numeric)
    if ((options.versionColumnName != null) &&
        (options.versioningMethod) != CollectionDescriptor.VERSION_NONE)
      sb.append(",?");

    sb.append(")");

    // The problem with the RETURNING clause is that it doesn't
    // work with JDBC statement batching. This flag disables
    // returning and presumes that all values are driven down from
    // bind variables.
    if (insertHasReturnClause(disableReturning))
    {
      sb.append(" returning ");

      int count = 0;

      if (returnInsertedKey())
      {
        if (options.isDualityView())
           sb.append("rawtohex(");
        sb.append("\"");
        sb.append(options.keyColumnName);
        sb.append("\"");
        if (options.isDualityView())
           sb.append(")");

        ++count;
      }

      if (returnInsertedTime())
      {
        addComma(sb, count);

        sb.append("\"");
        // Only last-mod timestamp or creation time needs
        // to be returned, as they are the same in the
        // case of insert.
        if (options.timestampColumnName != null)
          sb.append(options.timestampColumnName);
        else
          sb.append(options.creationColumnName);

        sb.append('"');

        ++count;
      }

      if (returnInsertedVersion())
      {
        addComma(sb, count);

        if (options.isDualityView())
           sb.append("rawtohex(");
        sb.append("\"");
        sb.append(options.versionColumnName);
        sb.append("\"");
        if (options.isDualityView())
             sb.append(")");

        ++count;
      }

      // Return a "flag" column that is never NULL
      if (useCallableReturns)
      {
        addComma(sb, count);
        sb.append("'1'");
        ++count;
      }

      addInto(sb, count);
    }

    // Make this a callable statement for non-Oracle drivers
    if (insertHasReturnClause(disableReturning) && useCallableReturns)
      sb.append(";\nend;\n");

    return(sb.toString());
  }

  private void addWhereKey(StringBuilder sb, boolean gtKey, boolean materializedEmbeddedId)
  {
    sb.append(" where \"");
    sb.append(options.keyColumnName);
    sb.append(gtKey ? "\" > " : "\" = ");
    if (materializedEmbeddedId) 
      sb.append("json_value(?, '$._id' returning any ora_rawcompare error on error)");
    else
      addKey(sb);
  }

  void addKey(StringBuilder sb)
  {
    switch (options.keyDataType)
    {
    case CollectionDescriptor.INTEGER_KEY:
      sb.append("to_number(?)");
      break;
    case CollectionDescriptor.RAW_KEY:
      // Assumes caller will bind with setBytes()
      sb.append("?");
      break;
    case CollectionDescriptor.STRING_KEY:
    case CollectionDescriptor.NCHAR_KEY:
    default:
      sb.append("?");break;
    }
  }

  private String buildUpsert(String hint)
  {
    sb.setLength(0);

    sb.append("merge ");
    if (hint != null)
      sb.append("/*+ " + hint + " */ ");
    sb.append("into ");
    appendTable(sb);
    sb.append(" JSON$TARGET using (select ");
    if (options.hasMaterializedEmbeddedID())
      sb.append("json_value(?, '$._id' returning any ora_rawcompare error on error) \"");
    else
      sb.append(" ? \"");
    sb.append(options.keyColumnName);
    sb.append("\" from SYS.DUAL) JSON$SOURCE");

    sb.append(" on (JSON$TARGET.\"");
    sb.append(options.keyColumnName);
    sb.append("\" = JSON$SOURCE.\"");
    sb.append(options.keyColumnName);
    sb.append("\")");

    sb.append(" when matched then update set JSON$TARGET.\"");
    sb.append(options.contentColumnName);
    sb.append("\" = ?");

    if (options.timestampColumnName != null)
    {
      sb.append(", JSON$TARGET.\"");
      sb.append(options.timestampColumnName);
      OracleDatabaseImpl.addToTimestamp("\" = ", sb);
    }

    if ((options.versionColumnName != null) &&
        (options.versioningMethod) != CollectionDescriptor.VERSION_NONE)
    {
      sb.append(", JSON$TARGET.\"");
      sb.append(options.versionColumnName);
      sb.append("\" = ");
      if (options.versioningMethod == CollectionDescriptor.VERSION_SEQUENTIAL)
      {
        sb.append("(JSON$TARGET.\"");
        sb.append(options.versionColumnName);
        sb.append("\" + 1)");
      }
      else
      {
        sb.append("?");
      }
    }

    if (options.doctypeColumnName != null)
    {
      sb.append(", JSON$TARGET.\"");
      sb.append(options.doctypeColumnName);
      sb.append("\" = ?");
    }

    sb.append(" when not matched then insert (");
    if (!options.hasMaterializedEmbeddedID())
    {
      sb.append("JSON$TARGET.\"");
      sb.append(options.keyColumnName);
      sb.append("\",JSON$TARGET.\"");
    }
    else
      sb.append("JSON$TARGET.\"");

    sb.append(options.contentColumnName);
    sb.append("\"");

    if (options.timestampColumnName != null)
    {
      sb.append(",JSON$TARGET.\"");
      sb.append(options.timestampColumnName);
      sb.append("\"");
    }

    if (options.creationColumnName != null)
    {
      sb.append(",JSON$TARGET.\"");
      sb.append(options.creationColumnName);
      sb.append("\"");
    }

    if ((options.versionColumnName != null) &&
        (options.versioningMethod) != CollectionDescriptor.VERSION_NONE)
    {
      sb.append(",JSON$TARGET.\"");
      sb.append(options.versionColumnName);
      sb.append("\"");
    }

    if (options.doctypeColumnName != null)
    {
      sb.append(",JSON$TARGET.\"");
      sb.append(options.doctypeColumnName);
      sb.append("\"");
    }

    sb.append(") values (");
    if (!options.hasMaterializedEmbeddedID())
      sb.append("?,?");
    else if (OracleCollectionImpl.NO_JSON_TRANSFORM)
      sb.append("?");
    else
      addJsonTransformForID(sb);
    
    if (options.timestampColumnName != null)
      OracleDatabaseImpl.addToTimestamp(",", sb);
    if (options.creationColumnName != null)
      OracleDatabaseImpl.addToTimestamp(",", sb);
    if ((options.versionColumnName != null) &&
        (options.versioningMethod) != CollectionDescriptor.VERSION_NONE)
      sb.append(",?");
    if (options.doctypeColumnName != null)
      sb.append(",?");
    sb.append(")");

    return(sb.toString());
  }

  void setStreamBind(PreparedStatement stmt, OracleDocument document, int num)
    throws OracleException, SQLException
  {

    // This exception should never occur, since streamContent
    // is only true if the collection is heterogeneous (which
    // requires blob content).
    if (options.contentDataType != CollectionDescriptor.BLOB_CONTENT)
      throw SODAUtils.makeException(SODAMessage.EX_UNSUPPORTED_MODE,
                                    options.uriName,
                                    options.getContentDataType());

    // This means it needs to be streamed without materializing
    InputStream dataStream = ((OracleDocumentImpl) document).getContentAsStream();

    long nbytes = -1L;
    if (dataStream instanceof LimitedInputStream)
    {
      // Available will report the total length
      nbytes = ((LimitedInputStream)dataStream).availableLong();
    }

    if (nbytes == 0)
      // ### Is VARBINARY type best for a BLOB Column?
      stmt.setNull(num, Types.VARBINARY);
    else if (nbytes > 0)
      stmt.setBlob(num, dataStream, nbytes);
    // ### Not clear what kind of binding this will use under the covers:
    //     LOB or stream. If it uses LOB, is stream binding
    //     (i.e. setBinaryStream()) a better option?
    else // Total length is unknown
      stmt.setBlob(num, dataStream);
  }

  public void insert(OracleDocument document) throws OracleException
  {
    checkJDBCVersion();
    // ### This can be made more efficient then
    //     simply calling saveAndGet(...), since the "Get"
    //     part is not required.
    insertAndGet(document);
  }

  /**
   * Insert a new row into a collection. Returns the key assigned.
   */
  public OracleDocument insertAndGet(OracleDocument document)
    throws OracleException
  {
    checkJDBCVersion();
    return insertAndGet(document, null);
  }
  
  /**
   * Insert a new row into a collection. Returns the key assigned.
   */
  public OracleDocument insertAndGet(OracleDocument document,
                                     Map<String, ?> insertOrSaveOptions)
    throws OracleException
  {
    checkJDBCVersion();
    if (document == null)
    {
      throw SODAUtils.makeException(SODAMessage.EX_ARG_CANNOT_BE_NULL,
                                    "document");
    }
    String hintStr = getHintString(insertOrSaveOptions);
    Boolean eJSON = getEJSONBoolean(insertOrSaveOptions);

    if (eJSON && !options.hasBinaryFormat() && !options.hasJsonType())
      throw SODAUtils.makeException(SODAMessage.EX_EJSON_CANNOT_BE_USED);

    if (OracleDocumentImpl.isBinary(document) && eJSON)
      throw SODAUtils.makeException(SODAMessage.EX_EJSON_CANNOT_BE_USED_WITH_BINARY_DOC);

    writeCheck("insert");

    // Get the document's key, generating if necessary
    Pair<String, Object> docKeyAndContentPayload = null;
    String dockey = null;
    Object contentWithInjectedKey = null;

    if (!options.hasMaterializedEmbeddedID())
    {
      docKeyAndContentPayload = getDocumentKey(document, true, eJSON);
      dockey = docKeyAndContentPayload.getFirst();
      contentWithInjectedKey = docKeyAndContentPayload.getSecond();
    }

    // If it has a key but should not, it's an error
    if ((dockey != null) && hasExtrinsicServerKey())
    {
      throw SODAUtils.makeException(SODAMessage.EX_INPUT_DOC_HAS_KEY);
    }

    PreparedStatement stmt = null;

    byte[]      dataBytes = EMPTY_DATA;

    String key = null;
    String version = null;
    String tstamp = null;
    
    boolean disableReturning = internalDriver;

    // Disable use of DML RETURNING clauses for non-Oracle drivers unless
    // the non-Oracle driver supports callable statements as an alternative.
    if (!oracleDriver && !useCallableReturns)
      disableReturning = true;

    switch (options.keyAssignmentMethod)
    {
    case CollectionDescriptor.KEY_ASSIGN_SERVER:	     
      break;
    case CollectionDescriptor.KEY_ASSIGN_IDENTITY:
      if (disableReturning)
      {
        // We can't support IDENTITY columns if RETURNING doesn't work
        throw SODAUtils.makeException(SODAMessage.EX_IDENTITY_ASSIGN_RETURNING);
      }
      break;
    case CollectionDescriptor.KEY_ASSIGN_SEQUENCE:
      // Forced to select the key immediately
      if (disableReturning)
        key = Long.toString(this.nextSequenceValue());
      break;
    case CollectionDescriptor.KEY_ASSIGN_GUID:
      // Forced to select the key immediately
      if (disableReturning)
      {
        key = db.nextGuid();
        if (options.keyDataType == CollectionDescriptor.INTEGER_KEY)
          key = uidToDecimal(key);
      }
      break;
    case CollectionDescriptor.KEY_ASSIGN_UUID:
      key = db.generateKey();
      if (options.keyDataType == CollectionDescriptor.INTEGER_KEY)
        key = uidToDecimal(key);
      break;
    default:
      if (options.hasMaterializedEmbeddedID())
        break;
      key = canonicalKey(dockey);
      break;
    }
    
    String sqltext = buildInsert(disableReturning, hintStr, eJSON);

    //if (OracleLog.isLoggingEnabled())
    //  log.info("Insert: " + sqltext);

    try
    {
      CallableStatement cstmt = null;

      metrics.startTiming();

      if (insertHasReturnClause(disableReturning) && useCallableReturns)
        stmt = cstmt = conn.prepareCall(sqltext);
      else
        stmt = conn.prepareStatement(sqltext);

      int num = 0;

      if (!returnInsertedKey() || disableReturning)
      {
        bindKeyColumn(stmt, ++num, key);
      }

      num = bindMediaTypeColumn(stmt, num, document);
      
      boolean materializeContent = true;

      if (!payloadBasedVersioning() &&
          admin().isHeterogeneous() &&
          ((OracleDocumentImpl) document).hasStreamContent())
      {
        // ### throw an error for eJSON here
        
        // This means it needs to be streamed without materializing.
        
        // ### perhaps use setBinaryStream with explicit LONGVARBINARY
        setStreamBind(stmt, document, ++num);

        materializeContent = false;
      }
      // ### Might be good to materialize in certain cases even
      //     if the versioning is not content based. For now,
      //     leaving a comment here to register this.
      else
      {
        // This means we need to materialize the payload 
        dataBytes = bindPayloadColumn(stmt, ++num, document, eJSON, contentWithInjectedKey);
      }

      // If we need the timestamp but can't use the RETURNING clause
      if (returnInsertedTime() && disableReturning)
      {
        // Get the time and drive it down as a parameter
        tstamp = ComponentTime.instantToString(db.getDatabaseTime(), false);

        if (options.timestampColumnName != null)
          stmt.setString(++num, tstamp);

        if (options.creationColumnName != null)
          stmt.setString(++num, tstamp);
      }
      // Else timestamp is generated on the server

      if ((options.versionColumnName != null) &&
          (options.versioningMethod) != CollectionDescriptor.VERSION_NONE)
      {
        switch (options.versioningMethod)
        {
        case CollectionDescriptor.VERSION_SEQUENTIAL:
          long lver = 1L;
          stmt.setLong(++num, lver);
          version = Long.toString(lver);
          break;
        case CollectionDescriptor.VERSION_TIMESTAMP:
          long lstamp = db.getDatabaseTimeVersion(db.getDatabaseTime());
          stmt.setLong(++num, lstamp);
          version = Long.toString(lstamp);
          break;
        case CollectionDescriptor.VERSION_UUID:
          version = db.generateKey();
          stmt.setString(++num, version);
          break;
        default: /* Hashes */
          if (!materializeContent)
          {
            // Not Feasible
            throw SODAUtils.makeException(SODAMessage.EX_NO_HASH_VERSION,
                                          options.uriName, options.getVersioningMethod());
          }
          version = computeVersion(dataBytes);
          stmt.setString(++num, version);
          break;
        }
      }

      int returnParameterIndex = -1;

      // Parameters for RETURNING clause
      if (insertHasReturnClause(disableReturning))
      {
        // Oracle-specific binding mode
        if (!useCallableReturns)
        {
          if (returnInsertedKey())
            db.registerReturnString(stmt, ++num);
          if (returnInsertedTime())
            db.registerReturnTimestamp(stmt, ++num);
          if (returnInsertedVersion())
            db.registerReturnString(stmt, ++num);
        }
        else
        {
          returnParameterIndex = num;
          if (returnInsertedKey())
            cstmt.registerOutParameter(++num, Types.VARCHAR);
          if (returnInsertedTime())
            cstmt.registerOutParameter(++num, Types.TIMESTAMP);
          if (returnInsertedVersion())
            cstmt.registerOutParameter(++num, Types.VARCHAR);
          // Register the "flag" column
          cstmt.registerOutParameter(++num, Types.VARCHAR);
        }
      }

      int nrows = stmt.executeUpdate();
      //
      // This is normally the count of rows updated; however, for a PL/SQL
      // call it's the number of executions of the anonymous block. In
      // such cases, nrows == 1 even if it didn't update any rows, and
      // we have to use a "flag" column to tell if it worked or not.
      //
      if (nrows != 1)
      {
        throw SODAUtils.makeException(SODAMessage.EX_INSERT_FAILED,
                                      options.uriName);
      }

      // If there's a RETURNING clause, retrieve the results
      if (insertHasReturnClause(disableReturning))
      {
        int onum = 0;

        // For the Oracle driver, these are returned as a ResultSet
        if (!useCallableReturns)
        {
          ResultSet rows = db.getReturnResultSet(stmt);
          if ((rows == null) || !rows.next())
          {
            throw SODAUtils.makeException(SODAMessage.EX_INSERT_FAILED,
                                          options.uriName);
          }

          if (returnInsertedKey())
            key = rows.getString(++onum);

          if (returnInsertedTime())
            tstamp = OracleDatabaseImpl.getTimestamp(rows, ++onum);

          if (returnInsertedVersion())
            version = rows.getString(++onum);

          rows.close();
          rows = null;
        }
        // Otherwise for a callable statement these are returned as OUT parameters
        else
        {
          onum = returnParameterIndex;

          if (returnInsertedKey())
            key = cstmt.getString(++onum);

          if (returnInsertedTime())
            tstamp = OracleDatabaseImpl.getTimestamp(cstmt, ++onum);

          if (returnInsertedVersion())
            version = cstmt.getString(++onum);

          String flagValue = cstmt.getString(++onum);

          // If this was wrapped in a PL/SQL block, the count is always 1
          // because it's the execution count, not the row count. This is
          // a work-around that relies on one of the returned columns to
          // be non-NULL, but that's not guaranteed. To guarantee it we
          // have to ensure that an additional flag column was returned.
          if ((flagValue == null) || (!flagValue.equals("1")))
          {
            throw SODAUtils.makeException(SODAMessage.EX_INSERT_FAILED,
                                          options.uriName);
          }
        }
      }

      stmt.close();
      stmt = null;

      metrics.recordWrites(1, 1);
    }
    catch (SQLException e)
    {
      if (OracleLog.isLoggingEnabled())
        log.severe(e.toString() + "\n" + sqltext);
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

    OracleDocumentImpl doc = new OracleDocumentImpl(key, version, tstamp);

    doc.setCreatedOn(tstamp);

    String ctype = document.getMediaType();

    setContentType(ctype, doc);

    return(doc);
  }

  public void save(OracleDocument document)
    throws OracleException
  {
    checkJDBCVersion();

    // ### This can be made more efficient then
    //     simply calling saveAndGet(...), since the "Get"
    //     part is not required.
    saveAndGet(document, null);
  }

  public OracleDocument saveAndGet(OracleDocument document)
    throws OracleException
  {
    checkJDBCVersion();
    return saveAndGet(document, null);
  }
  
  public OracleDocument saveAndGet(OracleDocument document, Map<String, ?> insertOrSaveOptions)
    throws OracleException
  {
    checkJDBCVersion();

    if (options.isDualityView())
    {
      throw SODAUtils.makeException(SODAMessage.EX_UNSUPPORTED_FOR_JSON_DUALITY_VIEW);
    }

    writeCheck("save");
    if (document == null)
    {
      throw SODAUtils.makeException(SODAMessage.EX_ARG_CANNOT_BE_NULL,
                                    "document");
    }

    String hintStr = getHintString(insertOrSaveOptions);

    Boolean eJSON = getEJSONBoolean(insertOrSaveOptions);

    if (eJSON)
      throw SODAUtils.makeException(SODAMessage.EX_EJSON_CANNOT_BE_USED);

    // See if the document has a key, extracting if necessary
    String dockey = getDocumentKey(document, false, false).getFirst();
    
    // If this collection doesn't use client-assigned keys, avoid upsert
    // ### This means we'll do the upsert for EXTRACT and OID - OK?
    if (hasExtrinsicServerKey())
      dockey = null;

    // If we have a key already, perform the upsert
    if (dockey != null)
      return(upsert(dockey, document, hintStr));

    return insertAndGet(document);
  }

  public void insert(Iterator<OracleDocument> documents)
    throws OracleBatchException
  {
    try
    {
      checkJDBCVersion();
    }
    catch (OracleException e)
    {
      throw convertToOracleBatchException(e, 0, null);
    }
    // ### This can be made more efficient then
    //     simply calling insertAndGet(...), since the "Get"
    //     part is not required.
    insertAndGet(documents, null, false);
  }

  // Not part of a public API. Made public for internal use.
  // SODA apps should not use this method, as it's subject to change.
  public void insert(Iterator<OracleDocument> documents, Map <String, ?> insertOrSaveOptions)
    throws OracleBatchException
  {
    try
    {
      checkJDBCVersion();
    }
    catch (OracleException e)
    {
      throw convertToOracleBatchException(e, 0, null);
    }
    // ### This can be made more efficient then
    //     simply calling insertAndGet(...), since the "Get"
    //     part is not required.
    insertAndGet(documents, insertOrSaveOptions, false);
  }

  /**
   * Insert a set of rows into a collection one row at a time
   * This code is for any conditions for which we cannot use JDBC batching.
   *  - IDENTITY key assignment method (because we have to use RETURNING)
   */
  private List<OracleDocument> insertRows(Iterator<OracleDocument> documents,
                                          Map <String, ?> insertOrSaveOptions)
    throws OracleBatchException
  {
    int rowCount = 0;

    if (!documents.hasNext())
      return(EMPTY_LIST);

    ArrayList<OracleDocument> results = new ArrayList<OracleDocument>();

    boolean manageTransaction = false;

    try
    {
      // If the connection is in auto-commit mode,
      // turn it off and take over transaction management
      // (we will commit if all statements succeed,
      // or rollback if any fail, and finally
      // restore the auto-commit mode).
      if (conn.getAutoCommit() == true)
      {
        if (avoidTxnManagement)
        {
          throw SODAUtils.makeBatchException(SODAMessage.EX_OPERATION_REQUIRES_TXN_MANAGEMENT,
                                             rowCount, "insertRows");
        }

        //        conn.setAutoCommit(false);
        //        manageTransaction = true;
      }

      while (documents.hasNext())
      {
        OracleDocument document = documents.next();

        if (document == null)
        {
          OracleBatchException bE = SODAUtils.makeBatchException(
              SODAMessage.EX_ITERATOR_RETURNED_NULL_ELEMENT,
              rowCount,
              "documents",
              rowCount);

          throw bE;
        }

        // See if the document has a key, extracting if necessary
	String dockey = null;
	if (!options.isDualityView())
          dockey = getDocumentKey(document, false, false).getFirst();
        // ### Since we're only running this code for the RETURNING case
        // ### and the IDENTITY key assignment method, is it necessary to
        // ### check the assignment method at all?
        if ((dockey != null) &&
            (options.keyAssignmentMethod !=
             CollectionDescriptor.KEY_ASSIGN_CLIENT))
        {
          OracleBatchException bE = SODAUtils.makeBatchException(
              SODAMessage.EX_ITERATOR_RETURNED_DOC_WITH_KEY,
              rowCount,
              "documents",
              rowCount);

          throw bE;
        }

        document = insertAndGet(document, insertOrSaveOptions);

        if (document == null)
        {
          throw SODAUtils.makeBatchException(SODAMessage.EX_INSERT_FAILED,
                                             rowCount, options.uriName);
        }

        results.add(document);

        ++rowCount;
      }
    }
    catch (OracleException e)
    {
      OracleBatchException bE = convertToOracleBatchException(e,
                                                              rowCount,
                                                              null);

      bE.setNextException(completeTxnAndRestoreAutoCommit(conn,
                                                          manageTransaction,
                                                          false));

      throw bE;
    }
    catch (SQLException e)
    {
      OracleBatchException bE = SODAUtils.makeBatchException(e, rowCount);

      bE.setNextException(completeTxnAndRestoreAutoCommit(conn,
                                                          manageTransaction,
                                                          false));

      if (OracleLog.isLoggingEnabled())
        log.severe(e.toString());

      throw bE;
    }

    return(results);
  }
 
  /**
   * Insert a set of rows into a collection.
   */
  public List<OracleDocument> insertAndGet(Iterator<OracleDocument> documents)
    throws OracleBatchException
  {
    try
    {
      checkJDBCVersion(true);
    }
    catch (OracleException e)
    {
      throw convertToOracleBatchException(e, 0, null);
    }

    return insertAndGet(documents, null, true);
  }

  /**
   * Insert a set of rows into a collection with options
   */
  public List<OracleDocument> insertAndGet(Iterator<OracleDocument> documents,
                                           Map <String, ?> insertOrSaveOptions)
          throws OracleBatchException
  {
    return insertAndGet(documents,insertOrSaveOptions, true);
  }

  private List<OracleDocument> insertAndGet(Iterator<OracleDocument> documents,
                                           Map <String, ?> insertOrSaveOptions,
                                           boolean isInsertAndGet)
    throws OracleBatchException
  {
    // Counter of input rows successfully inserted
    int insertedRowCount = 0;

    // Counter of input rows
    int rowCount = 0;

    int lastRowCount = 0;

    if (documents == null)
    {
      throw SODAUtils.makeBatchException(SODAMessage.EX_ARG_CANNOT_BE_NULL,
                                         rowCount,
                                         "documents");
    }

    try
    {
      checkJDBCVersion(isInsertAndGet);
    }
    catch (OracleException e)
    {
      throw convertToOracleBatchException(e, rowCount, null);
    }

    String hintStr = null;
    try
    {
      hintStr = getHintString(insertOrSaveOptions);
    } 
    catch (OracleException e)
    {
      throw convertToOracleBatchException(e, rowCount, null);
    }

    Boolean eJSON = null;
    try
    {
      eJSON = getEJSONBoolean(insertOrSaveOptions);
 
      if (eJSON && !options.hasBinaryFormat() && !options.hasJsonType())
        throw SODAUtils.makeException(SODAMessage.EX_EJSON_CANNOT_BE_USED);
    }
    catch (OracleException e)
    {
      throw convertToOracleBatchException(e, rowCount, null);
    }

    Integer maxBatchSize = null;
    try 
    {
      maxBatchSize = getMaxJDBCBatchSize(insertOrSaveOptions);
    } 
    catch (OracleException e)
    {
      throw convertToOracleBatchException(e, rowCount, null);
    }

    if (maxBatchSize == null)
      maxBatchSize = BATCH_MAX_SIZE;

    if (isReadOnly())
    {
      if (OracleLog.isLoggingEnabled())
        log.warning("Write to " + options.uriName + " not allowed");

      throw SODAUtils.makeBatchException(SODAMessage.EX_READ_ONLY,
                                         rowCount,
                                         options.uriName,
                                         "insert");
    }

    if (!documents.hasNext())
      return(EMPTY_LIST);

    //
    // Any conditions incompatible with batching need to use the
    // row-at-a-time method. The IDENTITY key assignment method
    // requires use of the RETURNING clause, we can't pre-fetch
    // sequence values and drive them in from JDBC.
    //
    if ((options.keyAssignmentMethod == CollectionDescriptor.KEY_ASSIGN_IDENTITY) ||
         options.isDualityView())
      return insertRows(documents, insertOrSaveOptions);

    ArrayList<OracleDocument> results = new ArrayList<OracleDocument>();
    ArrayList<String> outKeys = new ArrayList<String>();

    PreparedStatement stmt = null;

    String sqltext = buildInsert(true, hintStr, eJSON);

    boolean manageTransaction = false;

    try
    {
      // If the connection is in auto-commit mode,
      // turn it off and take over transaction management
      // (we will commit if all statements succeed,
      // or rollback if any fail, and finally
      // restore the auto-commit mode).
      if (conn.getAutoCommit() == true)
      {
        if (avoidTxnManagement)
        {
          throw SODAUtils.makeBatchException(SODAMessage.EX_OPERATION_REQUIRES_TXN_MANAGEMENT,
                                             rowCount, "insertAndGet");
        }

        //        conn.setAutoCommit(false);
        //        manageTransaction = true;
      }

      metrics.startTiming();

      if ((options.hasMaterializedEmbeddedID() || options.hasVersionNONE()) && isInsertAndGet) {
        if (options.hasMaterializedEmbeddedID() && options.hasVersionNONE())
          stmt = conn.prepareStatement(sqltext,  new String[] { options.keyColumnName, options.versionColumnName });
        else if (options.hasMaterializedEmbeddedID())
          stmt = conn.prepareStatement(sqltext,  new String[] { options.keyColumnName});
        else
          stmt = conn.prepareStatement(sqltext,  new String[] { options.versionColumnName});
      }
      else
        stmt = conn.prepareStatement(sqltext);

      // Use a stopped clock for all rows
      Instant dbTime = null;
      long    lstamp = 0;
      if (!options.isNative) {
        dbTime = db.getDatabaseTime();
        lstamp = db.getDatabaseTimeVersion(dbTime);
      }
      
      String  tstamp = null;

      if ((options.timestampColumnName != null) ||
          (options.creationColumnName != null))		      
        tstamp = ComponentTime.instantToString(dbTime, false);

      String key = null;
      String version = null;

      while (documents.hasNext())
      {
        key = null;
        version = null;

        OracleDocument document = documents.next();

        if (document == null)
        {
          OracleBatchException bE = SODAUtils.makeBatchException(
              SODAMessage.EX_ITERATOR_RETURNED_NULL_ELEMENT,
              rowCount,
              "documents",
              rowCount);

          throw bE;
        }

        if (OracleDocumentImpl.isBinary(document) && eJSON)
        {
          OracleBatchException bE = SODAUtils.makeBatchException(
              SODAMessage.EX_EJSON_CANNOT_BE_USED_WITH_BINARY_DOC,
              rowCount,
              "documents",
              rowCount);

          throw bE;
        }

        // Get the document's key, generating if necessary
        // Also get the OSON payload if it's supported
        Pair<String, Object> docKeyAndContentPayload = null;
        String dockey = null;
        Object contentWithInjectedKey = null;

        if (!options.hasMaterializedEmbeddedID())
        {
          docKeyAndContentPayload = getDocumentKey(document, true, eJSON);
          dockey = docKeyAndContentPayload.getFirst();
          contentWithInjectedKey = docKeyAndContentPayload.getSecond();
        }

        if ((dockey != null) && hasExtrinsicServerKey())
        {
          OracleBatchException bE = SODAUtils.makeBatchException(
              SODAMessage.EX_ITERATOR_RETURNED_DOC_WITH_KEY,
              rowCount,
              "documents",
              rowCount);

          throw bE;
        }

        switch (options.keyAssignmentMethod)
        {
        case CollectionDescriptor.KEY_ASSIGN_IDENTITY:
          // ### We can't support IDENTITY columns with JDBC batching
          // ### In theory this code is now unreachable?
          throw SODAUtils.makeException(SODAMessage.EX_IDENTITY_ASSIGN_RETURNING);
        case CollectionDescriptor.KEY_ASSIGN_SEQUENCE:
          // ### Ideally if we knew it was a large batch we could
          // ### pass a quantity hint to nextSequenceValue() to
          // ### reduce the number of round trips. Unfortunately we
          // ### don't have a good way to know that from the Iterator
          // ### interface.
          key = Long.toString(this.nextSequenceValue());
          break;
        case CollectionDescriptor.KEY_ASSIGN_GUID:
          key = db.nextGuid();
          if (options.keyDataType == CollectionDescriptor.INTEGER_KEY)
            key = uidToDecimal(key);
          break;
        case CollectionDescriptor.KEY_ASSIGN_UUID:
          key = db.generateKey();
          if (options.keyDataType == CollectionDescriptor.INTEGER_KEY)
            key = uidToDecimal(key);
          break;
        case CollectionDescriptor.KEY_ASSIGN_SERVER:
	      break;
        default:
          if (options.hasMaterializedEmbeddedID())
            break;
          key = canonicalKey(dockey);
          break;
        }

        int num = 0;

        if ((options.keyAssignmentMethod != CollectionDescriptor.KEY_ASSIGN_SERVER) &&
            !options.hasMaterializedEmbeddedID())
          bindKeyColumn(stmt, ++num, key);

        num = bindMediaTypeColumn(stmt, num, document);
        
        // Set the payload column
        byte[] data = bindPayloadColumn(stmt, ++num, document, eJSON, contentWithInjectedKey);

        if (options.timestampColumnName != null)
        {
          stmt.setString(++num, tstamp);
        }

        if (options.creationColumnName != null)
        {
          stmt.setString(++num, tstamp);
        }

        if ((options.versionColumnName != null) &&
            (options.versioningMethod) != CollectionDescriptor.VERSION_NONE)
        {
          switch (options.versioningMethod)
          {
          case CollectionDescriptor.VERSION_SEQUENTIAL:
            long lver = 1L;
            stmt.setLong(++num, lver);
            version = Long.toString(lver);
            break;
          case CollectionDescriptor.VERSION_TIMESTAMP:
            stmt.setLong(++num, lstamp);
            version = Long.toString(lstamp);
            break;
          case CollectionDescriptor.VERSION_UUID:
            version = db.generateKey();
            stmt.setString(++num, version);
            break;
          default: // Hashes
            version = computeVersion(data);
            stmt.setString(++num, version);
            break;
          }
        }

        stmt.addBatch();

        String lastModified = null;
        if (options.timestampColumnName != null)
          lastModified = tstamp;
        OracleDocumentImpl result = new OracleDocumentImpl(key, version, lastModified);

        String creationTime = null;
        if (options.creationColumnName != null)
          creationTime = tstamp;
        result.setCreatedOn(creationTime);

        String ctype = document.getMediaType();
        setContentType(ctype, result);

        results.add(result);

        ++rowCount;
        if ((rowCount % maxBatchSize) == 0)
        {
          int[] flags = stmt.executeBatch();

          if ((options.hasMaterializedEmbeddedID() || options.hasVersionNONE()) && isInsertAndGet)
          {
            ResultSet generatedKeys = stmt.getGeneratedKeys();

            int pos = lastRowCount;
            while (generatedKeys.next())
            {
              OracleDocument resDoc = results.get(pos);

              key = null;
              version = null;

              if (options.hasMaterializedEmbeddedID() && options.hasVersionNONE()) {
                key = generatedKeys.getString(1);
                version = generatedKeys.getString(2);
              }
	      else if (options.hasMaterializedEmbeddedID())
                key = generatedKeys.getString(1);
              else
                version = generatedKeys.getString(1);
              
              if ((key == null) && !options.hasMaterializedEmbeddedID())
                key = resDoc.getKey();
              if ((version == null) && !options.hasVersionNONE())
                version = resDoc.getVersion();

              OracleDocument newDoc = new OracleDocumentImpl(key, version,
                      resDoc.getLastModified(), (String) null,
                      resDoc.getMediaType());
              ((OracleDocumentImpl) newDoc).setCreatedOn(resDoc.getCreatedOn());
              results.set(pos, newDoc);
              pos++;
              lastRowCount++;
            }
            generatedKeys.close();
          }

          // Assumes the content of each element of the array is 1.
          // This should be true for a batched insert (if there's
          // an issue during insert, executeBatch() will throw an exception).
          insertedRowCount += flags.length;
        }

      }

      if ((rowCount % maxBatchSize) != 0)
      {
        int[] flags = stmt.executeBatch();

        int pos = lastRowCount;

        if (options.hasMaterializedEmbeddedID() && isInsertAndGet)
        {
          ResultSet generatedKeys = stmt.getGeneratedKeys();

          while (generatedKeys.next()) {

            key = null;
            version = null;

            if (options.hasMaterializedEmbeddedID() && options.hasVersionNONE()) {
              key = generatedKeys.getString(1);
              version = generatedKeys.getString(2);
            }
            else if (options.hasMaterializedEmbeddedID())
              key = generatedKeys.getString(1);
            else
              version = generatedKeys.getString(1);

            OracleDocument resDoc = results.get(pos);

            if ((key == null) && !options.hasMaterializedEmbeddedID())
              key = resDoc.getKey();
            if ((version == null) && !options.hasVersionNONE())
              version = resDoc.getVersion();

            OracleDocument newDoc = new OracleDocumentImpl(key, version,
                    resDoc.getLastModified(), (String) null,
                    resDoc.getMediaType());
            ((OracleDocumentImpl) newDoc).setCreatedOn(resDoc.getCreatedOn());
            results.set(pos, newDoc);
            pos++;
          }
          generatedKeys.close();
        }

        // Assumes the content of each element of the array is 1.
        // This should be true for a batched insert (if there's
        // an issue during insert, executeBatch() will throw an exception).
        insertedRowCount += flags.length;
      }

      stmt.close();
      stmt = null;

      metrics.recordWrites(rowCount, maxBatchSize);
    }
    catch (OracleException e)
    {
      OracleBatchException bE = convertToOracleBatchException(e,
                                                              rowCount,
                                                              sqltext);

      bE.setNextException(completeTxnAndRestoreAutoCommit(conn,
                                                          manageTransaction,
                                                          false));
      throw bE;
    }
    catch (SQLException e)
    {
      int count = 0;

      // If the exception occurred during executeBatch(),
      // the processed count reported to the user is
      // the number of rows processed by JDBC.
      if (e instanceof BatchUpdateException)
      {
        insertedRowCount += ((BatchUpdateException) e).getUpdateCounts().length;
        count = insertedRowCount;
      }
      // Otherwise, the processed count reported
      // to the user is the number of rows processed
      // from the input iterator (which could be
      // greater than the number of rows processed by JDBC).
      // This allows the user to tell on which
      // row the error occurred.
      else
      {
        count = rowCount;
      }

      OracleBatchException bE = SODAUtils.makeBatchExceptionWithSQLText(e,
                                                                        count,
                                                                        sqltext);

      bE.setNextException(completeTxnAndRestoreAutoCommit(conn,
                                                          manageTransaction,
                                                          false));

      if (OracleLog.isLoggingEnabled())
        log.severe(e.toString() + "\n" + sqltext);
      throw bE;
    }
    catch (RuntimeException e)
    {
      completeTxnAndRestoreAutoCommit(conn, manageTransaction, false);
      if (OracleLog.isLoggingEnabled())
        log.severe(e.toString());
      throw e;
    }
    catch (Error e)
    {
      completeTxnAndRestoreAutoCommit(conn, manageTransaction, false);
      if (OracleLog.isLoggingEnabled())
        log.severe(e.toString());
      throw e;
    }
    finally
    {
      for (String message : SODAUtils.closeCursor(stmt, null))
      {
        if (OracleLog.isLoggingEnabled())
          log.severe(message);
      }
    }

    OracleException e = completeTxnAndRestoreAutoCommit(conn, manageTransaction, true);

    if (e != null)
    {
      throw new OracleBatchException(e, rowCount);
    }

    return(results);
  }

  private OracleBatchException convertToOracleBatchException(OracleException e,
                                                             int processedRowCount,
                                                             String sqlText)
  {
    if (e instanceof OracleBatchException)
    {
      return (OracleBatchException)e;
    }

    OracleBatchException batchException = null;

    Throwable cause = e.getCause();

    if (cause != null && cause instanceof SQLException)
    {
      // ### What if sqlText is null?
      // ### Internally this creates an object that implements SQLTextCarrier
      // ### which may be wrong if the caller doesn't have any sqlText.
      batchException = SODAUtils.makeBatchExceptionWithSQLText(cause,
                                                               processedRowCount,
                                                               sqlText);
    }
    else
    {
      batchException = new OracleBatchException(e, processedRowCount);
    }

    return batchException;
  }

  private Integer getMaxJDBCBatchSize(Map<String, ?> insertOrSaveOptions) throws OracleException
  {
    if (insertOrSaveOptions == null || insertOrSaveOptions.isEmpty())
      return null;
    Object val = insertOrSaveOptions.get("maxBatchSize");
    if (val == null)
      return null;
    if (!(val instanceof Integer))
      throw SODAUtils.makeException(SODAMessage.EX_INVALID_BATCH_SIZE, val.toString());
    return (Integer) val;
  }

  private String getHintString(Map<String, ?> insertOrSaveOptions) throws OracleException
  {
    if (insertOrSaveOptions == null || insertOrSaveOptions.isEmpty())
      return null;
    Object val = insertOrSaveOptions.get("hint");
    if (val == null)
      return null;
    if (!(val instanceof String))
      throw SODAUtils.makeException(SODAMessage.EX_INVALID_HINT, val.toString());
    String hint = (String) val;
    if (hint.indexOf("/*") >= 0 || hint.indexOf("*/") >= 0)
      throw SODAUtils.makeException(SODAMessage.EX_INVALID_HINT, hint);
    return (hint == "")? null: hint;	 
  }

  private Boolean getEJSONBoolean(Map<String, ?> insertOrSaveOptions) throws OracleException
  {
    if (insertOrSaveOptions == null || insertOrSaveOptions.isEmpty())
      return false;
    Object val = insertOrSaveOptions.get("format");
    if (val == null)
      return false;
    if (!(val instanceof String) || !(val.equals("eJSON")))
      throw SODAUtils.makeException(SODAMessage.EX_INVALID_FORMAT, val.toString());
    return true;
  }

  static OracleException completeTxnAndRestoreAutoCommit(Connection conn,
                                                         boolean manageTransaction,
                                                         boolean commit)
  {
    OracleException oe = null;

    if (manageTransaction)
    {
      try
      {
        if (!commit)
        {
          conn.rollback();
        }
        else
        {
          conn.commit();
        }
      }
      catch (SQLException e)
      {
        if (OracleLog.isLoggingEnabled())
          log.severe(e.toString());
        oe = new OracleException(e);
      }

      try
      {
        conn.setAutoCommit(true);
      }
      catch (SQLException e)
      {
        if (OracleLog.isLoggingEnabled())
          log.severe(e.toString());
        if (oe == null)
        {
          oe = new OracleException(e);
        }
        else
        {
          oe.setNextException(new OracleException(e));
        }
      }
    }

    return oe;
  }

  private OracleDocument upsert(String key, OracleDocument document, String hint)
    throws OracleException
  {
    PreparedStatement stmt = null;
    ResultSet rows = null;

    String sqltext = buildUpsert(hint);

    OracleDocumentImpl result = null;

    key = canonicalKey(key);

    boolean manageTransaction = false;

    try
    {
      Instant dbTime = db.getDatabaseTime();
      long    lstamp = db.getDatabaseTimeVersion(dbTime);
      String  tstamp = ComponentTime.instantToString(dbTime, false);

      metrics.startTiming();

      // If the connection is in auto-commit mode,
      // turn it off and take over transaction management
      // (we will commit if all statements succeed,
      // or rollback if any fail, and finally
      // restore the auto-commit mode).
      if (conn.getAutoCommit() == true)
      {
        if (avoidTxnManagement)
        {
          throw SODAUtils.makeException(SODAMessage.EX_OPERATION_REQUIRES_TXN_MANAGEMENT,
                                        "save");
        }

        conn.setAutoCommit(false);
        manageTransaction = true;
      }

      stmt = conn.prepareStatement(sqltext);

      int num = 0;

      // Query portion of the SQL MERGE

      // Bind the key to drive the query portion
      if (!options.hasMaterializedEmbeddedID())
        bindKeyColumn(stmt, ++num, key);

      // Update portion of the SQL MERGE

      // Set the payload column
      byte[] data = null;

      // ### This is for future use. Right now, a collection with
      // client assigned keys will never store binary JSON (only
      // default collection can store binary JSON). So this "if"
      // code will never run.
      data = getContentForTransfer(document, false, null);     

      String sdata = null;

      switch (options.contentDataType)
      {
        case CollectionDescriptor.CHAR_CONTENT:
          sdata = stringFromBytes(data);
          stmt.setString(++num, sdata);
          break;

        case CollectionDescriptor.CLOB_CONTENT:
          sdata = stringFromBytes(data);
          setPayloadClobWorkaround(stmt, ++num, sdata);
          break;

        case CollectionDescriptor.NCHAR_CONTENT:
          sdata = stringFromBytes(data);
          stmt.setNString(++num, sdata);
          break;

        case CollectionDescriptor.NCLOB_CONTENT:
          sdata = stringFromBytes(data);
          setPayloadNclob(stmt, ++num, sdata);
          break;

        case CollectionDescriptor.RAW_CONTENT:
          stmt.setBytes(++num, data);
          break;

        case CollectionDescriptor.BLOB_CONTENT:
          setPayloadBlobWorkaround(stmt, ++num, data);
          break;

        case CollectionDescriptor.JSON_CONTENT:
	  if (options.hasMaterializedEmbeddedID())
            db.setBytesForJson(stmt, ++num, data);
          db.setBytesForJson(stmt, ++num, data);
          break;

        default:
          throw new IllegalStateException();
      }

      if (options.timestampColumnName != null)
        stmt.setString(++num, tstamp);

      String version = null;

      if ((options.versionColumnName != null) &&
          (options.versioningMethod) != CollectionDescriptor.VERSION_NONE)
      {
        switch (options.versioningMethod)
        {
        case CollectionDescriptor.VERSION_SEQUENTIAL:
          break;
        case CollectionDescriptor.VERSION_TIMESTAMP:
          stmt.setLong(++num, lstamp);
          version = Long.toString(lstamp);
          break;
        case CollectionDescriptor.VERSION_UUID:
          version = db.generateKey();
          stmt.setString(++num, version);
          break;
        default: /* Hashes */
          version = computeVersion(data);
          stmt.setString(++num, version);
          break;
        }
      }

      num = bindMediaTypeColumn(stmt, num, document);

      // Insert portion of the SQL MERGE

      if (!options.hasMaterializedEmbeddedID())
        bindKeyColumn(stmt, ++num, key);

      switch (options.contentDataType)
      {
        case CollectionDescriptor.CHAR_CONTENT:
          stmt.setString(++num, sdata);
          break;

        case CollectionDescriptor.CLOB_CONTENT:
          setPayloadClobWorkaround(stmt, ++num, sdata);
          break;

        case CollectionDescriptor.NCHAR_CONTENT:
          stmt.setNString(++num, sdata);
          break;

        case CollectionDescriptor.NCLOB_CONTENT:
          setPayloadNclob(stmt, ++num, sdata);
          break;

        case CollectionDescriptor.RAW_CONTENT:
          stmt.setBytes(++num, data);
          break;

        case CollectionDescriptor.BLOB_CONTENT:
          setPayloadBlobWorkaround(stmt, ++num, data);
          break;

        case CollectionDescriptor.JSON_CONTENT:
          db.setBytesForJson(stmt, ++num, data);
          break;

        default:
          throw new IllegalStateException();
      }

      if (options.timestampColumnName != null)
        stmt.setString(++num, tstamp);
      if (options.creationColumnName != null)
        stmt.setString(++num, tstamp);
      
      if ((options.versionColumnName != null) &&
          (options.versioningMethod) != CollectionDescriptor.VERSION_NONE)
      {
        switch (options.versioningMethod)
        {
        case CollectionDescriptor.VERSION_SEQUENTIAL:
          long lver = 1L;
          stmt.setLong(++num, lver);
          version = Long.toString(lver);
          break;
        case CollectionDescriptor.VERSION_TIMESTAMP:
          stmt.setLong(++num, lstamp);
          version = Long.toString(lstamp);
          break;
        default:
          // Assumes the version was computed above
          stmt.setString(++num, version);
          break;
        }
      }

      bindMediaTypeColumn(stmt, num, document);

      int nrows = stmt.executeUpdate();

      if (nrows != 1)
        throw SODAUtils.makeException(SODAMessage.EX_SAVE_FAILED,
                                      options.uriName);

      stmt.close();
      stmt = null;

      metrics.recordWrites(1, 1);

      String ctime = null;

      String newKey = null;

      // If we need the creation timestamp, or version (from the DB),
      // generate and run another select statement, since the
      // merge statement doesn't have a 'returning into' clause.
      if (options.creationColumnName != null || returnVersion() ||
          options.hasMaterializedEmbeddedID())
      {
        metrics.startTiming();

        //if (OracleLog.isLoggingEnabled())
        //  log.info("Generating an additional select");
        String selectForUpsert = buildSelectForUpsert(hint);
        stmt = conn.prepareStatement(selectForUpsert);

        if (options.hasMaterializedEmbeddedID())
        {
          db.setBytesForJson(stmt, 1, data);
        }
        else
        {
          bindKeyColumn(stmt, 1, key);
        }

        rows = stmt.executeQuery();

        num = 0;
        boolean hasNext = rows.next();

        if (!hasNext)
        {
          throw SODAUtils.makeException(SODAMessage.EX_SAVE_FAILED,
                                        options.uriName);
        }

        if (options.hasMaterializedEmbeddedID())
        {
          newKey = rows.getString(++num);
        }

        if (options.creationColumnName != null)
        {
          ctime = OracleDatabaseImpl.getTimestamp(rows, ++num);
        }

        if (returnVersion())
        {
          version = rows.getString(++num);
        }

        rows.close();
        rows = null;

        stmt.close();
        stmt = null;

        metrics.recordReads(1,1);
      }

      if (options.hasMaterializedEmbeddedID())
        result = new OracleDocumentImpl(newKey, version, tstamp);
      else
        result = new OracleDocumentImpl(key, version, tstamp);

      result.setCreatedOn(ctime);

      String ctype = document.getMediaType();
      setContentType(ctype, result);
    }
    catch (OracleException e)
    {
      e.setNextException(completeTxnAndRestoreAutoCommit(conn,
                                                         manageTransaction,
                                                         false));
      throw(e);
    }
    catch (SQLException e)
    {
      OracleException oE = SODAUtils.makeExceptionWithSQLText(e, sqltext);

      oE.setNextException(completeTxnAndRestoreAutoCommit(conn,
                                                          manageTransaction,
                                                          false));
      if (OracleLog.isLoggingEnabled())
        log.severe(e.toString() + "\n" + sqltext);
      throw(oE);
    }
    catch (RuntimeException e)
    {
      completeTxnAndRestoreAutoCommit(conn, manageTransaction, false);
      if (OracleLog.isLoggingEnabled())
        log.severe(e.toString());
      throw(e);
    }
    catch (Error e)
    {
      completeTxnAndRestoreAutoCommit(conn, manageTransaction, false);
      if (OracleLog.isLoggingEnabled())
        log.severe(e.toString());
      throw(e);
    }
    finally
    {
      for (String message : SODAUtils.closeCursor(stmt, rows))
      {
        if (OracleLog.isLoggingEnabled())
          log.severe(message);
      }
    }

    OracleException e = completeTxnAndRestoreAutoCommit(conn, manageTransaction, true);

    if (e != null)
    {
      throw(e);
    }

    return(result);
  }

  /**
   * Return a single object matching a key.
   * This version returns a byte range sub-set of the object (if possible).
   * The underlying object starts at offset 0.
   */
  public OracleDocumentFragmentImpl findFragment(String key, long offset, int length)
    throws OracleException
  {
    OracleDocumentFragmentImpl   result = null;
    PreparedStatement            stmt = null;
    ResultSet                    rows = null;
    byte[]                       payload = null;
    LobInputStream               payloadStream = null;
    boolean                      streamContent = false;
    String                       sqltext = buildQuery();

    key = canonicalKey(key);

    if (admin().isHeterogeneous())
      streamContent = true;

    try
    {
      metrics.startTiming();

      stmt = conn.prepareStatement(sqltext);

      bindKeyColumn(stmt, 1, key);

      rows = stmt.executeQuery();

      if (rows.next())
      {
        int num = 0;

        String keyval = rows.getString(++num);
        String ctype = null;

        String mtime   = null;
        String ctime   = null;
        String version = null;

        long datalen = -1L; // Length of LOB (streaming only)

        if (options.doctypeColumnName != null)
        {
          ctype = rows.getString(++num);

          // Use streaming responses only for non-JSON content types
          if (ctype == null)
            streamContent = false;
          else if (ctype.equalsIgnoreCase(OracleDocumentImpl.APPLICATION_JSON))
            streamContent = false;
        }
        // Always use the streaming mode for pre-parsed content,
        // This will force the use of a LobInputStream, which ensures
        // that the temporary LOB locator is freed after use.
        if (options.hasBinaryFormat())
          streamContent = true;

        if (streamContent)
        {

          // This exception should never occur, since streamContent
          // is only true if the collection is heterogeneous (which
          // requires blob content).
          if (options.contentDataType != CollectionDescriptor.BLOB_CONTENT)
          {
            throw SODAUtils.makeException(SODAMessage.EX_UNSUPPORTED_MODE,
                                          options.uriName,
                                          options.getContentDataType());
          }

          Blob loc = rows.getBlob(++num);
          if (loc != null)
          {
            datalen = loc.length(); // ### Limited to 2G for now
            if (datalen > 0L)
            {
              // If the limit is "unlimited", set it now to datalen
              if ((length < 0) || (((long)length + offset) > datalen))
                length = (int)(datalen - offset);

              // If this is a range transfer
              if ((offset > 0L) || (((long)length + offset) < datalen))
              {
                // Consider whether to honor the request
                // The range needs to be small enough that we are OK
                // to buffer it in memory. Also if the whole content is
                // small enough, we won't bother with the fragment.
                // Also the fragment needs to be for less than half
                // the content, otherwise we might as well stream it.
                if ((length <= MAX_RANGE_TRANSFER) &&
                    (datalen > MIN_RANGE_TRANSFER) &&
                    ((datalen >> 1) > (long)length))
                {
                  payload = loc.getBytes(offset + 1L, length);
                  streamContent = false;
                  // No longer going to use streaming response
                  loc.free();
                }
              }

              // If we still need to stream, open it up
              if (streamContent)
              {
                InputStream inp = loc.getBinaryStream();
                if (inp != null)
                {
                  payloadStream = new LobInputStream(loc, inp, datalen);
                  payloadStream.setMetrics(metrics);
                }
                // ### Else this should be an exception?
              }
            }

            if ((payloadStream == null) && (payload == null))
              payloadStream = new LobInputStream();
          }
        }
        else
        {
          payload = readPayloadColumn(rows, ++num);
        }

        if (options.timestampColumnName != null)
          mtime = OracleDatabaseImpl.getTimestamp(rows, ++num); 

        if (options.creationColumnName != null)
          ctime = OracleDatabaseImpl.getTimestamp(rows, ++num); 

        if (options.versionColumnName != null)
          version = rows.getString(++num);

        // If a LOB stream is available, return it
        if (payloadStream != null)
        {
          result = new OracleDocumentFragmentImpl(keyval, version, mtime,
                                                  payloadStream, ctype);
        }
        // Otherwise this is a whole or partial object as a byte array
        else
        {
          result = new OracleDocumentFragmentImpl(keyval, version, mtime,
                                                  payload);
          setContentType(ctype, result);

          // If this is a fragment of a larger object
          if (datalen > 0L)
            result.setFragmentInfo(offset, datalen);
        }

        if (ctime != null) result.setCreatedOn(ctime);
      }

      metrics.recordReads(1, 1);
    }
    catch (SQLException e)
    {
      if (OracleLog.isLoggingEnabled())
        log.severe(e.toString());

      try
      {
        // Ensure resources are closed
        if (payloadStream != null)
          payloadStream.close();
      }
      catch (IOException ie)
      {
        if (OracleLog.isLoggingEnabled())
          log.severe(ie.toString());
        // Nothing to to since we're already handling an exception
      }

      throw(SODAUtils.makeExceptionWithSQLText(e, sqltext));
    }
    finally
    {
      for (String message : SODAUtils.closeCursor(stmt, rows))
      {
        if (OracleLog.isLoggingEnabled())
          log.severe(message);
      }
    }

    return(result);
  }

  /**
   * Read the payload column into a byte array.
   */
  private byte[] readPayloadColumn(ResultSet rows, int columnIndex)
    throws SQLException
  {
    String str;
    byte[] payload = EMPTY_DATA;

    switch (options.contentDataType)
    {
      case CollectionDescriptor.CLOB_CONTENT:
      case CollectionDescriptor.CHAR_CONTENT:
        str = rows.getString(columnIndex);
        if (str != null)
          payload = str.getBytes(ByteArray.DEFAULT_CHARSET);
        break;
      case CollectionDescriptor.NCLOB_CONTENT:
      case CollectionDescriptor.NCHAR_CONTENT:
        str = rows.getNString(columnIndex);
        if (str != null)
          payload = str.getBytes(ByteArray.DEFAULT_CHARSET);
        break;
      case CollectionDescriptor.BLOB_CONTENT:
        // For now, get BLOB data using getBytes
        // avoid the LOB descriptor and/or InputStream
      case CollectionDescriptor.RAW_CONTENT:
        payload = rows.getBytes(columnIndex);
        break;
    }

    return(payload);
  }

  /*
  ** Internal sequence cache
  ** This keeps a small block of values assigned by a database sequence
  ** in a memory cache. When the cache is exhausted, a new set of
  ** IDs is fetched from the database in a single round trip.
  */
  private static final int SEQUENCE_BATCH_SIZE = 10;

  private final long[] seqCache = new long[BATCH_MAX_SIZE];
  private       int    seqCacheAvail = 0;
  private       int    seqCachePos = 0;

  private long nextSequenceValue(int quantity)
    throws OracleException
  {
    if (seqCacheAvail == 0)
      fetchSequence(quantity);
    --seqCacheAvail;
    return(seqCache[seqCachePos++]);
  }

  private long nextSequenceValue()
    throws OracleException
  {
    return(nextSequenceValue(SEQUENCE_BATCH_SIZE));
  }

  private String buildSequenceFetch()
  {
    // This builds a PL/SQL call to fill the batch.
    // This allows a flexible number of sequence values to
    // be returned in a single round-trip. However, it does
    // involve the extra cost of going through the PL/SQL
    // layer, and it requires a correct-sized array bind
    // for the return.
    //
    // It might be more efficient to build a SELECT statement
    // along these lines:
    //
    //    select SEQ.NEXTVAL from DUAL
    //    union all
    //    select SEQ.NEXTVAL from DUAL
    //    union all
    //    select SEQ.NEXTVAL from DUAL
    //     ...
    //
    // This would bypass PL/SQL, and allow the caller to
    // array-fetch the results in whatever sized chunks are
    // appropriate. One drawback: it would probably defeat
    // server-side cursor sharing.
    // 

    sb.setLength(0);
    sb.append("declare\n");
    sb.append("  N number;\n");
    sb.append("  X number;\n");
    sb.append("  K XDB.DBMS_SODA_ADMIN.NUMNTAB;\n");
    sb.append("begin\n");
    sb.append("  N := ?;\n");
    sb.append("  K := XDB.DBMS_SODA_ADMIN.NUMNTAB();\n");
    sb.append("  K.extend(N);\n");
    sb.append("  for I in 1..N loop\n");
    sb.append("    select \"");
    sb.append(options.keySequenceName);
    sb.append("\".NEXTVAL into X from SYS.DUAL;\n");
    sb.append("    K(I) := X;\n");
    sb.append("  end loop;\n");
    sb.append("  ? := K;\n");
    sb.append("end;");
    return(sb.toString());
  }

  private void fetchSequence(int quantity)
    throws OracleException
  {
    CallableStatement stmt = null;
    String sqltext = buildSequenceFetch();

    //
    // <quantity> is a hint about how many are likely to be needed
    // We will fetch no fewer than SEQUENCE_BATCH_SIZE nor more than
    // the size of the cache array (currently == BATCH_MAX_SIZE).
    //
    int count = quantity;

    if (count > seqCache.length)
      count = seqCache.length;
    else if (count < SEQUENCE_BATCH_SIZE)
      count = SEQUENCE_BATCH_SIZE;

    try
    {
      metrics.startTiming();

      stmt = conn.prepareCall(sqltext);
      stmt.setInt(1, count);
      stmt.registerOutParameter(2, Types.ARRAY, "XDB.DBMS_SODA_ADMIN.NUMNTAB");

      stmt.execute();

      // ### Somewhat inefficient, can we get it directly as a Long[] ?
      Array parray = stmt.getArray(2);
      BigDecimal[] numarr = (BigDecimal[])parray.getArray();

      count = numarr.length;

      for (int i = 0; i < count; ++i)
        seqCache[i] = numarr[i].longValue();

      seqCachePos = 0;
      seqCacheAvail = count;

      stmt.close();
      stmt = null;

      metrics.recordsSequenceBatchFetches();
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

  void setContentType(String ctype, OracleDocumentImpl document)
  {
    // If the content type is null, only set it if the media
    // type column is present. This means that it's truly
    // null, or unknown.
    if (ctype != null || options.doctypeColumnName != null)
      document.setContentType(ctype);
  }

  int bindMediaTypeColumn(PreparedStatement stmt,
                          int parameterIndex,
                          OracleDocument document)
    throws SQLException
  {
    String ctype = document.getMediaType();

    if (options.doctypeColumnName != null)
    {
      if (ctype != null)
      {
        stmt.setString(++parameterIndex, ctype);
      }
      else
      {
        stmt.setNull(++parameterIndex, Types.VARCHAR);
      }
    }

    return parameterIndex;
  }

  void bindKeyColumn(PreparedStatement stmt, int parameterIndex, String key)
    throws SQLException
  {
    switch (options.keyDataType)
    {
      case CollectionDescriptor.INTEGER_KEY:
        // Assumes SQL will do implicit TO_NUMBER conversion
        stmt.setString(parameterIndex, key);
        break;
      case CollectionDescriptor.RAW_KEY:
	//### Possible bug (normally the hextToRaw
	//conversion in the "else" branch works for
	//keys in columns of type RAW. But with duality view 
	//key it fails to match.
	if (options.isDualityView())
          stmt.setString(parameterIndex, key);
	else
          stmt.setBytes(parameterIndex, ByteArray.hexToRaw(key));
        break;
      case CollectionDescriptor.STRING_KEY:
        stmt.setString(parameterIndex, key);
        break;
      case CollectionDescriptor.NCHAR_KEY:
        stmt.setNString(parameterIndex, key);
        break;
      default:
        throw new IllegalStateException();
    }
  }

  private void setPayloadBlob(PreparedStatement stmt,
                              int parameterIndex,
                              byte[] data)
    throws SQLException, OracleException
  {
    // The RDBMS internal JDBC driver doesn't have the code path to
    // support setBytes(). Also, the OSON mode can't handle setBytes()
    // correctly because the server side is identified as a RAW, not
    // a BLOB, and reports a truncated length - therefore we must
    // use setBytesForBlob() whenever the length exceeds that of a RAW.
    //
    // ### Is 32767 always safe to use? Or might some servers use a
    // ### lower limit like 4000 or even 2000?
    //
    if ((internalDriver) ||
        ((options.hasBinaryFormat()) && (data != null) && (data.length > 32767)))
      // Previously we used setBlob, but setBytesForBlob(...) appears to be 
      // faster, especially for data  under 32k. According to JDBC folks, 
      // this is because direct bind path is used in that case (despite the
      // fact the jdbc 12c doc says that setBytesForBlob always uses the lob
      // binding path, see
      // https://docs.oracle.com/database/121/JJDBC/oralob.htm#JJDBC28537).
      // For larger data (tried up to and including 80k), it's still slightly
      // faster (the fact that it doesn't require ByteArrayInputStream(...)
      // wrapper might contribute).
      //
      //stmt.setBlob(parameterIndex,
      //             new ByteArrayInputStream(data),
      //             (long)data.length);
      //
    {
      if (!db.setBytesForBlob(stmt, parameterIndex, data))
        stmt.setBlob(parameterIndex, new ByteArrayInputStream(data), (long)data.length);
    }
    else
    {
      stmt.setBytes(parameterIndex, data);
    }
  }

  // ### There's a JDBC bug (23053015) which manifests when 
  //     merge statement is used with setBytes() and BLOB column,
  //     and data length exceeds 32767. This method uses
  //     the alternative setBytesForBlob(...).
  //
  //     If the bug is fixed and this workaround is removed in the future,
  //     make sure setBytesForBlob is still used with internal driver,
  //     at least when data.length exceeds 32767. The internal driver 
  //     doesn't support setBytes(...) for data exceeding 32767 bytes. 
  private void setPayloadBlobWorkaround(PreparedStatement stmt,
                                        int parameterIndex,
                                        byte[] data)
    throws SQLException, OracleException
  {
    if (!db.setBytesForBlob(stmt, parameterIndex, data))
      stmt.setBlob(parameterIndex, new ByteArrayInputStream(data), (long)data.length);
  }

  // setStringForClob(...) appears to be faster for smaller sizes of data,
  // especially for data under 32k. According to JDBC folks, this is because
  // direct bind path is used in that case (despite the fact the jdbc 12c 
  // doc says that setStringForClob always uses the lob binding path,
  // see https://docs.oracle.com/database/121/JJDBC/oralob.htm#JJDBC28537).
  //
  // ### For some of the larger cases, i.e. 80k, setClob() performs much better 
  // than setStringForClob (reason is not clear). This anamoly is specific to
  // internal driver.
  private void setClobInternalDriver(PreparedStatement stmt,
                                     int parameterIndex,
                                     String str)
    throws SQLException
  {
    if (str.length() < 32767)
      if (db.setStringForClob(stmt, parameterIndex, str))
        return;

    stmt.setClob(parameterIndex, new StringReader(str));
  }

  private void setPayloadClob(PreparedStatement stmt,
                              int parameterIndex,
                              String str)
    throws SQLException
  {
    if (internalDriver)
      setClobInternalDriver(stmt, parameterIndex, str);
    else
      stmt.setString(parameterIndex, str);
  }

  // ### There's a JDBC bug (23053015) which occurs when 
  //     merge statement is used with setString() and CLOB column,
  //     and string length exceeds 32767. This method uses
  //     the alternative setStringForClob(...).
  //
  //     If the bug is fixed and this workaround is removed in the future,
  //     make sure that setStringForClob/setClob are used with internal 
  //     driver, when string length exceeds 32767. The internal driver 
  //     doesn't support setString(...) for data exceeding 32767
  //     characters (note: JDBC doc is not clear whether this internal
  //     driver limitation of setString(...) is 32767 bytes or characters.
  //     Experimentally, however, it seems to be characters).
  private void setPayloadClobWorkaround(PreparedStatement stmt,
                                        int parameterIndex,
                                        String str)
    throws SQLException
  {
     
    if (internalDriver) 
       setClobInternalDriver(stmt, parameterIndex, str);
    else
      if (!db.setStringForClob(stmt, parameterIndex, str))
        stmt.setClob(parameterIndex, new StringReader(str));
  }

  private void setPayloadNclob(PreparedStatement stmt,
                               int parameterIndex,
                               String str)
    throws SQLException
  {
    if (internalDriver)
      stmt.setNClob(parameterIndex, new StringReader(str));
    else
      stmt.setNString(parameterIndex, str);
  }

  byte[] bindPayloadColumn(PreparedStatement stmt,
                           int parameterIndex,
                           OracleDocument document,
                           boolean eJSON,
                           Object contentWithInjectedKey)
    throws SQLException, OracleException
  {
    byte[] dataBytes = getContentForTransfer(document, eJSON, contentWithInjectedKey);

    bindPayloadColumn(stmt, parameterIndex, dataBytes, eJSON);

    return(dataBytes);
  }

  void bindPayloadColumn(PreparedStatement stmt, int parameterIndex, 
      byte[] dataBytes, boolean eJSON) throws OracleException, SQLException {
    String str;
    switch (options.contentDataType)
    {
    case CollectionDescriptor.CHAR_CONTENT:
      str = stringFromBytes(dataBytes);
      stmt.setString(parameterIndex, str);
      break;

    case CollectionDescriptor.CLOB_CONTENT:
      str = stringFromBytes(dataBytes);
      setPayloadClob(stmt, parameterIndex, str);
      break;

    case CollectionDescriptor.NCHAR_CONTENT:
      str = stringFromBytes(dataBytes);
      stmt.setNString(parameterIndex, str);
      break;

    case CollectionDescriptor.NCLOB_CONTENT:
      str = stringFromBytes(dataBytes);
      setPayloadNclob(stmt, parameterIndex, str);
      break;

    case CollectionDescriptor.RAW_CONTENT:
      stmt.setBytes(parameterIndex, dataBytes);
      break;

    case CollectionDescriptor.BLOB_CONTENT:
      setPayloadBlob(stmt, parameterIndex, dataBytes);
      break;

    case CollectionDescriptor.JSON_CONTENT:
      if (eJSON)
        setPayloadBlob(stmt, parameterIndex, dataBytes);
      else
        db.setBytesForJson(stmt, parameterIndex, dataBytes);
      break;

    default:
      throw new IllegalStateException();
    }
  }

  byte[] getContentForTransfer(OracleDocument document, boolean avoidConversionToOSON, Object contentWithInjectedKey)
    throws OracleException {
    if ((options.hasBinaryFormat() || options.hasJsonType()) && !avoidConversionToOSON)
    {
      if (contentWithInjectedKey != null && contentWithInjectedKey instanceof byte[])
      {
        return (byte[])contentWithInjectedKey;
      }
      else if (contentWithInjectedKey != null && contentWithInjectedKey instanceof String) 
      { 
        return convertToBinary(((String) contentWithInjectedKey).getBytes(ByteArray.DEFAULT_CHARSET));
      }
      else if (((OracleDocumentImpl) document).isBinary())
      {
        return ((OracleDocumentImpl) document).getBinaryContentAsByteArray();
      }
      else
      {
        return convertToBinary(document.getContentAsByteArray());
      }
    }
    else
    {
      if (contentWithInjectedKey != null && contentWithInjectedKey instanceof String) 
      {
        return ((String) contentWithInjectedKey).getBytes(ByteArray.DEFAULT_CHARSET);
      }
      else if (contentWithInjectedKey != null && contentWithInjectedKey instanceof byte[]) 
      { 
        byte[] json = null; 
        try 
        { 
          DocumentCodec osonCodec = this.db.getCodecFactory().getCodec();
          osonCodec.loadImage((byte[])contentWithInjectedKey); 
          json = osonCodec.getUnicode(); 
        } 
        catch (RuntimeException e) 
        { 
          throw SODAUtils.makeException(SODAMessage.EX_FROM_BINARY_CONVERSION_ERROR, e); 
        } 
        return json; 
      }
      else
      {
        return document.getContentAsByteArray();
      }
    }
  }
}
