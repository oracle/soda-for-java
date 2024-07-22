/* Copyright (c) 2014, 2024, Oracle and/or its affiliates. */
/* All rights reserved.*/

/*
   DESCRIPTION
    This is the RDBMS implementation of OracleCursor. It's backed
    by a SQL statement and result set.
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

import java.io.IOException;
import java.io.InputStream;

import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Blob;
import java.sql.Clob;

import oracle.json.logging.OracleLog;

import oracle.json.sodautil.JsonByteArray;
import oracle.soda.OracleCursor;

import java.util.logging.Logger;

import oracle.soda.OracleException;

import oracle.soda.OracleDocument;
import oracle.soda.OracleCollection;

import oracle.json.sodautil.ByteArray;

import oracle.json.sodacommon.MetricsCollector;
import oracle.json.sodacommon.LobInputStream;

public class OracleCursorImpl implements OracleCursor
{
  private static final Logger log =
    Logger.getLogger(OracleCursorImpl.class.getName());

  private Statement stmt;
  private ResultSet resultSet;
  private Operation operation;

  // Internal metrics
  private long cumTime;
  private int rowCount;

  // Internal limit on number of rows available to return
  private int maxRowLimit = -1;

  private final CollectionDescriptor desc;
  private final MetricsCollector metrics;
  private final OracleCollection coll;

  private OracleDocument nextDocument;

  boolean closed;

  // If (projectedContent == true) this is a new-style JSON REDACT-based
  // projection; the content column will be a String bind instead of the
  // base SQL data type.
  //
  private boolean projectedContent = false;

  private boolean patchedContent = false;

  private boolean hasDoctype   = false; // Has the media type column
  private boolean hasMetadata  = true;  // Has metadata/housekeeping columns

  private boolean eJSON = false;

  OracleCursorImpl(CollectionDescriptor desc,
                   MetricsCollector metrics,
                   Operation operation,
                   ResultSet resultSet)
                   throws OracleException
  {
    this.desc     = desc;
    this.metrics  = metrics;

    this.operation = operation;
    this.stmt      = operation.getStatement();
    this.coll      = operation.getCollection();
    this.resultSet = resultSet;
    this.hasDoctype = (desc.doctypeColumnName != null);

    closed = false;
  }

  OracleCursorImpl(CollectionDescriptor desc,
                   MetricsCollector metrics,
                   Operation operation,
                   ResultSet resultSet,
                   boolean projectedContent,
                   boolean patchedContent,
                   boolean eJSON)
                   throws OracleException
  {
    this(desc, metrics, operation, resultSet);
    this.projectedContent = projectedContent;
    this.patchedContent = patchedContent;
    this.hasMetadata = !patchedContent;
    this.eJSON = eJSON;
    //Note: for patched content, we also return the media type. This is
    //needed because for older codepaths after the patched content is
    //fetched we do a regular replace, which is going to replace the
    //content/version/last-modified *and* media type. The content we'll
    //have, and the version/last-modified will be generated. But we need
    //the original media type, hence it's fetched with the patched content.
  }

  void setElapsedTime(long elapsed)
  {
    this.cumTime += elapsed;
  }

  /**
   * Skip a number of rows internally
   * Not part of the public interface
   */
  public void skipRows(long offset) throws OracleException
  {
    for (long x = 0; x < offset; ++x)
    {
      if (closed)
        break;
      next();
    }
  }

  /**
   * Set a limit on how many rows to return before closing the cursor
   * This is allows only before beginning to fetch rows
   * Not part of the public interface
   */
  public void setLimit(int limit)
  {
    maxRowLimit = limit;
  }

  public boolean hasNext() throws OracleException
  { 
    if (closed)
      return false;
    
    if (nextDocument != null)
      return true;
    
    nextDocument = next();
    
    return (nextDocument != null);
  }

  private byte[] getJsonBinaryPayload()
    throws SQLException, OracleException
  {
    LobInputStream payloadStream = null;
    byte[] payload = null;

    Blob loc = resultSet.getBlob(1);
    if (loc != null) {
      long datalen = loc.length();
      if (datalen == 0L) {
        loc.free();
        return null;
      }
      else {
        InputStream inp = loc.getBinaryStream();

        if (inp != null) {
          payloadStream = new LobInputStream(loc, inp, datalen);
          payloadStream.setMetrics(metrics);
        }
      }
    }

    if (payloadStream != null)
    {
      try
      {
        payload = JsonByteArray.loadStream(payloadStream).toArray();
      }
      catch (IOException e)
      {
        if (OracleLog.isLoggingEnabled())
          log.severe(e.toString());
        // ### Revisit classification of this exception
        throw new OracleException(e);
      }
    }
    
    return payload;
  } 

  private byte[] getPatchedOrProjectedPayload()
    throws SQLException, OracleException
  {
    String str = null;

    //
    // If this is a projection, the content column is a transformation
    // of the underlying content and may be a different return type.
    //
    switch (desc.contentDataType)
    {
      // ### To-do: native BLOB return case when supported by PL/SQL.
      // ### For now, just fall through to the CLOB case. Note that
      // ### this might entail a lossy conversion through the CLOB
      // ### character set on single-byte databases.
      case CollectionDescriptor.NCLOB_CONTENT:
        // ### PL/SQL doesn't distinguish this from the CLOB case.
        // ### That's unfortunate since it means a possible lossy
        // ### character set conversion (NCLOB is guaranteed to be
        // ### AL16UTF16). For now, we can't do any better.
      case CollectionDescriptor.CLOB_CONTENT:
        // For now, all the LOB cases are returned using the toClob()
        // method. Since it's a temp LOB we need to access it explicitly
        // so we can free it.
        Clob tmp = resultSet.getClob(1);

        if (tmp != null)
        {
          str = tmp.getSubString(1L, (int) tmp.length());
          tmp.free();
        }

        break;
      case CollectionDescriptor.BLOB_CONTENT:
        if (desc.hasBinaryFormat())
          return getJsonBinaryPayload();

        /* FALLTHROUGH */

      case CollectionDescriptor.RAW_CONTENT:
        // RAW content is projected and returned as RAW
        // ### The underlying PL/SQL does UTL_RAW.CAST_TO_RAW;
        // ### perhaps it would be easier to just use toString()
        // ### and run the default case (below)? The reason not
        // ### to do that is a possible lossy conversion through
        // ### the VARCHAR2 character set.
        return resultSet.getBytes(1);
      /***
       case CollectionDescriptor.NCHAR_CONTENT:
       // Get the result as an NVARCHAR2
       str = resultSet.getNString(++num);
       break;
       // ### For now, there is no method to render to NCHAR in PL/SQL.
       // ### Instead, we use the toString() method. Therefore, for now,
       // ### use the default case (below).
       ***/
      case CollectionDescriptor.JSON_CONTENT:
        return OracleDatabaseImpl.getBytesForJson(resultSet, 1);
      default:
        // All other projections return VARCHAR2
        str = resultSet.getString(1);
        break;
    }

    if (str != null)
      return str.getBytes(ByteArray.DEFAULT_CHARSET);

    return null;
  }

  public OracleDocument next() throws OracleException
  {
    if (closed)
      throw SODAUtils.makeException(SODAMessage.EX_CANT_CALL_NEXT_ON_CLOSED_CURSOR);

    OracleDocumentImpl result = null;

    if (nextDocument != null)
    {
      result = (OracleDocumentImpl)nextDocument;
      nextDocument = null;
      return result;
    }

    LobInputStream payloadStream = null;

    long startTime = metrics.getTime();

    try
    {
      int num = 0;

      String key = null;
      byte[] payload = null;
      String mtime = null;
      String ctime = null;
      String version = null;
      String ctype = null;

      boolean hasNext = resultSet.next();

      // For projected content, skip documents with null
      // payload, to omit them from the output.
      // Null payload is returned by the
      // underlying PLSQL projection (i.e. json_select) methods,
      // if an error occurs while attempting to perform
      // a projection.
      if (hasNext && projectedContent)
      {
        payload = getPatchedOrProjectedPayload();

        while (payload == null)
        {
          hasNext = resultSet.next();
          if (hasNext)
          {
            payload = getPatchedOrProjectedPayload();
          }
          else
          {
            break;
          }
        }
        ++num;
      }

      if (hasNext)
      {
        boolean hasContent = true;
        boolean filterSpecBased = true;
        boolean singleRow  = false;
        if (operation != null)
        {
          hasContent = !operation.headerOnly();
          filterSpecBased = operation.isFilterSpecBased();
          singleRow  = operation.isSingleKeyBased();
        }

        if (patchedContent)
        {
          payload = getPatchedOrProjectedPayload();
          ++num;
        }
        else if (!projectedContent && hasContent)
        {
          switch (desc.contentDataType)
          {
            case CollectionDescriptor.CLOB_CONTENT:
            case CollectionDescriptor.CHAR_CONTENT:
              String str = resultSet.getString(++num);
              if (str != null)
                payload = str.getBytes(ByteArray.DEFAULT_CHARSET);
              break;
            case CollectionDescriptor.NCLOB_CONTENT:
            case CollectionDescriptor.NCHAR_CONTENT:
              String nstr = resultSet.getNString(++num);
              if (nstr != null)
                payload = nstr.getBytes(ByteArray.DEFAULT_CHARSET);
              break;
            case CollectionDescriptor.BLOB_CONTENT:
              // For a heterogeneous collection get the LOB locator,
              // except if the operation involved filter(filterSpec)
              // (a filterSpec based operation returns only JSON content,
              // and we try not to stream JSON content, on the assumption
              // that it's relatively small).
              boolean stream = hasDoctype && !filterSpecBased;

              // ### If the key() operation is involved, we can further
              //     refine the decision whether or not to stream:
              //     we will stream only if the ctype != null and is not JSON.
              //
              //     Apparently we can only do this additional check
              //     for a single row fetch, which key() implies.
              //
              //     For multiple row fetches JDBC might not be able
              //     to mix getBytes() and getBlob().getBinaryStream()
              //     (need to verify this).
              if (singleRow)
              {
                //if (OracleLog.isLoggingEnabled())
                //  log.info("Single key based");

                // ### When media type column is present, but the value
                //     there is null, we could consider streaming.
                //     Same situation occurs in getFragment() as well.
                // ### Sept 2018. This logic doesn't seem correct, since ctype has
                // not been fetched yet. So it will always be null, and stream will
                // be set to false for single key based operations. Revisit.
                if (ctype == null)
                {
                  stream = false;
                }
                else if (ctype.equalsIgnoreCase(OracleDocumentImpl.APPLICATION_JSON))
                {
                  stream = false;
                }
              }

              if (stream)
              {
                Blob loc = resultSet.getBlob(++num);
                if (loc != null)
                {
                  long datalen = loc.length();
                  if (datalen == 0)
                  {
                    payload = OracleCollectionImpl.EMPTY_DATA;
                    loc.free();
                  }
                  /***
                   // ### A possible alternative, using getBytes().
                   else if (datalen <= SODAConstants.LOB_PREFETCH_SIZE)
                   {
                   payload = loc.getBytes(1L, limit);
                   }
                   ***/
                  else
                  {
                    InputStream inp = loc.getBinaryStream();

                    //if (OracleLog.isLoggingEnabled())
                    //  log.info("Streaming");

                    if (inp != null)
                    {
                      payloadStream = new LobInputStream(loc, inp, datalen);
                      payloadStream.setMetrics(metrics);
                    }

                  }
                }

                if ((payloadStream == null) && (payload == null))
                  payloadStream = new LobInputStream();
                break;
              }
              // Otherwise get BLOB data using getBytes,
              // avoid the LOB descriptor and/or InputStream

              //if (OracleLog.isLoggingEnabled())
              //  log.info("Non-streaming");
            case CollectionDescriptor.RAW_CONTENT:
              payload = resultSet.getBytes(++num);
              break;

            case CollectionDescriptor.JSON_CONTENT:
              if (eJSON)
                payload = resultSet.getBytes(++num);
              else
                payload = OracleDatabaseImpl.getBytesForJson(resultSet, ++num);
              break;
          }
        }

        key = resultSet.getString(++num);

        //Note: for patched content, we also return the media type. This is
        //needed because for older codepaths after the patched content is
        //fetched we do a regular replace, which is going to replace the
        //content/version/last-modified *and* media type. The content we'll
        //have, and the version/last-modified will be generated. But we need
        //the original media type, hence it's fetched with the patched content.
        if (hasDoctype)
          ctype = resultSet.getString(++num);

        if (hasMetadata)
        {

          if (desc.timestampColumnName != null)
            mtime = OracleDatabaseImpl.getTimestamp(resultSet, ++num);

          if (desc.creationColumnName != null)
            ctime = OracleDatabaseImpl.getTimestamp(resultSet, ++num);

          if (desc.versionColumnName != null)
            version = resultSet.getString(++num);
        }

        // If a LOB stream is available, return it
        if (payloadStream != null)
        {
          result = new OracleDocumentImpl(key, version, mtime,
                                          payloadStream, ctype);
        }
        else
        {
          result = new OracleDocumentImpl(key, version, mtime, payload);
        }

        if (ctime != null) result.setCreatedOn(ctime);

        TableCollectionImpl tcoll = (TableCollectionImpl)coll;
        tcoll.setContentType(ctype, result);

        // ### Allow setting through constructor instead?
        result.setCodec(tcoll.getCodec());
        if (OracleDatabaseImpl.isOracleJsonAvailable())
          result.setJsonFactory(tcoll.getDatabase().getJsonFactory());
                                
        // ### Allow setting thru constructor instead?
        if ((desc.hasBinaryFormat() || desc.hasJsonType()) && !eJSON)
        {
          // If isOracleJsonAvailable returns false, that means the setJsonFactory
          // was not invoked on result. For processing binary documents, the json
          // factory is absolutely required.
          if (!OracleDatabaseImpl.isOracleJsonAvailable())
          { 
             try 
             {
               if (!closed)
                 closeInternal();
             }
             catch (Exception e) {}

             throw SODAUtils.makeException(SODAMessage.EX_JSON_FACTORY_MISSING_IN_JDBC);
          }
          result.setBinary();
        }

        ++rowCount;
        cumTime += metrics.getTimeDiff(startTime);
      }
      else
      {
        // Cursor is exhausted
        closeInternal();
      }

      // If there's a row limit, decrement it 
      if (maxRowLimit > 0)
        --maxRowLimit;
      // If it was already reached, we have to return a null
      else if (maxRowLimit == 0)
      {
        // ### Max: this unfortunately means you have to fetch past the
        // ### last row to get the cursor to close. What it means is that
        // ### the JDBC result set will remain open and pinned after the
        // ### last real row is delivered, until the caller tries to get
        // ### an extra row. Ideally we could release the JDBC resources
        // ### as soon as the last row was delivered.
        result = null;

        // If we're at the row limit now, close the cursor
        if (!closed)
          closeInternal();
      }
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
        // Nothing to do since we're already handling an exception
      }

      try
      {
        if (!closed)
          closeInternal();
      }
      catch (SQLException e1)
      {
        if (OracleLog.isLoggingEnabled())
          log.severe(e1.toString());
        // Nothing to do since we're already handling an exception
      }

      throw new OracleException(e);
    }

    return(result);
  }

  // Store JSON fragment with information about last QBE SQL statement.
  // This is enabled by default for the internal driver
  // (which is presumably for developers only).
  private byte[] sql_query = null;

  /**
   * Not part of a public API.
   *
   * Return the SQL statement associated with this cursor.
   * The statement is returned as a JSON fragment (so that
   * bind variable/value pairs can also be included).
   */
  public byte[] getQuery()
  {
    return(sql_query);
  }

  void setQuery(byte[] sql_query)
  {
    this.sql_query = sql_query;
  }

  public void remove() { throw new UnsupportedOperationException(); }

  private void closeInternal() throws SQLException
  {
    operation = null;

    closed = true;

    try
    {
      // First close the result set.
      // In general, closing the statement should close
      // the result set as well, but not clear what
      // happens when JDBC implicit statement caching
      // is turned on (with implicit statement caching,
      // a PreparedStatement close() doesn't close
      // the statement, but caches it). So close the
      // result set first, to make sure it's released.
      if (resultSet != null) resultSet.close();
      resultSet = null;

      if (stmt != null) stmt.close();
      stmt = null;
    }
    finally
    {
      for (String message : SODAUtils.closeCursor(stmt, resultSet))
      {
        if (OracleLog.isLoggingEnabled())
          log.severe(message);
      }

    }

  }

  @Override
  public void close() throws IOException
  {
    operation = null;

    try
    {
      closeInternal();
    }
    catch (SQLException e)
    {
      // Wrap the SQL exception because that's all we can do
      throw new IOException(e);
    }

    if (OracleLog.isLoggingEnabled())
      log.fine("Cursor read "+rowCount+" rows in "+
               metrics.nanosToString(cumTime));

    // Record the aggregated metrics
    metrics.recordCursorReads(rowCount,
                              SODAConstants.BATCH_FETCH_SIZE,
                              cumTime);
  }
}
