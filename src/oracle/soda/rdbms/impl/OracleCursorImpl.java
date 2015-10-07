/* Copyright (c) 2014, 2015, Oracle and/or its affiliates. 
All rights reserved.*/

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
import java.sql.PreparedStatement;
import java.sql.Blob;

import oracle.json.logging.OracleLog;
import oracle.soda.OracleCursor;
import java.util.logging.Logger;

import oracle.soda.OracleException;

import oracle.soda.OracleDocument;

import oracle.json.util.ByteArray;

import oracle.json.common.MetricsCollector;
import oracle.json.common.LobInputStream;

public class OracleCursorImpl implements OracleCursor
{
  private static final Logger log =
    Logger.getLogger(OracleCursorImpl.class.getName());

  private PreparedStatement stmt;
  private ResultSet resultSet;
  private Operation operation;

  // Internal metrics
  private long cumTime;
  private int rowCount;

  private final CollectionDescriptor desc;
  private final MetricsCollector metrics;

  private OracleDocument nextDocument;

  boolean closed;

  OracleCursorImpl(CollectionDescriptor desc,
                   MetricsCollector metrics,
                   Operation operation,
                   ResultSet resultSet)
                   throws OracleException
  {
    this.desc     = desc;
    this.metrics  = metrics;

    this.operation = operation;
    stmt = operation.getPreparedStatement();
    this.resultSet = resultSet;
    closed = false;
  }

  void setElapsedTime(long elapsed)
  {
    this.cumTime += elapsed;
  }

  public boolean hasNext() throws OracleException
  {
    if (closed)
      return false;

    if (nextDocument != null)
    {
      return true;
    }

    nextDocument = next();

    if (nextDocument == null)
    {
      return false;
    }

    return true;
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
      boolean hasNext = resultSet.next();

      if (hasNext)
      {
        int num = 0;

        String key;
        byte[] payload = null;
        String mtime = null;
        String ctime = null;
        String version = null;
        String ctype = null;
        boolean hasDoctype = (desc.doctypeColumnName != null);

        key = resultSet.getString(++num);
        if (hasDoctype)
          ctype = resultSet.getString(++num);

        if (!operation.headerOnly())
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
            boolean stream = hasDoctype && !operation.isFilterSpecBased();

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
            if (operation.isSingleKeyBased())
            {
              //if (OracleLog.isLoggingEnabled())
              //  log.info("Single key based");

              // ### When media type column is present, but the value
              //     there is null, we could consider streaming.
              //     Same situation occurs in getFragment() as well.
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

                // ### int is used for a limit, so the document
                //     size is limited to 2G for now. The assumption
                //     is that this is OK because the types of documents
                //     we want to target (PDFs, Word documents, mp3s, etc)
                //     will not be that huge. Huge documents, such as movies,
                //     are unlikely to fall within our use-cases.
                if (datalen > Integer.MAX_VALUE)
                {
                  loc.free();
                  throw SODAUtils.makeException(SODAMessage.EX_2G_SIZE_LIMIT_EXCEEDED,
                                                key,
                                                datalen);
                }

                int limit = (int)datalen;

                if (datalen == 0)
                {
                  payload = OracleCollectionImpl.EMPTY_DATA;
                  loc.free();
                }
                // ### A possible alternative, using getBytes().
                //
                // else if (datalen <= SODAConstants.LOB_PREFETCH_SIZE)
                // {
                //   payload = loc.getBytes(1L, limit);
                // }
                else
                {
                  InputStream inp = loc.getBinaryStream();

                  //if (OracleLog.isLoggingEnabled())
                  //  log.info("Streaming");

                  if (inp != null)
                  {
                    payloadStream = new LobInputStream(loc, inp, limit);
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
          }
        }

        if (desc.timestampColumnName != null)
          mtime = resultSet.getString(++num);

        if (desc.creationColumnName != null)
          ctime = resultSet.getString(++num);

        if (desc.versionColumnName != null)
          version = resultSet.getString(++num);

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

        ((TableCollectionImpl)operation.getCollection()).setContentType(ctype,
                                                                        result);

        ++rowCount;
        cumTime += metrics.getTimeDiff(startTime);
      }
      else
      {
        // Cursor is exhausted
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
      log.fine("Cursor read "+rowCount+
               " rows in "+metrics.nanosToString(cumTime));

    // Record the aggregated metrics
    metrics.recordCursorReads(rowCount,
                              SODAConstants.BATCH_FETCH_SIZE,
                              cumTime);
  }
}
