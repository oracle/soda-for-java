/**
 * PooledConnection.java
 *
* Copyright (c) 2013, 2024, Oracle and/or its affiliates.
 *
 * Wraps an Oracle JDBC connection for pool maintenance.
 * Tracks a timestamp for last usage, and tracks the usage count.
 *
 * ### This class shouldn't be necessary if connection pooling data
 * ### sources actually worked.
 *
 * ### Assumes the connections have built-in statement caching; the
 * ### old statement cache has been removed. Revisit this assumption
 * ### if using a JDBC driver that doesn't cache statements.
 *
 * @author   Doug McMahon
 *
 */

package oracle.json.common;

import java.util.logging.Logger;

import java.sql.SQLException;

import oracle.jdbc.OracleConnectionWrapper;

import oracle.jdbc.OracleConnection;

public class PooledConnection extends OracleConnectionWrapper
{
  private static final Logger log =
    Logger.getLogger(PooledConnection.class.getName());

  private long lastAccessed = System.currentTimeMillis();
  private int  counter = 0;

  public boolean getDefault()
  {
    return(ConnectionPool.isInternalConnection());
  }

  /**
   * Construct a wrapped connection for management by a pool.
   */
  PooledConnection(OracleConnection db)
  {
    super(db);
  }

  /**
   * Get the handle to the internal connection.
   */
  OracleConnection getConnection()
  {
    return(this.unwrap());
  }

  /**
   * Used by the connection pool to check the last access time for a pooled
   * connection.
   */
  long getLastAccessTime()
  {
    return(lastAccessed);
  }

  /**
   * Used by the connection pool to check in a connection.
   */
  void setLastAccessTime()
  {
    lastAccessed = System.currentTimeMillis();
  }

  void countUsage()
  {
    ++counter;
  }

  int getUsageCount()
  {
    return(counter);
  }

  /***************************************************************************
   * ### These constants were originally used by Doug's first
   * ### implementation of APIs, and then by SODA.
   * ###
   * ### The constants used by SODA have been moved there.
   * ###
   * ### Once the original APIs are removed, these can be deleted.
   ***************************************************************************/

  public static final int ORA_SQL_OBJECT_EXISTS     = 955;
  public static final int ORA_SQL_OBJECT_NOT_EXISTS = 942;
  public static final int ORA_SQL_INDEX_NOT_EXISTS  = 1418;

  public static final int BATCH_FETCH_SIZE       = 1000;
  public static final int LOB_PREFETCH_SIZE      = 65000;
  public static final int BATCH_ROUND_INCREMENT  = 10;
  public static final int BATCH_MAX_SIZE         = 100;
  public static final int SQL_STATEMENT_SIZE     = 1000;

  /**
   * Round the batch to a multiple of 10 to reduce number of statement
   * variations (this saves memory in the DB shared cursor cache).
   */
  public int roundRows(int numrows)
  {
    int batchrows;
    if (numrows == 1)
      batchrows = numrows;
    else if (numrows < BATCH_ROUND_INCREMENT)
      batchrows = BATCH_ROUND_INCREMENT;
    else if (numrows > BATCH_MAX_SIZE)
      batchrows = BATCH_MAX_SIZE;
    else
      batchrows = numrows - (numrows % BATCH_ROUND_INCREMENT);
    return(batchrows);
  }

  /**
   * Does a hard close of a connection that is deemed unhealthy or invalid.
   */
  public void closeInvalid()
  {
    try
    {
      close(OracleConnection.INVALID_CONNECTION);
    }
    catch (SQLException e)
    {
      log.warning(e.getMessage());
    }
  }

  /*
  ** To-Do:
  **   isProxySession()
  **   openProxySession(OracleConnection.PROXYTYPE_USER_NAME, Properties)
  **                        [OracleConnection.PROXY_USER_NAME, String]
  **   close() - returns to pool
  **   close(OracleConnection.PROXY_SESSION)      - pops off proxy
  */
}
