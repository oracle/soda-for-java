/**
 * ConnectionPool.java
 *
* Copyright (c) 2013, 2024, Oracle and/or its affiliates. 
 *
 * Manages a pool of JDBC connections drawn from a DataSource.
 *
 * ### In theory, the DataSource itself should be able to manage
 * ### the connections, however it doesn't seem to work, and even if
 * ### it did, there are issues with respect to the life cycle that
 * ### aren't well-described. In particular:
 * ###   - It's unclear what happens if a connection drawn from
 * ###     the pool is "lost" - does it get cleaned up?
 * ###   - What happens if a connection goes "bad", does it get
 * ###     removed from the pool?
 * ###   - There's no apparent way to set a reuse count limit.
 * ###   - Inactivity timeout mechanisms appear not to account for
 * ###     the fact that a connection is "in use" (e.g. by a long/slow
 * ###     request that makes infrequent DB I/Os)
 *
 * @author   Doug McMahon
 *
 */

package oracle.json.sodacommon;

import java.util.ArrayList;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

import java.lang.InterruptedException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import oracle.jdbc.OracleConnection;
import oracle.jdbc.driver.OracleDriver;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.DriverManager;

import java.util.logging.Logger;

import oracle.json.sodautil.LogFormatter;

public class ConnectionPool
{
  private static final int MIN_POOL_SIZE   = 5;
  private static final int MAX_POOL_SIZE   = 100;
  private static final int MAX_LOCK_WAIT   = 100;
  private static final int MAX_REUSE_COUNT = 1000;
  private static final int STATEMENT_CACHE_SIZE = 50;

  private static final Logger log =
    Logger.getLogger(ConnectionPool.class.getName());

  private static final boolean isDatabaseInternal =
    (System.getProperty("oracle.jserver.version") != null);

  private final ReentrantLock lock = new ReentrantLock();

  private final ArrayList<Connection> pool;

  // Set this to true when undergoing a shutdown
  private volatile boolean closeFlag = false;

  private final String dburi;
  private final String dbusr;
  private final String dbpwd;

  /**
   * Close the connection pool permanently
   */
  private void close()
  {
  }

  /**
   * Create a connection pool around a data source which uses the
   * database designated by the URI, plus the specified user/pwd
   * for all connections in the pool.
   */
  public ConnectionPool(String dburi, String dbusr, String dbpwd, int poolsz)
  {
    this.dburi = dburi;
    this.dbusr = dbusr;
    this.dbpwd = dbpwd;

    this.pool = new ArrayList<Connection>(poolsz);
  }

  public ConnectionPool(String dburi, String dbusr, String dbpwd)
  {
    this(dburi, dbusr, dbpwd, MAX_POOL_SIZE);
  }

  public ConnectionPool(String dburi, int poolsz)
  {
    this(dburi, null, null, poolsz);
  }

  public ConnectionPool(String dburi)
  {
    this(dburi, MAX_POOL_SIZE);
  }

  private boolean getLock(int waitTime)
  {
    boolean gotLock;

    if (waitTime == 0)
    {
      lock.lock();
      return(true);
    }

    try
    {
      gotLock = lock.tryLock(waitTime, TimeUnit.MILLISECONDS);
    }
    catch (InterruptedException e)
    {
      gotLock = false;
    }

    return(gotLock);
  }

  private void closeConnection(Connection db)
  {
    if (db != null)
    {
      try
      {
        db.close();
      }
      catch (SQLException e)
      {
        log.warning("Destroy connection failed with "+e.getMessage());
      }
    }
  }

  private static String ORA_DRIVER_NAME = "oracle.jdbc.driver.OracleDriver";

  private Connection createConnection(String dburi,
                                      String dbusr,
                                      String dbpwd)
    throws SQLException
  {
    Connection conn;
/***
    ConnectionPool.initDriver();
***/
    conn = DriverManager.getConnection(dburi, dbusr, dbpwd);

    return(conn);
  }

  /**
   * Create a new connection
   */
  private Connection createConnection()
  {
    Connection conn = null;

    try
    {
        conn = createConnection(dburi, dbusr, dbpwd);

        if (conn != null)
        {
/***
          // ### Normally setImplicitCachingEnabled()
          //     should just be invoked once on the datasource.
          //     However, because of bug 21685791, for now
          //     we need to invoke it on every connection.
          //     The bug should be fixed in 12.2, but that
          //     doesn't help us as long we're using ojdbc6.jar
          //     from 12.1.0.2
          if (!conn.getImplicitCachingEnabled())
          {
            log.fine("Connection started without statement caching");
            conn.setImplicitCachingEnabled(true);
          }
          conn.setStatementCacheSize(STATEMENT_CACHE_SIZE);
***/
        }
    }
    catch (SQLException e)
    {
      log.severe("Failed to create connection with "+e.getMessage());
    }

    return(conn);
  }

  /**
   * Get a connection from the pool.
   */
  public Connection getConnection()
  {
    Connection conn = null;

    if (closeFlag)
      return(conn);

    if (getLock(MAX_LOCK_WAIT))
    {
      try
      {
        int sz = pool.size();
        if (sz > 0)
        {
          --sz;
          conn = pool.get(sz);
          pool.remove(sz);
        }
      }
      finally
      {
        lock.unlock();
      }
    }

    if (conn == null)
      conn = createConnection();

    return(conn);
  }

  /**
   * Return a connection to the pool.
   */
  public void putConnection(Connection conn)
  {
    boolean mustCloseFlag = true;

    if (conn == null)
      return;

    if (!closeFlag)
    {
      if (getLock(MAX_LOCK_WAIT))
      {
        try
        {
          int sz = pool.size();
          if ((sz < MAX_POOL_SIZE) && (!closeFlag))
          {
            pool.add(sz, conn);
            conn = null;
          }
        }
        finally
        {
          lock.unlock();
        }
      }
    }

    closeConnection(conn);
  }

  // Reusable array to avoid garbage on frequent sweeps
  private volatile Connection[] destruct = null;

  /**
   * Free cached connections in the pool if their last access time is
   * older than a specified threshold.  Specify 0 to force all connections
   * to be closed.  The sweep will also kill connections to get the pool
   * down below any current size threshold.
   *
   * Returns the time remaining on the most recently used connection
   * before it would have to be closed.
   *
   * Synchronized because only one thread can run the sweep at a time.
   */
  public synchronized void sweep()
  {
    int           sz;
    int           ndestruct = 0;
    int           i;
    Connection    conn;

    // Hard wait lock to perform the sweep
    getLock(0);

    try
    {
      sz = pool.size();
      if (sz == 0) return;

      // Try to reuse the destruct array to avoid garbage
      if (destruct != null)
        if (destruct.length < sz)
          destruct = null;
      // Otherwise create a new one
      if (destruct == null)
        destruct = new Connection[sz];

      // Go through the pool quickly, extracting "old" connections
      i = sz;
      while (i > 0)
      {
        --i;
        conn = pool.get(i);

        pool.remove(i);
        destruct[ndestruct++] = conn;
      }
    }
    finally
    {
      lock.unlock();
    }

    // Destroy the connections outsize the synchronized block
    if (destruct != null)
    {
      if (ndestruct > 0)
        log.fine("Destroying "+ndestruct+" stale database connections");
      for (i = 0; i < ndestruct; ++i)
      {
        Connection db = destruct[i];
        destruct[i] = null; // Erase array entry so memory is truly freed
        closeConnection(db);
      }
    }
  }

  /**
   * Free all cached connections in the pool.
   */
  public void clear()
  {
    sweep();
    close();
  }

  /**
   * Free all cached connections in the pool, closing the pool permanently.
   */
  public void shutdown()
  {
    closeFlag = true; // Volatile should see these effects immediately
    clear();
    close();
  }

  private static void initDriver(ConnectionPool cpool)
    throws SQLException
  { 
    try
    { 
/***  
      if (cpool != null) 
        if (cpool.drv == null)
        { 
          cpool.drv = new OracleDriver();
          DriverManager.registerDriver(cpool.drv);
        }
***/
      Class.forName(ORA_DRIVER_NAME);
    }
    catch(Exception e)
    {
      throw new SQLException(ORA_DRIVER_NAME+":"+e.getMessage());
    }
  }


  /**
   * Returns true if running inside the RDBMS's JVM
   * and therefore using the internal (KPRB) JDBC connection mode.
   */
  public static boolean isInternalConnection()
  {
    return isDatabaseInternal;
  }

  /**
   * This method returns the internal default connection from within
   * an RDBMS process.
   */
  public static OracleConnection defaultConnection()
    throws SQLException
  {
    initDriver(null);    

    // Get the internal JDBC connection from within the RDBMS
    OracleDriver ora = new OracleDriver();
    OracleConnection conn = (OracleConnection)ora.defaultConnection();
    return(conn);
  }
}
