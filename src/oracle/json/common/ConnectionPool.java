/**
 * ConnectionPool.java
 *
* Copyright (c) 2013, 2019, Oracle and/or its affiliates. All rights reserved.
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

package oracle.json.common;

import java.util.ArrayList;

import java.lang.InterruptedException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import java.sql.Connection;
import java.sql.SQLException;

//import org.apache.log4j.Logger;
import java.util.logging.Logger;

import oracle.jdbc.pool.OracleConnectionPoolDataSource;
import oracle.jdbc.pool.OracleDataSource;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.driver.OracleDriver;

import oracle.json.common.PooledConnection;
import oracle.json.util.LogFormatter;

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

  private final OracleConnectionPoolDataSource ds;

  private final ReentrantLock lock = new ReentrantLock();

  private final ArrayList<PooledConnection> pool;

  // Set this to true when undergoing a shutdown
  private volatile boolean closeFlag = false;

  /**
   * Close the connection pool permanently
   */
  private void close()
  {
    lock.lock();

    if (ds != null)
    {
      log.fine("Closing Oracle data source");
/***
      try
      {
        ds.close();
      }
      catch (SQLException e)
      {
        log.severe("Connection pool close failed with "+e.getMessage());
      }
***/
    }

/***
    try
    {
      legacyCloser();
    }
    catch (SQLException e)
    {
      log.severe("Connection pool close failed with "+e.getMessage());
    }
***/
    lock.unlock();
  }

  /**
   * Create a connection pool around a data source which uses the
   * database designated by the URI, plus the specified user/pwd
   * for all connections in the pool.
   */
  public ConnectionPool(String dburi, String dbusr, String dbpwd, int poolsz)
  {
    OracleConnectionPoolDataSource ods = null;

    this.pool = new ArrayList<PooledConnection>(poolsz);

    // ### Total hack
    if (dburi == null)
    {
      this.ds = null;
      return;
    }

    // This is just to make Fortify happy -
    // the dburi can only come from a configuration file.
    dburi = LogFormatter.sanitizeString(dburi);

/***
    // ### This used to be the way to distinguish new-style
    // ### connect strings. If it has // it's new-style.
    // ### We also now allows the () Oracle descriptors.
    if (!dburi.contains("//") && !dburi.contains("("))
    {
      this.ds = null;
      return;
    }
***/
    try
    {
/***
      // ### The JNDI way of getting the data source doesn't work in Tomcat?
      Context initContext = new InitialContext();
      Context envContext  = (Context)initContext.lookup("java:/comp/env");
      ods = (OracleDataSource)envContext.lookup("jdbc/oracle");
***/

      ods = new OracleConnectionPoolDataSource();

/***
      // ### This doesn't work (bug 21685791).
      //     The bug should be fixed in 12.2, but
      //     it's as issue with ojdbc6.jar (12.1.0.2)
      //     that we currently ship with.
      //     For now, we instead invoke this method on
      //     the actual connection.
      ods.setImplicitCachingEnabled(true);
***/
      // Use this set of interfaces for components
/***
      ods.setDriverType("thin");
      ods.setServerName("localhost");
      ods.setServiceName("ORCL");
      ods.setNetworkProtocol("tcp");
      ods.setPortNumber(1521);
***/
      // Use this for the JNDI way of specifying the database
      // jdbc:oracle:thin:@//machine-name:1521/SERVICE
      ods.setURL(dburi);

      // Set the user/pwd (if you don't want to pass them in getConnection)
      if ((dbusr != null) && (dbpwd != null))
      {
        ods.setUser(dbusr);
        ods.setPassword(dbpwd);
      }
    }
    catch (SQLException e)
    {
      log.severe("Data source "+dburi+" creation failed with "+e.getMessage());
      ods = null;
    }

    if (ods != null)
      log.info("Data source "+dburi+" created");

    ds = ods;
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

  private void closeConnection(OracleConnection db)
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

  /**
   * Get a connection from the pool.
   */
  public OracleConnection getConnection()
  {
    PooledConnection conn = null;

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
    {
      OracleConnection db = null;
      try
      {
        if (ds != null)
        {
          db = (OracleConnection)(ds.getConnection());

          // ### Normally setImplicitCachingEnabled()
          //     should just be invoked once on the datasource.
          //     However, because of bug 21685791, for now
          //     we need to invoke it on every connection.
          //     The bug should be fixed in 12.2, but that
          //     doesn't help us as long we're using ojdbc6.jar
          //     from 12.1.0.2
          db.setImplicitCachingEnabled(true);
          db.setStatementCacheSize(STATEMENT_CACHE_SIZE);
        }
/***
        else
        {
          db = (OracleConnection)legacyConnection(db_usr, db_pwd, dburi);
        }
***/
        if (db != null)
        {
          if (!db.getImplicitCachingEnabled())
          {
            log.fine("Connection started without statement caching");
            db.setImplicitCachingEnabled(true);
          }
          if (db.getAutoCommit())
          {
            log.fine("Connection started in auto-commit mode");
            db.setAutoCommit(false);
          }

          conn = new PooledConnection(db);
        }
      }
      catch (SQLException e)
      {
        log.severe("Failed to create connection with "+e.getMessage());
      }
    }

    return(conn);
  }

  /**
   * Return a connection to the pool.
   */
  public void putConnection(OracleConnection db)
  {
    PooledConnection conn = null;
    boolean mustCloseFlag = true;

    if (db == null)
      return;
    if ((db.isUsable()) && (!closeFlag) && (db instanceof PooledConnection))
    {
      conn = (PooledConnection)db;
      conn.setLastAccessTime();
      conn.countUsage();
      mustCloseFlag = (conn.getUsageCount() >= MAX_REUSE_COUNT);
    }

    if (!mustCloseFlag)
    {
      db = null;
      if (getLock(MAX_LOCK_WAIT))
      {
        try
        {
          int sz = pool.size();
          if ((sz < MAX_POOL_SIZE) && (!closeFlag))
          {
            pool.add(sz, conn);
          }
        }
        finally
        {
          lock.unlock();
        }
      }
    }

    closeConnection(db);
  }

  // Reusable array to avoid garbage on frequent sweeps
  private volatile OracleConnection[] destruct = null;

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
  public synchronized long sweep(long elapsedMillis)
  {
    int                 sz;
    int                 ndestruct = 0;
    int                 mustkill;
    int                 i;
    long                delta = Long.MAX_VALUE;
    long                currTime = System.currentTimeMillis();
    PooledConnection    conn;

    // Hard wait lock to perform the sweep
    getLock(0);

    try
    {
      sz = pool.size();
      if (sz == 0)
        return(delta);

      // Compute the minimum number of connections to kill
      mustkill = sz - MIN_POOL_SIZE;

      // Try to reuse the destruct array to avoid garbage
      if (destruct != null)
        if (destruct.length < sz)
          destruct = null;
      // Otherwise create a new one
      if (destruct == null)
        destruct = new OracleConnection[sz];

      // Go through the pool quickly, extracting "old" connections
      i = sz;
      while (i > 0)
      {
        --i;
        conn = pool.get(i);

        long age = currTime - conn.getLastAccessTime();

        // If the pool is too large we must kill the connection,
        // otherwise it's killed if it's too old.
        if ((sz <= mustkill) || (age > elapsedMillis))
        {
          pool.remove(i);
          destruct[ndestruct++] = conn;
          --mustkill;
        }
        // Otherwise keep track of the time remaining until the next kill
        else
        {
          age = elapsedMillis - age;
          if (age < delta)
            delta = age;
        }
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
        OracleConnection db = destruct[i];
        destruct[i] = null; // Erase array entry so memory is truly freed
        closeConnection(db);
      }
    }

    if (delta < Long.MAX_VALUE)
      log.fine("Oldest database expires in "+delta);

    return(delta);
  }

  /**
   * Free all cached connections in the pool.
   */
  public void clear()
  {
    sweep(0L);
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

  private static String ORA_DRIVER_NAME = "oracle.jdbc.driver.OracleDriver";

/***
  // ### This legacy-mode code is kept around in case we need it.
  // ### At one time MacOS required it because data sources didn't work.

  private OracleDriver drv = null;

  private Connection legacyConnection(String p_Username,
                                      String p_Passwrd,
                                      String p_Database)
  throws SQLException
  {
    Connection conn;

    log.warning("Resorting to legacy connection mode in desperation");
    try
    {
//
//    if (drv == null)
//    {
//      drv = new OracleDriver();
//      DriverManager.registerDriver(drv);
//    }
//
      Class.forName(ORA_DRIVER_NAME);
    }
    catch(Exception e)
    {
      throw new SQLException(ORA_DRIVER_NAME+":"+e.getMessage());
    }

    conn = DriverManager.getConnection(p_Database, p_Username, p_Passwrd);

    return(conn);
  }

  private void legacyCloser()
    throws SQLException
  {
    if (drv != null)
    {
      log.info("De-registering Oracle driver");
      DriverManager.deregisterDriver(drv);
    }
  }
***/

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
    OracleConnection db = (OracleConnection)ora.defaultConnection();
    if (db != null) db = new PooledConnection(db);
    return(db);
  }

  /**
   * Does a hard close of a connection that is deemed unhealthy or invalid.
   */
  public static void closeInvalid(Connection conn)
  {
    try
    {
      if (conn instanceof PooledConnection)
        ((PooledConnection)conn).closeInvalid();
      else if (conn instanceof OracleConnection)
        ((OracleConnection)conn).close(OracleConnection.INVALID_CONNECTION);
    }
    catch (SQLException e)
    {
      log.warning(e.getMessage());
    }
  }
}
