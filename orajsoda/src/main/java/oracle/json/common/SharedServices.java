/**
 * SharedServices.java
 *
* Copyright (c) 2013, 2024, Oracle and/or its affiliates. 
 *
 * This class is designed to be used as a singleton that holds other
 * singletons. A single instance of this class is typically bound in
 * the servlet configuration context, ensuring that all requests and
 * background threads share the same service objects.
 *
 * The service objects are all stateful singletons that need to offer
 * thread-safe capabilities. Examples include the following:
 *
 * - Database connection pool
 *   Includes DataSource for connections
 * - Storage service
 *   Stateful global cache of metadata
 *
 * Typically, shared services will take a request context that provides
 * transient objects such as a database connection/transaction. They
 * must assume multi-threaded access; any internal members need to ensure
 * thread-safe access.
 */

package oracle.json.common;

import java.util.logging.Logger;

import oracle.json.common.ConnectionPool;
import oracle.json.common.JsonFactoryProvider;
import oracle.soda.rdbms.impl.cache.CacheOfDescriptorCaches;
import oracle.soda.rdbms.impl.cache.ConcurrentCacheOfDescriptorCaches;

public class SharedServices
{
  private static final Logger log =
    Logger.getLogger(SharedServices.class.getName());

  private final Configuration            conf;
  private final ConnectionPool           connPool;
  private final CacheOfDescriptorCaches  cacheOfDescriptorCaches;
  private final JsonFactoryProvider      jProvider;

  /**
   * Other objects whose creation might fail are set in the initialize
   * method.
   */
  public SharedServices(Configuration conf, boolean withPool)
  {
    this.conf       = conf;
    jProvider  = new JsonFactoryProvider();

    // ### The following specifies caches with up to 100 descriptors each.
    // ### Should these parameters be configurable?
    cacheOfDescriptorCaches   = new ConcurrentCacheOfDescriptorCaches(100);

    connPool = (withPool) ? createConnectionPool(conf) : null;
  }

  public SharedServices(Configuration conf)
  {
    this(conf, false);
  }

  /**
   * Return a shared provider (which is the provider of last resort)
   */
  public JsonFactoryProvider getJsonFactoryProvider()
  {
    return jProvider;
  }
  
  /**
   * Return a shared (thread-safe) connection pool.
   */
  public ConnectionPool getConnectionPool()
  {
    return(connPool);
  }

  /**
   * Return a shared (thread-safe) metadata cache.
   */
  public CacheOfDescriptorCaches getCacheOfDescriptorCaches()
  {
    return(cacheOfDescriptorCaches);
  }

  public Configuration getConfiguration()
  {
    return(conf);
  }

  public synchronized void destroy()
  {
    // Destroy the connection pool, if any
    if (connPool != null)
    {
      log.fine("Closing connection pool");
      connPool.shutdown();
    }
  }

  private ConnectionPool createConnectionPool(Configuration conf)
  {
    ConnectionPool pool;

    if (conf.poolSize > 0)
      pool = new ConnectionPool(conf.dbUri, conf.dbUser, conf.dbPasswrd,
                                conf.poolSize);
    else
      pool = new ConnectionPool(conf.dbUri, conf.dbUser, conf.dbPasswrd);

    return (pool);
  }
}
