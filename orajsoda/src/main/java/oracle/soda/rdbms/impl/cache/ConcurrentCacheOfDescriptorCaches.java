/* Copyright (c) 2015, 2021, Oracle and/or its affiliates. */
/* All rights reserved.*/

/*
   DESCRIPTION
    This a cache of <code>DescriptorCache</code> caches, each
    corresponding to an account (i.e. Oracle schema). This cache is
    thread-safe, and can be shared across threads.

    Thread safety is achieved by using a concurrent hash map. There is
    no eviction policy, so the hash map will slowly fill with caches
    corresponding to each SODA-enabled schema.

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

package oracle.soda.rdbms.impl.cache;

import java.util.concurrent.ConcurrentHashMap;

public class ConcurrentCacheOfDescriptorCaches
  implements CacheOfDescriptorCaches
{
  private final ConcurrentHashMap<String, DescriptorCache> cacheOfDescriptorCaches =
            new ConcurrentHashMap<String, DescriptorCache>(10);

  private final int numberOfDescriptors;

  public ConcurrentCacheOfDescriptorCaches(int numberOfDescriptors)
  {
    this.numberOfDescriptors = numberOfDescriptors;
  }

  /**
   * Get the collection descriptor cache matching an account name (i.e. an
   * Oracle schema). Creates a new collection descriptor cache if necessary.
   */
  public DescriptorCache putIfAbsentAndGet(String accountName)
  {
    DescriptorCache oldCache = cacheOfDescriptorCaches.get(accountName);
    if (oldCache != null)
      return oldCache;

    DescriptorCache newCache = new ConcurrentDescriptorCache(numberOfDescriptors);
    oldCache = cacheOfDescriptorCaches.putIfAbsent(accountName, newCache);

    return (oldCache != null) ? oldCache : newCache;
  }

  public DescriptorCache remove(String accountName)
  {
    return cacheOfDescriptorCaches.remove(accountName);
  }

  public void clear()
  {
    // We need to clear the caches one by one instead of clearing the
    // cache of caches, otherwise some objects with pointers to the
    // individual caches will still be holding the stale information.
    for (DescriptorCache dcache : cacheOfDescriptorCaches.values())
      dcache.clear();
  }

  public void clear(String accountName)
  {
    DescriptorCache dcache = cacheOfDescriptorCaches.get(accountName);
    if (dcache != null)
      dcache.clear();
  }
}
