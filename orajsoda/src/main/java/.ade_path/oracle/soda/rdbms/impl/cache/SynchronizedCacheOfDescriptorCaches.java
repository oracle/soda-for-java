/* Copyright (c) 2015, 2021, Oracle and/or its affiliates. */
/* All rights reserved.*/

/*
   DESCRIPTION
    This a cache of <code>DescriptorCache</code> caches, each
    corresponding to an account (i.e. Oracle schema). This cache is
    thread-safe, and can be shared across threads.

    Thread safety is achieved by controlling access to the 
    non-thread-safe LRUCache via synchronized methods of this class.

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

public class SynchronizedCacheOfDescriptorCaches
  implements CacheOfDescriptorCaches
{
  private final LRUCache<String, DescriptorCache> cacheOfDescriptorCaches;

  private final int numberOfDescriptors;

  public SynchronizedCacheOfDescriptorCaches(int numberOfEntries,
                                             int numberOfDescriptors)
  {
    cacheOfDescriptorCaches =
      new LRUCache<String, DescriptorCache>(numberOfEntries);

    this.numberOfDescriptors = numberOfDescriptors;
  }

  /**
   * Get the collection descriptor cache matching an account name (i.e. an
   * Oracle schema). Creates a new collection descriptor cache if necessary.
   */
  public synchronized DescriptorCache putIfAbsentAndGet(String accountName)
  {
    if (cacheOfDescriptorCaches.containsKey(accountName))
      return cacheOfDescriptorCaches.get(accountName);

    DescriptorCache descriptorCache = new SynchronizedDescriptorCache(numberOfDescriptors);
    cacheOfDescriptorCaches.put(accountName, descriptorCache);
    return descriptorCache;
  }

  public synchronized DescriptorCache remove(String accountName)
  {
    return cacheOfDescriptorCaches.remove(accountName);
  }

  public synchronized void clear()
  {
    // We need to clear the caches one by one instead of clearing the
    // cache of caches, otherwise some objects with pointers to the
    // individual caches will still be holding the stale information.
    for (DescriptorCache dcache : cacheOfDescriptorCaches.values())
      dcache.clear();
  }

  public synchronized void clear(String accountName)
  {
    DescriptorCache dcache = cacheOfDescriptorCaches.get(accountName);
    if (dcache != null)
      dcache.clear();
  }
}
