/* Copyright (c) 2015, 2021, Oracle and/or its affiliates. */
/* All rights reserved.*/

/*
   DESCRIPTION
    Cache of collection descriptors for a particular account
    (i.e. Oracle schema), that can be shared among different 
    threads. 

    Thread safety is achieved by using a concurrent hash map.
    Gets and puts are done without synchronization.

    Removals are done synchronously. They should be rare.
    The cache is cleared completely during a put if it's gotten
    large, a condition that should also be rare. Users should
    run a local cache in from of this type of cache to avoid
    needing to re-fetch recently accessed descriptors.

   NOTES

    ### This clearing is done in lieu of a real LRU policy.

 */

package oracle.soda.rdbms.impl.cache;

import java.util.concurrent.ConcurrentHashMap;

import oracle.soda.rdbms.impl.CollectionDescriptor;

/**
 *  This class is not part of the public API, and is
 *  subject to change.
 *
 *  Do not rely on it in your application code.
 *
 *  @author  Doug McMahon
 *  @author  Max Orgiyan
 */

class ConcurrentDescriptorCache implements DescriptorCache
{
  private final ConcurrentHashMap<String, CollectionDescriptor> cache;

  private final int numberOfEntries;

  public ConcurrentDescriptorCache(int numberOfEntries)
  {
    this.numberOfEntries = numberOfEntries;
    this.cache = new ConcurrentHashMap<String, CollectionDescriptor>(numberOfEntries);
  }

  public CollectionDescriptor get(String collectionName)
  {
    return cache.get(collectionName);
  }

  public CollectionDescriptor putIfAbsent(CollectionDescriptor desc)
  {
    // Clear the cache if it's grown too large
    // ### We should really have a concurrent LRU cache
    if (cache.size() > numberOfEntries)
      clear();
    return cache.putIfAbsent(desc.getName(), desc);
  }

  public CollectionDescriptor put(CollectionDescriptor desc)
  {
    // Clear the cache if it's grown too large
    // ### We should really have a concurrent LRU cache
    if (cache.size() > numberOfEntries)
      clear();
    return cache.put(desc.getName(), desc);
  }

  public boolean containsDescriptor(String collectionName)
  {
    return cache.containsKey(collectionName);
  }

  public synchronized void remove(String collectionName)
  {
    cache.remove(collectionName);
  }

  public synchronized void clear()
  {
    cache.clear();
  }
}
