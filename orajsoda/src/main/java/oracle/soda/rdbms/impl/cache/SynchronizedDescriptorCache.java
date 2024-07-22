/* Copyright (c) 2015, 2021, Oracle and/or its affiliates. */
/* All rights reserved.*/

/*
   DESCRIPTION
    Cache of collection descriptors for a particular account
    (i.e. Oracle schema), that can be shared among different 
    threads. 

    Thread safety is achieved by controlling access
    to the non-thread-safe LRUCache via synchronized methods
    of this class.
 */

package oracle.soda.rdbms.impl.cache;

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

class SynchronizedDescriptorCache implements DescriptorCache
{
  private final LRUCache<String, CollectionDescriptor> cache;

  public SynchronizedDescriptorCache(int numberOfEntries)
  {
    cache = new LRUCache<String, CollectionDescriptor>(numberOfEntries);
  }

  public synchronized CollectionDescriptor get(String collectionName)
  {
    return cache.get(collectionName);
  }

  public synchronized CollectionDescriptor putIfAbsent(CollectionDescriptor desc)
  {
    String collectionName = desc.getName();

    if (cache.containsKey(collectionName))
      return cache.get(collectionName);

    cache.put(collectionName, desc);
    return null;
  }

  public synchronized CollectionDescriptor put(CollectionDescriptor desc)
  {
    String collectionName = desc.getName();
    return cache.put(collectionName, desc);
  }

  public synchronized boolean containsDescriptor(String collectionName)
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
