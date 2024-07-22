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

public interface CacheOfDescriptorCaches
{
  /**
   * Get the collection descriptor cache matching an account name (i.e. an
   * Oracle schema). Creates a new collection descriptor cache if necessary.
   */
  public DescriptorCache putIfAbsentAndGet(String accountName);

  public DescriptorCache remove(String accountName);

  public void clear();

  public void clear(String accountName);
}
