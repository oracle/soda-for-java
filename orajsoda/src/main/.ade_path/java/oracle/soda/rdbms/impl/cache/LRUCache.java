/* Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.*/

/*
   DESCRIPTION
    LRU cache based on <code>LinkedHashMap</code>. Not thread safe.
 */

/**
 *  This class is not part of the public API, and is
 *  subject to change.
 *
 *  Do not rely on it in your application code.
 *
 *  @author  Max Orgiyan
 */
package oracle.soda.rdbms.impl.cache;

import java.util.LinkedHashMap;
import java.util.Map;

public class LRUCache<K, V> extends LinkedHashMap<K, V> {
  private int numberOfEntries;

  public LRUCache(int numberOfEntries) {
    // Use the default HashMap initial capacity (16) and load factor (0.75).
    // The last parameter is 'true', which forces access ordering,
    // as opposed to insertion ordering. This makes the cache LRU.
    super(16, 0.75f, true);
    this.numberOfEntries = numberOfEntries;
  }

  protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
    return size() >= numberOfEntries;
  }
}
