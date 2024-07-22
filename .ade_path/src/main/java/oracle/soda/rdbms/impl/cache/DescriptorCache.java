/* Copyright (c) 2015, 2021, Oracle and/or its affiliates. */
/* All rights reserved.*/

/*
   DESCRIPTION
    Cache of collection descriptors for a particular account
    (i.e. Oracle schema), that can be shared among different 
    threads. 

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

public interface DescriptorCache 
{
  public CollectionDescriptor get(String collectionName);

  public CollectionDescriptor putIfAbsent(CollectionDescriptor desc);

  public CollectionDescriptor put(CollectionDescriptor desc);

  public boolean containsDescriptor(String collectionName);

  public void remove(String collectionName);

  public void clear();
}
