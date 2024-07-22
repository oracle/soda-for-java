/* Copyright (c) 2015, 2018, Oracle and/or its affiliates. 
All rights reserved.*/

/*
   DESCRIPTION

    Represents result of a write operation.

    ### Currently resides under oracle/soda/rdbms/impl, not
    part of a public interface. If methods using it become
    public in the future, will need to be moved to 
    oracle/soda. Currently there's no plan to make these
    methods public though.
 */

/**
 *  This class is not part of the public API, and is
 *  subject to change.
 *
 *  Do not rely on it in your application code.
 *
 *  @author  Max Orgiyan
 */
package oracle.soda.rdbms.impl;

public interface WriteResult {
  /**
   * Returns total number of documents processed
   * by the operation.
   *
   * @return total number of documents processed
   */
  long getProcessedCount();

  /**
   * Returns number of documents processed successfully
   * by the operation.
   *
   * @return total number of documents processed successfully
   */
  long getSuccessfulCount();
}
