/* Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.*/

/*
   DESCRIPTION

    Implements WriteResult
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

class WriteResultImpl implements WriteResult {

  final long processed;
  final long successful;

  WriteResultImpl(long processed, long successful) {
    this.processed = processed;
    this.successful = successful;
  }

  /**
   * Returns total number of documents processed
   * by the operation.
   *
   * @return total number of documents processed
   */
  public long getProcessedCount() {
    return processed;
  }

  /**
   * Returns number of documents processed successfully
   * by the operation.
   *
   * @return total number of documents processed successfully
   */
  public long getSuccessfulCount() {
    return successful;
  }
}
