/* Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.*/

/*
   DESCRIPTION
    Various SODA constants. Each one of these is used
    by multiple SODA classes, so they are centralized here.
 */

/**
 *  This class is not part of the public API, and is
 *  subject to change.
 *
 *  Do not rely on it in your application code.
 *
 *  @author  Max Orgiyan
 *  @author  Doug McMahon
 */

package oracle.soda.rdbms.impl;

final class SODAConstants
{
  static final int BATCH_FETCH_SIZE       = 1000;
  static final int LOB_PREFETCH_SIZE      = 65000;
  static final int SQL_STATEMENT_SIZE     = 1000;

  // Private, so that this class can't be instantiated
  private SODAConstants() {};
}
