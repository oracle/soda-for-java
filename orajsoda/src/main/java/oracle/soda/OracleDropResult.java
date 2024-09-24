/* Copyright (c) 2014, 2016, Oracle and/or its affiliates. 
All rights reserved.*/

package oracle.soda;


/**
 * Holds the collection name that could not be dropped, 
 * and the error encountered when attempting to drop this collection.
 * <p>
 * An array of <code>OracleDropResult</code> objects is
 * returned by {@link OracleDatabaseAdmin#dropCollections(boolean)}.
 */
public interface OracleDropResult
{
  /**
   * Gets the name of the collection that could not be dropped.
   *
   * @return             collection name as a <code>String</code>
   */
  public String getName();

  /**
   * Gets the error encountered when attempting to drop the collection.
   *
   * @return             error as a <code>String</code>
   */
  public String getError();
}
