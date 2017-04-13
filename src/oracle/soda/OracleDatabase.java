/* Copyright (c) 2014, 2017, Oracle and/or its affiliates. 
All rights reserved.*/

package oracle.soda;

/**
 * A database of document collections.
 */
public interface OracleDatabase extends OracleDocumentFactory
{
  /**
   * Opens a collection with the specified name.
   *
   * @param collectionName      the name of the collection. Cannot be 
   *                            <code>null</code>
   * @return                    an <code>OracleCollection</code>,
   *                            <code>null</code> if the collection
   *                            with the specified <code>collectionName</code>
   *                            doesn't exist
   *
   * @throws OracleException    if (1) the provided <code>collectioName</code> is 
   *                            <code>null</code>, or (2) there's an
   *                            error opening the collection
   */
  public OracleCollection openCollection(String collectionName)
    throws OracleException;

  /**
   * Gets a <code>OracleDatabaseAdmin</code> object
   *
   * @return                    a <code>OracleDatabaseAdmin</code>
   *                            object
   */
  public OracleDatabaseAdmin admin();

}
