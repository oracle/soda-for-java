/* Copyright (c) 2014, 2021, Oracle and/or its affiliates.*/
/* All rights reserved.*/

package oracle.soda;

import java.util.List;

/**
 * Provides DDL and metadata methods for the {@link OracleCollection}
 * administration: index creation and destruction, collection deletion
 * (ie "drop"), metadata information about the collection,
 * etc.
 * <p>
 * An <code>OracleCollectionAdmin</code> object is associated
 * with a particular <code>OracleCollection</code> object.
 */
public interface OracleCollectionAdmin
{
  /**
   * Gets the collection's name.
   *
   * @return                         collection name as a <code>String</code>
   */
  public String getName();

  /**
   * Drops the collection.
   *
   * @throws OracleException         if an error occurs while dropping
   *                                 the collection
   */
  public void drop()
    throws OracleException;

  /**
   * Drops the collection.
   *
   * Passing <code>false</code> for <code>purge</code> 
   * and <code>false</code> for <code>dropMappedObject</code> is equivalent
   * to invoking {@link #drop()}.
   *
   * @param  purge                    set to <code>true</code> to release the space
   *                                  associated with the table underlying the collection.
   *                                  If set to <code>true</code> the database does not 
   *                                  place the table backing the collection into the
   *                                  recycle bin.
   *                                  <b>Note</b>: you cannot recover the table if the
   *                                  collection was dropped with <code>purge</code> set 
   *                                  to <code>true</code>.
   *                                  Setting <code>purge</code> to <code>true</code> is
   *                                  only supported if the Oracle Database release is 21c
   *                                  or above.
   * @param dropMappedObject          set to <code>true</code> to drop the object (table or
   *                                  view) underlying the mapped collection. If set to 
   *                                  <code>false</code> the object underlying the mapped
   *                                  collection will not be dropped.
   *                                  Setting <code>dropMappedObject</code> to 
   *                                  <code>true</code> is only supported if the Oracle 
   *                                  Database release is 21c or above.
   * @throws OracleException          if an error occurs while dropping
   *                                  the collection
   */
  public void drop(boolean purge, boolean dropMappedObject)
    throws OracleException;

  /**
   * Deletes all documents in the collection.
   *
   * @throws OracleException         if an error occurs while truncating
   *                                 the collection
   */
  public void truncate()
    throws OracleException;

  /**
   * Drops the named index.
   *
   * @param indexName                name of the index to drop
   * @throws OracleException         if an error occurs while dropping
   *                                 the index
   */
  public void dropIndex(String indexName)
    throws OracleException;

  /**
   * Drops the named index.
   *
   * @param indexName                name of the index to drop
   * @param force                    force index drop. Can only be used with
   *                                 json search or spatial indexes.
   * @throws OracleException         if an error occurs while dropping
   *                                 the index
   */
  public void dropIndex(String indexName, boolean force)
    throws OracleException;


  /**
   * Create an index using an index specification (expressed in JSON).
   * 
   * @param indexSpecification       an index specification. Cannot be
   *                                 <code>null</code>
   * @throws OracleException         if (1) the index specification is
   *                                 <code>null</code>, or (2) an error
   *                                 occurs while dropping the index
   */
  public void createIndex(OracleDocument indexSpecification)
    throws OracleException;

  /**
   * Turns on Json Search Index.
   *
   * @param indexName                name of the index. Cannot be
   *                                 <code>null</code>
   *
   * @throws OracleException         if an error occurs while creating
   *                                 the index
   */
  public void createJsonSearchIndex(String indexName)
    throws OracleException;

  /**
   * Returns the name of the table or view backing the collection.
   *
   * @return                         object name as a <code>String</code>
   */
  public String getDBObjectName();

  /**
   * Returns the name of the schema that owns the table or view backing the
   * collection.
   *
   * @return                         schema name as a <code>String</code>
   */
  public String getDBObjectSchemaName();

  /**
   * Indicates whether the collection can store non-JSON data.
   *
   * @return                         <code>true</code> if the collection can
   *                                 store non-JSON data; <code>false</code>
   *                                 otherwise
   */
  public boolean isHeterogeneous();

  /**
   * Indicates whether the collection is read-only.
   *
   * @return                         <code>true</code> if the collection is read-only;
   *                                 <code>false</code> otherwise
   */
  public boolean isReadOnly();

  /**
   * Returns a JSON data guide for the collection. Requires 12.2.0.1
   * and above Oracle Database release.
   *
   * @return                         <code>OracleDocument</code> representing
   *                                 JSON data guide for the collection. <code>null</code>
   *                                 if the data guide is not available.
   * @throws OracleException         if an error occurs while fetching the JSON
   *                                 data guide.
   */
  public OracleDocument getDataGuide() throws OracleException;

  /**
   * Returns collection metadata expressed in JSON.
   *
   * @return                         collection metadata in JSON
   */
  public OracleDocument getMetadata();
}
