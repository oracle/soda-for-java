/* Copyright (c) 2014, 2017, Oracle and/or its affiliates. 
All rights reserved.*/

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
   * @param language                 language of JSON documents
   * 
   * @throws OracleException         if an error occurs while creating
   *                                 the index
   */
  public void createJsonSearchIndex(String indexName, String language)
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
