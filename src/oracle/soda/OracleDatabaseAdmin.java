/* Copyright (c) 2014, 2016, Oracle and/or its affiliates. 
All rights reserved.*/

package oracle.soda;

import oracle.soda.rdbms.impl.CollectionDescriptor;
import oracle.soda.rdbms.OracleRDBMSMetadataBuilder;

import java.util.List;

import java.sql.Connection;

/**
 * Provides DDL and metadata methods for the {@link OracleDatabase}
 * administration: collection creation, retrieval of collection names, etc.
 * <p>
 * An <code>OracleCollectionAdmin</code> object is associated
 * with a particular <code>OracleCollection</code> object.
 */
public interface OracleDatabaseAdmin
{
  /**
   * Creates a collection with the specified name.
   * <p>
   * Collection will be configured according to default implementation-specific
   * collection metadata.
   * <p>
   * If the collection with the specified <code>collectionName</code> exists,
   * it's returned.
   * <p>
   * @see OracleCollectionAdmin#drop()
   *
   * @param  collectionName                collection name
   * @return OracleCollection              created collection
   * @throws OracleException               if the collection could not
   *                                       be created
   */
  public OracleCollection createCollection(String collectionName)
    throws OracleException;

  /**
   * Creates a collection with the specified name and
   * implementation-specific collection metadata, expressed in
   * JSON.
   * <p>
   * Passing <code>null</code> for <code>collectionMetadata</code> is equivalent
   * to invoking {@link #createCollection(String)}.
   * <p>
   * If the collection with the specified <code>collectionName</code> exists,
   * it's returned (unless its metadata doesn't match the metadata
   * specified via <code>collectionMetadata</code>,
   * in which case an exception is thrown).
   * <p>
   * In case of the Oracle RDBMS implementation of SODA,
   * the <code>collectionMetadata</code> document can be created with {@link
   * oracle.soda.rdbms.OracleRDBMSMetadataBuilder}.
   * <p>
   * @see OracleCollectionAdmin#drop()
   *
   * @param collectionName                 collection name
   * @param collectionMetadata             implementation-specific collection
   *                                       metadata
   * @return OracleCollection              created collection
   * @throws OracleException               if (1) the collection could not
   *                                       be created, or (2) the collection
   *                                       with the provided <code>collectionName</code>
   *                                       already exists and its metadata
   *                                       does not match
   *                                       the metadata specified in
   *                                       <code>collectionMetadata</code>
   */
  public OracleCollection createCollection(String collectionName,
                                           OracleDocument collectionMetadata)
    throws OracleException;

  /**
   * Gets a list of the names of all collections in the database.
   *
   * @return                               a list of collection names
   * @throws OracleException               if there is an error retrieving
   *                                       collection names from the
   *                                       database
   */
  public List<String> getCollectionNames()
    throws OracleException;

  /**
   * Gets a list of the names of collections in the database with a
   * limit on the number returned.
   *
   * This method implies that the list is ordered.
   *
   * @param limit                          a limit on the number of
   *                                       names returned. Must be
   *                                       positive
   * @return                               a list of collection names
   * @throws OracleException               if (1) the limit is negative,
   *                                       or (2) there is an error retrieving
   *                                       collection names from the
   *                                       database
   */
  public List<String> getCollectionNames(int limit)
    throws OracleException;

  /**
   * Gets a list of the names of collections in the database with a
   * limit on the number returned, starting at a specific offset in
   * the list.
   *
   * This method implies that the list is ordered.
   *
   * @param limit                          a limit on the number of
   *                                       names returned. Must be
   *                                       positive
   * @param skip                           a number of names to skip.
   *                                       Must not be negative
   * @return                               a list of collection names
   * @throws OracleException               if (1) the limit or skip are negative,
   *                                       or (2) there is an error retrieving
   *                                       collection names from the
   *                                       database
   */
  public List<String> getCollectionNames(int limit, int skip)
    throws OracleException;

  /**
   * Gets a list of the names of collections in the database with a
   * limit on the number returned, starting at the first name greater
   * than or equal to <code>startName</code>.
   *
   * This method implies that the list is ordered.
   *
   * @param limit                          a limit on the number of
   *                                       names returned. Must be
   *                                       positive
   * @param startName                      the starting name. All names
   *                                       greater than or equal to this name
   *                                       will be returned. Cannot be
   *                                       <code>null</code>
   * @return                               a list of collection names.
   *                                       If there is no collection name
   *                                       greater than <code>startName</code>,
   *                                       null is returned
   * @throws OracleException               if (1) the limit is negative,
   *                                       or (2) the startName is null
   *                                       or empty, or (3) there is an
   *                                       error retrieving collection
   *                                       names from the database
   */
  public List<String> getCollectionNames(int limit, String startName)
    throws OracleException;

  /**
   * Return the JDBC connection backing this database.
   *
   * @return                               the JDBC connection backing this database.
   *                                       <code>null</code> if this database is not
   *                                       backed by a single JDBC connection
   */
  public Connection getConnection();

  /**
   * Drop all collections associated with this {@link OracleDatabase}.
   * <p>
   * Caution: using the force option (i.e. setting the force parameter
   * to true), will delete all collection metadata for the given
   * {@link OracleDatabase}. In the case of Oracle RDBMS, an {@link OracleDatabase}
   * is associated with a schema (i.e. user), so all collection metadata
   * for that schema will be deleted. However, the underlying tables or
   * views of these collections might not be deleted (if, for
   * example, they have uncommitted writes). This intended use
   * of the force option is to clear collection metadadata for the
   * given schema, in preparation for dropping the whole schema.
   * Otherwise, the <code>force</code> parameter should be set
   * to <code>false</code>. With this option, collections will
   * not be dropped if they have underlying tables or views which
   * could not be dropped.
   * 
   * @param force                  if set to <code>true</code> all collection
   *                               metadata will be erased, even if some of the
   *                               collections could not be dropped. Normally
   *                               this parameter should be set to <code>false</code>,
   *                               so that no danling tables or views backing
   *                               collections are left in the database.
   *                               The intended use of this option is to clear
   *                               the collection metadata in preparation for
   *                               dropping the whole schema.
   * @return                       A list of {@link OracleDropResult}. Each of 
   *                               these results contains the name of a 
   *                               collection that could not be dropped and 
   *                               a corresponding encountered error
   *                               message. 
   * @throws OracleException       if fatal error is encountered.
   */
  public List<OracleDropResult> dropCollections(boolean force)
    throws OracleException;
}
