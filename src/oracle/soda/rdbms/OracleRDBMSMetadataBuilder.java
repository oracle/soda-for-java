/* Copyright (c) 2014, 2015, Oracle and/or its affiliates. 
All rights reserved.*/

package oracle.soda.rdbms;

import oracle.soda.OracleDocument;
import oracle.soda.OracleException;

/**
 * Builds custom collection metadata, expressed as a JSON {@link OracleDocument}.
 * The resulting {@link OracleDocument} must be supplied to
 * {@link oracle.soda.OracleDatabaseAdmin#createCollection(String, OracleDocument)}
 * method to create a collection with the provided metadata.
 * Custom collection metadata is needed when you want to specify, for example,
 * how the collection is stored, or that the collection is read-only,
 * or remove some of the optional (e.g. version, created-on timestamp, last
 * modified timestamp) columns from the underlying table storing the collection. 
 * Custom collection metadata can also be used to map a pre-existing SQL table to
 * a new SODA collection.
 * <p>
 * The builder is created as follows:
 * <br>
 * <pre>
 *     OracleRDBMSClient client = ...
 *     OracleRDBMSMetadataBuilder builder = client.createMetadataBuilder();
 * </pre>
 * The resulting builder is initialized with default metadata settings.
 * These settings can be modified using various {@link OracleRDBMSMetadataBuilder}
 * methods. For example, to change the key assignment method to client-assigned, and
 * to change the content column type to {@code VARCHAR2}, invoke
 * {@link #keyColumnAssignmentMethod(String)} and {@link #contentColumnType(String)}
 * as follows:
 * <br>
 * <br>
 * <pre>
 *     builder.keyColumnAssignmentMethod("CLIENT").contentColumnType("VARCHAR2");
 * </pre>
 * Once the desired changes to the default metadata settings are made, the
 * {@link OracleDocument} representing collection metadata in JSON should be
 * created as follows:
 * <br>
 * <br>
 * <pre>
 *     OracleDocument metadata = builder.build();
 * </pre>
 * This metadata document is supplied to the
 * {@link oracle.soda.OracleDatabaseAdmin#createCollection(String, OracleDocument)}
 * method to create a collection with the specified custom metadata:
 * <br>
 * <br>
 * <pre>
 *     OracleDatabase db = ...
 *     OracleCollection collection = db.admin().createCollection("myCollection", metadata);
 * </pre>
 * @see OracleRDBMSClient#createMetadataBuilder()
 * @see oracle.soda.OracleDatabaseAdmin#createCollection(String, oracle.soda.OracleDocument)
 * @author  Josh Spiegel
 */
public interface OracleRDBMSMetadataBuilder 
{

    /**
     * Builds a JSON object containing the collection metadata.
     * 
     * An {@code OracleException} will be raised if an invalid configuration 
     * is detected.  The specific validation errors that may occur are documented 
     * throughout this interface.
     *
     * @see oracle.soda.OracleDatabaseAdmin#createCollection(String, OracleDocument)
     *
     * @return The JSON collection metadata encoded as UTF-8
     * @throws OracleException
     *      if an invalid combination is detected
     */
    OracleDocument build() throws OracleException;

    /**
     * Sets the SQL schema name for the underlying database object.  If {@code schemaName} is 
     * {@code null}, the schema associated with the connection will be used.
     * 
     * @param schemaName 
     *      the name of the schema or {@code null}
     * @return
     *      a reference to this object
     */
    OracleRDBMSMetadataBuilder schemaName(String schemaName);

    /**
     * Sets the SQL table name.  A collection is based off of a table or a view.
     * Calling this method will override the view name if previously set using
     * {@link #viewName(String)}. If {@code tableName} is {@code null}, the 
     * collection will be created using a table name based on the name of the
     * collection name.
     * 
     * @see #viewName(String)
     * @param tableName
     *      the name of the table. May be {@code null}
     * @return
     *      a reference to this object
     */

    OracleRDBMSMetadataBuilder tableName(String tableName);

    /**
     * Sets the SQL view name. A collection is based off of a table or a view.
     * Calling this method will override the table name 
     * if previously set using {@link #tableName(String)}
     * If {@code viewName} is {@code null}, the collection will be created using
     * a table name based on the name of the collection name.
     * 
     * @see #tableName(String)
     * @param viewName
     *      the name of the view. May be {@code null}
     * @return
     *      a reference to this object
     */
    OracleRDBMSMetadataBuilder viewName(String viewName);

    /*
     * Sets the SQL package name. A collection is based off of a table, view, 
     * or package. Calling this method will override the table or package name 
     * if previously set using {@link #tableName(String)}
     * or {@link #viewName(String)}. If {@code packageName} is {@code null}, the 
     * collection will be created using a table name based on the name of the
     * collection name.
     *
     * @see #tableName(String)
     * @see #viewName(String)
     * @param packageName
     *      the name of the package. May be {@code null}
     * @return
     *      a reference to this object
     */
    //OracleRDBMSMetadataBuilder packageName(String packageName);

    /**
     * Sets the name of the content column. If {@code name} is {@code null},
     * the default value {@code "JSON_DOCUMENT"} is used.
     * 
     * @param name
     *      the name of the column. May be {@code null}
     * @return
     *      a reference to this object
     */
    OracleRDBMSMetadataBuilder contentColumnName(String name);

    /**
     * Sets the SQL type of the content column. The value must be either
     * {@code "VARCHAR2"}, {@code "NVARCHAR2"}, {@code "RAW"}, {@code "BLOB"}, 
     * {@code "CLOB"}, or {@code "NCLOB"}. If {@code sqlType} is {@code null}, 
     * the default value {@code "BLOB"} is used.
     * <p>
     * Changing the content column type may automatically remove other related settings:
     * </p>
     * <ul>
     * <li>Setting to LOB type will remove the content column maximum length.</li>
     * <li>Setting to a non-LOB type will remove all SecureFile LOB settings.</li>
     * </ul>
     * 
     * @param sqlType
     *      the type of the content column. May be {@code null}
     * @return
     *      a reference to this object
     * @throws OracleException 
     *      if {@code sqlType} is not one of the expected values
     */
    OracleRDBMSMetadataBuilder contentColumnType(String sqlType)
            throws OracleException;

    /**
     * Sets the maximum length of the content column. If 
     * {@code maxLength} is 0, then the maximum is unspecified and
     * the default value of 2000 will be used, if applicable. If the content column type
     * is a LOB type and a maximum length is specified, an error will be raised when
     * {@link #build()} is called.
     * 
     * @see #contentColumnType(String)
     * @param maxLength
     *      the maximum size
     * @return
     *      a reference to this object
     * @throws OracleException
     *      if {@code maxLength} is negative
     */
    OracleRDBMSMetadataBuilder contentColumnMaxLength(int maxLength)
            throws OracleException;

    /**
     * Sets the validation mode for the content column.  
     * The value of {@code validation} must be one of
     * {@code "STANDARD"}, {@code "STRICT"}, or {@code "LAX"}. 
     * If {@code validation} is {@code null}, the default {@code "STANDARD"}
     * mode is used.
     * 
     * @param validation
     *      the validation mode.  May be {@code null}
     *      
     * @return
     *      a reference to this object.  
     * @throws OracleException 
     *      if {@code validation} is not {@code null}, {@code "STANDARD"}, 
     *      {@code "STRICT"}, or {@code "LAX"}
     */
    OracleRDBMSMetadataBuilder contentColumnValidation(String validation) 
            throws OracleException;

    /**
     * Sets the SecureFiles LOB compress setting for the content column.
     * The value of {@code compress} must be one of {@code "NONE"}, 
     * {@code "HIGH"}, {@code "MEDIUM"},  or {@code "LOW"}.
     * This setting is not applicable if the 
     * {@link #contentColumnType(String) content column type} is not a 
     * LOB type. If {@code compress} is {@code null}, the default value {@code "NONE"}
     * is used.
     * 
     * <p>
     * If the compression is set to something other than {@code "NONE"} and
     * the column is not a LOB type, an error will be raised when {@link #build()}
     * is called.
     * </p>
     * 
     * @see #contentColumnType(String)
     * @param compress
     *      the compression mode.  May be {@code null}
     *      
     * @return
     *      a reference to this object
     * @throws OracleException
     *      if {@code compress} is not one of the expected values
     */
    OracleRDBMSMetadataBuilder contentColumnCompress(String compress)
            throws OracleException;

    /**
     * Sets the SecureFiles LOB cache setting for the content column.
     * This setting is not applicable if the 
     * {@link #contentColumnType(String) content column type} is not a 
     * LOB type. The default value is {@code true}.
     * 
     * <p>
     * If the {@code cache} is {@code true} and the column is not a LOB type, 
     * an error will be raised when {@link #build()} is called.
     * </p>
     * 
     * @see #contentColumnType(String)
     * @param cache
     *      specifies if caching is used
     * @return
     *      a reference to this object
     */
    OracleRDBMSMetadataBuilder contentColumnCache(boolean cache);

    /**
     * Sets the SecureFiles LOB encryption setting for the content column.  The
     * value of {@code encrypt} must be {@code "NONE"}, {@code "3DES168"}, {@code "AES128"}, 
     * {@code "AES192"}, {@code "AES256"}. If {@code encrypt} is null, the default
     * value {@code "NONE"} is used. This setting is not applicable if the 
     * {@link #contentColumnType(String) content column type} is not a 
     * LOB type. If {@code encrypt} is anything other than {@code "NONE"} and the column 
     * is not a LOB type, an error will be raised when {@link #build()} is called.
     * 
     * @see #contentColumnType(String)
     * @param encrypt
     *      the encryption type.  May be {@code null}
     * @return
     *      a reference to this object
     * @throws OracleException
     *      if {@code encrypt} is not one of the expected values
     */
    OracleRDBMSMetadataBuilder contentColumnEncrypt(String encrypt)
            throws OracleException;

    /**
     * Sets the name of the key column. If {@code name} is null, 
     * they default value {@code "ID"} is used. 
     * 
     * @param name
     *      the name of the key column.  May be {@code null}
     * @return
     *      a reference to this object
     */
    OracleRDBMSMetadataBuilder keyColumnName(String name);

    /**
     * Sets the key column type.  The value of {@code sqlType} must be one of 
     * {@code "VARCHAR2"}, {@code "NVARCHAR2"}, {@code "NUMBER"}, 
     * or {@code "RAW"}.  If {@code sqlType} is {@code null}, the default
     * value {@code "VARCHAR2"} will be used.  Setting the key column type to 
     * {@code "NUMBER"} or {@code "RAW"} automatically removes the maximum size of 
     * the key column. 
     * 
     * @param sqlType
     *      the key column type.  May be {@code null}
     * @return 
     *      a reference to this object
     * @throws OracleException
     *      if {@code sqlType} is not one of the expected values.  
     */
    OracleRDBMSMetadataBuilder keyColumnType(String sqlType)
          throws OracleException;

    /**
     * Sets the maximum size of the key column.  
     * If {@code maxLength} is 0, then the maximum is unspecified and
     * the default value of 255 will be used, if applicable. If the key column type
     * is {@code NUMBER} or {@code RAW} and a maximum length is specified, an error will 
     * be raised when {@link #build()} is called.
     * 
     * @see #keyColumnType(String)
     * @param maxLength
     *      the maximum size
     * @return
     *      a reference to this object
     * @throws OracleException
     *      if {@code maxLength} is negative
     */
    OracleRDBMSMetadataBuilder keyColumnMaxLength(int maxLength)
            throws OracleException;

    /**
     * Sets the key column assignment method.  The value of {@code assignmentMethod}
     * must be one of {@code "SEQUENCE"}, {@code "GUID"}, {@code "UUID"},
     * or {@code "CLIENT"}.  If {@code assignmentMethod} is {@code null},
     * the default value {@code "UUID"} will be used.  If {@code assignmentMethod}
     * is anything other than {@code "SEQUENCE"}, the key column sequence name
     * will be automatically removed.
     * If the assignment method is {@code "SEQUENCE"} but no key column
     * sequence name is specified, an error will be raised when {@link #build()}
     * is called.
     * 
     * @see #keyColumnSequenceName(String)
     * @param assignmentMethod
     *      the key column assignment method.  May be {@code null}
     * @return
     *      a reference to this object.  May be {@code null}
     * @throws OracleException
     *      if {@code assignmentMethod} is not one of the expected values
     */
    OracleRDBMSMetadataBuilder keyColumnAssignmentMethod(String assignmentMethod)
            throws OracleException;

    /**
     * Sets the sequence name for sequence-based keys. If {@code sequenceName}
     * is {@code null}, the sequence name is unspecified.  Specifying a sequence 
     * name automatically changes the key column assignment method to {@code "SEQUENCE"}.
     *
     * @see #keyColumnAssignmentMethod(String)
     * @param sequenceName
     *      the name of the sequence.  May be {@code null}
     * @return
     *      a reference to this object
     */
    OracleRDBMSMetadataBuilder keyColumnSequenceName(String sequenceName);

    /**
     * Sets the optional creation time column name.  If {@code name} is
     * {@code null}, no creation time column will be used.  The default
     * value is {@code "CREATED_ON"}.
     *
     * @param name
     *      the name of the column.  May be {@code null}
     * @return
     *      a reference to this object
     */
    OracleRDBMSMetadataBuilder creationTimeColumnName(String name);

    /**
     * Sets the optional last-modified timestamp column name.  If {@code name}
     * is {@code null}, no last-modified column will be used and
     * the last-modified column index will automatically be removed.  The default
     * value is {@code "LAST_MODIFIED"}.
     *
     * @see #lastModifiedColumnIndex(String)
     * @param name
     *      the name of the column. May be {@code null}
     * @return
     *      a reference to this object
     */
    OracleRDBMSMetadataBuilder lastModifiedColumnName(String name);

    /**
     * Sets the last-modified column index name. If {@code index}
     * is {@code null}, the index name is unspecified. If the 
     * last-modified index name is specified but the last-modified 
     * column name is not set, an error will be raised when {@link #build()}
     * is called.
     *
     * @see #lastModifiedColumnName(String)
     * @param index
     *      the name of the index. May be {@code null}
     * @return
     *      a reference to this object
     */
    OracleRDBMSMetadataBuilder lastModifiedColumnIndex(String index);

    /**
     * Sets the version (ETag) column name.  If {@code name} is {@code null},
     * the version column will not be used and the version column method
     * will automatically set to {@code "NONE"}.  The default value is
     * {@code "VERSION"}.
     *
     * @see #versionColumnMethod(String)
     * @param name
     *      the name of the column. May be {@code null}
     * @return
     *      a reference to this object
     */
    OracleRDBMSMetadataBuilder versionColumnName(String name);

    /**
     * Sets version column method.  The value of {@code method} must be 
     * {@code "SEQUENTIAL"}, {@code "TIMESTAMP"}, {@code "UUID"}, {@code "SHA256"}, 
     * {@code "MD5"}, or {@code "NONE"}.  If {@code method} is {@code null}, the default
     * value {@code "NONE"} is used. If the version column method is not {@code "NONE"}
     * and the version column name is not specified, an error will be raised when {@link #build()}
     * is called. 
     * 
     * @param method
     *      the version method. May be {@code null}
     * @return
     *      a reference to this object
     * @throws OracleException
     *      if {@code method} is not one of the expected values
     */
    OracleRDBMSMetadataBuilder versionColumnMethod(String method) 
            throws OracleException;

    /**
     * Sets the media type column name.  If {@code name} is {@code null},
     * the media type column is unspecified and a media type column will not be used.  
     * If the {@link #contentColumnType(String) 
     * content column type} is not {@code "BLOB"} and the media type column is specified, 
     * an error will be raised when {@link #build()} is called.  By default, the media type
     * column is unspecified.
     *
     * @param name
     *      the name of the media type column.  May be {@code null}
     * @return
     *      a reference to this object
     */
    OracleRDBMSMetadataBuilder mediaTypeColumnName(String name);

    /**
     * Sets the read/write policy.  The default value is {@code false}.
     * 
     * @param readOnly
     *      when true, specifies the collection is read only
     * @return
     *      a reference to this object
     */
    OracleRDBMSMetadataBuilder readOnly(boolean readOnly);
    
    /**
     * Removes the optional metadata columns. This method is useful
     * when creating a collection over an existing table where these
     * columns may not exist.
     * <p>
     * Calling this method is equivalent to the following:
     * </p>
     * <pre>
     *   this
     *    .creationTimeColumnName(null)
     *    .lastModifiedColumnName(null)
     *    .versionColumnName(null)
     *    .mediaTypeColumnName(null);
     * </pre>
     *
     * @return
     *     a reference to this object
     */
    OracleRDBMSMetadataBuilder removeOptionalColumns();

}
