/* Copyright (c) 2014, 2019, Oracle and/or its affiliates. 
All rights reserved.*/

package oracle.soda.rdbms;

import oracle.soda.rdbms.impl.cache.CacheOfDescriptorCaches;
import oracle.soda.rdbms.impl.cache.DescriptorCache;
import oracle.soda.rdbms.impl.CollectionDescriptor;
import oracle.soda.rdbms.impl.OracleDatabaseImpl;
import oracle.soda.rdbms.impl.SODAMessage;
import oracle.soda.rdbms.impl.SODAUtils;

import oracle.json.common.MetricsCollector;

import oracle.soda.OracleClient;
import oracle.soda.OracleDatabase;
import oracle.soda.OracleException;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import oracle.jdbc.OracleConnection;

import java.util.Properties;
import java.util.logging.Logger;

import oracle.json.logging.OracleLog;

/**
 *  Oracle RDBMS implementation of {@link oracle.soda.OracleClient}.
 *  <br>
 *  <br>
 *  Entry point to the SODA API. Provides a way to get an 
 *  {@link oracle.soda.OracleDatabase} object, from which document
 *  collections can be created and opened.
 *  <p>
 *  This class is thread-safe. Other API classes (implementing
 *  {@link oracle.soda.OracleDatabase}, {@link oracle.soda.OracleCollection}, 
 *  {@link oracle.soda.OracleDocument}, etc) are not thread-safe
 *  and should not be shared between different threads.
 *  </p>
 *  @author  Max Orgiyan
 */
public class OracleRDBMSClient implements OracleClient
{
    private CacheOfDescriptorCaches cacheOfDescriptorCaches;

    private boolean localMetadataCache;

    private final MetricsCollector mcollector;

    private static final String SELECT_USER_NAME =
      "select SYS_CONTEXT('USERENV','CURRENT_USER') from SYS.DUAL";

    private static final Logger log =
      Logger.getLogger(OracleRDBMSClient.class.getName());

    public OracleRDBMSClient() {
        mcollector = new MetricsCollector();
    }

    /**
     * The following properties are currently supported:
     * <p>
     * <code>oracle.soda.sharedMetadataCache</code> - if set to <code>true</code>,
     *                                                the shared cache of collection
     *                                                metadata will be turned on.
     * <code>oracle.soda.localMetadataCache</code> -  if set to <code>true</code>,
     *                                                the local cache of collection
     *                                                metadata will be turned on.
     * <p>
     * @param props                                   <code>Properties</code> object,
     *                                                populated with 0 or more of the
     *                                                supported properties and their values.
     */
    public OracleRDBMSClient(Properties props) {

        this();

        if (props != null) {
            String sharedMetadataCacheProp = props.getProperty("oracle.soda.sharedMetadataCache");
            String localMetadataCacheProp = props.getProperty("oracle.soda.localMetadataCache");

            if (sharedMetadataCacheProp != null && sharedMetadataCacheProp.equalsIgnoreCase("true")) {
                // Default cache size: 100 accounts (i.e. schemas), with 100 descriptors
                // each.
                cacheOfDescriptorCaches = new CacheOfDescriptorCaches(100, 100);
            }

            if (localMetadataCacheProp != null && localMetadataCacheProp.equalsIgnoreCase("true")) {
                localMetadataCache = true;
            }

        }
    }

    /**
     * Gets the document collections database.
     * <p>
     * The same JDBC connection should not be used to back more than
     * one {@link oracle.soda.OracleDatabase}
     *
     * @param connection       JDBC connection.The passed in JDBC
     *                         connection must be an instance of
     *                         <code>oracle.jdbc.OracleConnection</code>
     *
     * @return                 document collections database
     * @throws OracleException if there's an error getting the
     *                         database
     *
     */
    @Override
    public OracleDatabase getDatabase(Connection connection) 
        throws OracleException {
      return getDatabase(connection, false);   
    }
    
   /**
     * Gets the document collections database. This method serves
     * the same purpose as {@link #getDatabase(Connection)}, except
     * it also provides the <code>avoidTxnManagement</code>
     * flag. If this flag is set to <code>true</code> SODA will report
     * an error for any operation that requires
     * transaction management. This is useful when running
     * distributed transactions, to ensure that SODA's local
     * transaction management does not interfere with global
     * transaction decisions. Note: unless you're performing
     * distributed transactions, you should not use this
     * method. Instead, use {@link #getDatabase(Connection)}.
     * <p>
     * The same JDBC connection should not be used to back more than
     * one {@link oracle.soda.OracleDatabase}
     *
     * @param connection             JDBC connection.The passed in JDBC
     *                               connection must be an instance of
     *                               <code>oracle.jdbc.OracleConnection</code>
     * @param avoidTxnManagement     flag specifying whether to avoid transaction
     *                               management. If set to <code>true</code>,
     *                               an error will be thrown if a SODA operation
     *                               requires transaction management. If set to
     *                               <code>false</code>, it has no effect.
     * @return                       document collections database
     * @throws OracleException       if there's an error getting the database
     */
    public OracleDatabase getDatabase(Connection connection, boolean avoidTxnManagement)
        throws OracleException {
        OracleConnection oconn = null;

        String name = null;

        try
        {
          if (connection instanceof OracleConnection)
            oconn = (OracleConnection)connection;
          else if (connection.isWrapperFor(oracle.jdbc.OracleConnection.class))
            oconn = (OracleConnection)connection.unwrap(oracle.jdbc.OracleConnection.class);
          else
            throw SODAUtils.makeException(SODAMessage.EX_NOT_ORACLE_CONNECTION);
        }
        catch (SQLException e)
        {
          throw new OracleException(e);
        }

        DescriptorCache cache = null;

        if (cacheOfDescriptorCaches != null) {

            // To create the cache key, get the database URL,
            // and append the current username to it. It's not sufficient
            // to just use the current username by itself, because connections
            // could be coming from multiple databases with
            // the same username.
            PreparedStatement stmt = null;
            ResultSet rows = null;
            try {
              DatabaseMetaData dbmd = oconn.getMetaData();
              name = dbmd.getURL();
              name += "/";

              stmt = oconn.prepareStatement(SELECT_USER_NAME);
              rows = stmt.executeQuery();
              stmt.setFetchSize(1);
              if (rows.next())
                name += rows.getString(1);
              else 
                name = null;
            }
            catch (SQLException e)
            {
              throw new OracleException(e);
            }
            finally
            {
              for (String message : SODAUtils.closeCursor(stmt, rows))
              {
                if (OracleLog.isLoggingEnabled())
                  log.severe(message);
              }
            }

            if (name == null) {
              throw SODAUtils.makeException(SODAMessage.EX_UNABLE_TO_FETCH_USER_NAME);
            }

            cache = cacheOfDescriptorCaches.putIfAbsentAndGet(name);
        }

        // ### Might be better to have metrics collector use be
        //     optional, since collecting metrics carries a cost
        return new OracleDatabaseImpl(oconn,
                                      cache,
                                      mcollector,
                                      localMetadataCache,
                                      avoidTxnManagement);
    }

    /**
     * Creates an {@link OracleRDBMSMetadataBuilder} initialized with default
     * collection metadata settings.
     *
     * @see oracle.soda.OracleDatabaseAdmin#createCollection(String, oracle.soda.OracleDocument)
     *
     * @return
     *    a collection metadata builder
     */
    public OracleRDBMSMetadataBuilder createMetadataBuilder() {
        return CollectionDescriptor.createStandardBuilder();
    }
}
