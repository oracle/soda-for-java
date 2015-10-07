/* Copyright (c) 2014, 2015, Oracle and/or its affiliates. 
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
import java.sql.SQLException;

import oracle.jdbc.OracleConnection;

import java.util.Properties;

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

        OracleConnection oconn = (OracleConnection) connection;

        String name;

        try {
            name = oconn.getCurrentSchema();
        }
        catch (SQLException e) {
            throw new OracleException(e);
        }

        if (name == null && cacheOfDescriptorCaches != null) {
            throw SODAUtils.makeException(SODAMessage.EX_SCHEMA_NAME_IS_NULL);
        }

        DescriptorCache cache = null;

        if (cacheOfDescriptorCaches != null) {
            cache = cacheOfDescriptorCaches.putIfAbsentAndGet(name);
        }

        // ### Might be better to have metrics collector use be
        //     optional, since collecting metrics carries a cost 
        return new OracleDatabaseImpl(oconn, cache, mcollector, localMetadataCache);
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
