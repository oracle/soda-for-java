/* Copyright (c) 2014, 2015, Oracle and/or its affiliates. 
All rights reserved.*/

package oracle.soda;

import java.sql.Connection;

/**
 * Entry point for SODA (Simple Oracle Document Access). Provides
 * a way to get an {@link OracleDatabase} object.
 * <p>
 * An implementation must provide a public class implementing this
 * interface, that the user application is able to instantiate. For 
 * example, for the Oracle RDBMS implementation of SODA, this
 * class is {@link oracle.soda.rdbms.OracleRDBMSClient}.
 * Then, a SODA application would obtain an instance of <code>OracleClient</code>
 * as follows:
 * </p>
 * <pre>
 * OracleClient cl = new OracleRDBMSClient();
 * </pre>
 */
public interface OracleClient
{
  /**
   * Gets the document collections database.
   * <p>
   * The same JDBC connection should not be used to back more than
   * one <code>OracleDatabase</code>
   *
   * @param connection       JDBC connection. Some SODA implementations
   *                         (e.g. non-JDBC based) can accept
   *                         <code>null</code> as a valid parameter.
   * @return                 document collections database
   * @throws OracleException if there's an error getting the database
   */
  OracleDatabase getDatabase(Connection connection)
    throws OracleException;
}
