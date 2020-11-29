/* Copyright (c) 2014, 2019, Oracle and/or its affiliates. 
All rights reserved.*/

/**
 *    DESCRIPTION
 *      ConnectionFactory
 */

/**
 * @author Josh Spiegel
 */

package oracle.json.testharness;

import java.sql.DriverManager;
import java.sql.SQLException;

import oracle.jdbc.OracleConnection;

/**
 * The tests depend on a database.  The connection information
 * for the database is specified when the tests are run:
 * 
 *  -Ddatasource=file.properties
 * 
 * The format of the property file is as follows:
 * 
 *   UserName=...
 *   Password=...
 *   Server=...
 *   Port=...
 *   DBName=...
 * 
 * DBName is the SID.
 * 
 * Note, this is the same format used by XQJ DB testing.  
 * See xdk/test/tkxq/src/xdk_xqjdb.tsc
 */
public final class ConnectionFactory {

  public static final String USER_NAME = System.getProperty("UserName");
  public static final String PASSWORD = System.getProperty("Password");
  public static final String SERVER = System.getProperty("Server");
  public static final String PORT = System.getProperty("Port");
  public static final String ServiceName = System.getProperty("DBName");

  public static OracleConnection createConnection() throws SQLException {
    return createConnection(USER_NAME, PASSWORD);
  }

  public static OracleConnection createConnection(String user, String pass) throws SQLException {
    return (OracleConnection) DriverManager.getConnection(connectionString(user, pass));
  }

  private static String connectionString(String user, String pass) {
    StringBuilder builder = new StringBuilder();
    builder.append("jdbc:oracle:thin:");
//    builder.append(user).append("/");
//    builder.append(pass).append("@//");
//    builder.append(SERVER).append(":");
//    builder.append(PORT).append("/");
//    builder.append(ServiceName);
    // Currently dataguide and context index doesn't support MTS mode, and we'll probably not support it in any near future.
    // We need to make sure our soda tests run in DEDICATED mode.
    builder.append(user).append("/");
    builder.append(pass).append("@");
    builder.append("(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=").append(SERVER);
    builder.append(")(PORT=").append(PORT);
    builder.append("))(CONNECT_DATA=(SERVICE_NAME=").append(ServiceName);
    builder.append(")(SERVER=DEDICATED)))");

    return builder.toString();
  }

}
