/* Copyright (c) 2014, 2020, Oracle and/or its affiliates. 
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
import oracle.jdbc.pool.OracleDataSource;

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
  public static final String WALLET = System.getProperty("Wallet", "");
  public static final String WALLET_PASSWORD = System.getProperty("WalletPassword", "");
  
  public static OracleConnection createConnection() throws SQLException {
    return createConnection(USER_NAME, PASSWORD);
  }

  public static OracleConnection createConnection(String user, String pass) throws SQLException {
    if ("".contentEquals(WALLET))
      return (OracleConnection) DriverManager.getConnection(connectionString(user, pass));
    else
      return createWalletConnection(user, pass);
  }

  private static OracleConnection createWalletConnection(String user, String pass) throws SQLException {
     OracleDataSource ds = new OracleDataSource();
     ds.setURL("jdbc:oracle:thin:@" + ServiceName);   
     ds.setUser(USER_NAME);
     ds.setPassword(PASSWORD);
     return (OracleConnection) ds.getConnection();
  }

  private static String connectionString(String user, String pass) {
    StringBuilder builder = new StringBuilder();
    builder.append("jdbc:oracle:thin:");
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

  static {
    if (!"".equals(WALLET)) {
      System.setProperty("oracle.net.ssl_server_dn_match","true");
      System.setProperty("oracle.net.tns_admin", WALLET);
      System.setProperty("javax.net.ssl.trustStore", WALLET + "/truststore.jks");
      System.setProperty("javax.net.ssl.trustStorePassword", WALLET_PASSWORD);
      System.setProperty("javax.net.ssl.keyStore", WALLET + "/keystore.jks");
      System.setProperty("javax.net.ssl.keyStorePassword", WALLET_PASSWORD);  
    }
  }

}


