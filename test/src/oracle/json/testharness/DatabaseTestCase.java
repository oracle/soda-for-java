/* Copyright (c) 2014, 2017, Oracle and/or its affiliates. 
All rights reserved.*/

/**
 *  DESCRIPTION
 *   DatabaseTestCase
 */

/**
 *  @author  Josh Spiegel
 */

package oracle.json.testharness;

import oracle.jdbc.OracleConnection;
import oracle.soda.rdbms.impl.SODAUtils;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;

public abstract class DatabaseTestCase extends JsonTestCase {

    public static final int PATCH1 = 1;
    public static final int PATCH2 = 2;

    public static final Integer PATCH_VERSION = Integer.getInteger("patch.version");

    protected OracleConnection conn;

    protected static SODAUtils.SQLSyntaxLevel sqlSyntaxLevel =
      SODAUtils.SQLSyntaxLevel.SQL_SYNTAX_UNKNOWN;

    @Override
    protected void setUp() throws Exception {
        conn = ConnectionFactory.createConnection();

        if (sqlSyntaxLevel == SODAUtils.SQLSyntaxLevel.SQL_SYNTAX_UNKNOWN)
            sqlSyntaxLevel = SODAUtils.getDatabaseVersion(conn);
    }

    public static boolean isPatch(Integer... patches) {
        if (patches == null || patches.length == 0) {
            throw new IllegalArgumentException();
        }
        for (Integer p : patches) {
            if (p.equals(PATCH_VERSION)) {
                return true;
            }
        }
        return false;
    }
    
    // Method to check whether DB's character set is UTF8 or not
    public boolean isUTF8DBCharacterSet(OracleConnection con) throws Exception {
      final String sqlForCharSet = "SELECT value FROM nls_database_parameters" +
                                   " WHERE parameter='NLS_CHARACTERSET'";
      final String keyForUTF8CharSet = "AL32UTF8";
      String charSet = null;
      
      if (con == null) {
        throw new IllegalArgumentException();
      }
      
      Exception e = null;
      Statement statement = null;
      ResultSet resultSet = null;
      try {
        statement = con.createStatement();
        resultSet = statement.executeQuery(sqlForCharSet);
        if (resultSet.next()) {
          charSet = resultSet.getString(1);
        }
      }
      finally {
        // Close the result set
        try {
          if (resultSet != null)
            resultSet.close();
        }
        catch (Exception closeException) { 
          e = closeException;
        }

        // Close the statement
        try {
           if (statement != null)
             statement.close();
        }
        catch (Exception closeException) { 
          if (e != null) 
            throw e;
          else 
            throw closeException;
        }
      }
      
      if (charSet == null) {
        // failed to get character set value via the connection
        throw new IllegalStateException();
      }
      
      if (charSet.equalsIgnoreCase(keyForUTF8CharSet)) {
        // matched UTF8 character set
        return true;
      }

      return false;
    }

    // QBEs will not be supported on heterogeneous collections
    // due to new json search index not working on heterogeneous
    // collections. It's possible to enable json search index on
    // heterogeneous collections with more work, but management
    // decision was made to reduce surface area instead. Original
    // tests for QBEs on heterogeneous collection will be disabled
    // for now.
    public static boolean supportHeterogeneousQBEs() {
        return false;
    }

    public static String createTextIndexSpec(String indexName) {
      return createTextIndexSpec(indexName, null,false);
    }

    public static String createTextIndexSpec(String indexName,
                                             String language) {
      return createTextIndexSpec(indexName,language,false);
    }

    public static String createTextIndexSpec(String indexName,
                                             String language,
                                             boolean textIndex121WithLang) {
        String indexSpec = null;

        if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel)) {
            indexSpec = "{\"name\" : \"" + indexName;

            if (textIndex121WithLang) {
              indexSpec += "\",";

              if (language != null)
                indexSpec += "\"language\" : \"" + language + "\",";

              // ### Not officially supported due to multiple
              // issues. Do not use in production!
              indexSpec += "\"textIndex121WithLang\" : true}";
            }
            else {
              indexSpec += "\"}";
            }
        } else {
            indexSpec = "{\"name\" : \"" + indexName + "\",";

            if (language != null)
                indexSpec += "\"language\" : \"" + language + "\",";

            indexSpec += "\"search_on\" : \"text_value\",";

            indexSpec += "\"dataguide\" : \"on\"}";
        }

        return indexSpec;
    }
}
