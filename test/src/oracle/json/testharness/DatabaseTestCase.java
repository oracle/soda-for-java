/* Copyright (c) 2014, 2018, Oracle and/or its affiliates. 
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
    
    // if there's no property with "jdcs.mode" specified, false will be returned.
    protected static final boolean JDCS_MODE = Boolean.getBoolean("jdcs.mode");

    // Project and patch tests only execute when running against 18 and above.
    // Project and patch first appeared in 12.2.0.1, but was unusable due to
    // a number of bugs. This property can be used to run project and patch tests
    // against 12.2.0.1 (this might be needed if project and patch fixes ever need
    // to be backported to 12.2.0.1).
    protected static final boolean PROJECT_AND_PATCH = Boolean.getBoolean("test.projectAndPatch");

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
   
    public static boolean includeProjectAndPatchTests() {
        if (!SODAUtils.sqlSyntaxBelow_18(sqlSyntaxLevel) ||
            (SODAUtils.sqlSyntax_12_2_0_1(sqlSyntaxLevel) && PROJECT_AND_PATCH))
            return true;
        return false;
    }

    // jdcs.mode property is specified in the entry of junit run.(the default value is false)
    // when it's true, all explicit create/delete table/view/sequence SQL will fail.
    public static boolean isJDCSMode() {
      return JDCS_MODE;
    }
    
    // the method to check whether DB's character set is UTF8 or not
    public boolean isUTF8DBCharacterSet(OracleConnection con) throws Exception {
      final String sqlForCharSet = "SELECT value FROM nls_database_parameters WHERE parameter='NLS_CHARACTERSET'";
      final String keyForUTF8CharSet = "AL32UTF8";
      String charSet = null;
      
      if (con == null) {
        throw new IllegalArgumentException();
      }
      
      Statement statement = con.createStatement();
      ResultSet resultset = statement.executeQuery(sqlForCharSet);
      if (resultset.next()) {
        charSet = resultset.getString(1);
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
      return createTextIndexSpec(indexName, null, false);
    }

    public static String createTextIndexSpec(String indexName,
                                             String language) {
      return createTextIndexSpec(indexName, language, false);
    }

    // ### Note: language is not officially supported, do not use in production!!!
    public static String createTextIndexSpec(String indexName,
                                             String language,
                                             boolean textIndex121WithLang) {
        String indexSpec = null;

        if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel)) {
            indexSpec = "{\"name\" : \"" + indexName;

            if (textIndex121WithLang) {
              indexSpec += "\",";

              // ### Note: language is not officially supported,
              // do not use in production!!!
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

            // ### Note: language is not officially supported,
            // do not use in production!!!
            if (language != null)
                indexSpec += "\"language\" : \"" + language + "\",";

            indexSpec += "\"search_on\" : \"text_value\",";

            indexSpec += "\"dataguide\" : \"on\"}";
        }

        return indexSpec;
    }

}
