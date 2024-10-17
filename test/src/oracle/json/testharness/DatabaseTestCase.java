/* Copyright (c) 2014, 2024, Oracle and/or its affiliates.*/
/* All rights reserved.*/

/**
 *  DESCRIPTION
 *   DatabaseTestCase
 */

/**
 *  @author  Josh Spiegel
 */

package oracle.json.testharness;

import java.io.StringReader;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import oracle.jdbc.OracleConnection;
import oracle.json.util.ByteArray;

import oracle.soda.OracleCollection;
import oracle.soda.OracleCollectionAdmin;
import oracle.soda.OracleCursor;
import oracle.soda.OracleDatabase;
import oracle.soda.OracleDocument;
import oracle.soda.OracleDropResult;
import oracle.soda.OracleException;
import oracle.soda.rdbms.OracleRDBMSMetadataBuilder;
import oracle.soda.rdbms.impl.OracleOperationBuilderImpl;
import oracle.soda.rdbms.impl.SODAUtils;

import oracle.sql.json.OracleJsonFactory;
import oracle.sql.json.OracleJsonGenerator;
import oracle.sql.json.OracleJsonObject;
import oracle.sql.json.OracleJsonParser;
import oracle.sql.json.OracleJsonValue;

public abstract class DatabaseTestCase extends JsonTestCase {

    public static final int PATCH1 = 1;
    public static final int PATCH2 = 2;
    public static final int COMPATIBLE_20 = 20;
    public static final int COMPATIBLE_23 = 23;

    public static final Integer PATCH_VERSION = Integer.getInteger("patch.version");
    
    // if there's no property with "jdcs.mode" specified, false will be returned.
    protected static final boolean JDCS_MODE = Boolean.getBoolean("jdcs.mode");
    protected static final boolean BUG_36425758 = Boolean.getBoolean("bug.36425758");
    protected static final boolean ATP_MODE = Boolean.getBoolean("atp.mode");
    protected static final String COMPATIBLE = System.getProperty("compatible");

    // Project and patch tests only execute when running against 18 and above.
    // Project and patch first appeared in 12.2.0.1, but was unusable due to
    // a number of bugs. This property can be used to run project and patch tests
    // against 12.2.0.1 (this might be needed if project and patch fixes ever need
    // to be backported to 12.2.0.1).
    protected static final boolean PROJECT_AND_PATCH = Boolean.getBoolean("test.projectAndPatch");

    protected OracleConnection conn;

    protected String cloudServiceType;

    protected static SODAUtils.SQLSyntaxLevel sqlSyntaxLevel =
      SODAUtils.SQLSyntaxLevel.SQL_SYNTAX_UNKNOWN;

    @Override
    protected void setUp() throws Exception {
        conn = ConnectionFactory.createConnection();

        if (sqlSyntaxLevel == SODAUtils.SQLSyntaxLevel.SQL_SYNTAX_UNKNOWN)
            sqlSyntaxLevel = SODAUtils.getDatabaseVersion(conn);
    }
    
    @Override
    protected void tearDown() throws Exception {
        conn.close();
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

    public boolean isAutonomousShared() throws Exception {
      PreparedStatement st = conn.prepareStatement("select sys_context('userenv','cloud_service') from dual");
      ResultSet rs=st.executeQuery();
      rs.next();
      cloudServiceType = rs.getString(1);
      rs.close();
      if (cloudServiceType == "OLTP" ||
          cloudServiceType == "JDCS" ||
	  cloudServiceType == "ADWC")
        return true; 
      return false;
    }
    
    public int[] getDBVersion(DatabaseMetaData dbmd) throws Exception {
        Pattern BANNER_PATTERN = Pattern.compile(".*\nVersion (?<major>\\d+?)\\.(?<minor>\\d+?)\\..*");
        Matcher matcher = BANNER_PATTERN.matcher(dbmd.getDatabaseProductVersion());
        int[] dbVersion = new int[2];
        if(matcher.matches()) {
            dbVersion[0] = Integer.parseInt(matcher.group("major"));
            dbVersion[1] = Integer.parseInt(matcher.group("minor"));
        }
        return dbVersion;
    }
    
    public boolean is23ButNotDot2() throws Exception {
      DatabaseMetaData dbmd = (conn == null) ? null : conn.getMetaData();
      int[] dbVersion = getDBVersion(dbmd);
      int dbMajor = dbVersion[0];
      int dbMinor = dbVersion[1];
      if (dbMajor > 23 || (dbMajor == 23 && dbMinor != 2))
          return true;
      return false;
    }

    public boolean isDBVersion23dot2() throws Exception {
      DatabaseMetaData dbmd = (conn == null) ? null : conn.getMetaData();
      int[] dbVersion = getDBVersion(dbmd);
      int dbMajor = dbVersion[0];
      int dbMinor = dbVersion[1];
      if (dbMajor == 23 && dbMinor == 2)
          return true;
      return false;
    }

    public boolean isDBVersionBelow(int major, int minor) throws Exception {
       DatabaseMetaData dbmd = (conn == null) ? null : conn.getMetaData();
       int[] dbVersion = getDBVersion(dbmd);
       int dbMajor = dbVersion[0];
       int dbMinor = dbVersion[1];
       if (dbMajor < major || (dbMajor == major && dbMinor < minor))
           return true;
       return false;
    }
    
    public boolean isDBMajorVersion(int major) throws Exception {
      DatabaseMetaData dbmd = (conn == null) ? null : conn.getMetaData();
      int[] dbVersion = getDBVersion(dbmd);
      int dbMajor = dbVersion[0];
      if (dbMajor == major)
          return true;
      return false;
   }

    // jdcs.mode property is specified in the entry of junit run.(the default value is false)
    // when it's true, all explicit create/delete table/view/sequence SQL will fail.
    public static boolean isJDCSMode() {
      return JDCS_MODE;
    }

    public static boolean isBug36425758() {
      return BUG_36425758;
    }

    public static boolean isATPMode() {
      return ATP_MODE;
    }

    public static boolean isJDCSOrATPMode() {
      return JDCS_MODE || ATP_MODE;
    }
    
    public static String compatible() {
      return COMPATIBLE;
    }

    public static boolean isCompatibleOrGreater(int compatible) {
      if (COMPATIBLE != null) {
        try {
          return Double.parseDouble(COMPATIBLE) >= compatible;
        } catch (NumberFormatException e) {
          throw new NumberFormatException();
        }  
      }
      return false;      
    } 

    public void isKeyColumnValueForEbmeddedIDValid (OracleDocument doc, String key, OracleCollection col, OracleDatabase db) throws Exception {

    OracleCollectionAdmin colAdmin = col.admin();
    OracleDocument metadata = colAdmin.getMetadata();

    OracleJsonFactory jsonFactory = new OracleJsonFactory();
    OracleJsonValue jsonValue = jsonFactory.createJsonTextValue(new StringReader(metadata.getContentAsString()));
    OracleJsonObject jsonObject = jsonValue.asJsonObject();

    OracleJsonValue keycolumn = jsonObject.get("keyColumn");

    OracleJsonValue jsonValue2 = jsonFactory.createJsonTextValue(new StringReader(keycolumn.toString()));
    OracleJsonObject jsonObject2 = jsonValue2.asJsonObject();

    String sqlType = jsonObject2.getString("sqlType");
    String assignmentMethod = jsonObject2.getString("assignmentMethod");

    if (!(sqlType.equalsIgnoreCase("RAW") && assignmentMethod.equalsIgnoreCase("EMBEDDED_OID")))
      return;

    doc = ((OracleOperationBuilderImpl) col.find().key(key)).project(db.createDocumentFromString("{\"_id\" : 1}")).getOne();

    if (doc == null || doc.getContentAsString().equals("{}"))
    {
      throw new Exception("_id is missing in the object.");
    }

    String idValue = doc.getKey();

    if (!ByteArray.isHex(idValue))
      throw new Exception("_id is not of type Hexadecimal.");

    // res.getKey() appends 2 extra characters "08" at the beginning of 24 character hex _id value
    if (idValue.length() != 26)
      throw new Exception("_id is not 24 characters long.");
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
