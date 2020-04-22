/* Copyright (c) 2014, 2020, Oracle and/or its affiliates. 
All rights reserved.*/

/**
 *  @author  Josh Speigel
 */
package oracle.json.tests.soda;

import oracle.jdbc.OracleConnection;
import oracle.json.testharness.ConnectionFactory;
import oracle.json.testharness.JsonTestCase;
import oracle.json.testharness.DatabaseTestCase;
import oracle.soda.OracleCollection;
import oracle.soda.OracleDatabase;
import oracle.soda.OracleDocument;
import oracle.soda.OracleDatabaseAdmin;
import oracle.soda.rdbms.OracleRDBMSClient;
import junit.framework.TestCase;

/**
 * Unit tests in oracle.json.tests.soda should be running with out orarestsoda.jar
 * in the classpath to avoid unintentional dependencies on that JAR.
 */
public final class test_SodaDependencies extends DatabaseTestCase {
  
    public void testClasspath() throws Exception {
        try {
            Class.forName("oracle.json.web.RestRequest");
            fail("Expected exception");
        } catch (ClassNotFoundException e) {
            // expected
        }
    }
    
    public void testSodaSanity() throws Exception {
        OracleConnection con = ConnectionFactory.createConnection();
        OracleRDBMSClient client = new OracleRDBMSClient();
        OracleDatabase database = client.getDatabase(con);
        OracleDatabaseAdmin dba = database.admin();
        OracleDocument metaDoc = null;

        if (isJDCSMode()) {
          // ### replace with new builder once it becomes available
          metaDoc = database.createDocumentFromString("{\"keyColumn\":{\"name\":\"ID\",\"sqlType\":\"VARCHAR2\",\"maxLength\":255,\"assignmentMethod\":\"UUID\"},\"contentColumn\":{\"name\":\"JSON_DOCUMENT\",\"sqlType\":\"BLOB\"},\"lastModifiedColumn\":{\"name\":\"LAST_MODIFIED\"},\"versionColumn\":{\"name\":\"VERSION\",\"method\":\"UUID\"},\"creationTimeColumn\":{\"name\":\"CREATED_ON\"},\"readOnly\":false}");
        }
        else {
          metaDoc = client.createMetadataBuilder().build();
        }
 
        OracleCollection col = dba.createCollection("FOO", metaDoc);
        try {
            col = database.openCollection("FOO");
            col.insert(client.createMetadataBuilder().build());
        } finally {
            col.admin().drop();
        }
        con.close();
    }
  
}
