/* Copyright (c) 2014, 2015, Oracle and/or its affiliates. 
All rights reserved.*/

/*
   DESCRIPTION
    SODA collection metadata caching
 */

/**
 *  @author  Vincent Liu
 */

package oracle.json.tests.soda;

import java.io.File;
import java.io.FileInputStream;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import oracle.jdbc.OracleConnection;

import oracle.soda.OracleCursor;
import oracle.soda.OracleException;
import oracle.soda.OracleCollection;
import oracle.soda.OracleCollectionAdmin;
import oracle.soda.OracleDocument;
import oracle.soda.OracleOperationBuilder;
import oracle.soda.OracleDatabase;
import oracle.soda.OracleDatabaseAdmin;
import oracle.soda.rdbms.OracleRDBMSClient;
import oracle.soda.rdbms.impl.OracleDatabaseImpl;

import oracle.json.testharness.ConnectionFactory;
import oracle.json.testharness.SodaTestCase;

public class test_OracleSodaCache extends SodaTestCase {

  private final String existingColName1 = "existingCol1";
  private final String existingColName2 = "existingCol2";
  
  private void cacheTests1() throws Exception {
    // Test create followed by open, and another create.
    OracleDocument metaDoc1 = client.createMetadataBuilder()
            .mediaTypeColumnName("CONTENT_TYPE").contentColumnType("BLOB").build();
    OracleCollection col1 = dbAdmin.createCollection("cacheTests", metaDoc1);

    // the open should fetch metadata from cache
    OracleCollection col1_1 = db.openCollection("cacheTests");
    assertEquals("cacheTests", col1_1.admin().getName());

    // the create should fetch metadata from cache
    // (collection exists already)
    OracleCollection col1_2 = dbAdmin.createCollection("cacheTests", metaDoc1);
    assertEquals("cacheTests", col1_2.admin().getName());
  }

  private void cacheTests2() throws Exception {
    // Test open followed by open.
    OracleCollection col2 = db.openCollection(existingColName1);
    assertNotNull(col2);
    assertEquals(existingColName1, col2.admin().getName());

    // the open should fetch metadata from cache
    OracleCollection col2_1 = db.openCollection(existingColName1);
    assertNotNull(col2_1);
    assertEquals(existingColName1, col2_1.admin().getName());

    OracleDocument metaDoc2_3 = client.createMetadataBuilder().contentColumnType("VARCHAR2").build();
    try {
      dbAdmin.createCollection(existingColName1, metaDoc2_3);
      fail("No exception when mismatch on collection metadata");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Descriptor does not match existing collection.", e.getMessage());
    }
  }

  private void cacheTests3() throws Exception {
    // Test getCollectionNames followed by open
    List<String> namesList = dbAdmin.getCollectionNames();
    assertEquals(3, namesList.size());
    
    //Test with getCollectionNames(int limit)
    namesList = dbAdmin.getCollectionNames(2);
    assertEquals(2, namesList.size());
    
    //Test with getCollectionNames(int limit, int skip)
    namesList = dbAdmin.getCollectionNames(1, 2);
    assertEquals(1, namesList.size());
    
    //Test with getCollectionNames(int limit, java.lang.String startName)
    namesList = dbAdmin.getCollectionNames(3, existingColName2);
    assertEquals(1, namesList.size());
    assertEquals(existingColName2, namesList.iterator().next());

    // Metadata should be fetched from the cache
    // by these open calls
    OracleCollection col3_1 = db.openCollection(existingColName1);
    assertNotNull(col3_1);
    assertEquals(existingColName1, col3_1.admin().getName());
    OracleCollection col3_2 = db.openCollection(existingColName2);
    assertNotNull(col3_2);
    assertEquals(existingColName2, col3_2.admin().getName());
    OracleCollection col3_3 = db.openCollection("cacheTests");
    assertNotNull(col3_3);
    assertEquals("cacheTests", col3_3.admin().getName());
  }

  private void resetClientAndDB() throws Exception {
    // Re-create db object on the newly created client

    Properties props = new Properties();
    client = new OracleRDBMSClient(props);
    conn = ConnectionFactory.createConnection();
    db = client.getDatabase(conn);
    dbAdmin = db.admin();
  }

  private void cacheTests(boolean local, boolean shared) throws Exception {
    // create collections for later tests
    dbAdmin.createCollection(existingColName1);
    dbAdmin.createCollection(existingColName2);
    // close the connection obtained in setUp()
    conn.close();

    Properties props = new Properties();

    if (local)
      props.setProperty("oracle.soda.localMetadataCache", "true");

    if (shared)
      props.setProperty("oracle.soda.sharedMetadataCache", "true");

    // run cacheTests1, cacheTest2, and cacheTests3.
    // Before each of these, we recreate the client/db,
    // so that previous existing caches are not used.
    resetClientAndDB();
    cacheTests1();
    conn.close();

    resetClientAndDB();
    cacheTests2();
    conn.close();

    resetClientAndDB();
    cacheTests3();
    // Don't close the connection, so that SodaTestCase.tearDown 
    // can do its job
  }
  public void testSharedCache() throws Exception {
    cacheTests(false, true);
  }

  public void testLocalCache() throws Exception {
    cacheTests(true, false);
  }
  
  public void testBothCaches() throws Exception {
    cacheTests(true, true);
  }
  
  private void cacheTestsWithMultipleDbObjs(boolean noLocalCache) throws Exception {
    OracleConnection connA = conn;
    OracleDatabase dbA = db;
    OracleDatabaseAdmin dbAdminA = dbAdmin;
    
    OracleConnection connB = ConnectionFactory.createConnection();
    OracleDatabase dbB = client.getDatabase(connB);
    OracleDatabaseAdmin dbAdminB = dbB.admin();
    OracleDocument metaDoc = client.createMetadataBuilder()
        .contentColumnType("CLOB").build();
    
    //1.Test with "dbB:open -> dbA:create -> dbB:open -> dbA:drop -> dbB:open again"
    //1.1 try to open collection before create
    OracleCollection colB1 = dbB.openCollection("cacheTestsWithMultipleDbObjs1");
    assertNull(colB1);
    
    //1.2 create and open collection in databaseA
    dbAdminA.createCollection("cacheTestsWithMultipleDbObjs1", metaDoc);

    OracleCollection colA1 = dbA.openCollection("cacheTestsWithMultipleDbObjs1");
    assertEquals("cacheTestsWithMultipleDbObjs1", colA1.admin().getName());
    
    //1.3 create the collection in databaseB (collection
    //created already so this should open it).
    colB1 = dbAdminB.createCollection("cacheTestsWithMultipleDbObjs1", metaDoc);
    assertNotNull(colB1);
    assertEquals("cacheTestsWithMultipleDbObjs1", colB1.admin().getName());
    
    //1.4 drop the collection in databaseA
    colA1.admin().drop();
    assertNull(dbA.openCollection("cacheTestsWithMultipleDbObjs1"));
    
    //1.5 try to open the collection in databaseB
    if (noLocalCache) {
      assertNull(dbB.openCollection("cacheTestsWithMultipleDbObjs1"));
    }
    else {
      // If the collection was dropped from databaseA, this will not update databaseB's local cache.
      // If local cache existed, a non-null collection still can be returned by databaseB.
      assertEquals("cacheTestsWithMultipleDbObjs1", dbB.openCollection("cacheTestsWithMultipleDbObjs1").admin().getName());
    }
    
    //2.Test with "dbA:open -> dbB:open"
    OracleCollection colA2 = dbA.openCollection(existingColName1);
    assertNotNull(colA2);
    assertEquals(existingColName1, colA2.admin().getName());
    
    OracleCollection colB2 = dbB.openCollection(existingColName1);
    assertNotNull(colB2);
    assertEquals(existingColName1, colB2.admin().getName());
    
    //clean existingCol
    colA2.admin().drop();
    
    //3.Test with "dbB:getCollectionNames -> dbA:create, dbA:getCollectionNames -> dbB:open,
    //             dbB:getCollectionNames -> dbA:drop -> dbB:getCollectionNames"
    //3.1 getCollectionNames in databaseB
    assertEquals(0, dbAdminB.getCollectionNames().size());
    
    //3.2 create collection and getCollectionNames in databaseA
    dbAdminA.createCollection("cacheTestsWithMultipleDbObjs2", metaDoc);
    assertEquals(1, dbAdminA.getCollectionNames().size());
    assertEquals("cacheTestsWithMultipleDbObjs2", dbAdminA.getCollectionNames().iterator().next());
    OracleCollection colA3 = dbA.openCollection("cacheTestsWithMultipleDbObjs2");
    assertNotNull(colA3);
    assertEquals("cacheTestsWithMultipleDbObjs2", colA3.admin().getName());
    
    //3.3 open collection and getCollectionNames in databaseB
    OracleCollection colB3 = dbB.openCollection("cacheTestsWithMultipleDbObjs2");
    assertNotNull(colB3);
    assertEquals("cacheTestsWithMultipleDbObjs2", colB3.admin().getName());
    assertEquals(1, dbAdminB.getCollectionNames().size());
    assertEquals("cacheTestsWithMultipleDbObjs2", dbAdminB.getCollectionNames().iterator().next());
    
    //3.4 drop collection in databaseA
    colA3.admin().drop();
    assertNull(dbA.openCollection("cacheTestsWithMultipleDbObjs2"));
    assertEquals(0, dbAdminA.getCollectionNames().size());
    assertFalse(dbAdminA.getCollectionNames().iterator().hasNext());
    
    //3.5 getCollectionNames in databaseB
    assertEquals(0, dbAdminB.getCollectionNames().size());
    assertFalse(dbAdminB.getCollectionNames().iterator().hasNext());
    
    //release the connection 
    connB.close();
  }
  
  public void testSharedCacheWithMultipleDbObjs() throws Exception {
    //create the collection for later test
    dbAdmin.createCollection(existingColName1);
    //close the connection
    conn.close();
    
    Properties props = new Properties();
    props.setProperty("oracle.soda.sharedMetadataCache", "true");
    
    //reset client with sharedMetadataCache
    client = new OracleRDBMSClient(props);
    //re-create db object on the newly created client
    conn = ConnectionFactory.createConnection();
    db = client.getDatabase(conn);
    dbAdmin = db.admin();
    
    cacheTestsWithMultipleDbObjs(true);
  }
  
  public void testLocalCacheWithMultipleDbObjs() throws Exception {
    //create the collection for later test
    dbAdmin.createCollection(existingColName1);
    //close the connection
    conn.close();
    
    Properties props = new Properties();
    props.setProperty("oracle.soda.localMetadataCache", "true");
    
    //reset client with localMetadataCache
    client = new OracleRDBMSClient(props);
    //re-create db object on the newly created client
    conn = ConnectionFactory.createConnection();
    db = client.getDatabase(conn);
    dbAdmin = db.admin();
    
    cacheTestsWithMultipleDbObjs(false);
  }
  
  public void testBothCachesWithMultipleDbObjs() throws Exception {
    //create the collection for later test
    dbAdmin.createCollection(existingColName1);
    //close the connection
    conn.close();
    
    Properties props = new Properties();
    props.setProperty("oracle.soda.sharedMetadataCache", "true");
    props.setProperty("oracle.soda.localMetadataCache", "true");
    
    //reset client with sharedMetadataCache and localMetadataCache
    client = new OracleRDBMSClient(props);
    //re-create db object on the newly created client
    conn = ConnectionFactory.createConnection();
    db = client.getDatabase(conn);
    dbAdmin = db.admin();
    
    cacheTestsWithMultipleDbObjs(false);
  }
  
  public void testNoCacheWithMultipleDbObjs() throws Exception {
    //create the collection for later test
    dbAdmin.createCollection(existingColName1);
    
    cacheTestsWithMultipleDbObjs(true);
  }
  
  private void unitTestForCacheProperties() throws Exception {
    String colName = "unitTestForCacheProperties" ;
    
    OracleDocument metaDoc = client.createMetadataBuilder().build();
    dbAdmin.createCollection(colName, metaDoc);
    OracleCollection col = db.openCollection(colName);
    assertNotNull(col);
    
    col.insert(db.createDocumentFromString("{ \"data\" : \"v1\" }"));
    assertEquals(1, col.find().count());
    col.find().remove();
    
    assertEquals(1, dbAdmin.getCollectionNames().size());
    assertEquals(colName, dbAdmin.getCollectionNames().iterator().next());
    
    col.admin().drop();
    assertEquals(0, dbAdmin.getCollectionNames().size());
    assertNull(db.openCollection(colName));
  }
 
  public void testCacheProperties() throws Exception {
    //close the connection obtained in Setup() 
    conn.close();
    
    // Test with empty Properties object
    Properties props1 = new Properties();
    client = new OracleRDBMSClient(props1);
    conn = ConnectionFactory.createConnection();
    db = client.getDatabase(conn);
    dbAdmin = db.admin();
    unitTestForCacheProperties();
    conn.close();
    
    // Test with null Properties object (just ignore it?)
    client = new OracleRDBMSClient(null);
    conn = ConnectionFactory.createConnection();
    db = client.getDatabase(conn);
    dbAdmin = db.admin();
    unitTestForCacheProperties();
    conn.close();
    
    // Test with "false" or other non "true" for properties value (just ignore it?)
    Properties props2 = new Properties();
    props2.setProperty("oracle.soda.localMetadataCache", "false");
    props2.setProperty("oracle.soda.localMetadataCache", "1");
    
    client = new OracleRDBMSClient(props2);
    conn = ConnectionFactory.createConnection();
    db = client.getDatabase(conn);
    dbAdmin = db.admin();
    unitTestForCacheProperties();
    conn.close();
    
    // Test with 100+ collection to trigger LRU replacement in shared cache
    Properties props3 = new Properties();
    props3.setProperty("oracle.soda.localMetadataCache", "true");
    client = new OracleRDBMSClient(props3);
    conn = ConnectionFactory.createConnection();
    db = client.getDatabase(conn);
    dbAdmin = db.admin();
    
    dbAdmin.createCollection("testCacheProperties3");
    assertEquals("testCacheProperties3", db.openCollection("testCacheProperties3").admin().getName());
    for(int counter = 1; counter <= 120; counter++) {
      String colName = "testCacheProperties3-" + counter;
      dbAdmin.createCollection(colName);
      assertNotNull(db.openCollection(colName));
    }
    // the collection should have been taken away from for the shared cache.
    OracleCollection col4 = db.openCollection("testCacheProperties3");
    assertEquals("testCacheProperties3", col4.admin().getName());
    
    // this is the final test, and leave the cleanup to tearDown()
    
  }

} 
