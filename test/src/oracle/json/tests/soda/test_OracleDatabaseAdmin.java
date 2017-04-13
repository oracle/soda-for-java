/* Copyright (c) 2014, 2017, Oracle and/or its affiliates. 
All rights reserved.*/

/*
   DESCRIPTION
    OracleDatabaseAdmin
 */

/**
 *  @author  Vincent Liu
 */
package oracle.json.tests.soda;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Iterator;

import javax.json.JsonString;
import javax.json.JsonNumber;
import javax.json.JsonValue;
import javax.json.JsonException;
import javax.json.stream.JsonParsingException;

import oracle.jdbc.OraclePreparedStatement;

import oracle.soda.OracleCursor;
import oracle.soda.OracleException;
import oracle.soda.OracleCollection;
import oracle.soda.OracleCollectionAdmin;
import oracle.soda.OracleDocument;

import oracle.soda.rdbms.OracleRDBMSMetadataBuilder;

import oracle.soda.rdbms.impl.OracleDocumentImpl;

import oracle.json.testharness.SodaTestCase;
import oracle.soda.rdbms.impl.SODAUtils;

public class test_OracleDatabaseAdmin extends SodaTestCase {
  String metaData =
    "{\"keyColumn\":{\"name\":\"ID\",\"sqlType\":\"VARCHAR2\",\"maxLength\":255,\"assignmentMethod\":\"UUID\"}," +
    "\"contentColumn\":{\"name\":\"JSON_DOCUMENT\",\"sqlType\":\"BLOB\",\"compress\":\"LOW\",\"cache\":true,\"encrypt\":\"NONE\",\"validation\":\"STRICT\"}," +
    "\"versionColumn\":{\"name\":\"VERSION\",\"type\":\"String\",\"method\":\"SHA256\"}," +
    "\"lastModifiedColumn\":{\"name\":\"LAST_MODIFIED\"}," +
    "\"creationTimeColumn\":{\"name\":\"CREATED_ON\"}," +
    "\"mediaTypeColumn\":{\"name\":\"CONTENT_TYPE\"}," +
    "\"readOnly\":false}";
  
  public void testCreateCollection() throws Exception {

    // Test createCollection(...) with an new collectionName
    OracleCollection col = dbAdmin.createCollection("testCreateCollection");
    assertNotNull(col);
    assertEquals("testCreateCollection", col.admin().getName());
    
    // Test createCollection(...) with an known collectionName   
    OracleDocument metaDoc = db.createDocumentFromString(metaData);
    String colName = "testCreateCollectionB";
    dbAdmin.createCollection(colName, metaDoc);
    // Test with an known collectionName and no metadata 
    OracleCollection col2 = dbAdmin.createCollection(colName);
    assertNotNull(col2);
    assertEquals(colName, col2.admin().getName());
   
    OracleDocument tempdoc = col2.admin().getMetadata();
    
    // Test with an known collectionName and metadata
    OracleDocument metaDoc2 = db.createDocumentFromString(metaData);
    col2 = dbAdmin.createCollection(colName, metaDoc2);
    assertEquals(colName, col2.admin().getName());
 
    // Negative Test
    try {
      // Pass null for collectionName
      OracleCollection col3 = dbAdmin.createCollection(null);
      fail("No exception when collectionName is null");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("collectionName argument cannot be null.", e.getMessage());
    }
    
  }
  
  public void testCreateCollection2() throws Exception {

    // Test with an new collectionName and null metadata
    String colName1 = "testCreateCollection2";

    OracleCollection col = dbAdmin.createCollection(colName1, (OracleDocument)null);
    assertNotNull(col);
    assertEquals(colName1, col.admin().getName());
    
    OracleDocument metaDoc2 = client.createMetadataBuilder().build();
    String colName2 = "testCreateCollection2-2";
    // Test createCollection(...) with an new collectionName and valid metadata
    OracleCollection col2 = dbAdmin.createCollection(colName2, metaDoc2);
    assertNotNull(col2);
    assertEquals(colName2, col2.admin().getName());
    
    // Test with known collectionName and valid metadata
    col2 = dbAdmin.createCollection(colName2, metaDoc2);
    assertNotNull(col2);
    assertEquals(colName2, col2.admin().getName());
    
    // Test with heterogeneous collection
    OracleDocument metaDoc3 = client.createMetadataBuilder().mediaTypeColumnName("CONTENT_TYPE")
        .contentColumnType("BLOB").mediaTypeColumnName("CONTTYPE").build();
    OracleCollection col3 = dbAdmin.createCollection("testCreateCollection2-3", metaDoc3);
    assertNotNull(col3);
    assertTrue(col3.admin().isHeterogeneous());
    
    col3.insertAndGet(db.createDocumentFromString(null, "12345ffff", "text/plain"));
    assertEquals(1, col3.find().count());
    OracleDocument doc = col3.find().getOne();
    assertEquals("text/plain", doc.getMediaType());
    
    // Negative Tests
    try {
      // Pass known collectionName and unmatched collectionMetadata
      OracleDocument metaDoc4 = client.createMetadataBuilder()
          .contentColumnType("VARCHAR2").build();
      dbAdmin.createCollection("testCreateCollection2-3", metaDoc4);
      fail("No exception for unmatched collectionMetadata");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Descriptor does not match existing collection.", e.getMessage());
    }
    
    try {
      // Pass medaData document with empty content
      OracleDocument metaDoc5 = db.createDocumentFromString("");
      dbAdmin.createCollection("testCreateCollection2-5", metaDoc5);
      fail("No exception when metaData document is invalid");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Collection metadata document has no content or has empty content.", e.getMessage());
    }
    
    try {
      // Pass medaData document with non-JSON content
      OracleDocument metaDoc6 = db.createDocumentFromString("{abcd efgh}");
      dbAdmin.createCollection("testCreateCollection2-6", metaDoc6);
      fail("No exception when metaData document is invalid");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Collection metadata document is not valid JSON.", e.getMessage());
    }

    //Test with null collectionName
    try {
      dbAdmin.createCollection(null, metaDoc2);
      fail("No exception when collection name is null");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("collectionName argument cannot be null.", e.getMessage());
    }
    
    //The following negative tests are used to cover unmatched metadata item
    //any mismatching should cause exception

    //Test with unmatched contentColumnCompression
    OracleDocument metaDoc7 = client.createMetadataBuilder().contentColumnCompress("HIGH").build();
    String colName7 = "testCreateCollection7";
    dbAdmin.createCollection(colName7, metaDoc7);
    OracleDocument metaDoc7_1 = client.createMetadataBuilder().contentColumnCompress("LOW").build();
    try {
      dbAdmin.createCollection(colName7, metaDoc7_1);
      fail("No exception when mismatch on collection metadata");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Descriptor does not match existing collection.", e.getMessage());
    }

    //Test with unmatched contentColumnCaching
    OracleDocument metaDoc8 = client.createMetadataBuilder().contentColumnCache(true).build();
    String colName8 = "testCreateCollection8";
    dbAdmin.createCollection(colName8, metaDoc8);
    OracleDocument metaDoc8_1 = client.createMetadataBuilder().contentColumnCache(false).build();
    try {
      dbAdmin.createCollection(colName8, metaDoc8_1);
      fail("No exception when mismatch on collection metadata");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Descriptor does not match existing collection.", e.getMessage());
    }

    //Test with unmatched contentColumnEncryption
    OracleDocument metaDoc9 = client.createMetadataBuilder().contentColumnEncrypt("3DES168").build();
    String colName9 = "testCreateCollection9";
    dbAdmin.createCollection(colName9, metaDoc9);
    OracleDocument metaDoc9_1 = client.createMetadataBuilder().contentColumnEncrypt("AES128").build();
    try {
      dbAdmin.createCollection(colName9, metaDoc9_1);
      fail("No exception when mismatch on collection metadata");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Descriptor does not match existing collection.", e.getMessage());
    }

    //Test with unmatched contentColumnMaxLength
    OracleDocument metaDoc10 = client.createMetadataBuilder().contentColumnType("VARCHAR2").contentColumnMaxLength(1024).build();
    String colName10 = "testCreateCollection10";
    dbAdmin.createCollection(colName10, metaDoc10);
    OracleDocument metaDoc10_1 = client.createMetadataBuilder().contentColumnType("VARCHAR2").contentColumnMaxLength(256).build();
    try {
      dbAdmin.createCollection(colName10, metaDoc10_1);
      fail("No exception when mismatch on collection metadata");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Descriptor does not match existing collection.", e.getMessage());
    }

    //Test with unmatched keyColumnName
    OracleDocument metaDoc11 = client.createMetadataBuilder().keyColumnName("keyCol1").build();
    String colName11 = "testCreateCollection11";
    dbAdmin.createCollection(colName11, metaDoc11);
    OracleDocument metaDoc11_1 = client.createMetadataBuilder().keyColumnName("keyCol2").build();
    try {
      dbAdmin.createCollection(colName11, metaDoc11_1);
      fail("No exception when mismatch on collection metadata");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Descriptor does not match existing collection.", e.getMessage());
    }

    //Test with unmatched keyColumnSqlType
    OracleDocument metaDoc12 = client.createMetadataBuilder().keyColumnType("NUMBER").build();
    String colName12 = "testCreateCollection12";
    dbAdmin.createCollection(colName12, metaDoc12);
    OracleDocument metaDoc12_1 = client.createMetadataBuilder().keyColumnType("VARCHAR2").build();
    try {
      dbAdmin.createCollection(colName12, metaDoc12_1);
      fail("No exception when mismatch on collection metadata");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Descriptor does not match existing collection.", e.getMessage());
    }

    //Test with unmatched keyColumnMaxLength
    OracleDocument metaDoc13 = client.createMetadataBuilder().keyColumnMaxLength(128).build();
    String colName13 = "testCreateCollection13";
    dbAdmin.createCollection(colName13, metaDoc13);
    OracleDocument metaDoc13_1 = client.createMetadataBuilder().keyColumnMaxLength(255).build();
    try {
      dbAdmin.createCollection(colName13, metaDoc13_1);
      fail("No exception when mismatch on collection metadata");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Descriptor does not match existing collection.", e.getMessage());
    }

    //Test with unmatched keyColumnAssignmentMethod
    OracleDocument metaDoc14 = client.createMetadataBuilder().keyColumnAssignmentMethod("GUID").build();
    String colName14 = "testCreateCollection14";
    dbAdmin.createCollection(colName14, metaDoc14);
    OracleDocument metaDoc14_1 = client.createMetadataBuilder().keyColumnAssignmentMethod("UUID").build();
    try {
      dbAdmin.createCollection(colName14, metaDoc14_1);
      fail("No exception when mismatch on collection metadata");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Descriptor does not match existing collection.", e.getMessage());
    }

    //Test with unmatched keyColumnSequenceName
    OracleDocument metaDoc15 = client.createMetadataBuilder().keyColumnAssignmentMethod("SEQUENCE").keyColumnSequenceName("SEQ1").build();
    String colName15 = "testCreateCollection15";
    dbAdmin.createCollection(colName15, metaDoc15);
    OracleDocument metaDoc15_1 = client.createMetadataBuilder().keyColumnAssignmentMethod("SEQUENCE").keyColumnSequenceName("SEQ2").build();
    try {
      dbAdmin.createCollection(colName15, metaDoc15_1);
      fail("No exception when mismatch on collection metadata");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Descriptor does not match existing collection.", e.getMessage());
    }
 
    //Test with unmatched creationTimeColumnName
    OracleDocument metaDoc16 = client.createMetadataBuilder().creationTimeColumnName("creationCol1").build();
    String colName16 = "testCreateCollection16";
    dbAdmin.createCollection(colName16, metaDoc16);
    OracleDocument metaDoc16_1 = client.createMetadataBuilder().creationTimeColumnName("creationCol2").build();
    try {
      dbAdmin.createCollection(colName16, metaDoc16_1);
      fail("No exception when mismatch on collection metadata");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Descriptor does not match existing collection.", e.getMessage());
    }

    //Test with unmatched lastModifiedColumnName
    OracleDocument metaDoc17 = client.createMetadataBuilder().lastModifiedColumnName("lastModifiedCol1").build();
    String colName17 = "testCreateCollection17";
    dbAdmin.createCollection(colName17, metaDoc17);
    OracleDocument metaDoc17_1 = client.createMetadataBuilder().lastModifiedColumnName("lastModifiedCol2").build();
    try {
      dbAdmin.createCollection(colName17, metaDoc17_1);
      fail("No exception when mismatch on collection metadata");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Descriptor does not match existing collection.", e.getMessage());
    }

    //Test with unmatched lastModifiedColumnIndex
    OracleDocument metaDoc18 = client.createMetadataBuilder().lastModifiedColumnIndex("lastModifiedColIndex1").build();
    String colName18 = "testCreateCollection18";
    dbAdmin.createCollection(colName18, metaDoc18);
    OracleDocument metaDoc18_1 = client.createMetadataBuilder().lastModifiedColumnIndex("lastModifiedColIndex2").build();
    try {
      dbAdmin.createCollection(colName18, metaDoc18_1);
      fail("No exception when mismatch on collection metadata");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Descriptor does not match existing collection.", e.getMessage());
    }

    //Test with unmatched versionColumnName
    OracleDocument metaDoc19 = client.createMetadataBuilder().versionColumnName("versionColIndex1").build();
    String colName19 = "testCreateCollection19";
    dbAdmin.createCollection(colName19, metaDoc19);
    OracleDocument metaDoc19_1 = client.createMetadataBuilder().versionColumnName("versionColIndex2").build();
    try {
      dbAdmin.createCollection(colName19, metaDoc19_1);
      fail("No exception when mismatch on collection metadata");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Descriptor does not match existing collection.", e.getMessage());
    }

    //Test with unmatched versionColumnMethod
    OracleDocument metaDoc20 = client.createMetadataBuilder().versionColumnMethod("UUID").build();
    String colName20 = "testCreateCollection20";
    dbAdmin.createCollection(colName20, metaDoc20);
    OracleDocument metaDoc20_1 = client.createMetadataBuilder().versionColumnMethod("MD5").build();
    try {
      dbAdmin.createCollection(colName20, metaDoc20_1);
      fail("No exception when mismatch on collection metadata");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Descriptor does not match existing collection.", e.getMessage());
    }

    //Test with unmatched mediaTypeColumnName
    OracleDocument metaDoc21 = client.createMetadataBuilder().contentColumnType("BLOB").mediaTypeColumnName("mediaTYpeCol1").build();
    String colName21 = "testCreateCollection21";
    dbAdmin.createCollection(colName21, metaDoc21);
    OracleDocument metaDoc21_1 = client.createMetadataBuilder().contentColumnType("BLOB").mediaTypeColumnName("mediaTYpeCol2").build();
    try {
      dbAdmin.createCollection(colName21, metaDoc21_1);
      fail("No exception when mismatch on collection metadata");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Descriptor does not match existing collection.", e.getMessage());
    }
    
    // Test for with mediaTypeColumn vs without mediaTypeColumn
    OracleDocument metaDoc22 = client.createMetadataBuilder().contentColumnType("BLOB")
        .mediaTypeColumnName("MediaType").build();
    String colName22 = "testOpenCollection22";
    dbAdmin.createCollection(colName22, metaDoc22);
    OracleDocument metaDoc22_1 = client.createMetadataBuilder().contentColumnType("BLOB").build();
    try {
      dbAdmin.createCollection(colName22, metaDoc22_1);
      fail("No exception when mismatch on collection metadata");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Descriptor does not match existing collection.", e.getMessage());
    }
   
    //Test with unmatched readOnly
    OracleDocument metaDoc23 = client.createMetadataBuilder().readOnly(true).build();
    String colName23 = "testCreateCollection23";
    dbAdmin.createCollection(colName23, metaDoc23);
    OracleDocument metaDoc23_1 = client.createMetadataBuilder().readOnly(false).build();
    try {
      dbAdmin.createCollection(colName23, metaDoc23_1);
      fail("No exception when mismatch on collection metadata");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Descriptor does not match existing collection.", e.getMessage());
    }
    
    //Test with unmatched contentColumnType
    OracleDocument metaDoc24 = client.createMetadataBuilder().contentColumnType("CLOB").build();
    String colName24 = "testCreateCollection24";
    dbAdmin.createCollection(colName24, metaDoc24);
    OracleDocument metaDoc24_1 = client.createMetadataBuilder().contentColumnType("BLOB").build();
    try {
      dbAdmin.createCollection(colName24, metaDoc24_1);
      fail("No exception when mismatch on collection metadata");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Descriptor does not match existing collection.", e.getMessage());
    }

    //Test with unmatched contentColumnName
    OracleDocument metaDoc25 = client.createMetadataBuilder().contentColumnName("contentCol1").build();
    String colName25 = "testCreateCollection25";
    dbAdmin.createCollection(colName25, metaDoc25);
    OracleDocument metaDoc25_1 = client.createMetadataBuilder().contentColumnName("contentCol2").build();
    try {
      dbAdmin.createCollection(colName25, metaDoc25_1);
      fail("No exception when mismatch on collection metadata");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Descriptor does not match existing collection.", e.getMessage());
    }

    //Test with unmatched contentColumnValidation
    OracleDocument metaDoc26 = client.createMetadataBuilder().contentColumnValidation("STRICT").build();
    String colName26 = "testCreateCollection26";
    dbAdmin.createCollection(colName26, metaDoc26);
    OracleDocument metaDoc26_1 = client.createMetadataBuilder().contentColumnName("STANDARD").build();
    try {
      dbAdmin.createCollection(colName26, metaDoc26_1);
      fail("No exception when mismatch on collection metadata");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Descriptor does not match existing collection.", e.getMessage());
    }

  }

  public void testGetCollectionNames() throws Exception {

    List<String> list = dbAdmin.getCollectionNames();
    int initCount = list.size();
    assertEquals(0, list.size());
    list = dbAdmin.getCollectionNames(5);
    assertEquals(0, list.size());
    
    OracleDocument metaDoc = client.createMetadataBuilder().build();
    dbAdmin.createCollection("colName1", metaDoc);
    dbAdmin.createCollection("colName2", metaDoc);
    dbAdmin.createCollection("colName3", metaDoc);
    
    // Test when there is 1+ collections
    list = dbAdmin.getCollectionNames();
    assertEquals(initCount+3, list.size());
    
    // Test with limit < collection count
    list = dbAdmin.getCollectionNames(2);
    assertEquals(2, list.size());
    
    // Test with limit >= collection count
    list = dbAdmin.getCollectionNames(100);
    assertEquals(initCount+3, list.size());
    
    // Negative tests
    try {
       // Test with 0 for limit 
      dbAdmin.getCollectionNames(0);
      fail("No exception when limit is 0");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("limit argument must be positive.", e.getMessage());
    }

    try {
      // Test with negative for limit 
      dbAdmin.getCollectionNames(-3);
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("limit argument must be positive.", e.getMessage());
    }
  }
  
  // test method for getCollectionNames(limit, skip)
  public void testGetCollectionNames2() throws Exception {
    List<String> list = dbAdmin.getCollectionNames();
    int initCount = list.size();
    
    OracleDocument metaDoc = client.createMetadataBuilder().build();
    
    dbAdmin.createCollection("colName1", metaDoc);
    dbAdmin.createCollection("colName2", metaDoc);
    dbAdmin.createCollection("colName3", metaDoc);
    dbAdmin.createCollection("colName4", metaDoc);
    dbAdmin.createCollection("colName5", metaDoc);
    dbAdmin.createCollection("colName6", metaDoc);
    
    // Test with limit + skip <= collection count
    list = dbAdmin.getCollectionNames(3, 3);
    assertEquals(3, list.size());
    
    // Test with limit + skip > collection count
    list = dbAdmin.getCollectionNames(100, 5);
    assertEquals(initCount+6-5, list.size());
    
    // Test with skip > collection count
    list = dbAdmin.getCollectionNames(100, 100);
    assertEquals(0, list.size());
    
    // Test with 0 for skip and positive for limit
    list = dbAdmin.getCollectionNames(100, 0);
    assertEquals(initCount+6, list.size());
    
    // Negative Tests

    try {
      // Test with 0 for limit
      dbAdmin.getCollectionNames(0, 5);
      fail("No exception when limit is 0");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("limit argument must be positive.", e.getMessage());
    }
    
    try {
      // Pass negative for limit
      dbAdmin.getCollectionNames(-5, 5);
      fail("No exception when limit is negative");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("limit argument must be positive.", e.getMessage());
    }
    
    try {
      // Pass negative for skip
      dbAdmin.getCollectionNames(5, -5);
      fail("No exception when skip is negative");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("skip argument must be nonnegative.", e.getMessage());
    }
  }
  
  //test method for getCollectionNames(limit, startName)
  public void testGetCollectionNames3() throws Exception {     
    List<String> list = null;

    OracleDocument metaDoc = client.createMetadataBuilder().build();
    
    dbAdmin.createCollection("xxxColName1", metaDoc);
    dbAdmin.createCollection("xxxColName2", metaDoc);
    dbAdmin.createCollection("xxxColName3", metaDoc);
    dbAdmin.createCollection("xxxColName4", metaDoc);
    dbAdmin.createCollection("xxxColName5", metaDoc);
    dbAdmin.createCollection("xxxColName6", metaDoc);
    
    // Test with startName > all the collection names
    list = dbAdmin.getCollectionNames(3, "xxxx");
    assertEquals(0, list.size());
    
    // Test with startName > part of the collection names
    list = dbAdmin.getCollectionNames(5, "xxxColName4");
    assertEquals(3, list.size());
    Iterator<String> it = list.iterator();
    assertEquals("xxxColName4", it.next());
    assertEquals("xxxColName5", it.next());
    assertEquals("xxxColName6", it.next());
    
    // Test with startName < all the collection names
    list = dbAdmin.getCollectionNames(5, "a");
    assertEquals(5, list.size());
    
    // Pass empty String for startName
    list = dbAdmin.getCollectionNames(5, "");
    assertEquals(5, list.size());
    
    // Negative Tests

    try {
       // Test with 0 for limit
       list = dbAdmin.getCollectionNames(0, "xxxColName1");
       fail("No exception when limit is 0.");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("limit argument must be positive.", e.getMessage());
    }
    
    try {
      // Pass negative for limit
      dbAdmin.getCollectionNames(-5, "xxxColName1");
      fail("No exception when limit is negative");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("limit argument must be positive.", e.getMessage());
    }
    
    try {
      // Pass null for startName
      dbAdmin.getCollectionNames(5, null);
      fail("No exception when startName is null");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("startName argument cannot be null.", e.getMessage());
    }
  }
    
  public void testGetConnection() throws Exception {
    Connection conn1 = dbAdmin.getConnection();
    assertNotNull(conn1);
    assertSame(conn, conn1);
  }
  

  private void testExistingViewOrTable(boolean testView) throws Exception {
  
    // cleanup and make sure no other rows polluted test run.
    String deleteSql = "delete from soda_view";
    PreparedStatement stmt = conn.prepareStatement(deleteSql);
    stmt.execute();
 
    String uuidKey = "D362D2C133DB4F20A5AFEC15929DFEF9";
    String insertSql = 
      "insert into soda_view values('" + uuidKey + "', 'application/json', SYSTIMESTAMP, SYSTIMESTAMP, 'ver000001', c2b('{ \"data\" : \"1\" }'))"; 
    stmt = conn.prepareStatement(insertSql);
    stmt.execute();
    // auto-commit set on
    //conn.commit(); 
    stmt = (OraclePreparedStatement) conn.prepareStatement("select count(*) from soda_view");
    ResultSet rs = stmt.executeQuery();

    OracleRDBMSMetadataBuilder mBuilder = client.createMetadataBuilder().mediaTypeColumnName("CONTENT_TYPE").keyColumnAssignmentMethod("UUID");
    // "soda_view" and "soda_table" have been created by sql setup script
    if(testView)
      mBuilder.viewName("SODA_VIEW");
    else
      mBuilder.tableName("SODA_TABLE");
    
    OracleDocument metaDoc = mBuilder.build();
    String colName = testView ? "testExistingView":"testExistingTable";
    OracleCollection col = dbAdmin.createCollection(colName, metaDoc);
    
    OracleDocument doc = null;
    assertEquals(1, col.find().count());
    doc = col.find().key(uuidKey).getOne();
    assertEquals("{ \"data\" : \"1\" }", new String(doc.getContentAsByteArray(), "UTF-8"));
    assertEquals(uuidKey, doc.getKey());
    assertEquals("application/json", doc.getMediaType());
    assertNotNull(doc.getVersion());
    assertNotNull(doc.getLastModified());
    assertNotNull(doc.getCreatedOn());
    
    // Test about insert/read the new document
    doc = col.insertAndGet(db.createDocumentFromString("{ \"data\" : 2 }"));
    String key = doc.getKey(), version = doc.getVersion(), createdOn = doc.getCreatedOn();
    doc = col.find().key(key).getOne();
    assertEquals("{ \"data\" : 2 }", new String(doc.getContentAsByteArray(), "UTF-8"));
    assertEquals(key, doc.getKey());
    assertEquals("application/json", doc.getMediaType());
    assertEquals(version, doc.getVersion());
    assertEquals(createdOn, doc.getLastModified());
    assertEquals(createdOn, doc.getCreatedOn());
   
    assertEquals(2, col.find().count());
    
    // clean up the created rows for repeated run
    assertEquals(2, col.find().remove());
    assertEquals(0, col.find().count());
    
    // Negative Tests
    // mapping the collection to nonexistent table/view does not cause error
    // but inserting data into the nonexistent table/view can not succeed
    if(testView)
    {
      try {
        // Map to an unknown view
        OracleDocument metaDoc2 = client.createMetadataBuilder().viewName("UNKNOWN_VIEW").build();
        OracleCollection col2 = dbAdmin.createCollection("testExistingView2", metaDoc2);
        col2.insertAndGet(db.createDocumentFromString("{ \"d\" : 2 }"));
        fail("No exception when the specified view does not exist");
      } catch (OracleException e) {
        // Expect an OracleException
        // ORA-01403: no data found
        Throwable t = e.getCause();
        if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel)) {
          assertTrue(t.getMessage().contains("ORA-01403"));
        }
        else {
          assertTrue(t.getMessage().contains("The view intended to back the collection does not exist."));
        }
      }
    }
    
    try {
      // Test different content column type between the view(BLOB) and collection(CLOB)
      OracleRDBMSMetadataBuilder mBuilder3 = client.createMetadataBuilder().contentColumnType("CLOB");
      if(testView)
        mBuilder3.viewName("SODA_VIEW");
      else
        mBuilder3.tableName("SODA_TABLE");
      
      OracleDocument metaDoc3 = mBuilder3.build();
      OracleCollection col3 = dbAdmin.createCollection(testView?"testExistingView3":"testExistingTable3", metaDoc3);
      col3.insertAndGet(db.createDocumentFromString("{ \"d\" : 3 }"));
      fail("No exception when CONTENT_COLUMN_TYPE is different between view and collection");
    } catch (OracleException e) {
      // Expect an OracleException
      Throwable t = e.getCause();
      if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel)) {
        // ORA-01403: no data found
        assertTrue(t.getMessage().contains("ORA-01403"));
      }
      else {
        String obj = testView ? "view" : "table";
        assertTrue(t.getMessage().contains("Columns of the mapped " + obj + " backing the " +
                                           "collection do not match collection metadata."));
      }
    }
    
    try {
      // Test different content column name between the view(BLOB) and collection(CLOB)
      //props4.setProperty(OracleRDBMSProperties.CONTENT_COLUMN_NAME, "content");
      OracleRDBMSMetadataBuilder mBuilder4 = client.createMetadataBuilder().mediaTypeColumnName("CONTENT_TYPE").contentColumnName("content");
      if(testView)
        //props4.setProperty(OracleRDBMSProperties.VIEW_NAME, "soda_view");
        mBuilder4.viewName("SODA_VIEW");
      else
        //props4.setProperty(OracleRDBMSProperties.VIEW_NAME, "soda_table");
        mBuilder4.tableName("SODA_TABLE");
      
      OracleDocument mDoc4 = mBuilder4.build();
      OracleCollection col4 = dbAdmin.createCollection(testView?"testExistingView4":"testExistingTable4", mDoc4);
      col4.insertAndGet(db.createDocumentFromString("{ \"d\" : 4 }"));
      fail("No exception when CONTENT_COLUMN_NAME is different between view and collection");
    } catch (OracleException e) {
      // Expect an OracleException
      Throwable t = e.getCause();
      if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel)) {
        // ORA-01403: no data found
        assertTrue(t.getMessage().contains("ORA-01403"));
      }
      else {
        String obj = testView ? "view" : "table";
        assertTrue(t.getMessage().contains("Columns of the mapped " + obj + " backing the " +
                                           "collection do not match collection metadata."));
      }
    }

    // Insert an row data with null media type
    String key5 = "id-5";
    insertSql = 
      "insert into soda_view values('" + key5 
      + "', null, SYSTIMESTAMP, SYSTIMESTAMP, 'ver000001', c2b('{ \"data\" : \"1\" }'))";
    stmt = conn.prepareStatement(insertSql);
    stmt.execute();
    stmt.close();
    
    OracleRDBMSMetadataBuilder mBuilder5 = client.createMetadataBuilder()
        .mediaTypeColumnName("CONTENT_TYPE").keyColumnAssignmentMethod("CLIENT");
    // "soda_view" and "soda_table" have been created by sql setup script
    if(testView)
      mBuilder5.viewName("SODA_VIEW");
    else
      mBuilder5.tableName("SODA_TABLE");
    
    OracleDocument metaDoc5 = mBuilder5.build();
    String colName5 = testView ? "testExistingView5":"testExistingTable5";
    OracleCollection col5 = dbAdmin.createCollection(colName5, metaDoc5);
    
    assertEquals(1, col5.find().count());
    OracleDocument doc5 = col5.findOne(key5);
    assertNull(doc5.getMediaType());
    
    col5.save(db.createDocumentFromString(key5, "{ \"data\" : \"v1\" }", null));
    doc5 = col5.findOne(key5);
    assertEquals("{ \"data\" : \"v1\" }", doc5.getContentAsString());
    assertEquals("application/json", doc5.getMediaType());
 
    col5.find().key(key5).replaceOne(db.createDocumentFromString(key5, "{ \"data\" : \"v2\" }", "application/json"));
    doc5 = col5.findOne(key5);
    assertEquals("{ \"data\" : \"v2\" }", doc5.getContentAsString());
    assertEquals("application/json", doc5.getMediaType());
 
    col5.save(db.createDocumentFromString(key5, "Hello World", "text/plain"));
    doc5 = col5.find().key(key5).getOne();
    assertEquals("Hello World", new String(doc5.getContentAsByteArray(), "UTF-8"));
    assertEquals("text/plain", doc5.getMediaType());
    
    col5.find().key(key5).replaceOne(db.createDocumentFromString(key5, "<doc/>", "application/xml"));
    doc5 = col5.find().key(key5).getOne();
    assertEquals("<doc/>", new String(doc5.getContentAsByteArray(), "UTF-8"));
    assertEquals("application/xml", doc5.getMediaType());
    
    col5.find().remove();
    assertEquals(0, col5.find().count());
  }
  
  public void testExistingView() throws Exception {
    testExistingViewOrTable(true);
  }
  
  public void testExistingTable() throws Exception {
    testExistingViewOrTable(false);
  }
 
  private void testContColType(String contColType) throws Exception {

    OracleRDBMSMetadataBuilder b = client.createMetadataBuilder();
    if ("BLOB".equals(contColType)) { 
      b.mediaTypeColumnName("CONTENT_TYPE");
    }
    OracleDocument mDoc = b.contentColumnType(contColType).build();
    OracleCollection col = db.admin().createCollection("testContentColumnType" + contColType, mDoc);
    
    // Test insert/query for JSON data
    String jsonString = 
      "{\n" +
      "  \"name\": \"Manu Sporny\", \n" + 
      "  \"homepage\": \"http://manu.sporny.org/\", \n" +
      "  \"image\": \"http://manu.sporny.org/images/manu.png\" \n" +
      "}";
    
    OracleDocument doc = db.createDocumentFromString(jsonString);
    assertEquals(jsonString.length(), doc.getContentLength());
    col.insertAndGet(doc);
    doc = col.find().getOne();
    assertEquals(jsonString.length(), doc.getContentLength());
    
    assertEquals("application/json", doc.getMediaType());
    assertEquals(jsonString, new String(doc.getContentAsByteArray(), "UTF-8"));
    
    // Test insert/query for non JSON data
    if (col.admin().isHeterogeneous()) {
      String text = "Hello Word!";
      OracleDocument doc2 = col.insertAndGet(db.createDocumentFromString(null, text, "text/plain"));
      
      String key2 = doc2.getKey();
      doc2 = col.find().key(key2).getOne();
      assertEquals(11, doc2.getContentLength());
      assertEquals("text/plain", doc2.getMediaType());
      // test with getContentAsByteArray()
      assertEquals(text, new String(doc2.getContentAsByteArray(), "UTF-8"));
      
      // test with getContentAsString()
      // for non-JSON content, getContentAsString might not work, since we can not tell whether it's safe to convert the content to text
      // refer to bug19619420 for more details
      //assertEquals(text, doc2.getContentAsString());
      
      // test with getContentAsStream()
      OracleDocumentImpl doc2Impl = (OracleDocumentImpl) doc2;
      assertEquals(text, inputStream2String(doc2Impl.getContentAsStream()));
      
      assertEquals(2, col.find().count());
    }
   
    // Test Null content
    OracleDocument doc3 = col.insertAndGet(db.createDocumentFromString(null, null));
    OracleDocumentImpl resultDoc = (OracleDocumentImpl) col.findOne(doc3.getKey());
    assertNull(resultDoc.getContentAsStream());
    assertNull(col.findOne(doc3.getKey()).getContentAsByteArray());
    assertNull(col.findOne(doc3.getKey()).getContentAsString());
 
    // Test with replace
    col.find().key(doc3.getKey()).replaceOne(db.createDocumentFromString("{\"data\":\"v1\"}"));
    OracleCursor cursor = col.find().key(doc3.getKey()).getCursor();
    doc3 = cursor.next();
    cursor.close();
    assertEquals("{\"data\":\"v1\"}", doc3.getContentAsString());
    assertEquals("{\"data\":\"v1\"}", new String(doc3.getContentAsByteArray(), "UTF-8"));
    OracleDocumentImpl doc3Impl = (OracleDocumentImpl) doc3;
    assertEquals("{\"data\":\"v1\"}", inputStream2String(doc3Impl.getContentAsStream()));
    
    col.find().key(doc3.getKey()).replaceOne(db.createDocumentFromString(null));
    cursor = col.find().key(doc3.getKey()).getCursor();
    doc3 = cursor.next();
    cursor.close();
    doc3Impl = (OracleDocumentImpl) doc3;
    assertNull(doc3Impl.getContentAsStream());
    assertNull(doc3.getContentAsByteArray());
    assertNull(doc3.getContentAsString());
    
    // Test with save
    OracleDocument doc4 = col.saveAndGet(db.createDocumentFromString(null));
    cursor = col.find().key(doc4.getKey()).getCursor();
    doc4 = cursor.next();
    cursor.close();
    OracleDocumentImpl doc4Impl = (OracleDocumentImpl) doc4;
    assertNull(doc4Impl.getContentAsStream());
    assertNull(doc4.getContentAsByteArray());
    assertNull(doc4.getContentAsString());
    
    // Test with remove
    assertEquals(1, col.find().key(doc4.getKey()).remove());
  
    col.admin().truncate();
  }
  
  public void testContentColumnTypes() throws Exception {
    testContColType("VARCHAR2");
    
    testContColType("BLOB");

    testContColType("CLOB");

    /* ### Oracle Database does not support NVARCHAR2, NCLOB, or RAW storage for JSON
    testContColType("NVARCHAR2");
    
    testContColType("RAW");
    
    testContColType("NCLOB");
    */

    // Test with invalid CONTENT_COLUMN_TYPE for collection creation
    try {
      OracleDocument mDoc = client.createMetadataBuilder().
          contentColumnType("UNKNOWNTYPE").build();
      db.admin().createCollection("testContentColumnTypes2", mDoc);
      fail("No exception when the specified CONTENT_COLUMN_TYPE value is invalid");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Invalid argument value \"UNKNOWNTYPE\".", e.getMessage());
    }
  }

  public void testJSONValidation() throws Exception {
 
    // Test with content column validation = STRICT
    OracleDocument mDoc = client.createMetadataBuilder().
        contentColumnValidation("STRICT").build();
    OracleCollection col = db.admin().createCollection("testJSONValidation", mDoc);
    
    String strictJSONStr = 
      "{\n" +
      "  \"context\": {\n" +
      "                  \"name\": \"http://example.com/person#name\",\n" +
      "                  \"details\": \"http://example.com/person#details\"\n" +
      "               },\n" +
      "  \"name\": \"Markus Lanthaler\" \n" +  
      "}";
    
    // Test about inserting strict JSON document
    OracleDocument doc = col.insertAndGet(db.createDocumentFromString(strictJSONStr));
    doc = col.find().key(doc.getKey()).getOne();
    assertEquals(strictJSONStr, new String(doc.getContentAsByteArray(), "UTF-8"));

    // the following doc does conform to standard JSON check, but contain the duplicate field names
    String standardJSONStr = 
      "{\n" +
        "\"context\":{ \"name\": \"http://example.com/person#name\"},\n" +
        "\"name\": \"Markus Lanthaler\", \n" +
        "\"context\":{ \"name\": \"http://example.com/organization#name\"},\n" +
        "\"homepage\": \"http://manu.sporny.org/\"\n" +
      "}";

    try {
      // the doc containing duplicate field name, so should fail with STRICT validation
      doc = col.insertAndGet(db.createDocumentFromString(standardJSONStr));
      fail("No exception when the document violated strict JSON validation");
    } catch (OracleException e) {
      // Expect an OracleException
      // ORA-02290: check constraint (SYS_C0049214) violated
      Throwable t = e.getCause();
      assertTrue(t.getMessage().contains("ORA-02290"));
    } 

    String laxJSONStr =
      "{\n" +
        "'id': \"http://manu.sporny.org/about#manu\",\n" +
        "'type': 'foaf:Person',\n" +
        "'name': 'Manu Sporny',\n" +
        "'knows': \"http://greggkellogg.net/foaf#me\"\n" +
      "}";
    
    try {
      // Try to insert lax JSON document into the collection with strict validation 
      doc = col.insertAndGet(db.createDocumentFromString(laxJSONStr));
      fail("No exception when the document violated strict JSON validation");
    } catch (OracleException e) {
      // Expect an OracleException
      // ORA-02290: check constraint (SYS_C0049214) violated
      Throwable t = e.getCause();
      assertTrue(t.getMessage().contains("ORA-02290"));
    }
    
    String nonJSONStr = "Hello World";
    try {
      // Try to insert non JSON document into the collection with strict validation 
      doc = col.insertAndGet(db.createDocumentFromString(nonJSONStr));
      fail("No exception when the document violated strict JSON validation");
    } catch (OracleException e) {
      // Expect an OracleException
      // ORA-02290: check constraint (SYS_C0049214) violated
      Throwable t = e.getCause();
      assertTrue(t.getMessage().contains("ORA-02290"));
    }
    
    // Test with content column validation = LAX
    //props2.setProperty(OracleRDBMSProperties.VALIDATION, "LAX");
    OracleDocument mDoc2 = client.createMetadataBuilder().mediaTypeColumnName("CONTENT_TYPE")
        .contentColumnValidation("LAX").build();
    OracleCollection col2 = db.admin().createCollection("testJSONValidation2", mDoc2);
    
    // Test about inserting strict JSON document
    OracleDocument doc2 = col2.insertAndGet(db.createDocumentFromString(strictJSONStr));
    doc2 = col2.find().key(doc2.getKey()).getOne();
    assertEquals(strictJSONStr, new String(doc2.getContentAsByteArray(), "UTF-8"));

    // Standard JSON doc should pass LAX validation 
    doc2 = col2.insertAndGet(db.createDocumentFromString(standardJSONStr));
    doc2 = col2.find().key(doc2.getKey()).getOne();
    assertEquals(standardJSONStr, new String(doc2.getContentAsByteArray(), "UTF-8"));
 
    // Test about inserting lax JSON document
    doc2 = col2.insertAndGet(db.createDocumentFromString(laxJSONStr));
    doc2 = col2.find().key(doc2.getKey()).getOne();
    assertEquals(laxJSONStr, new String(doc2.getContentAsByteArray(), "UTF-8"));
    
    try {
      // Try to insert non JSON document into the collection with lax validation 
      doc2 = col2.insertAndGet(db.createDocumentFromString(nonJSONStr));
      fail("No exception when the document violated lax JSON validation");
    } catch (OracleException e) {
      // Expect an OracleException
      // ORA-02290: check constraint (SYS_C0049214) violated
      Throwable t = e.getCause();
      assertTrue(t.getMessage().contains("ORA-02290"));
    }
   
    // Test with content column validation = STANDARD
    OracleDocument mDoc3 = client.createMetadataBuilder().mediaTypeColumnName("CONTENT_TYPE")
        .contentColumnValidation("STANDARD")
        .build();
    OracleCollection col3 = db.admin().createCollection("testJSONValidation3", mDoc3);
    OracleDocument descDoc = col3.admin().getMetadata();
 
    // strict JSON document can pass "STANDARD" validation
    OracleDocument doc3 = col3.insertAndGet(db.createDocumentFromString(strictJSONStr));
    doc3 = col3.find().key(doc3.getKey()).getOne();
    assertEquals(strictJSONStr, new String(doc3.getContentAsByteArray(), "UTF-8"));

    // standard JSON document can pass "STANDARD" validation
    doc3 = col3.insertAndGet(db.createDocumentFromString(standardJSONStr));
    doc3 = col3.find().key(doc3.getKey()).getOne();
    assertEquals(standardJSONStr, new String(doc3.getContentAsByteArray(), "UTF-8"));
    
    try {
      // lax JSON document should fail with "STANDARD" validation 
      doc3 = col3.insertAndGet(db.createDocumentFromString(laxJSONStr));
      fail("No exception when the document violated standard JSON validation");
    } catch (OracleException e) {
      // Expect an OracleException
      Throwable t = e.getCause();
      assertTrue(t.getMessage().contains("ORA-02290"));
    }
    
    try {
      // non JSON document should fail with "STANDARD" validation 
      doc3 = col3.insertAndGet(db.createDocumentFromString(nonJSONStr));
      fail("No exception when the document violated standard JSON validation");
    } catch (OracleException e) {
      // Expect an OracleException
      Throwable t = e.getCause();
      assertTrue(t.getMessage().contains("ORA-02290"));
    }
 
  }
 
  private void testContentMaxLength(String contentSQLType) throws Exception {
    
    OracleDocument mDoc = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("CLIENT").contentColumnType(contentSQLType)
        .contentColumnMaxLength(32).build();
    OracleCollection col = db.admin().createCollection("TestContentMaxLength-" + contentSQLType, mDoc);
    
    String contSize31 = "{\"data\":1234567890123456789012}";
    col.insertAndGet(db.createDocumentFromString("id-1", contSize31, null));
    OracleDocument doc = col.find().getOne();
    assertEquals(contSize31, new String(doc.getContentAsByteArray(), "UTF-8"));
    
    try {
      // the document content size > max length
      String contSize33 = "{\"data\":1234567890123456789012234}";
      col.insertAndGet(db.createDocumentFromString("id-2", contSize33, null));
      fail("No exception when the document content size is bigger than column max length");
    } catch (OracleException e) {
      // Expect an OracleException
      // ORA-12899: value too large for column "AAA"."JSON_DOCUMENT" (actual: 40, maximum: 32)
      Throwable t = e.getCause();
      assertTrue(t.getMessage().contains("ORA-12899"));
    }
    
    // 50000 is greater than sql type's upper limit
    mDoc = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("CLIENT").contentColumnType(contentSQLType)
        .contentColumnMaxLength(50000).build();

    try {
      col = db.admin().createCollection("TestContentMaxLengthN", mDoc);
      fail("No exception when the specified CONTENT_MAX_LENGTH is too big");
    } catch (OracleException e) {
      // Expect an OracleException
      // ORA-00910: specified length too long for its datatype
      Throwable t = e.getCause();
      assertTrue(t.getMessage().contains("ORA-00910"));
    }
  }
  
  public void testContentMaxLength() throws Exception {
    
    testContentMaxLength("VARCHAR2");

    /* ### Oracle Database does not support NVARCHAR2 or RAW storage for JSON
    testContentMaxLength("NVARCHAR2");
    testContentMaxLength("RAW");
    */
    
    // Test about specifying CONTENT_MAX_LENGTH for "BLOB" type
    try { 
      OracleDocument mDoc = client.createMetadataBuilder()
          .keyColumnAssignmentMethod("CLIENT").contentColumnType("BLOB")
          .contentColumnMaxLength(16).build();
      OracleCollection col = db.admin().createCollection("TestContentMaxLength2", mDoc);
      fail("No exception when specifying CONTENT_MAX_LENGTH for \"BLOB\" type");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("A maximum length cannot be set for LOB types.", e.getMessage());
    }

    // Test with negative for CONTENT_MAX_LENGTH
    try { 
      OracleDocument mDoc3 = client.createMetadataBuilder()
          .keyColumnAssignmentMethod("CLIENT").contentColumnType("VARCHAR2")
          .contentColumnMaxLength(-16).build();
      db.admin().createCollection("TestContentMaxLengthN", mDoc3);
      fail("No exception when the specified MAX_LENGTH is negative");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Invalid argument value \"-16\".", e.getMessage());
    }

    // Test with contentColumnType=VARCHAR2, CONTENT_MAX_LENGTH=0
    OracleDocument mDoc4 = client.createMetadataBuilder()
        .contentColumnType("VARCHAR2")
        .contentColumnMaxLength(0).build();
    OracleCollection col4 = db.admin().createCollection("TestContentMaxLength4", mDoc4);
    OracleDocument descDoc = col4.admin().getMetadata();
    JsonNumber jNum = (JsonNumber) getValue(descDoc, path("contentColumn", "maxLength"));
    assertEquals(4000, jNum.intValue());
    
    // Test about specifying CONTENT_MAX_LENGTH for "CLOB" type
    try { 
      OracleDocument mDoc5 = client.createMetadataBuilder()
          .keyColumnAssignmentMethod("CLIENT").contentColumnType("CLOB")
          .contentColumnMaxLength(128).build();
      OracleCollection col5 = db.admin().createCollection("TestContentMaxLength5", mDoc5);
      fail("No exception when specifying CONTENT_MAX_LENGTH for \"CLOB\" type");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("A maximum length cannot be set for LOB types.", e.getMessage());
    }
    
    // Test about specifying CONTENT_MAX_LENGTH for "NCLOB" type
    /* ### Oracle Database does not support NCLOB storage for JSON
    try { 
      OracleDocument mDoc6 = client.createMetadataBuilder()
          .keyColumnAssignmentMethod("CLIENT").contentColumnType("NCLOB")
          .contentColumnMaxLength(256).build();
      OracleCollection col6 = db.admin().createCollection("TestContentMaxLength6", mDoc6);
      fail("No exception when specifying CONTENT_MAX_LENGTH for \"NCLOB\" type");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("A maximum length cannot be set for LOB types.", e.getMessage());
    }
    */

  }

  public void testKeyMetadata() throws Exception {
    
    // Test with key column type=VARCHAR2 && max key length=32 && key method=CLIENT
    OracleDocument mDoc = client.createMetadataBuilder()
        .keyColumnType("VARCHAR2").keyColumnMaxLength(32)
        .keyColumnAssignmentMethod("CLIENT").build();
    
    OracleCollection col = dbAdmin.createCollection("testKeyMetadata", mDoc);
    col.insertAndGet(db.createDocumentFromString("id-1234567890", "{ \"d\" : 1 }", null));
    assertEquals("id-1234567890", col.find().getOne().getKey());
    
    try {
      // Test with key length is greater than max key length
      col.insert(db.createDocumentFromString("id-123456789012345678901234567890", "{ \"d\" : 1 }", null));
      fail("No exception when the specified key is greater than max key length");
    } catch (OracleException e) {
      // Expect an OracleException
      // ORA-12899: value too large for column
      Throwable t = e.getCause();
      assertTrue(t.getMessage().contains("ORA-12899"));
    }
    
    // Test with key column type=NVARCHAR2 && max key length=512 && key method=GUID
    OracleDocument mDoc2 = client.createMetadataBuilder()
        .keyColumnType("NVARCHAR2").keyColumnMaxLength(512)
        .keyColumnAssignmentMethod("GUID").build();
    
    OracleCollection col2 = dbAdmin.createCollection("testKeyMetadata2", mDoc2);
    OracleDocument doc2 = col2.insertAndGet(db.createDocumentFromString(null, "{ \"d\" : 2 }", null));
    assertEquals(doc2.getKey(), col2.find().key(doc2.getKey()).getOne().getKey());
    assertEquals("{ \"d\" : 2 }", new String(col2.find().getOne().getContentAsByteArray(), "UTF-8"));
    
    // Test with key column type=RAW && key method=UUID
    OracleDocument mDoc3 = client.createMetadataBuilder()
        .keyColumnType("RAW").keyColumnAssignmentMethod("UUID").build();
    
    OracleCollection col3 = dbAdmin.createCollection("testKeyMetadata3", mDoc3);
    OracleDocument doc3 = col3.insertAndGet(db.createDocumentFromString(null, "{ \"d\" : 3 }", null));
    assertEquals(doc3.getKey(), col3.find().key(doc3.getKey()).getOne().getKey());
    assertEquals("{ \"d\" : 3 }", new String(col3.find().getOne().getContentAsByteArray(), "UTF-8"));
    
    // Test with key column type=NUMBER && key method=SEQUENCE && the specified sequenceName
    OracleDocument mDoc4 = client.createMetadataBuilder()
        .keyColumnType("NUMBER")
        .keyColumnAssignmentMethod("SEQUENCE").keyColumnSequenceName("KEY_SEQ").build();
    
    OracleCollection col4 = dbAdmin.createCollection("testKeyMetadata4", mDoc4);
    OracleDocument doc4 = col4.insertAndGet(db.createDocumentFromString(null, "{ \"d\" : 4 }", null));
    assertEquals(doc4.getKey(), col4.find().key(doc4.getKey()).getOne().getKey());
    assertEquals("{ \"d\" : 4 }", new String(col4.find().getOne().getContentAsByteArray(), "UTF-8")); 
    
    try {
      // Test with key column type=NUMBER && key method=CLIENT && max key length=512
      OracleDocument mDoc5 = client.createMetadataBuilder()
          .keyColumnType("NUMBER")
          .keyColumnAssignmentMethod("CLIENT").keyColumnMaxLength(512).build();
  
      dbAdmin.createCollection("testKeyMetadata5", mDoc5);
      fail("No exception when specifying max length for \"NUMBER\" type");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("The specified key column type cannot be used with a maximum length.", e.getMessage());
    }
    
    try {
      // Test with key is not a number
      OracleDocument mDoc6 = client.createMetadataBuilder()
          .keyColumnType("NUMBER").keyColumnAssignmentMethod("CLIENT").build();
      OracleCollection col6 = dbAdmin.createCollection("testKeyMetadata6", mDoc6);
      
      col6.insert(db.createDocumentFromString("id-1", "{ \"d\" : 15 }", null));
      fail("No exception when the specified key is NOT a number");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("The value id-1 is not a valid key of type NUMBER.", e.getMessage());
    }
    
    try {
      // Test with KeyAssignedMethod="SEQUENCE" but not specifying sequence name
      OracleDocument mDoc7 = client.createMetadataBuilder().keyColumnAssignmentMethod("SEQUENCE").build();
      dbAdmin.createCollection("testKeyMetadata6", mDoc7);
      fail("No exception when setting KeyAssignedMethod to \"SEQUENCE\", but not specifying sequence name");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("If the assignment method is \"SEQUENCE\", a key column sequence name must be specified.", e.getMessage());
    }

    // Test with negative for keyColumnMaxLength
    try {
      OracleDocument mDoc8 = client.createMetadataBuilder()
          .keyColumnAssignmentMethod("CLIENT")
          .keyColumnType("VARCHAR2").keyColumnMaxLength(-512).build();
      dbAdmin.createCollection("testKeyMetadata8", mDoc8);
      fail("No exception when specifying negative for key max length");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Invalid argument value \"-512\".", e.getMessage());
    }

    // Test with 0 for keyColumnMaxLength and keyColumnType = "VARCHAR2"
    OracleDocument mDoc9 = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("CLIENT")
        .keyColumnType("VARCHAR2").keyColumnMaxLength(0).build();
    OracleCollection col9 = dbAdmin.createCollection("testKeyMetadata9", mDoc9);
    OracleDocument descDoc = col9.admin().getMetadata();
    JsonNumber jNum = (JsonNumber) getValue(descDoc, path("keyColumn", "maxLength"));
    // the default value, 255, should be used
    assertEquals(255, jNum.intValue());
    
    // Test with 0 for keyColumnMaxLength and keyColumnType = "NVARCHAR2"
    OracleDocument mDoc10 = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("CLIENT")
        .keyColumnType("NVARCHAR2").keyColumnMaxLength(0).build();
    OracleCollection col10 = dbAdmin.createCollection("testKeyMetadata10", mDoc10);
    descDoc = col10.admin().getMetadata();
    jNum = (JsonNumber) getValue(descDoc, path("keyColumn", "maxLength"));
    // the default value, 255, should be used
    assertEquals(255, jNum.intValue());
    
    // Test with 0 for keyColumnMaxLength and keyColumnType = "RAW"
    OracleDocument mDoc11 = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("CLIENT")
        .keyColumnType("RAW").keyColumnMaxLength(0).build();
    OracleCollection col11 = dbAdmin.createCollection("testKeyMetadata11", mDoc11);
    descDoc = col11.admin().getMetadata();
    jNum = (JsonNumber) getValue(descDoc, path("keyColumn", "maxLength"));
    // "raw" key column type can not be used with maximum length
    //assertEquals(255, jNum.intValue());

    // Test about Specifying keyColumnMaxLength for keyColumnType = "RAW"
    try {
      OracleDocument mDoc12 = client.createMetadataBuilder()
          .keyColumnAssignmentMethod("CLIENT")
          .keyColumnType("RAW").keyColumnMaxLength(255).build();
      OracleCollection col12 = dbAdmin.createCollection("testKeyMetadata12", mDoc12);
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("The specified key column type cannot be used with a maximum length.", e.getMessage());
    }
 
  }

  private void basicTestsforKeyMetadata2(OracleCollection col) throws Exception{
    String content = "{ \"data\": \"Hello World!\" }";
    String key = null;
    OracleDocument doc = null;
    OracleDocument colDescDoc = col.admin().getMetadata();
    
    String assignmentMethodStr = ((JsonString) getValue(colDescDoc, path("keyColumn", "assignmentMethod"))).getString();
    String sqlTypeStr = ((JsonString) getValue(colDescDoc, path("keyColumn", "sqlType"))).getString();
    
    if(assignmentMethodStr.equalsIgnoreCase("CLIENT")) {
      if(sqlTypeStr.equalsIgnoreCase("NVARCHAR2") || sqlTypeStr.equalsIgnoreCase("VARCHAR2"))
        key = "id-0000TY598";
      else if(sqlTypeStr.equalsIgnoreCase("NUMBER"))
        key = "420102101";
      else if(sqlTypeStr.equalsIgnoreCase("RAW"))
        key = "09283F2FAA3F36F9E053AB18F50A1BDA";
    }
    
    if(key == null) {
      doc = col.insertAndGet(db.createDocumentFromString(key, content, null));
      key = doc.getKey();
    } else {
      col.insert(db.createDocumentFromString(key, content, null));
    }
    
    assertEquals(1, col.find().count());
   
    OracleCursor cursor = col.find().key(key).getCursor();
    assertEquals(true, cursor.hasNext());
    doc = cursor.next();
    assertEquals(key, doc.getKey());
    assertEquals(content, doc.getContentAsString());
    assertEquals(false, cursor.hasNext());
    cursor.close();
   
    OracleDocument filterDoc = db.createDocumentFromString("{ \"data\": {\"$exists\" : true} }");
    assertEquals(key, col.find().filter(filterDoc).getOne().getKey());
    
  }

  // testKeyMetadata2 is the supplement test to testKeyMetadata()
  public void testKeyMetadata2() throws Exception {
    
    final String[] assignmentMethodStrings = { "CLIENT", "SEQUENCE", "GUID", "UUID"};
    final String[] sqlTypeStrings = { "VARCHAR2", "NVARCHAR2", "NUMBER", "RAW"};
    int counter = 1;
    
    for (String assignmentMethod : assignmentMethodStrings) {
      for (String sqlType : sqlTypeStrings) {
        OracleRDBMSMetadataBuilder mBuilder = client.createMetadataBuilder();
        if(assignmentMethod.equalsIgnoreCase("SEQUENCE"))
          mBuilder.keyColumnAssignmentMethod("SEQUENCE").keyColumnSequenceName("KEY_SEQ");
        else
          mBuilder.keyColumnAssignmentMethod(assignmentMethod);
        
        OracleDocument mDoc = mBuilder.build();
        OracleCollection col = dbAdmin.createCollection("testKeyMetadata2-" + counter, mDoc);
        counter++;
        try {
          basicTestsforKeyMetadata2(col);
        } 
        catch(Exception ex) {
          throw ex;
        }
      }
      
    }
 
  }


  private void testBasicOperations(OracleCollection col) throws Exception {
   
    OracleDocument doc = col.insertAndGet(db.createDocumentFromString(null, "{ \"data\" : 10 }", null));
    String key = doc.getKey();
    OracleCursor cursor = col.find().key(key).getCursor();
    doc = cursor.next();
    assertEquals(key, doc.getKey());
    assertEquals("{ \"data\" : 10 }", new String(doc.getContentAsByteArray(), "UTF-8"));
    cursor.close();
    
    OracleDocument filterDoc = db.createDocumentFromString(null, "{ \"$id\" : [\"" + key + "\"] }", null);
    doc = col.find().filter(filterDoc).getOne();
    assertEquals("{ \"data\" : 10 }", new String(doc.getContentAsByteArray(), "UTF-8"));
    
    String fStr = "{ \"$query\" : {\"data\" : {\"$gt\" : 15 }}, \"$orderby\" : {\"data\" : 1} }";
    filterDoc = db.createDocumentFromString(fStr);
    assertEquals(0, col.find().filter(filterDoc).count());
    
  }
  
  public void testSecureFileMetadata() throws Exception {

    // ### Oracle Database does not support NVARCHAR2 or NCLOB storage for JSON
    //final String[] sqlTypes = { "BLOB", "CLOB", "NCLOB"};
    
    final String[] sqlTypes = { "BLOB", "CLOB"};
    final String[] compressions = { "NONE", "HIGH", "MEDIUM", "LOW"};
    final boolean[] cachings = {true, false};
    final String[] encryptions = {"3DES168", "AES128", "AES192", "AES256"};
    int counter = 0;
    
    // cross tests for LOB types
    for (String sqlType : sqlTypes) {
      for (String compression : compressions) {
        for (boolean caching : cachings) {
          for (String encryption : encryptions) {

            OracleDocument mDoc = client.createMetadataBuilder()
                .contentColumnType(sqlType).contentColumnCompress(compression)
                .contentColumnCache(caching).contentColumnEncrypt(encryption)
                .build();
            counter++;
            OracleCollection col = null;

            try {
              col = dbAdmin.createCollection("testSecureFileMetadata" + counter, mDoc);
              testBasicOperations(col);
            } catch (Exception ex) {
              throw ex;
            } // end for try catch
            
          }
        }
      }
    } // end for "for lobTypes" loop
 
    // ### Oracle Database does not support NVARCHAR2 or RAW storage for JSON
    //final String[] nonLobTypes = { "VARCHAR2", "NVARCHAR2", "RAW"};
    final String[] nonLobTypes = { "VARCHAR2" };
    counter = 100;
    for (String nonLobType : nonLobTypes) {
      OracleDocument mDoc = client.createMetadataBuilder()
          .contentColumnType(nonLobType).contentColumnCompress("NONE")
          .contentColumnCache(false)
          .build();
      counter++;
      OracleCollection col = null;

      try {
        col = dbAdmin
            .createCollection("testSecureFileMetadata" + counter, mDoc);
        testBasicOperations(col);
      } catch (Exception ex) {
        throw ex;
      } // end for try catch
    }
 
    // Negative tests: enabling Securefile for non-LOB types
    try {
      OracleDocument mDoc = client.createMetadataBuilder()
          .contentColumnType("VARCHAR2")
          .contentColumnCompress("NONE").contentColumnCache(true).contentColumnEncrypt("3DES168")
          .build();
      OracleCollection col = dbAdmin.createCollection("testSecureFileMetadataN1", mDoc);
      fail("No exception when enabling Securefile for non-LOB types");
    } catch (OracleException e) {
      // Expect an OracleException  
      assertEquals("SecureFile LOB settings cannot be used when the content column type is \"VARCHAR2\"", e.getMessage());
    } 

    /* ### Oracle Database does not support NVARCHAR2 or RAW for JSON
    try {
      OracleDocument mDoc2 = client.createMetadataBuilder()
          .contentColumnType("NVARCHAR2")
          .contentColumnCompress("MEDIUM").contentColumnCache(false)
          .build();
      dbAdmin.createCollection("testSecureFileMetadataN2", mDoc2);
      fail("No exception when enabling Securefile for non-LOB types");
    } catch (OracleException e) {
      // Expect an OracleException  
      assertEquals("SecureFile LOB settings cannot be used when the content column type is \"NVARCHAR2\"", e.getMessage());
    }
 
    try {
      OracleDocument mDoc3 = client.createMetadataBuilder()
          .contentColumnType("RAW")
          .contentColumnCompress("MEDIUM").contentColumnCache(false).contentColumnEncrypt("AES256")
          .build();
      OracleCollection col3 = dbAdmin.createCollection("testSecureFileMetadataN3", mDoc3);
      fail("No exception when enabling Securefile for non-LOB types");
    } catch (OracleException e) {
      // Expect an OracleException  
      assertEquals("SecureFile LOB settings cannot be used when the content column type is \"RAW\"", e.getMessage());
    }
    */
 
  }

  private void basicTestforMiniCol(OracleCollection col, boolean clientAssignedKey, boolean version, boolean createdOn, boolean lastModified) throws Exception {
    OracleDocument doc = null;
    OracleCollectionAdmin colAdmin = col.admin();
    String createdOn1 = null;
    
    // Test with insert()
    String key = null;
    if(clientAssignedKey) {
      key = "id-1";
      doc = db.createDocumentFromString(key, "{ \"key\": \"val1\" }", null);
      col.insert(doc);
    } else {
      doc = db.createDocumentFromString("{ \"key\": \"val1\" }");
      doc = col.insertAndGet(doc);
      key = doc.getKey();
    }
    doc = col.findOne(key);
    assertEquals("{ \"key\": \"val1\" }", new String(doc.getContentAsByteArray(), "UTF-8"));
    if(version)
      assertNotNull(doc.getVersion());
    if(createdOn) {
      assertNotNull(doc.getCreatedOn());
      createdOn1 = doc.getCreatedOn();
    }
    if(lastModified)
      assertNotNull(doc.getLastModified());
    
    // Test with save()
    if(clientAssignedKey) {
      doc = db.createDocumentFromString(key, "{ \"key\": \"val2\" }");
      col.save(doc);
    } else {
      doc = db.createDocumentFromString("{ \"key\": \"val2\" }");
      doc = col.saveAndGet(doc);
      key = doc.getKey();
    }
    doc = col.find().key(key).getOne();
    assertEquals("{ \"key\": \"val2\" }", new String(doc.getContentAsByteArray(), "UTF-8"));
    if(version)
      assertNotNull(doc.getVersion());
    if(createdOn) {
      assertNotNull(doc.getCreatedOn());
      if (clientAssignedKey) {
        // createdOn should not be changed with update operation
        assertEquals(createdOn1, doc.getCreatedOn());
      }
      createdOn1 = doc.getCreatedOn();
    }
    if(lastModified)
      assertNotNull(doc.getLastModified());
    
    // Test with replaceOne
    doc = db.createDocumentFromString("{ \"key\": \"val3\" }");
    assertTrue(col.find().key(key).replaceOne(doc));
    doc = col.findOne(key);
    assertEquals("{ \"key\": \"val3\" }", new String(doc.getContentAsByteArray(), "UTF-8"));
    if(version)
      assertNotNull(doc.getVersion());
    if(createdOn) {
      assertNotNull(doc.getCreatedOn());
      assertEquals(createdOn1, doc.getCreatedOn());
    }
    if(lastModified)
      assertNotNull(doc.getLastModified());
    
    
    // Test with replaceOneAndGet
    doc = db.createDocumentFromString("{ \"key\": \"val4\" }");
    doc = col.find().key(key).replaceOneAndGet(doc);
    assertEquals(key, doc.getKey());
    
    OracleCursor cursor = col.find().key(key).getCursor();
    assertTrue(cursor.hasNext());
    doc = cursor.next();
    assertEquals("{ \"key\": \"val4\" }", new String(doc.getContentAsByteArray(), "UTF-8"));
    if(version)
      assertNotNull(doc.getVersion());
    if(createdOn) {
      assertEquals(createdOn1, doc.getCreatedOn());
    }
    if(lastModified)
      assertNotNull(doc.getLastModified());
  }
  
  // Test method for the collection without any optional columns
  public void testMiniCol() throws Exception {
    
    // Test with key/content and client-assigned key collection
    OracleDocument mDoc = client.createMetadataBuilder()
        .removeOptionalColumns()
        .keyColumnAssignmentMethod("CLIENT").mediaTypeColumnName("MediaType")
        .build();
    OracleCollection col = dbAdmin.createCollection("testMiniCol", mDoc);
    basicTestforMiniCol(col, true, false, false, false);

    // Test with key/content and auto-generated key collection
    OracleDocument mDoc2 = client.createMetadataBuilder()
        .removeOptionalColumns()
        .keyColumnAssignmentMethod("GUID")
        .contentColumnType("CLOB")
        .build();
    OracleCollection col2 = dbAdmin.createCollection("testMiniCol2", mDoc2);
    basicTestforMiniCol(col2, false, false, false, false);
    
    String sName = schemaName.toUpperCase();

    // Test with key/content/version 
    String mData3 =
      "{\"schemaName\":\"" + sName + "\", \"keyColumn\":{\"name\":\"ID\",\"sqlType\":\"VARCHAR2\",\"maxLength\":255,\"assignmentMethod\":\"UUID\"}," +
      "\"contentColumn\":{\"name\":\"JSON_DOCUMENT\",\"sqlType\":\"BLOB\",\"compress\":\"LOW\",\"cache\":true,\"encrypt\":\"NONE\",\"validation\":\"STRICT\"}," +
      "\"versionColumn\":{\"name\":\"VERSION\",\"type\":\"String\",\"method\":\"SHA256\"}," +
      "\"readOnly\":false}";
    OracleDocument mDoc3 = db.createDocumentFromString(mData3);
    OracleCollection col3 = dbAdmin.createCollection("testMiniCol3", mDoc3);
    basicTestforMiniCol(col3, false, true, false, false);
    
    // Test with key/content/createdOn and client-assigned key collection
    String mData4 =
      "{\"schemaName\":\"" + sName + "\", \"keyColumn\":{\"name\":\"ID\",\"sqlType\":\"VARCHAR2\",\"maxLength\":255,\"assignmentMethod\":\"CLIENT\"}," +
      "\"contentColumn\":{\"name\":\"JSON_DOCUMENT\",\"sqlType\":\"BLOB\",\"compress\":\"LOW\",\"cache\":true,\"encrypt\":\"NONE\",\"validation\":\"STRICT\"}," +
      "\"creationTimeColumn\":{\"name\":\"CREATED_ON\"}," +
      "\"readOnly\":false}";
    OracleDocument mDoc4 = db.createDocumentFromString(mData4);
    OracleCollection col4 = dbAdmin.createCollection("testMiniCol4", mDoc4);
    basicTestforMiniCol(col4, true, false, true, false);
    
    // Test with key/content/last-mod
    String mData5 =
      "{\"schemaName\":\"" + sName + "\", \"keyColumn\":{\"name\":\"ID\",\"sqlType\":\"VARCHAR2\",\"maxLength\":255,\"assignmentMethod\":\"GUID\"}," +
      "\"contentColumn\":{\"name\":\"JSON_DOCUMENT\",\"sqlType\":\"CLOB\",\"compress\":\"LOW\",\"cache\":true,\"encrypt\":\"NONE\",\"validation\":\"STRICT\"}," +
      "\"lastModifiedColumn\":{\"name\":\"LAST_MODIFIED\"}," +
      "\"readOnly\":false}";
    OracleDocument mDoc5 = db.createDocumentFromString(mData5);
    OracleCollection col5 = dbAdmin.createCollection("testMiniCol5", mDoc5);
    basicTestforMiniCol(col5, false, false, false, true);
    
    // Test with key/content/version/createdOn and client-assigned key collection
    String mData6 =
      "{\"schemaName\":\"" + sName + "\", \"keyColumn\":{\"name\":\"ID\",\"sqlType\":\"VARCHAR2\",\"maxLength\":255,\"assignmentMethod\":\"CLIENT\"}," +
      "\"contentColumn\":{\"name\":\"JSON_DOCUMENT\",\"sqlType\":\"CLOB\",\"compress\":\"LOW\",\"cache\":true,\"encrypt\":\"NONE\",\"validation\":\"STRICT\"}," +
      "\"versionColumn\":{\"name\":\"VERSION\",\"type\":\"String\",\"method\":\"SHA256\"}," +
      "\"creationTimeColumn\":{\"name\":\"CREATED_ON\"}," +
      "\"readOnly\":false}";
    OracleDocument mDoc6 = db.createDocumentFromString(mData6);
    OracleCollection col6 = dbAdmin.createCollection("testMiniCol6", mDoc6);
    basicTestforMiniCol(col6, true, true, true, false);

    // Test with key/content/version/last-mod  
    String mData7 =
      "{\"schemaName\":\"" + sName + "\", \"keyColumn\":{\"name\":\"ID\",\"sqlType\":\"VARCHAR2\",\"maxLength\":255,\"assignmentMethod\":\"UUID\"}," +
      "\"contentColumn\":{\"name\":\"JSON_DOCUMENT\",\"sqlType\":\"CLOB\",\"compress\":\"LOW\",\"cache\":true,\"encrypt\":\"NONE\",\"validation\":\"STRICT\"}," +
      "\"versionColumn\":{\"name\":\"VERSION\",\"type\":\"String\",\"method\":\"SHA256\"}," +
      "\"lastModifiedColumn\":{\"name\":\"LAST_MODIFIED\"}," +
      "\"readOnly\":false}";
    OracleDocument mDoc7 = db.createDocumentFromString(mData7);
    OracleCollection col7 = dbAdmin.createCollection("testMiniCol7", mDoc7);
    basicTestforMiniCol(col7, false, true, false, true);
    
    // Test with key/content/createdOn/last-mod and client-assigned key collection
    String mData8 =
      "{\"schemaName\":\"" + sName + "\", \"keyColumn\":{\"name\":\"ID\",\"sqlType\":\"VARCHAR2\",\"maxLength\":255,\"assignmentMethod\":\"CLIENT\"}," +
      "\"contentColumn\":{\"name\":\"JSON_DOCUMENT\",\"sqlType\":\"CLOB\",\"compress\":\"LOW\",\"cache\":true,\"encrypt\":\"NONE\",\"validation\":\"STRICT\"}," +
      "\"creationTimeColumn\":{\"name\":\"CREATED_ON\"}," +
      "\"lastModifiedColumn\":{\"name\":\"LAST_MODIFIED\"}," +
      "\"readOnly\":false}";
    OracleDocument mDoc8 = db.createDocumentFromString(mData8);
    OracleCollection col8 = dbAdmin.createCollection("testMiniCol8", mDoc8);
    basicTestforMiniCol(col8, true, false, true, true);
 
  } 

  // Test method for collection specification check 
  public void testMetadataCheck() throws Exception {
    OracleDocument descDoc = null;
    JsonString jStr = null;
 

    // Test with missing schemaName
    String mData1 =
      "{\"keyColumn\":{\"name\":\"ID\",\"sqlType\":\"VARCHAR2\",\"maxLength\":255,\"assignmentMethod\":\"CLIENT\"}," +
      "\"contentColumn\":{\"name\":\"JSON_DOCUMENT\", \"sqlType\":\"BLOB\",\"compress\":\"LOW\",\"cache\":true,\"encrypt\":\"NONE\",\"validation\":\"STRICT\"}," +
      "\"creationTimeColumn\":{\"name\":\"CREATED_ON\"}," +
      "\"readOnly\":false}";
    OracleDocument mDoc1 = db.createDocumentFromString(mData1);
    OracleCollection col1 = dbAdmin.createCollection("testMetadataCheck1", mDoc1);  
    assertEquals("testMetadataCheck1", col1.admin().getName());
    descDoc = col1.admin().getMetadata();
    jStr = (JsonString) getValue(descDoc, path("schemaName"));
    assertEquals(schemaName.toUpperCase(), jStr.getString());
 
    String sName = schemaName.toUpperCase();
    // Test with missing contentColumn name (the default value will be used)
    String mData2 =
      "{\"schemaName\":\"" + sName + "\", \"keyColumn\":{\"name\":\"ID\",\"sqlType\":\"VARCHAR2\",\"maxLength\":255,\"assignmentMethod\":\"CLIENT\"}," +
      "\"contentColumn\":{\"sqlType\":\"BLOB\",\"compress\":\"LOW\",\"cache\":true,\"encrypt\":\"NONE\",\"validation\":\"STRICT\"}," +
      "\"creationTimeColumn\":{\"name\":\"CREATED_ON\"}," +
      "\"readOnly\":false}";
    OracleDocument mDoc2 = db.createDocumentFromString(mData2);
    OracleCollection col2 = dbAdmin.createCollection("testMetadataCheck2", mDoc2);
    assertEquals("testMetadataCheck2", col2.admin().getName());
    descDoc = col2.admin().getMetadata();
    jStr = (JsonString) getValue(descDoc, path("contentColumn", "name"));
    assertEquals("JSON_DOCUMENT", jStr.getString());

    // missing keyColumnName
    String mData3 =
      "{\"keyColumn\":{\"sqlType\":\"VARCHAR2\",\"maxLength\":255,\"assignmentMethod\":\"CLIENT\"}," +
      "\"contentColumn\":{\"name\":\"JSON_DOCUMENT\", \"sqlType\":\"BLOB\",\"compress\":\"LOW\",\"cache\":true,\"encrypt\":\"NONE\",\"validation\":\"STRICT\"}," +
      "\"creationTimeColumn\":{\"name\":\"CREATED_ON\"}," +
      "\"readOnly\":false}";
    OracleDocument mDoc3 = db.createDocumentFromString(mData3);
    OracleCollection col3 = dbAdmin.createCollection("testMetadataCheck3", mDoc3);
    assertEquals("testMetadataCheck3", col3.admin().getName());
    descDoc = col3.admin().getMetadata();
    jStr = (JsonString) getValue(descDoc, path("keyColumn", "name"));
    assertEquals("ID", jStr.getString());

    // Test with missing lastModifiedColumnName, and specifying lastModifiedColumn index
    String msg = "The last-modified column index can only be set when the last-modified column name is set.";
    try {
      OracleDocument mDoc4 = client.createMetadataBuilder()
        .removeOptionalColumns()
        .lastModifiedColumnIndex("Seq_On_LastMod").build();
      OracleCollection col4 = dbAdmin.createCollection("testMetadataCheck4", mDoc4);
      fail("Expected exception");
    } catch (OracleException e) {
      assertEquals(msg, e.getMessage());
    }

    // Test with missing versionColumnName, and but specifying versionColumn method
    msg = "A version method was specified but the version column name is unspecified.";
    try {
      OracleDocument mDoc5 = client.createMetadataBuilder()
          .removeOptionalColumns()
          .versionColumnMethod("SEQUENTIAL").build();
      OracleCollection col5 = dbAdmin.createCollection("testMetadataCheck5", mDoc5);
      fail("Expected exception");
    } catch (OracleException e) {
      assertEquals(msg, e.getMessage());
    }

    // Test with invalid contentColumnValidation     
    try {
      OracleDocument mDoc6 = client.createMetadataBuilder()
          .contentColumnValidation("unknownValidationOpt").build();
      OracleCollection col6 = dbAdmin.createCollection("testMetadataCheck6", mDoc6);
      fail("Expected exception");
    } catch (OracleException e) {
      assertEquals("Invalid argument value \"unknownValidationOpt\".", e.getMessage());
    }

    // Test with invalid keyColumnAssignmentMethod     
    try {
      OracleDocument mDoc7 = client.createMetadataBuilder()
          .keyColumnAssignmentMethod("unknownKeyMethodOpt").build();
      OracleCollection col7 = dbAdmin.createCollection("testMetadataCheck7", mDoc7);
      fail("Expected exception");
    } catch (OracleException e) {
      assertEquals("Invalid argument value \"unknownKeyMethodOpt\".", e.getMessage());
    }

    // Test with invalid versionColumnMethod
    try {
      OracleDocument mDoc8 = client.createMetadataBuilder()
          .versionColumnMethod("unknownVersionMethodOpt").versionColumnName("Version")
          .build();
      OracleCollection col8 = dbAdmin.createCollection("testMetadataCheck8", mDoc8);
      fail("Expected exception");
    } catch (OracleException e) {
      assertEquals("Invalid argument value \"unknownVersionMethodOpt\".", e.getMessage());
    }

    // The following are about invalid value from text doc, instead of MetadataBuilder.
    // Test with invalid keyColumn sqlType
    String mData9 =
      "{\"schemaName\":\"" + sName + "\", \"keyColumn\":{\"name\":\"ID\",\"sqlType\":\"UnknownSqlType\",\"maxLength\":255,\"assignmentMethod\":\"UUID\"}," +
      "\"contentColumn\":{\"name\":\"JSON_DOCUMENT\",\"sqlType\":\"BLOB\",\"compress\":\"LOW\",\"cache\":true,\"encrypt\":\"NONE\",\"validation\":\"STRICT\"}," +
      "\"versionColumn\":{\"name\":\"VERSION\",\"type\":\"String\",\"method\":\"SHA256\"}," +
      "\"readOnly\":false}";
    OracleDocument mDoc9 = db.createDocumentFromString(mData9);
    try {
      OracleCollection col9 = dbAdmin.createCollection("testMetadataCheck9", mDoc9);
      fail("No exception when invalid keyColumn sqlType is presented");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Invalid argument value \"UnknownSqlType\".", e.getMessage());
    }

    // Test with invalid keyColumn maxLength
    String mData10 =
      "{\"schemaName\":\"" + sName + "\", \"keyColumn\":{\"name\":\"ID\",\"sqlType\":\"VARCHAR2\",\"maxLength\":true,\"assignmentMethod\":\"UUID\"}," +
      "\"contentColumn\":{\"name\":\"JSON_DOCUMENT\",\"sqlType\":\"BLOB\",\"compress\":\"LOW\",\"cache\":true,\"encrypt\":\"NONE\",\"validation\":\"STRICT\"}," +
      "\"versionColumn\":{\"name\":\"VERSION\",\"type\":\"String\",\"method\":\"SHA256\"}," +
      "\"readOnly\":false}";
    OracleDocument mDoc10 = db.createDocumentFromString(mData10);
    try {
      OracleCollection col10 = dbAdmin.createCollection("testMetadataCheck10", mDoc10);
      fail("No exception when invalid keyColumn maxLength is presented");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Invalid value for \"maxLength\" in the collection metadata.", e.getMessage());
    }

    // Test with invalid keyColumn assignmentMethod
    String mData11 =
      "{\"schemaName\":\"" + sName + "\", \"keyColumn\":{\"name\":\"ID\",\"sqlType\":\"VARCHAR2\",\"maxLength\":255,\"assignmentMethod\":\"UnknownAssignmentMethod\"}," +
      "\"contentColumn\":{\"name\":\"JSON_DOCUMENT\",\"sqlType\":\"BLOB\",\"compress\":\"LOW\",\"cache\":true,\"encrypt\":\"NONE\",\"validation\":\"STRICT\"}," +
      "\"versionColumn\":{\"name\":\"VERSION\",\"type\":\"String\",\"method\":\"SHA256\"}," +
      "\"readOnly\":false}";
    OracleDocument mDoc11 = db.createDocumentFromString(mData11);
    try {
      OracleCollection col11 = dbAdmin.createCollection("testMetadataCheck11", mDoc11);
      fail("No exception when invalid keyColumn assignmentMethod is presented");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Invalid argument value \"UnknownAssignmentMethod\".", e.getMessage());
    }

    // Test with invalid contentColumn sqlType
    String mData12 =
      "{\"schemaName\":\"" + sName + "\", \"keyColumn\":{\"name\":\"ID\",\"sqlType\":\"VARCHAR2\",\"maxLength\":255,\"assignmentMethod\":\"UUID\"}," +
      "\"contentColumn\":{\"name\":\"JSON_DOCUMENT\",\"sqlType\":\"UnknownSqlType\",\"compress\":\"LOW\",\"cache\":true,\"encrypt\":\"NONE\",\"validation\":\"STRICT\"}," +
      "\"versionColumn\":{\"name\":\"VERSION\",\"type\":\"String\",\"method\":\"SHA256\"}," +
      "\"readOnly\":false}";
    OracleDocument mDoc12 = db.createDocumentFromString(mData12);
    try {
      OracleCollection col12 = dbAdmin.createCollection("testMetadataCheck12", mDoc12);
      fail("No exception when invalid contentColumn sqlType is presented");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Invalid argument value \"UnknownSqlType\".", e.getMessage());
    }

    // Test with invalid contentColumn compress
    String mData13 =
      "{\"schemaName\":\"" + sName + "\", \"keyColumn\":{\"name\":\"ID\",\"sqlType\":\"VARCHAR2\",\"maxLength\":255,\"assignmentMethod\":\"UUID\"}," +
      "\"contentColumn\":{\"name\":\"JSON_DOCUMENT\",\"sqlType\":\"BLOB\",\"compress\":\"UnknownCompression\",\"cache\":true,\"encrypt\":\"NONE\",\"validation\":\"STRICT\"}," +
      "\"versionColumn\":{\"name\":\"VERSION\",\"type\":\"String\",\"method\":\"SHA256\"}," +
      "\"readOnly\":false}";
    OracleDocument mDoc13 = db.createDocumentFromString(mData13);
    try {
      OracleCollection col13 = dbAdmin.createCollection("testMetadataCheck13", mDoc13);
      fail("No exception when invalid contentColumn compress is presented");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Invalid argument value \"UnknownCompression\".", e.getMessage());
    }
 
    // Test with invalid contentColumn cache
    String mData14 =
      "{\"schemaName\":\"" + sName + "\", \"keyColumn\":{\"name\":\"ID\",\"sqlType\":\"VARCHAR2\",\"maxLength\":255,\"assignmentMethod\":\"UUID\"}," +
      "\"contentColumn\":{\"name\":\"JSON_DOCUMENT\",\"sqlType\":\"BLOB\",\"compress\":\"LOW\",\"cache\":100,\"encrypt\":\"NONE\",\"validation\":\"STRICT\"}," +
      "\"versionColumn\":{\"name\":\"VERSION\",\"type\":\"String\",\"method\":\"SHA256\"}," +
      "\"readOnly\":false}";
    OracleDocument mDoc14 = db.createDocumentFromString(mData14);
    try {
      OracleCollection col14 = dbAdmin.createCollection("testMetadataCheck14", mDoc14);
      fail("No exception when invalid contentColumn cache is presented");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Invalid value for \"cache\" in the collection metadata.", e.getMessage());
    }

    // Test with invalid contentColumn encrypt
    String mData15 =
      "{\"schemaName\":\"" + sName + "\", \"keyColumn\":{\"name\":\"ID\",\"sqlType\":\"VARCHAR2\",\"maxLength\":255,\"assignmentMethod\":\"UUID\"}," +
      "\"contentColumn\":{\"name\":\"JSON_DOCUMENT\",\"sqlType\":\"BLOB\",\"compress\":\"LOW\",\"cache\":true,\"encrypt\":\"UnknownEncryption\",\"validation\":\"STRICT\"}," +
      "\"versionColumn\":{\"name\":\"VERSION\",\"type\":\"String\",\"method\":\"SHA256\"}," +
      "\"readOnly\":false}";
    OracleDocument mDoc15 = db.createDocumentFromString(mData15);
    try {
      OracleCollection col15 = dbAdmin.createCollection("testMetadataCheck15", mDoc15);
      fail("No exception when invalid contentColumn encrypt is presented");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Invalid argument value \"UnknownEncryption\".", e.getMessage());
    }

    // Test with invalid contentColumn validation
    String mData16 =
      "{\"schemaName\":\"" + sName + "\", \"keyColumn\":{\"name\":\"ID\",\"sqlType\":\"VARCHAR2\",\"maxLength\":255,\"assignmentMethod\":\"UUID\"}," +
      "\"contentColumn\":{\"name\":\"JSON_DOCUMENT\",\"sqlType\":\"BLOB\",\"compress\":\"LOW\",\"cache\":true,\"encrypt\":\"NONE\",\"validation\":\"UnknownValidation\"}," +
      "\"versionColumn\":{\"name\":\"VERSION\",\"type\":\"String\",\"method\":\"SHA256\"}," +
      "\"readOnly\":false}";
    OracleDocument mDoc16 = db.createDocumentFromString(mData16);
    try {
      OracleCollection col16 = dbAdmin.createCollection("testMetadataCheck16", mDoc16);
      fail("No exception when invalid contentColumn validation is presented");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Invalid argument value \"UnknownValidation\".", e.getMessage());
    }

    // Test with invalid versionColumn method
    String mData17 =
      "{\"schemaName\":\"" + sName + "\", \"keyColumn\":{\"name\":\"ID\",\"sqlType\":\"VARCHAR2\",\"maxLength\":255,\"assignmentMethod\":\"UUID\"}," +
      "\"contentColumn\":{\"name\":\"JSON_DOCUMENT\",\"sqlType\":\"BLOB\",\"compress\":\"LOW\",\"cache\":true,\"encrypt\":\"NONE\",\"validation\":\"STRICT\"}," +
      "\"versionColumn\":{\"name\":\"VERSION\",\"method\":\"UnknownVersionMethod\"}," +
      "\"readOnly\":false}";
    OracleDocument mDoc17 = db.createDocumentFromString(mData17);
    try {
      OracleCollection col17 = dbAdmin.createCollection("testMetadataCheck17", mDoc17);
      fail("No exception when invalid versionColumn method is presented");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Invalid argument value \"UnknownVersionMethod\".", e.getMessage());
    }

    // Test with invalid readOnly value
    String mData18 =
      "{\"schemaName\":\"" + sName + "\", \"keyColumn\":{\"name\":\"ID\",\"sqlType\":\"VARCHAR2\",\"maxLength\":255,\"assignmentMethod\":\"UUID\"}," +
      "\"contentColumn\":{\"name\":\"JSON_DOCUMENT\",\"sqlType\":\"BLOB\",\"compress\":\"LOW\",\"cache\":true,\"encrypt\":\"NONE\",\"validation\":\"STRICT\"}," +
      "\"versionColumn\":{\"name\":\"VERSION\",\"method\":\"SEQUENTIAL\"}," +
      "\"readOnly\":100}";
    OracleDocument mDoc18 = db.createDocumentFromString(mData18);
    try {
      OracleCollection col18 = dbAdmin.createCollection("testMetadataCheck18", mDoc18);
      fail("No exception when invalid readOnly value is presented");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Invalid value for \"readOnly\" in the collection metadata.", e.getMessage());
    }

  }
}
