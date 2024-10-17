/* Copyright (c) 2014, 2024, Oracle and/or its affiliates. */
/* All rights reserved.*/

/*
   DESCRIPTION
    OracleCollectionAdmin tests
 */

/**
 *  @author  Vincent Liu
 */
package oracle.json.tests.soda;

import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import jakarta.json.JsonNumber;
import jakarta.json.JsonString;
import jakarta.json.JsonValue;
import jakarta.json.JsonObject;
import jakarta.json.Json;
import jakarta.json.JsonReader;
import java.io.StringReader;

import oracle.json.parser.QueryException;

import oracle.json.util.Pair;
import oracle.soda.OracleCollectionAdmin;
import oracle.soda.OracleException;
import oracle.soda.OracleCollection;
import oracle.soda.OracleDocument;
import oracle.soda.OracleCursor;

import oracle.soda.rdbms.impl.OracleCollectionImpl;
import oracle.soda.rdbms.impl.OracleOperationBuilderImpl;
import oracle.soda.rdbms.impl.SODAUtils;

import oracle.json.testharness.SodaTestCase;
import oracle.json.testharness.ConnectionFactory;

public class test_OracleCollectionAdmin extends SodaTestCase {
  
  public void testGetName() throws Exception {
    OracleDocument metaDoc;
    if (isJDCSOrATPMode())
    {
      // ### replace with new builder once it becomes available
      metaDoc = db.createDocumentFromString("{\"keyColumn\":{\"name\":\"ID\",\"sqlType\":\"VARCHAR2\",\"maxLength\":255,\"assignmentMethod\":\"UUID\"},\"contentColumn\":{\"name\":\"JSON_DOCUMENT\",\"sqlType\":\"BLOB\"},\"lastModifiedColumn\":{\"name\":\"LAST_MODIFIED\"},\"versionColumn\":{\"name\":\"VERSION\",\"method\":\"UUID\"},\"creationTimeColumn\":{\"name\":\"CREATED_ON\"},\"readOnly\":false}");
    } else if (isCompatibleOrGreater(COMPATIBLE_20)) {
      metaDoc = null;
    }
    else
    {
      metaDoc = client.createMetadataBuilder().build();
    }

    String colName = "testGetName";
    OracleCollection col = dbAdmin.createCollection("testGetName", metaDoc);
    OracleCollectionAdmin colAdmin = col.admin();
    assertEquals(colName, colAdmin.getName());
  }

  public void testIsHeterogeneous() throws Exception {
    if (isJDCSOrATPMode())
    {
      OracleCollection col = dbAdmin.createCollection("testIsHeterogeneous", null);
      assertEquals(false, col.admin().isHeterogeneous());
      return;
    }
    
    // Test with mediaTypeColumn and content type = "BLOB" 
    OracleDocument metaDoc = client.createMetadataBuilder().mediaTypeColumnName("CONTENT_TYPE").contentColumnType("BLOB")
        .build();
    OracleCollection col = dbAdmin.createCollection("testIsHeterogeneous", metaDoc);
    assertEquals(true, col.admin().isHeterogeneous());
    
    String msg = "The collection may not have a media type column unless the content column type is BLOB.";
    try {
      // Test with mediaTypeColumn, but content type = "VARCHAR2" 
      client.createMetadataBuilder().mediaTypeColumnName("CONTENT_TYPE").contentColumnType("VARCHAR2")
        .build();
      fail("Expected exception");
    } catch (OracleException e) {
      assertEquals(msg, e.getMessage());
    }
    
    /* ### Oracle Database does not support NVARCHAR2 and RAW storage for JSON
    try {
      // Test with mediaTypeColumn, but content type = "NVARCHAR2" 
      client.createMetadataBuilder().mediaTypeColumnName("CONTENT_TYPE").contentColumnType("NVARCHAR2")
        .build();
      fail("Expected exception");
    } catch (OracleException e) {
      assertEquals(msg, e.getMessage());
    }
    
    try {
      // Test with mediaTypeColumn, but content type = "RAW" 
      client.createMetadataBuilder().mediaTypeColumnName("CONTENT_TYPE").contentColumnType("RAW")
        .build();
      fail("Expected exception");
    } catch (OracleException e) {
      assertEquals(msg, e.getMessage());
    }
    */
    
    try {
      // Test with mediaTypeColumn, but content type = "CLOB" 
      client.createMetadataBuilder().mediaTypeColumnName("CONTENT_TYPE").contentColumnType("CLOB")
        .build();
      fail("Expected exception");
    } catch (OracleException e) {
      assertEquals(msg, e.getMessage());
    }
    
    /*  ### Oracle Database does not support NCLOB storage for JSON
    try {
      // Test with mediaTypeColumn, but content type = "NCLOB" 
      client.createMetadataBuilder().mediaTypeColumnName("CONTENT_TYPE").contentColumnType("NCLOB")
        .build();
      fail("Expected exception");
    } catch (OracleException e) {
      assertEquals(msg, e.getMessage());
    }
    */

    OracleCollection col6 = dbAdmin.createCollection("testIsHeterogeneous6");
    assertEquals(false, col6.admin().isHeterogeneous());
    try {
      // try to insert non-JSON document into non-heterogeneous collection
      col6.insert(db.createDocumentFromString(null, "abcd efgh ", "text/plain"));
      fail("No exception when inserting non-JSON document into non-heterogeneous collection");
    } catch (OracleException e) {
      // Expect an OracleException
      Throwable t = e.getCause();
      String res = t.getMessage();
      if (isCompatibleOrGreater(COMPATIBLE_20)) {
        assertTrue(res, res.contains("Unexpected char 97 at (line no=1, column no=1, offset=0)"));     

      //} else if (!isCompatibleOrGreater(COMPATIBLE_20)) {
      //  assertTrue(res, res.contains("Unexpected character 'a' at line 1, column 1")); 
      
      //  This is the behavior on 23c (compatible set below 20)
      //  Or 19c (DBRU)
      } else {
        // ORA-02290: check constraint (SYS_C0010955) violated
        assertTrue(t.getMessage().contains("ORA-02290"));
      }
    }  
    
    // Test with no mediaTypeColumn, and content type = "BLOB" 
    OracleDocument metaDoc7 = client.createMetadataBuilder().removeOptionalColumns().contentColumnType("BLOB")
        .build();
    OracleCollection col7 = dbAdmin.createCollection("testIsHeterogeneous7", metaDoc7);
    assertEquals(false, col7.admin().isHeterogeneous());
    
    try {
      // try to insert non-JSON document into non-heterogeneous collection
      col7.insert(db.createDocumentFromString(null, "Hello World", null));
      fail("No exception when inserting non-JSON document into non-heterogeneous collection");
    } catch (OracleException e) {
      // Expect an OracleException
      // ORA-02290: check constraint (SYS_C0010955) violated
      Throwable t = e.getCause();
      assertTrue(t.getMessage().contains("ORA-02290"));
    }
    
    // Test with no mediaTypeColumn, and content type = "BLOB"
    OracleDocument metaDoc8 = client.createMetadataBuilder().removeOptionalColumns().contentColumnType("BLOB")
        .build();
    OracleCollection col8 = dbAdmin.createCollection("testIsHeterogeneous8", metaDoc8);
    assertEquals(false, col8.admin().isHeterogeneous());
    
    try {
      // try to insert non-JSON document into non-heterogeneous collection
      col8.insert(db.createDocumentFromString(null, "Hello World", null));
      fail("No exception when inserting non-JSON document into non-heterogeneous collection");
    } catch (OracleException e) {
      // Expect an OracleException
      // ORA-02290: check constraint (SYS_C0010955) violated
      Throwable t = e.getCause();
      assertTrue(t.getMessage().contains("ORA-02290"));
    }
    
  }
  
  public void testIsReadOnly() throws Exception {
    // Test with READONLY = false
    OracleDocument metaDoc;
    if (isJDCSOrATPMode() || isCompatibleOrGreater(COMPATIBLE_20))
    {
      metaDoc = null;
    } else
    {
      metaDoc = client.createMetadataBuilder().removeOptionalColumns().readOnly(false).build();
    }

    OracleCollection col = dbAdmin.createCollection("testIsReadOnly", metaDoc);
    assertEquals(false, col.admin().isReadOnly());
    col.insert(db.createDocumentFromString(null, "{ \"data\" : 1001 }", null));
    assertEquals(1, col.find().count());
    
    if (isJDCSOrATPMode())
        return;

    // Test with READONLY = true
    OracleDocument metaDoc2 = client.createMetadataBuilder().removeOptionalColumns().readOnly(true).build();
    if (isCompatibleOrGreater(COMPATIBLE_20)) {
      metaDoc2 = client.createMetadataBuilder().contentColumnType("JSON")
          .versionColumnMethod("UUID").readOnly(true).build();
    }

    OracleCollection col2 = dbAdmin.createCollection("testIsReadOnly2", metaDoc2);
    assertEquals(true, col2.admin().isReadOnly());
    
    try {
      // try to insert document into readOnly collection
      col2.insert(db.createDocumentFromString(null, "{ \"data\" : 1002 }", null));
      fail("No exception when inserting document into readOnly collection");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Collection testIsReadOnly2 is read-only, insert not allowed.", e.getMessage());
    }
  }
  
  final static String colSpecExample =
  "{ " +
    "\"schemaName\":\"" + ConnectionFactory.USER_NAME.toUpperCase() + "\"," +
    "\"tableName\" : \"EMPLOYEES\"," +
    "\"contentColumn\":" +
    "  { \"name\":\"EMP_DOC\", \"sqlType\":\"BLOB\", \"validation\":\"STRICT\"," +
    "    \"compress\":\"HIGH\", \"cache\":true, \"encrypt\":\"AES128\"}," +
    "\"keyColumn\":" +
    "  {\"name\":\"EMP_ID\", \"sqlType\":\"VARCHAR2\", \"maxLength\": 40," +
    "  \"sequenceName\":\"EMPLOYEE_ID_SEQ\", \"assignmentMethod\":\"SEQUENCE\" }, " +
    "\"creationTimeColumn\":{ \"name\": \"CREATED_ON\" }, " +
    "\"lastModifiedColumn\":{ \"name\":\"LAST_UPDATED\", \"index\":\"empLastModIndexName\"}, " +
    "\"versionColumn\":{ \"name\":\"VERSION_NUM\", \"method\":\"SEQUENTIAL\"}, " +
    "\"mediaTypeColumn\": { \"name\":\"CONTENT_TYPE\" }, " +
    "\"readOnly\":true " +
  "}";
  
  public void testGetDescription() throws Exception {
    if (isJDCSOrATPMode())
        return;
    OracleDocument metaDoc = db.createDocumentFromString(colSpecExample);
    
    OracleCollection col = dbAdmin.createCollection("testGetDescription", metaDoc);
    OracleCollectionAdmin colAdmin = col.admin();
    OracleDocument descDoc = colAdmin.getMetadata();
    assertNotNull(descDoc);
    
    JsonString jStr = null;
    JsonValue jVal = null;
    JsonNumber jNum = null;
    
    jStr = (JsonString) getValue(descDoc, path("schemaName"));
    assertEquals(schemaName.toUpperCase(), jStr.getString());
    jStr = (JsonString) getValue(descDoc, path("tableName"));
    assertEquals("EMPLOYEES", jStr.getString());
    
    jStr = (JsonString) getValue(descDoc, path("contentColumn", "name"));
    assertEquals("EMP_DOC", jStr.getString());
    jStr = (JsonString) getValue(descDoc, path("contentColumn", "sqlType"));
    assertEquals("BLOB", jStr.getString());
    jStr = (JsonString) getValue(descDoc, path("contentColumn", "validation"));
    assertEquals("STRICT", jStr.getString());
    jStr = (JsonString) getValue(descDoc, path("contentColumn", "compress"));
    assertEquals("HIGH", jStr.getString());
    jVal = getValue(descDoc, path("contentColumn", "cache"));
    assertEquals(JsonValue.TRUE, jVal);
    jStr = (JsonString) getValue(descDoc, path("contentColumn", "encrypt"));
    assertEquals("AES128", jStr.getString());
   
    jStr = (JsonString) getValue(descDoc, path("keyColumn", "name"));
    assertEquals("EMP_ID", jStr.getString());
    jStr = (JsonString) getValue(descDoc, path("keyColumn", "sqlType"));
    assertEquals("VARCHAR2", jStr.getString());
    jNum = (JsonNumber) getValue(descDoc, path("keyColumn", "maxLength"));
    assertEquals(40, jNum.intValue());
    jStr = (JsonString) getValue(descDoc, path("keyColumn", "assignmentMethod"));
    assertEquals("SEQUENCE", jStr.getString());
    jStr = (JsonString) getValue(descDoc, path("keyColumn", "sequenceName"));
    assertEquals("EMPLOYEE_ID_SEQ", jStr.getString());
    
    jStr = (JsonString) getValue(descDoc, path("creationTimeColumn", "name"));
    assertEquals("CREATED_ON", jStr.getString());
    jStr = (JsonString) getValue(descDoc, path("lastModifiedColumn", "name"));
    assertEquals("LAST_UPDATED", jStr.getString());
    jStr = (JsonString) getValue(descDoc, path("lastModifiedColumn", "index"));
    assertEquals("empLastModIndexName", jStr.getString());
    
    jStr = (JsonString) getValue(descDoc, path("versionColumn", "name"));
    assertEquals("VERSION_NUM", jStr.getString());
    jStr = (JsonString) getValue(descDoc, path("versionColumn", "method"));
    assertEquals("SEQUENTIAL", jStr.getString());
    jStr = (JsonString) getValue(descDoc, path("mediaTypeColumn", "name"));
    assertEquals("CONTENT_TYPE", jStr.getString());
    
    jVal = getValue(descDoc, path("readOnly"));
    assertEquals(JsonValue.TRUE, jVal);
    
    // Test with contentColumnEncrypt, keyColumnAssignmentMethod, keyColumnSequenceName, and lastModifiedColumnIndex calls
    OracleDocument metaDoc2 = client.createMetadataBuilder().mediaTypeColumnName("CONTENT_TYPE")
        .contentColumnType("BLOB")
        .contentColumnEncrypt("AES128")
        .keyColumnAssignmentMethod("SEQUENCE").keyColumnSequenceName("keyseql").readOnly(true)
        .lastModifiedColumnIndex("lastModIndexName").build();
    OracleCollection col2 = dbAdmin.createCollection("testGetDescription2", metaDoc2);
    OracleCollectionAdmin colAdmin2 = col2.admin();
    OracleDocument descDoc2 = colAdmin2.getMetadata();
    
    jStr = (JsonString) getValue(descDoc2, path("contentColumn", "encrypt"));
    assertEquals("AES128", jStr.getString());
    
    jStr = (JsonString) getValue(descDoc2, path("keyColumn", "assignmentMethod"));
    assertEquals("SEQUENCE", jStr.getString());
    jStr = (JsonString) getValue(descDoc2, path("keyColumn", "sequenceName"));
    assertEquals("keyseql", jStr.getString());
    
    jStr = (JsonString) getValue(descDoc2, path("lastModifiedColumn", "index"));
    assertEquals("lastModIndexName", jStr.getString());
    
  }

  public void testDrop() throws Exception {
    OracleDocument metaDoc = null;

    if (isJDCSOrATPMode())
    {
      // ### replace with new builder once it becomes available
      metaDoc = db.createDocumentFromString("{\"keyColumn\":{\"name\":\"ID\",\"sqlType\":\"VARCHAR2\",\"maxLength\":255,\"assignmentMethod\":\"UUID\"},\"contentColumn\":{\"name\":\"JSON_DOCUMENT\",\"sqlType\":\"BLOB\"},\"lastModifiedColumn\":{\"name\":\"LAST_MODIFIED\"},\"versionColumn\":{\"name\":\"VERSION\",\"method\":\"UUID\"},\"creationTimeColumn\":{\"name\":\"CREATED_ON\"},\"readOnly\":false}");
    } else if (isJDCSOrATPMode()) {
      metaDoc = null;
    } else
    {
      metaDoc = client.createMetadataBuilder().build();
    }

    // Test to drop an empty collection 
    OracleCollection col = dbAdmin.createCollection("testDrop", metaDoc);
    OracleCollectionAdmin colAdmin = col.admin();
    colAdmin.drop();
    
    // Test to drop again
    // blocked by bug26647072: the second drop() call caused "ORA-40671:Collection not found."
    // only when install.plsql is disabled(in RDBMS lrgs), the issue can be hit
    // colAdmin.drop();
    
    // Test to drop an non-empty collection
    OracleCollection col2 = dbAdmin.createCollection("testDrop2", metaDoc);
    
    OracleCollectionAdmin colAdmin2 = col2.admin();
    for (int i = 1; i <= 100; i++) {
        col2.insert(db.createDocumentFromString("{ \"value\" : \"v" + i + "\" }"));
    }
    colAdmin2.drop();
    
    // Test to drop the collection mapping to the existing table
    if (isJDCSOrATPMode() || isCompatibleOrGreater(COMPATIBLE_20))
        return;

    OracleDocument metaDoc3 = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("CLIENT")
        .versionColumnMethod("NONE").tableName("SODATBL").build();
    OracleCollection col3 = dbAdmin.createCollection("testDrop3", metaDoc3);
    OracleCollectionAdmin colAdmin3 = col3.admin();
    colAdmin3.drop();
    
  }

  public void testTruncate() throws Exception {
    OracleDocument metaDoc = null;

    if (isJDCSOrATPMode())
    {
      // ### replace with new builder once it becomes available
      metaDoc = db.createDocumentFromString("{\"keyColumn\":{\"name\":\"ID\",\"sqlType\":\"VARCHAR2\",\"maxLength\":255,\"assignmentMethod\":\"UUID\"},\"contentColumn\":{\"name\":\"JSON_DOCUMENT\",\"sqlType\":\"BLOB\"},\"lastModifiedColumn\":{\"name\":\"LAST_MODIFIED\"},\"versionColumn\":{\"name\":\"VERSION\",\"method\":\"UUID\"},\"creationTimeColumn\":{\"name\":\"CREATED_ON\"},\"readOnly\":false}");
    } else if (isCompatibleOrGreater(COMPATIBLE_20)) {
      metaDoc = null;
    } else
    {
      metaDoc = client.createMetadataBuilder().build();
    }

    OracleCollection col = dbAdmin.createCollection("testTruncate", metaDoc); 
    OracleCollectionAdmin colAdmin = col.admin();
    
    for (int i = 1; i <= 10; i++) {
      col.insertAndGet(db.createDocumentFromString("{ \"data\" : \"v" + i + "\" }"));
    }
    assertEquals(10, col.find().count());
    
    // Test it when there are 1+ documents
    colAdmin.truncate();
    assertEquals(0, col.find().count());
    
    // Test it when there is 0 documents
    colAdmin.truncate();
    assertEquals(0, col.find().count());
    
    // Test it with existing table
    if (isJDCSOrATPMode() || isCompatibleOrGreater(COMPATIBLE_20))
        return;
    OracleDocument metaDoc2 = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("CLIENT")
        .versionColumnMethod("NONE").tableName("SODATBL").build();
    OracleCollection col2 = dbAdmin.createCollection("testTruncate2", metaDoc2);
    OracleCollectionAdmin colAdmin2 = col2.admin();
    
    col2.insert(db.createDocumentFromString("id-1", "{ \"data\" : \"vvv\" }"));
    assertEquals(1, col2.find().count());
    colAdmin2.truncate();
    assertEquals(0, col2.find().count());

    if (!isJDCSOrATPMode()) {
      // the creation of "SODA_VIEW"(see sodatestsetup.sql) is blocked by jdcs lockdown.
 
      // Test it with existing view
      OracleDocument metaDoc3 = client.createMetadataBuilder()
          .keyColumnAssignmentMethod("CLIENT")
          .viewName("SODA_VIEW").build();
      OracleCollection col3 = dbAdmin.createCollection("testTruncate3", metaDoc3);
      OracleCollectionAdmin colAdmin3 = col3.admin();
    
      col3.insert(db.createDocumentFromString("id-1", "{ \"data\" : \"www\" }"));
      assertEquals(1, col3.find().count());

      try {
        // Test it on a view based collection
        colAdmin3.truncate();
        fail("No exception when call truncate on a view based collection");
      } catch (OracleException e) {
        // Expect an OracleException
        assertEquals("Truncation not supported, collection testTruncate3 is not table based.", e.getMessage());
      }
    } 

    OracleDocument metaDoc4 = client.createMetadataBuilder()
        .readOnly(true).build();
    OracleCollection col4 = dbAdmin.createCollection("testTruncate4", metaDoc4);
    OracleCollectionAdmin colAdmin4 = col4.admin();
    
    try { 
      // Test it on an read-only collection
      colAdmin4.truncate();
      fail("No exception when call truncate on a readOnly collection");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Collection testTruncate4 is read-only, truncate not allowed.", e.getMessage());
    }

    try { 
      // Test it when the collection has been dropped
      colAdmin.drop();
      colAdmin.truncate();
      fail("No exception when truncate a dropped collection");
    } catch (OracleException e) {
      // Expect an OracleException
      Throwable t = e.getCause();
      assertTrue(t.getMessage().contains("ORA-00942: table or view does not exist\n"));
    }

  }

  public void testDropIndex() throws Exception {
    OracleDocument mDoc = null;
    if (isJDCSOrATPMode())
    {
      // ### replace with new builder once it becomes available
      mDoc = db.createDocumentFromString("{\"keyColumn\":{\"name\":\"ID\",\"sqlType\":\"VARCHAR2\",\"maxLength\":255,\"assignmentMethod\":\"UUID\"},\"contentColumn\":{\"name\":\"JSON_DOCUMENT\",\"sqlType\":\"BLOB\"},\"lastModifiedColumn\":{\"name\":\"LAST_MODIFIED\"},\"versionColumn\":{\"name\":\"VERSION\",\"method\":\"UUID\"},\"creationTimeColumn\":{\"name\":\"CREATED_ON\"},\"readOnly\":false}");
    } else if (isCompatibleOrGreater(COMPATIBLE_20)) {
      mDoc = null;
    }
    else
    {
      mDoc = client.createMetadataBuilder().build();
    }
    OracleCollection col = dbAdmin.createCollection("testDropIndex", mDoc);
    OracleCollectionAdmin colAdmin = col.admin();

    col.insert(db.createDocumentFromString("{ \"name\": \"Andy Murray\" }"));
    col.insert(db.createDocumentFromString("{ \"name\": \"Rafael Nadal\" }"));
    col.insert(db.createDocumentFromString("{ \"name\": \"Roger Federer\" }"));

    String indexSpec =
      "{ \"name\":\"NAME_INDEX\", \"unique\":true, \n";

    if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel))
      indexSpec += "\"scalarRequired\" : true,\n";

    indexSpec += "   \"fields\": [ { \"path\":\"name\"," +
                     "\"datatype\":\"string\", \"order\":\"asc\" } ]\n}";


    colAdmin.createIndex(db.createDocumentFromString(indexSpec));
    col.insert(db.createDocumentFromString("{ \"name\": \"Novak Djokovic\" }"));
    assertEquals(4, col.find().count());

    // see whether unique index make effective
    OracleDocument doc = db.createDocumentFromString("{ \"name\": \"Andy Murray\" }");
    try {
      col.insert(doc);
    }
    catch(OracleException e) {
      Throwable t = e.getCause(); 
      // ORA-00001: unique constraint (NAME_INDEX) violated
      assertTrue(t.getMessage().contains("ORA-00001"));
    }

    // Test to drop an existing index
    colAdmin.dropIndex("NAME_INDEX");

    // then the document having the duplicated name can be inserted
    col.insert(doc);

    // Test to drop again
    // There will not be error when index does not exist
    colAdmin.dropIndex("NAME_INDEX");

    try {
      // Test null for indexName
      colAdmin.dropIndex(null);
      fail("No error when indexName is null");
    }
    catch(OracleException e) {
      assertEquals("indexName argument cannot be null.", e.getMessage());
    }

    String fullIndexName = "jsonSearchIndex-1";
    String jsonSearchIndexSpec = createTextIndexSpec(fullIndexName);
    OracleDocument d = db.createDocumentFromString(jsonSearchIndexSpec);
    colAdmin.createIndex(d);

    // Test to drop json search text index
    colAdmin.dropIndex(fullIndexName);
  }

  private void testJsonSearchIndexWithCol(OracleCollection col) throws Exception {
    OracleCollectionAdmin colAdmin = col.admin();

    if (!supportHeterogeneousQBEs() && colAdmin.isHeterogeneous())
      return;

    col.insert(db.createDocumentFromString("{ \"data\" : \"v1\" }"));
    if (!isCompatibleOrGreater(COMPATIBLE_23)) {
      col.insert(db.createDocumentFromString(null));
    }
    if (colAdmin.isHeterogeneous()) {
      col.insert(db.createDocumentFromString(null, "abcd", "text/plain"));
      col.insert(db.createDocumentFromString(null, null, "text/plain"));
    }
    
    String indexName1 = "index1";
    
    // Create json search text index for the first time
    colAdmin.createJsonSearchIndex(indexName1);

    try { 
      // Create json search text index with a different name
      // on the same collection (this is not allowed)
      colAdmin.createJsonSearchIndex("indexName2");
      fail("No exception when creating full-text index for second time");
    } catch (OracleException e) {
      // Expect an OracleException
      //ORA-29879: cannot create multiple domain indexes on a column list using same indextype
      Throwable t = e.getCause();
      assertTrue(t.getMessage().contains("ORA-29879"));
    }
    
    try { 
      // Test with already used index name
      colAdmin.createJsonSearchIndex(indexName1);
      // we lack any ability to check if the index is the same or not
      // so for now it's just ignored if the name matches
      // fail("No exception when the specified index name has been used");
    } catch (OracleException e) {
      // Expect an OracleException
      //assertEquals("", e.getMessage());
    }
    
    colAdmin.dropIndex(indexName1);

    try {
      // Test with null index name
      colAdmin.createJsonSearchIndex(null);
      fail("No exception when the index name is null");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("indexName argument cannot be null.", e.getMessage());
    }

    String jsonSearchIndexSpec;
    OracleDocument d;

    try {
      // Test with null index name, and language field set.
      // ### Note: language is not officially supported, do not use in production!!!
      if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel)) {
        jsonSearchIndexSpec = "{\"textindex121\" : true, \"language\" : \"english\"}";
      } else {
        jsonSearchIndexSpec = "{\"language\" : \"english\"," +
                               "\"dataguide\" :\"on\"}";
      }
      d = db.createDocumentFromString(jsonSearchIndexSpec);
      colAdmin.createIndex(d);
      fail("No exception when the index name is null");
    } catch (OracleException e) {
      Throwable t = e.getCause();
      assertEquals("Missing name property.", t.getMessage());
    }

    try {
      // Test with invalid language field
      // ### Note: language is not officially supported, do not use in production!!!
      jsonSearchIndexSpec = createTextIndexSpec("index4", "invalidLanguage", true);
      d = db.createDocumentFromString(jsonSearchIndexSpec);
      colAdmin.createIndex(d);

      fail("No exception when language is invalid");

    } catch (OracleException e) {
      // Expect an OracleException
      Throwable t = e.getCause();
      assertEquals("Language invalidLanguage is not recognized.", t.getMessage());
    }

    colAdmin.dropIndex("index4");

    // Create a json search text index with
    // language field explicitly set
    // ### Note: language is not officially supported, do not use in production!!!
    jsonSearchIndexSpec = createTextIndexSpec("index5", "english");

    d = db.createDocumentFromString(jsonSearchIndexSpec);
    colAdmin.createIndex(d);

    try {
      // Try to create a different second json search text index with
      // indexName, language fields in the spec
      // ### Note: language is not officially supported, do not use in production!!!
      jsonSearchIndexSpec = createTextIndexSpec("index6", "english");

      d = db.createDocumentFromString(jsonSearchIndexSpec);
      colAdmin.createIndex(d);
      fail("No exception when creating full-text index for second time");
    } catch (OracleException e) {
      // Expect an OracleException
      //ORA-29879: cannot create multiple domain indexes on a column list using same indextype
      Throwable t = e.getCause();
      assertTrue(t.getMessage().contains("ORA-29879"));
    }

    colAdmin.dropIndex("index5");
    colAdmin.dropIndex("index6");

    // Test creating json search text index on an empty collection
    col.find().remove();
    assertEquals(0, col.find().count());

    // ### Note: language is not officially supported, do not use in production!!!
    jsonSearchIndexSpec = createTextIndexSpec("index7", "english");
    d = db.createDocumentFromString(jsonSearchIndexSpec);
    colAdmin.createIndex(d);

    colAdmin.dropIndex("index7");
    
    // Test with empty key in key:value pair.(e.g. "":"T1IDX")
    try {
      if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel)) {
        jsonSearchIndexSpec = "{\"\" : \"T1IDX\", \"textindex121\":true }";
      } else {
        jsonSearchIndexSpec = "{\"\" : \"T1IDX\"}";
      }
      d = db.createDocumentFromString(jsonSearchIndexSpec);
      colAdmin.createIndex(d);
      fail("No exception when the index name is not specified.");
    } catch (OracleException e) {
      Throwable t = e.getCause();
      assertEquals("Missing name property.", t.getMessage());
    }
    
    if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel)) {
      jsonSearchIndexSpec = "{\"\" : \"T1IDX\",  \"name\":\"index8\", \"textindex121\":true }";
    } else {
      jsonSearchIndexSpec = "{\"\" : \"T1IDX\", \"name\":\"index8\" }";
    }
    d = db.createDocumentFromString(jsonSearchIndexSpec);
    colAdmin.createIndex(d);
    colAdmin.dropIndex("index8");
  }
  
  public void testJsonSearchIndex() throws Exception {
    OracleCollection col = dbAdmin.createCollection("testIndexAll1");
    testJsonSearchIndexWithCol(col);

    if (isJDCSOrATPMode())
        return;
    
    // Test with contentColumnType=CLOB
    OracleDocument mDoc2 = client.createMetadataBuilder()
        .contentColumnType("CLOB").build();
    OracleCollection col2 = dbAdmin.createCollection("testIndexAll2", mDoc2);
    testJsonSearchIndexWithCol(col2);
    
    // Test with contentColumnType=VARCHAR2
    OracleDocument mDoc3 = client.createMetadataBuilder()
        .contentColumnType("VARCHAR2").build();
    OracleCollection col3 = dbAdmin.createCollection("testIndexAll3", mDoc3);
    testJsonSearchIndexWithCol(col3);
    
    /* ### Oracle Database does not support NVARCHAR2, NCLOB, or RAW storage for JSON
    // Test with contentColumnType=RAW
    OracleDocument mDoc5 = client.createMetadataBuilder()
        .contentColumnType("RAW").build();
    OracleCollection col5 = dbAdmin.createCollection("testIndexAll5", mDoc5);
    testJsonSearchIndexWithCol(col5);
    
    // Json search text index is NOT enabled for contentColumnType=NVARCHAR2 or NCLOB
    OracleDocument mDoc4 = client.createMetadataBuilder()
        .contentColumnType("NVARCHAR2").build();
    OracleCollection col4 = dbAdmin.createCollection("testIndexAll4", mDoc4);
    //testJsonSearchIndexWithCol(col4);
    try {
      String fullIndexName = "jsonSearchIndex";
      String jsonSearchIndexSpec = null;
      jsonSearchIndexSpec = createTextIndexSpec(fullIndexName);
      OracleDocument d = db.createDocumentFromString(jsonSearchIndexSpec);
      col4.admin().createIndex(d);

      fail("No exception when creating JSON search index on content columnType NVARCHAR2 collection");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("JSON search index is not implemented for content columns with type NVARCHAR2.", e.getMessage());
    }
 
    OracleDocument mDoc6 = client.createMetadataBuilder()
        .contentColumnType("NCLOB").build();
    OracleCollection col6 = dbAdmin.createCollection("testIndexAll6", mDoc6);
    //testJsonSearchIndexWithCol(col6);
    try {

      String fullIndexName = "jsonSearchIndex";
      String jsonSearchIndexSpec = null;
      jsonSearchIndexSpec = createTextIndexSpec(fullIndexName);
      OracleDocument d = db.createDocumentFromString(jsonSearchIndexSpec);
      col6.admin().createIndex(d);
      fail("No exception when creating JSON search index on content columnType NCLOB collection");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("JSON search index is not implemented for content columns with type NCLOB.", e.getMessage());
    }
    */
  }

  public void testMultiValueIndex() throws Exception {

    if (!isCompatibleOrGreater(COMPATIBLE_23))
      return;

    OracleDocument mDoc = client.createMetadataBuilder().keyColumnAssignmentMethod("EMBEDDED_OID").keyColumnType("RAW").contentColumnType("JSON").build();

    OracleCollection col = dbAdmin.createCollection("testMultiValueIndex", mDoc);
    OracleCollectionAdmin colAdmin = col.admin();

    String str1 = null;
    String str2 = null;

    for (int i=0; i<5000; i++)
    {
      str1 = String.format("{ \"empid\" : \"ved%s\"}", String.valueOf(i));
      str2 = String.format("{ \"empid\" : %s}", i);
      OracleDocument ret1 = col.insertAndGet(db.createDocumentFromString(str1));
      OracleDocument ret2 = col.insertAndGet(db.createDocumentFromString(str2));
    }

    col.insertAndGet(db.createDocumentFromString("{\"name\" : 1}"));

    String indexName1 = "my_index";
    String indexSpec1 =
            "{ \"name\":\"" + indexName1 + "\", \n" +
                    "  \"multivalue\": true , \n" +
                    "  \"fields\": [\n" +
                    "    { \"path\":\"empid\"} \n" +
                    "] }";

    try
    {
      col.admin().createIndex(db.createDocumentFromString(indexSpec1));
    }
    catch (OracleException e)
    {
      fail ("No error should have occured");
    }

    String planNum = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{ \"empid\" : 500}"))).explainPlan("all");

    if (!planNum.matches("(?s).*INDEX RANGE SCAN.*") || !planNum.matches("(?s).*MULTI VALUE.*"))
    {
      fail ("Multivalue Index range scan is not found.");
    }

    String planStr = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{ \"empid\" : \"ved500\"}"))).explainPlan("all");

    if (!planStr.matches("(?s).*INDEX RANGE SCAN.*") || !planStr.matches("(?s).*MULTI VALUE.*"))
    {
      fail ("Multivalue Index range scan is not found.");
    }

    String plan1Str = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$gt\":\"ved4900\"}}"))).explainPlan("all");

    if (!plan1Str.matches("(?s).*INDEX RANGE SCAN.*") || !plan1Str.matches("(?s).*MULTI VALUE.*"))
    {
      fail ("Multivalue Index range scan is not found.");
    }

    String plan1Num = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$gt\":4900}}"))).explainPlan("all");

    if (!plan1Num.matches("(?s).*INDEX RANGE SCAN.*") || !plan1Num.matches("(?s).*MULTI VALUE.*"))
    {
      fail ("Multivalue Index range scan is not found.");
    }

    String plan2Num = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$gte\":4900}}"))).explainPlan("all");

    if (!plan2Num.matches("(?s).*INDEX RANGE SCAN.*") || !plan2Num.matches("(?s).*MULTI VALUE.*"))
    {
      fail ("Multivalue Index range scan is not found.");
    }

    String plan2Str = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$gte\":\"ved4900\"}}"))).explainPlan("all");

    if (!plan2Str.matches("(?s).*INDEX RANGE SCAN.*") || !plan2Str.matches("(?s).*MULTI VALUE.*"))
    {
      fail ("Multivalue Index range scan is not found.");
    }

    String plan3Str = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$lt\":\"ved100\"}}"))).explainPlan("all");

    if (!plan3Str.matches("(?s).*INDEX RANGE SCAN.*") || !plan3Str.matches("(?s).*MULTI VALUE.*"))
    {
      fail ("Multivalue Index range scan is not found.");
    }

    String plan3Num = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$lt\":100}}"))).explainPlan("all");

    if (!plan3Num.matches("(?s).*INDEX RANGE SCAN.*") || !plan3Num.matches("(?s).*MULTI VALUE.*"))
    {
      fail ("Multivalue Index range scan is not found.");
    }

    String plan4Num = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$lte\":100}}"))).explainPlan("all");

    if (!plan4Num.matches("(?s).*INDEX RANGE SCAN.*") || !plan4Num.matches("(?s).*MULTI VALUE.*"))
    {
      fail ("Multivalue Index range scan is not found.");
    }

    String plan4Str = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$lte\":\"ved100\"}}"))).explainPlan("all");

    if (!plan4Str.matches("(?s).*INDEX RANGE SCAN.*") || !plan4Str.matches("(?s).*MULTI VALUE.*"))
    {
      fail ("Multivalue Index range scan is not found.");
    }

    // bug - 35402218

    String plan5 = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$in\":[\"ved20\",\"ved30\",\"ved50\", 500, 7000]}}"))).explainPlan("all");

    if (!plan5.matches("(?s).*INDEX UNIQUE SCAN.*") || !plan5.matches("(?s).*MULTI VALUE.*"))
    {
      //fail ("Multivalue Index unique scan is not found.");
    }

    String plan6Num = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$eq\":100}}"))).explainPlan("all");

    if (!plan6Num.matches("(?s).*INDEX RANGE SCAN.*") || !plan6Num.matches("(?s).*MULTI VALUE.*"))
    {
      fail ("Multivalue Index range scan is not found.");
    }

    String plan6Str = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$eq\":\"ved100\"}}"))).explainPlan("all");

    if (!plan6Str.matches("(?s).*INDEX RANGE SCAN.*") || !plan6Str.matches("(?s).*MULTI VALUE.*"))
    {
      fail ("Multivalue Index range scan is not found.");
    }

    // bug - 35402218

    String plan7 = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"$or\":[{\"empid\":\"ved3\"},{\"empid\":\"ved5\"},{\"empid\":\"ved6\"},{\"empid\":{\"$gt\":4900}}]}"))).explainPlan("all");

    if (!plan7.matches("(?s).*INDEX RANGE SCAN.*") && !plan7.matches("(?s).*MULTI VALUE.*"))
    {
      //fail ("Multivalue Index range scan is not found.");
    }

    String plan8= ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"$and\":[{\"empid\":{\"$lt\":100}},{\"empid\":{\"$lt\":\"ved500\"}}]}"))).explainPlan("all");

    if (!plan8.matches("(?s).*INDEX RANGE SCAN.*") || !plan8.matches("(?s).*MULTI VALUE.*"))
    {
      fail ("Multivalue Index range scan is not found.");
    }

    String plan9Str = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$not\":{\"$gt\":\"ved5\"}}}"))).explainPlan("all");

    if (!plan9Str.matches("(?s).*INDEX RANGE SCAN.*") || !plan9Str.matches("(?s).*MULTI VALUE.*"))
    {
      fail ("Multivalue Index range scan is not found.");
    }

    String plan9Num = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$not\":{\"$gt\":5}}}"))).explainPlan("all");

    if (!plan9Num.matches("(?s).*INDEX RANGE SCAN.*") || !plan9Num.matches("(?s).*MULTI VALUE.*"))
    {
      fail ("Multivalue Index range scan is not found.");
    }

    // bug - 35402218

    String plan10 = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"$nor\":[{\"empid\":{\"$lt\":\"ved100\"}},{\"empid\":{\"$gt\":\"ved500\"}}]}"))).explainPlan("all");

    if (!plan10.matches("(?s).*INDEX RANGE SCAN.*") || !plan10.matches("(?s).*MULTI VALUE.*"))
    {
      //fail ("Multivalue Index range scan is not found.");
    }

    String plan11Str = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$between\":[\"ved2500\",\"ved4000\"]}}"))).explainPlan("all");

    if (!plan11Str.matches("(?s).*INDEX RANGE SCAN.*") || !plan11Str.matches("(?s).*MULTI VALUE.*"))
    {
      fail ("Multivalue Index range scan is not found.");
    }

    String plan11Num = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$between\":[2500,4000]}}"))).explainPlan("all");

    if (!plan11Num.matches("(?s).*INDEX RANGE SCAN.*") || !plan11Num.matches("(?s).*MULTI VALUE.*"))
    {
      fail ("Multivalue Index range scan is not found.");
    }

    String plan12 = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$all\":[500,\"ved1000\"]}}"))).explainPlan("all");

    if (!plan12.matches("(?s).*INDEX RANGE SCAN.*") || !plan12.matches("(?s).*MULTI VALUE.*"))
    {
      fail ("Multivalue Index unique scan is not found.");
    }

    // bug - 35402218

    String plan13 = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$ne\":1000}}"))).explainPlan("all");

    if (!plan13.matches("(?s).*INDEX RANGE SCAN.*") || !plan13.matches("(?s).*MULTI VALUE.*"))
    {
      //fail ("Multivalue Index range scan is not found.");
    }

    // bug - 35402218

    String plan14 = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$nin\":[1000, 2000, \"ved3000\", \"ved4000\"]}}"))).explainPlan("all");

    if (!plan14.matches("(?s).*INDEX RANGE SCAN.*") || !plan14.matches("(?s).*MULTI VALUE.*"))
    {
      // fail ("Multivalue Index range scan is not found.");
    }

    colAdmin.dropIndex(indexName1);
  }

  public void testAnyScalarIndex() throws Exception {

    if (!isCompatibleOrGreater(COMPATIBLE_23))
      return;

    OracleDocument mDoc = client.createMetadataBuilder().keyColumnAssignmentMethod("EMBEDDED_OID").keyColumnType("RAW").contentColumnType("JSON").build();

    OracleCollection col = dbAdmin.createCollection("testAnyScalarIndex", mDoc);
    OracleCollectionAdmin colAdmin = col.admin();

    String str1 = null;
    String str2 = null;

    for (int i=0; i<5000; i++)
    {
      str1 = String.format("{ \"empid\" : \"ved%s\"}", String.valueOf(i));
      str2 = String.format("{ \"empid\" : %s}", i);
      OracleDocument ret1 = col.insertAndGet(db.createDocumentFromString(str1));
      OracleDocument ret2 = col.insertAndGet(db.createDocumentFromString(str2));
    }

    String indexName1 = "my_index";
    String indexSpec1 =
            "{ \"name\":\"" + indexName1 + "\", \n" +
                    "  \"fields\": [\n" +
                    "    { \"path\":\"empid\"} \n" +
                    "] }";

    try
    {
      col.admin().createIndex(db.createDocumentFromString(indexSpec1));
    }
    catch (OracleException e)
    {
      fail ("No error should have occured while creating index.");
    }

    String planNum = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{ \"empid\" : 500}"))).explainPlan("all");

    if (!planNum.matches("(?s).*INDEX RANGE SCAN.*"))
    {
      fail ("Any Scalar Index range scan is not found.");
    }

    String planStr = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{ \"empid\" : \"ved500\"}"))).explainPlan("all");

    if (!planStr.matches("(?s).*INDEX RANGE SCAN.*"))
    {
      fail ("Any Scalar Index range scan is not found.");
    }

    String plan1Str = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$gt\":\"ved4900\"}}"))).explainPlan("all");

    if (!plan1Str.matches("(?s).*INDEX RANGE SCAN.*"))
    {
      fail ("Any Scalar Index range scan is not found.");
    }

    String plan1Num = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$gt\":4900}}"))).explainPlan("all");

    if (!plan1Num.matches("(?s).*INDEX RANGE SCAN.*"))
    {
      fail ("Any Scalar Index range scan is not found.");
    }

    String plan2Num = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$gte\":4900}}"))).explainPlan("all");

    if (!plan2Num.matches("(?s).*INDEX RANGE SCAN.*"))
    {
      fail ("Any Scalar Index range scan is not found.");
    }

    String plan2Str = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$gte\":\"ved4900\"}}"))).explainPlan("all");

    if (!plan2Str.matches("(?s).*INDEX RANGE SCAN.*"))
    {
      fail ("Any Scalar Index range scan is not found.");
    }

    String plan3Str = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$lt\":\"ved100\"}}"))).explainPlan("all");

    if (!plan3Str.matches("(?s).*INDEX RANGE SCAN.*"))
    {
      fail ("Any Scalar Index range scan is not found.");
    }

    String plan3Num = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$lt\":100}}"))).explainPlan("all");

    if (!plan3Num.matches("(?s).*INDEX RANGE SCAN.*"))
    {
      fail ("Any Scalar Index range scan is not found.");
    }

    String plan4Num = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$lte\":100}}"))).explainPlan("all");

    if (!plan4Num.matches("(?s).*INDEX RANGE SCAN.*"))
    {
      fail ("Any Scalar Index range scan is not found.");
    }

    String plan4Str = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$lte\":\"ved100\"}}"))).explainPlan("all");

    if (!plan4Str.matches("(?s).*INDEX RANGE SCAN.*"))
    {
      fail ("Any Scalar Index range scan is not found.");
    }

    // bug - 35402218

    String plan5 = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$in\":[\"ved20\",\"ved30\",\"ved50\", 500, 7000]}}"))).explainPlan("all");

    if (!plan5.matches("(?s).*INDEX UNIQUE SCAN.*"))
    {
      //fail ("Multivalue Index unique scan is not found.");
    }

    String plan6Num = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$eq\":100}}"))).explainPlan("all");

    if (!plan6Num.matches("(?s).*INDEX RANGE SCAN.*"))
    {
      fail ("Any Scalar Index range scan is not found.");
    }

    String plan6Str = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$eq\":\"ved100\"}}"))).explainPlan("all");

    if (!plan6Str.matches("(?s).*INDEX RANGE SCAN.*"))
    {
      fail ("Any Scalar Index range scan is not found.");
    }

    // bug - 35402218

    String plan7 = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"$or\":[{\"empid\":\"ved3\"},{\"empid\":\"ved5\"},{\"empid\":\"ved6\"},{\"empid\":{\"$gt\":4900}}]}"))).explainPlan("all");

    if (!plan7.matches("(?s).*INDEX RANGE SCAN.*"))
    {
      fail ("Any Scalar Index range scan is not found.");
    }

    String plan8= ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"$and\":[{\"empid\":{\"$lt\":100}},{\"empid\":{\"$lt\":\"ved500\"}}]}"))).explainPlan("all");

    if (!plan8.matches("(?s).*INDEX RANGE SCAN.*"))
    {
      fail ("Any Scalar Index range scan is not found.");
    }

    String plan9Str = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$not\":{\"$gt\":\"ved5\"}}}"))).explainPlan("all");
    
    if (!plan9Str.matches("(?s).*TABLE ACCESS FULL.*"))
    {
      fail ("TABLE ACCESS FULL is not found.");
    }

    String plan9Num = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$not\":{\"$gt\":5}}}"))).explainPlan("all");

    if (!plan9Num.matches("(?s).*TABLE ACCESS FULL.*"))
    {
      fail ("TABLE ACCESS FULL is not found.");
    }

    String plan10 = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"$nor\":[{\"empid\":{\"$lt\":\"ved100\"}},{\"empid\":{\"$gt\":\"ved500\"}}]}"))).explainPlan("all");

    if (!plan10.matches("(?s).*TABLE ACCESS FULL.*"))
    {
      
      fail ("TABLE ACCESS FULL is not found.");
    }

    String plan11Str = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$between\":[\"ved2500\",\"ved4000\"]}}"))).explainPlan("all");

    if (!plan11Str.matches("(?s).*INDEX RANGE SCAN.*"))
    {
      fail ("Any Scalar Index range scan is not found.");
    }

    String plan11Num = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$between\":[2500,4000]}}"))).explainPlan("all");

    if (!plan11Num.matches("(?s).*INDEX RANGE SCAN.*"))
    {
      fail ("Any Scalar Index range scan is not found.");
    }

    String plan12 = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$all\":[500,\"ved1000\"]}}"))).explainPlan("all");

    if (!plan12.matches("(?s).*INDEX RANGE SCAN.*"))
    {
      fail ("Any Scalar Index range scan is not found.");
    }

    String plan13 = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$ne\":1000}}"))).explainPlan("all");

    if (!plan13.matches("(?s).*TABLE ACCESS FULL.*"))
    {
      fail ("TABLE ACCESS FULL is not found.");
    }

    String plan14 = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$nin\":[1000, 2000, \"ved3000\", \"ved4000\"]}}"))).explainPlan("all");

    if (!plan14.matches("(?s).*TABLE ACCESS FULL.*"))
    {
      fail ("TABLE ACCESS FULL is not found.");
    }

    colAdmin.dropIndex(indexName1);
  }

  public void testAnyScalarJsonIndex() throws Exception {

    if (!isCompatibleOrGreater(COMPATIBLE_23))
      return;

    OracleDocument mDoc = client.createMetadataBuilder().keyColumnAssignmentMethod("EMBEDDED_OID").keyColumnType("RAW").contentColumnType("JSON").build();

    OracleCollection col = dbAdmin.createCollection("testAnyScalarIndex", mDoc);
    OracleCollectionAdmin colAdmin = col.admin();

    String str1 = null;
    String str2 = null;

    for (int i=0; i<5000; i++)
    {
      str1 = String.format("{ \"empid\" : \"ved%s\"}", String.valueOf(i));
      str2 = String.format("{ \"empid\" : %s}", i);
      OracleDocument ret1 = col.insertAndGet(db.createDocumentFromString(str1));
      OracleDocument ret2 = col.insertAndGet(db.createDocumentFromString(str2));
    }

    String indexName1 = "my_index";
    String indexSpec1 =
            "{ \"name\":\"" + indexName1 + "\", \n" +
                    "  \"fields\": [\n" +
                    "    { \"path\":\"empid\" , \"datatype\":\"any_scalar\"} \n" +
                    "] }";

    try
    {
      col.admin().createIndex(db.createDocumentFromString(indexSpec1));
    }
    catch (OracleException e)
    {
      fail ("No error should have occured while creating index.");
    }

    String planNum = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{ \"empid\" : 500}"))).explainPlan("all");

    if (!planNum.matches("(?s).*INDEX RANGE SCAN.*"))
    {
      fail ("Any Scalar Index range scan is not found.");
    }

    String planStr = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{ \"empid\" : \"ved500\"}"))).explainPlan("all");

    if (!planStr.matches("(?s).*INDEX RANGE SCAN.*"))
    {
      fail ("Any Scalar Index range scan is not found.");
    }

    String plan1Str = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$gt\":\"ved4900\"}}"))).explainPlan("all");

    if (!plan1Str.matches("(?s).*INDEX RANGE SCAN.*"))
    {
      fail ("Any Scalar Index range scan is not found.");
    }

    String plan1Num = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$gt\":4900}}"))).explainPlan("all");

    if (!plan1Num.matches("(?s).*INDEX RANGE SCAN.*"))
    {
      fail ("Any Scalar Index range scan is not found.");
    }

    String plan2Num = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$gte\":4900}}"))).explainPlan("all");

    if (!plan2Num.matches("(?s).*INDEX RANGE SCAN.*"))
    {
      fail ("Any Scalar Index range scan is not found.");
    }

    String plan2Str = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$gte\":\"ved4900\"}}"))).explainPlan("all");

    if (!plan2Str.matches("(?s).*INDEX RANGE SCAN.*"))
    {
      fail ("Any Scalar Index range scan is not found.");
    }

    String plan3Str = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$lt\":\"ved100\"}}"))).explainPlan("all");

    if (!plan3Str.matches("(?s).*INDEX RANGE SCAN.*"))
    {
      fail ("Any Scalar Index range scan is not found.");
    }

    String plan3Num = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$lt\":100}}"))).explainPlan("all");

    if (!plan3Num.matches("(?s).*INDEX RANGE SCAN.*"))
    {
      fail ("Any Scalar Index range scan is not found.");
    }

    String plan4Num = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$lte\":100}}"))).explainPlan("all");

    if (!plan4Num.matches("(?s).*INDEX RANGE SCAN.*"))
    {
      fail ("Any Scalar Index range scan is not found.");
    }

    String plan4Str = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$lte\":\"ved100\"}}"))).explainPlan("all");

    if (!plan4Str.matches("(?s).*INDEX RANGE SCAN.*"))
    {
      fail ("Any Scalar Index range scan is not found.");
    }

    // bug - 35402218

    String plan5 = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$in\":[\"ved20\",\"ved30\",\"ved50\", 500, 7000]}}"))).explainPlan("all");

    if (!plan5.matches("(?s).*INDEX UNIQUE SCAN.*"))
    {
      //fail ("Multivalue Index unique scan is not found.");
    }

    String plan6Num = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$eq\":100}}"))).explainPlan("all");

    if (!plan6Num.matches("(?s).*INDEX RANGE SCAN.*"))
    {
      fail ("Any Scalar Index range scan is not found.");
    }

    String plan6Str = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$eq\":\"ved100\"}}"))).explainPlan("all");

    if (!plan6Str.matches("(?s).*INDEX RANGE SCAN.*"))
    {
      fail ("Any Scalar Index unique scan is not found.");
    }

    // bug - 35402218

    String plan7 = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"$or\":[{\"empid\":\"ved3\"},{\"empid\":\"ved5\"},{\"empid\":\"ved6\"},{\"empid\":{\"$gt\":4900}}]}"))).explainPlan("all");

    if (!plan7.matches("(?s).*INDEX RANGE SCAN.*"))
    {
      fail ("Any Scalar Index range scan is not found.");
    }

    String plan8= ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"$and\":[{\"empid\":{\"$lt\":100}},{\"empid\":{\"$lt\":\"ved500\"}}]}"))).explainPlan("all");

    if (!plan8.matches("(?s).*INDEX RANGE SCAN.*"))
    {
      fail ("Any Scalar Index range scan is not found.");
    }

    String plan9Str = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$not\":{\"$gt\":\"ved5\"}}}"))).explainPlan("all");

    if (!plan9Str.matches("(?s).*TABLE ACCESS FULL.*"))
    {
      fail ("TABLE ACCESS FULL is not found.");
    }

    String plan9Num = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$not\":{\"$gt\":5}}}"))).explainPlan("all");

    if (!plan9Num.matches("(?s).*TABLE ACCESS FULL.*"))
    {
      
      fail ("TABLE ACCESS FULL is not found.");
    }

    String plan10 = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"$nor\":[{\"empid\":{\"$lt\":\"ved100\"}},{\"empid\":{\"$gt\":\"ved500\"}}]}"))).explainPlan("all");

    if (!plan10.matches("(?s).*TABLE ACCESS FULL.*"))
    {
      fail ("TABLE ACCESS FULL is not found.");
    }

    String plan11Str = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$between\":[\"ved2500\",\"ved4000\"]}}"))).explainPlan("all");

    if (!plan11Str.matches("(?s).*INDEX RANGE SCAN.*"))
    {
      fail ("Any Scalar Index range scan is not found.");
    }

    String plan11Num = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$between\":[2500,4000]}}"))).explainPlan("all");

    if (!plan11Num.matches("(?s).*INDEX RANGE SCAN.*"))
    {
      fail ("Any Scalar Index range scan is not found.");
    }

    String plan12 = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$all\":[500,\"ved1000\"]}}"))).explainPlan("all");

    if (!plan12.matches("(?s).*INDEX RANGE SCAN.*"))
    {
      fail ("Any Scalar Index range scan is not found.");
    }

    String plan13 = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$ne\":1000}}"))).explainPlan("all");

    if (!plan13.matches("(?s).*TABLE ACCESS FULL.*"))
    {
      fail ("TABLE ACCESS FULL is not found.");
    }

    String plan14 = ((OracleOperationBuilderImpl) col.find().filter(db.createDocumentFromString("{\"empid\":{\"$nin\":[1000, 2000, \"ved3000\", \"ved4000\"]}}"))).explainPlan("all");

    if (!plan14.matches("(?s).*TABLE ACCESS FULL.*"))
    {
      fail ("TABLE ACCESS FULL is not found.");
    }

    colAdmin.dropIndex(indexName1);
  }

  public void testCreateIndex() throws Exception {
    OracleDocument mDoc;
    if (isJDCSOrATPMode() || isCompatibleOrGreater(COMPATIBLE_20))
    {
      mDoc = null;
    } else
    {
      mDoc = client.createMetadataBuilder().contentColumnType("CLOB").build();
    }

    OracleCollection col = dbAdmin.createCollection("testCreateIndex", mDoc);
    OracleCollectionAdmin colAdmin = col.admin();

    String docStr1 =
      "{ \"name\":\"Mike\", \"num\":1001, \"birthday\":\"1990-08-21\", \"loggingtime\":\"2004-05-23T14:25:10\" }";
    col.insert(db.createDocumentFromString(docStr1));
    String docStr2 =
      "{ \"name\":\"Kate\", \"num\":1002, \"birthday\":\"1993-01-01\", \"loggingtime\":\"2010-10-23T18:00:00\"}";
    col.insert(db.createDocumentFromString(docStr2));
    String docStr3 =
      "{ \"name\":\"Taylor\", \"num\":[1003, 1004], \"birthday\":\"\"}";
    col.insert(db.createDocumentFromString(docStr3));

    // Test when unique is missing, use "lax"
    // since we use arrays.
    // Test with "string" and "numer" datatypes
    // Test with "asc" and "desc" for "order" value
    String indexSpec1 =
      "{ \"name\":\"STUDENT_INDEX1\", \n" +
      "  \"lax\" : true, \n" +
      "  \"fields\": [\n" +
      "    { \"path\":\"name\", \"datatype\":\"string\", \"maxLength\":100, \"order\":\"asc\"}, \n" +
      "    { \"path\":\"num\", \"datatype\":\"number\", \"order\":\"desc\"} ]\n" +
      "}";
    colAdmin.createIndex(db.createDocumentFromString(indexSpec1));

    colAdmin.dropIndex("STUDENT_INDEX1");

    // Negative test: datatype is number and maxLength is specified
    indexSpec1 =
      "{ \"name\":\"STUDENT_INDEX1\", \n" +
      "  \"fields\": [\n" +
      "    { \"path\":\"name\", \"datatype\":\"string\", \"maxLength\":100, \"order\":\"asc\"}, \n" +
      "    { \"path\":\"num\", \"datatype\":\"number\", \"maxLength\":100, \"order\":\"desc\"} ]\n" +
      "}";

    try
    {
      colAdmin.createIndex(db.createDocumentFromString(indexSpec1));
      junit.framework.Assert.fail("No error when creating an index with datatype number and maxLength");
    }
    catch(OracleException e) {
      Throwable t = e.getCause();
      assertTrue(t.getMessage().contains("\"maxLength\" can only be specified if the \"datatype\" value is \"string\""));
    }

    colAdmin.dropIndex("STUDENT_INDEX1");

    // Test to create an duplicated name index
    // we lack any ability to check if the index is the same or not 
    // so for now it's just ignored if the name matches
    String indexSpecNeg1 =
      "{ \"name\":\"STUDENT_INDEX1\",";

    if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel))
      indexSpecNeg1 += "\"scalarRequired\" : true,";

    indexSpecNeg1 += "\"unique\":true, \"fields\": [ { \"path\":\"name\" }] }";
    colAdmin.createIndex(db.createDocumentFromString(indexSpecNeg1));
    
    colAdmin.dropIndex("STUDENT_INDEX1");

    // Test with "date" and "timestamp" for datatypes
    // Test with 1 and -1 for "order" value
    String indexSpec2 =
      "{ \"name\":\"STUDENT_INDEX2\", \n" +
      "  \"unique\":true,\n";

    if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel))
      indexSpec2 += " \"lax\" : true,\n";

    indexSpec2 += "  \"fields\": [\n" +
      // createIndex() reported blocked by ORA-01858 and ORA-01861, because of the existing documents
      "    { \"path\":\"birthday\", \"datatype\":\"date\", \"order\":1 } \n" +
      "  , { \"path\":\"loggingtime\", \"datatype\":\"timestamp\", \"order\":-1} \n" +
      "   ] \n" +
      "}";
    
    colAdmin.createIndex(db.createDocumentFromString(indexSpec2));
    col.insert(db.createDocumentFromString("{ \"name\":\"Candy\", \"num\":1009, \"birthday\":\"1998-08-21\"}"));

    // time zone is NOT supported
    col.insert(db.createDocumentFromString("{ \"name\":\"Jay\", \"num\":1001, \"loggingtime\":\"2004-05-03T17:30:08.45\" }"));

    colAdmin.dropIndex("STUDENT_INDEX2");

    // Negative test: same as above, but maxLength is specified with datatype timestamp.
    // ### Note: language is not officially supported, do not use in production!!!
    indexSpec2 =
      "{ \"name\":\"STUDENT_INDEX2\", \n" +
      "  \"unique\":true,\n" +
      "  \"language\":\"english\", \n" +
      "  \"fields\": [\n" +
      // createIndex() reported blocked by ORA-01858 and ORA-01861, because of the existing documents
      "    { \"path\":\"birthday\", \"datatype\":\"date\", \"order\":1 } \n" +
      "  , { \"path\":\"loggingtime\", \"datatype\":\"timestamp\", \"maxLength\":100, \"order\":-1} \n" +
      "   ] \n" +
      "}";

    try
    {
      colAdmin.createIndex(db.createDocumentFromString(indexSpec2));
      junit.framework.Assert.fail("No error when creating an index with datatype timestamp and maxLength");
    }
    catch(OracleException e) {
      Throwable t = e.getCause();
      assertTrue(t.getMessage().contains("\"maxLength\" can only be specified if the \"datatype\" value is \"string\""));
    }

    // Negative test: both "language" and "fields" specified
    // ### Note: language is not officially supported, do not use in production!!!
    indexSpec2 =
      "{ \"name\":\"STUDENT_INDEX2\", \n" +
      "  \"language\":\"english\", \n" +
      "  \"fields\": [\n" +
      "    { \"path\":\"birthday\", \"datatype\":\"date\", \"order\":1 } \n" +
      "  , { \"path\":\"loggingtime\", \"maxLength\":100, \"order\":-1} \n" +
      "   ] \n" +
      "}";

    try
    {
      colAdmin.createIndex(db.createDocumentFromString(indexSpec2));
      junit.framework.Assert.fail("No error when creating an index with \"language\" and \"fields\"");
    }
    catch(OracleException e) {
      Throwable t = e.getCause();
      assertTrue(t.getMessage().contains("\"language\" should only be specified for a JSON text index"));
    }

    // Test with minor index specification
    String indexSpec3 = "{ \"name\":\"STUDENT_INDEX3\",";

    if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel)) {
      indexSpec3 += "\"scalarRequired\" : true,";
    }

    indexSpec3 += "\"fields\": [ { \"path\":\"name\" }] }";

    colAdmin.createIndex(db.createDocumentFromString(indexSpec3));

    colAdmin.dropIndex("STUDENT_INDEX3");
    
    // Negative test: "unique" must only be specified with "fields".
    // ### Note: language is not officially supported, do not use in production!!!
    String indexSpec4 =
      "{ \"name\":\"STUDENT_INDEX4\", \n" +
      "  \"unique\":true,\n" +
      "  \"language\":\"english\" \n" +
      "}";
    try {
      colAdmin.createIndex(db.createDocumentFromString(indexSpec4));
      junit.framework.Assert.fail("No error when \"unique\" is specified with \"fields\"");
    } 
    catch(OracleException e) {
      Throwable t = e.getCause();
      assertTrue(t.getMessage().contains("In an index specification, \"unique\" " +
                                         "cannot be specified without specifying \"fields\"."));
    }

    // Negative test: "scalarRequired" must only be specified with "fields".
    // ### Note: language is not officially supported, do not use in production!!!
    indexSpec4 =
      "{ \"name\":\"STUDENT_INDEX4\", \n" +
      "  \"scalarRequired\":true,\n" +
      "  \"language\":\"english\" \n" +
      "}";
    try {
      colAdmin.createIndex(db.createDocumentFromString(indexSpec4));
      junit.framework.Assert.fail("No error when \"scalarRequired\" is specified with \"fields\"");
    } 
    catch(OracleException e) {
      Throwable t = e.getCause();
      assertTrue(t.getMessage().contains("In an index specification, \"scalarRequired\" " +
                                         "cannot be specified without specifying \"fields\" or " +
                                         "\"spatial\"."));
    }

    // Negative test: "lax" must only be specified with "fields".
    // ### Note: language is not officially supported, do not use in production!!!
    indexSpec4 =
      "{ \"name\":\"STUDENT_INDEX4\", \n" +
      "  \"lax\":true,\n" +
      "  \"language\":\"english\" \n" +
      "}";
    try {
      colAdmin.createIndex(db.createDocumentFromString(indexSpec4));
      junit.framework.Assert.fail("No error when \"lax\" is specified with \"fields\"");
    }
    catch(OracleException e) {
      Throwable t = e.getCause();
      assertTrue(t.getMessage().contains("In an index specification, \"lax\" " +
                                         "cannot be specified without specifying \"fields\" or " +
                                         "\"spatial\"."));
    }

    // Create a json search index
    String fullIndexName = "STUDENT_INDEX4";
    String jsonSearchIndexSpec = createTextIndexSpec(fullIndexName);
    OracleDocument d = db.createDocumentFromString(jsonSearchIndexSpec);
    colAdmin.createIndex(d);

    try {
      // Try to create the second json search index
      fullIndexName = "STUDENT_INDEX5";
      jsonSearchIndexSpec = createTextIndexSpec(fullIndexName);
      colAdmin.createIndex(db.createDocumentFromString(jsonSearchIndexSpec));
      fail("No error when creating a second json search index");
    } catch (OracleException e) {
      //ORA-29879: cannot create multiple domain indexes on a column list using same indextype
      Throwable t = e.getCause();
      assertTrue(t.getMessage().contains("ORA-29879"));
    }
    colAdmin.dropIndex("STUDENT_INDEX5");

    try {
      // Try to create the second json search index by createJsonSearchIndex
      colAdmin.createJsonSearchIndex("indexAll");
      fail("No error when create a second json search index");
    }
    catch(OracleException e) {
      //ORA-29879: cannot create multiple domain indexes on a column list using same indextype
      Throwable t = e.getCause();
      assertTrue(t.getMessage().contains("ORA-29879"));
    }

    try {
      // Test with null for index Spec
      colAdmin.createIndex(null);
      fail("No error when indexSpec is null");
    }
    catch(OracleException e) {
      assertEquals("indexSpecification argument cannot be null.", e.getMessage());
    }

    try {
      // Test with non-JSON data for index Spec
      colAdmin.createIndex(db.createDocumentFromString("{data}"));
      fail("No error when indexSpec is non-JSON data");
    }
    catch(OracleException e) {
      Throwable t = e.getCause();
      assertEquals("Unexpected char 100 at (line no=1, column no=2, offset=1)", t.getMessage());
    }

    try {
      // Test with index specification missing name
      colAdmin.createIndex(db.createDocumentFromString("{ \"fields\": [ { \"path\":\"name\" }] }"));
      fail("No error when name is missing in index specification");
    }
    catch(OracleException e) {
      Throwable t = e.getCause();
      assertEquals("Missing name property.", t.getMessage());
    }

    try {
      // Test with an invalid value for "language" in index specification
      // ### Note: language is not officially supported, do not use in production!!!
      String indexSpecN2 =
        "{ \"name\":\"STUDENT_INDEXN2\", \"language\" : 1001, \"fields\": [ { \"path\":\"name\" }] }";
      colAdmin.createIndex(db.createDocumentFromString(indexSpecN2));
      fail("No error when there is invalid \"language\" value  in index specification");
    }
    catch(OracleException e) {
      Throwable t = e.getCause();
      assertEquals("Invalid value for property language: expected STRING, found NUMBER.", t.getMessage());
    }

    try {
      // Test with an invalid value for "unique" in index specification
      String indexSpecN3 =
        "{ \"name\":\"STUDENT_INDEXN3\", \"language\" : \"english\", \"unique\":1001, \"fields\": [ { \"path\":\"name\" }] }";
      colAdmin.createIndex(db.createDocumentFromString(indexSpecN3));
      fail("No error when there is invalid \"unique\" value  in index specification");
    }
    catch(OracleException e) {
      Throwable t = e.getCause();
      assertEquals("Invalid value for property unique: expected BOOLEAN, found NUMBER.", t.getMessage());
    }

    try {
      // Test with an invalid value for "datatype" in index specification
      // ### Note: language is not officially supported, do not use in production!!!
      String indexSpecN4 =
        "{ \"name\":\"STUDENT_INDEXN4\", \"language\" : \"english\", \n" +
        "  \"fields\": [ { \"path\":\"num\", \"datatype\"  : \"abcd\" }] }";
      colAdmin.createIndex(db.createDocumentFromString(indexSpecN4));
      fail("No error when there is invalid \"datatype\" value  in index specification");
    }
    catch(OracleException e) {
      Throwable t = e.getCause();
      assertEquals("Data type abcd not recognized or acceptable for an index column.", t.getMessage());
    }
    
    try {
      // Test with "path" is missing in "fields"
      // ### Note: language is not officially supported, do not use in production!!!
      String indexSpecN5 =
        "{ \"name\":\"STUDENT_INDEXN5\", \"language\" : \"english\", \n" +
        "  \"fields\": [ {\"datatype\":\"string\" }] }";
      colAdmin.createIndex(db.createDocumentFromString(indexSpecN5));
      fail("No error when \"path\" is missing in \"fields\"");
    }
    catch(OracleException e) {
      Throwable t = e.getCause();
      assertEquals("Missing fields.path property.", t.getMessage());
    }

    try {
      // Test with an invalid value for "fields.maxLength" in index specification
      // ### Note: language is not officially supported, do not use in production!!!
      String indexSpecN6 =
        "{ \"name\":\"STUDENT_INDEXN6\", \"language\" : \"english\", \n" +
        "  \"fields\": [ { \"path\":\"num\", \"datatype\"  : \"string\", \"maxLength\" : \"p1\" }] }";
      colAdmin.createIndex(db.createDocumentFromString(indexSpecN6));
      fail("No error when there is invalid \"fields.maxLength\" value in index specification");
      colAdmin.dropIndex("STUDENT_INDEXN6");
    }
    catch(OracleException e) {
      Throwable t = e.getCause();
      assertEquals("Invalid value for property fields.maxLength: expected NUMBER, found STRING.", t.getMessage());
    }

    try {
      // Test with an invalid value for "fields.order" in index specification
      String indexSpecN7 =
        "{ \"name\":\"STUDENT_INDEXN7\", \n" +
        "  \"fields\": [ { \"path\":\"num\", \"order\" : \"p0\" }] }";
      colAdmin.createIndex(db.createDocumentFromString(indexSpecN7));
      junit.framework.Assert.fail("No error when there is invalid \"fields.order\" value in index specification");
      colAdmin.dropIndex("STUDENT_INDEXN7");
    }
    catch(OracleException e) {
      Throwable cause = e.getCause();
      assertTrue(cause.getMessage().contains("Valid values are \"asc\", \"desc\", \"1\", \"-1\", 1, or -1."));
    }

    String docStr4 =
      "{ \"name\":\"Rojer\", \"num\":1001, \"birthday\":\"21-Oct-88\", \"loggingtime\":\"05-SEP-12 02.14.59.542029 AM\" }";
    col.insert(db.createDocumentFromString(docStr4));

    try {
      // there is conflict between the specified index and the existing row 
      String indexSpecN8 =
        "{ \"name\":\"STUDENT_INDEXN8\", \"unique\" : true, \n" +
        "  \"lax\":true, " +
        "  \"fields\": [ { \"path\":\"num\", \"datatype\":\"number\" }] }";
      colAdmin.createIndex(db.createDocumentFromString(indexSpecN8));
      fail("No error when there is conflict between the specified index and the existing documents");
    }
    catch(OracleException e) {
      //ORA-01452: cannot CREATE UNIQUE INDEX; duplicate keys found
      Throwable t = e.getCause();
      assertTrue(t.getMessage().contains("ORA-01452"));
    }

    //### Oracle Database does not support NVARCHAR2 or NCLOB storage for JSON
    /*
    // ### Note: language is not officially supported, do not use in production!!!
    String indexSpecN9 = createTextIndexSpec("STUDENT_INDEXN9", "english");

    try {
      // Json search index is NOT supported for NVARCHAR2 contentColumnType
      OracleDocument mDoc2 = client.createMetadataBuilder()
        .contentColumnType("NVARCHAR2").build();
      OracleCollection col2 = dbAdmin.createCollection("testCreateIndex2", mDoc2);
      col2.admin().createIndex(db.createDocumentFromString(indexSpecN9));
      fail("No exception when creating json search index on NVARCHAR2 collection");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("JSON search index is not implemented for content columns with type NVARCHAR2.", e.getMessage());
    }

    try {
      // Json search index is NOT supported for NCLOB contentColumnType
      OracleDocument mDoc3 = client.createMetadataBuilder()
        .contentColumnType("NCLOB").build();
      OracleCollection col3 = dbAdmin.createCollection("testCreateIndex3", mDoc3);
      col3.admin().createIndex(db.createDocumentFromString(indexSpecN9));
      fail("No exception when creating json search index on NCLOB collection");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("JSON search index is not implemented for content columns with type NCLOB.", e.getMessage());
    }
    */
  } 
  
  public void testCreateIndex2() throws Exception {
    OracleCollection col = dbAdmin.createCollection("testCreateIndex2");
    OracleCollectionAdmin colAdmin = col.admin();
    
    // test with "lax":true index
    String indexSpec1 =
      "{ \"name\":\"FUNC_INDEX1\", " +
      "  \"lax\" : true, " +
      "  \"fields\": [" +
      "    { \"path\":\"name\", \"orderDate\":\"date\", \"order\":\"asc\"}] " +
      "}";
    
    col.admin().createIndex(db.createDocumentFromString(indexSpec1));
    // all the following "orderDate" field values are invalid for "date" type
    // but "lax" index allows these violations
    col.insert(db.createDocumentFromString("{ \"name\":\"Mike\" }"));
    col.insert(db.createDocumentFromString("{ \"orderDate\": \"2016\"}"));
    col.insert(db.createDocumentFromString("{ \"orderDate\": {\"date\":\"2016-07-15\"} }"));
    
    col.admin().dropIndex("FUNC_INDEX1");
    col.find().remove();
    
    if (!SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel)) {
      // test with "scalarRequired":false(the default index will be created)
      String indexSpec2 =
        "{ \"name\":\"FUNC_INDEX2\", " +
        "  \"scalarRequired\" : false, " +
        "  \"fields\": [" +
        "    { \"path\":\"orderName\", \"datatype\":\"string\", \"maxLength\":100 }] " +
        "}";
    
      col.admin().createIndex(db.createDocumentFromString(indexSpec2));
      // missing "orderName" field is allowed
      col.insert(db.createDocumentFromString("{ \"name\":\"Mike\" }"));
    
      try {
        // non-scalar value is not allowed
        col.insert(db.createDocumentFromString("{ \"orderName\":{\"name1\":\"Mike\", \"name2\":\"John\" } }"));
        fail("Error should have been generated because the non-scalar value is beging inserted");
      } catch (OracleException e) {
        Throwable c = e.getCause();
        if (isDBVersionBelow(23, 0)) {
          assertTrue(c.getMessage().contains("JSON_VALUE evaluated to non-scalar value"));
        } else {
          assertTrue(c.getMessage().contains("JSON path '$.orderName' evaluated to non-scalar value"));
        }
        
      }
    
      col.admin().dropIndex("FUNC_INDEX2");
      col.find().remove();
    
      // test with "lax":false(the default index will be created)
      String indexSpec3 =
        "{ \"name\":\"FUNC_INDEX3\", " +
        "  \"lax\" : false, " +
        "  \"fields\": [" +
        "    { \"path\":\"orderDate\", \"datatype\":\"timestamp\" }] " +
        "}";
    
      col.admin().createIndex(db.createDocumentFromString(indexSpec3));
      // missing "orderName" field is allowed
      col.insert(db.createDocumentFromString("{ \"date\":\"2016\" }"));
    
      try {
        // the value does not match "TIMESTAMP" format(required ISO8601)
        col.insert(db.createDocumentFromString("{\"orderDate\":\"2016-07-15 18:00:00\"}"));
        fail("Error should have been generated because invalid timestamp value is beging inserted");
      }
      catch (OracleException e) {
        Throwable c = e.getCause();
        //###  for json type content, the error is "Expected EOF token, but got END_OBJECT"
        assertTrue(c.getMessage().contains("ORA-01861"));
      }
    
      col.admin().dropIndex("FUNC_INDEX3");
      col.find().remove();
    }
    
    // test with field path containing array steps.
    String indexSpecN1 = "{ \"name\":\"FUNC_INDEXN1\", ";

    if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel)) {
      indexSpecN1 += "\"scalarRequired\" : true,";
    }

    indexSpecN1 += " \"fields\": [" +
                   " { \"path\":\"order[0].orderNo\", \"datatype\":\"number\" }]}";

    try
    {
      col.admin().createIndex(db.createDocumentFromString(indexSpecN1));
      col.admin().dropIndex("FUNC_INDEXN1");
      fail("Error should have been generated because field path is invalid");
    }
    catch (OracleException e)
    {
      assertTrue(e.getMessage(), e.getMessage().contains("Path for an index or order by condition should not contain array steps."));
    }
    
  }

  public void testCreateIndexNeg() throws Exception {

    // On 12.1.0.2, default indexing mode is not supported.
    if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel)) {
      String errorMsg = "Default functional index mode relies on json_value " +
        "\"null on empty\" clause supported starting with " +
        "12.2.0.1 Oracle Database release. \"scalarRequired\" " +
        "and \"lax\" modes are supported on 12.1.0.2. Note: \"lax\" " +
        "indexes are not used by QBEs, so \"scalarRequired\" is recommended.";

      OracleCollection col = dbAdmin.createCollection("testCreateIndex");
      OracleCollectionAdmin colAdmin = col.admin();

      String indexSpec1 =
        "{ \"name\":\"STUDENT_INDEX1\", " +
          "  \"fields\": [" +
          "   { \"path\":\"name\", \"datatype\":\"string\"," +
          "   \"maxLength\":100, \"order\":\"asc\"}]}";

      try {
        colAdmin.createIndex(db.createDocumentFromString(indexSpec1));
      }
      catch (OracleException e) {
        assertEquals(errorMsg, e.getMessage());
      }

      indexSpec1 =
        "{ \"name\":\"STUDENT_INDEX1\", " +
          "  \"lax\" : false," +
          "  \"fields\": [" +
          "   { \"path\":\"name\", \"datatype\":\"string\"," +
          "   \"maxLength\":100, \"order\":\"asc\"}]}";

      try {
        colAdmin.createIndex(db.createDocumentFromString(indexSpec1));
      }
      catch (OracleException e) {
        assertEquals(errorMsg, e.getMessage());
      }

      indexSpec1 =
        "{ \"name\":\"STUDENT_INDEX1\", " +
          "  \"scalarRequired\" : false," +
          "  \"fields\": [" +
          "   { \"path\":\"name\", \"datatype\":\"string\"," +
          "   \"maxLength\":100, \"order\":\"asc\"}]}";

      try {
        colAdmin.createIndex(db.createDocumentFromString(indexSpec1));
      }
      catch (OracleException e) {
        assertEquals(errorMsg, e.getMessage());
      }
    }
  }

  public void testCreateIndexScalarRequired() throws Exception {

    OracleCollection col = dbAdmin.createCollection("testCreateIndex");
    OracleCollectionAdmin colAdmin = col.admin();

    String docStr1 =
      "{ \"name\":\"Mike\" }";
    col.insert(db.createDocumentFromString(docStr1));
    String docStr2 =
      "{ \"name\":\"Kate\"}";
    col.insert(db.createDocumentFromString(docStr2));
    String docStr3 =
      "{ \"name\": \"Timothy\"}";
    col.insert(db.createDocumentFromString(docStr3));

    String indexSpec1 =
      "{ \"name\":\"STUDENT_INDEX1\", " +
      "  \"scalarRequired\" : true, " +
      "  \"fields\": [" +
      "    { \"path\":\"name\", \"datatype\":\"string\", \"maxLength\":100, \"order\":\"asc\"}] " +
      "}";

    colAdmin.createIndex(db.createDocumentFromString(indexSpec1));
    colAdmin.dropIndex("STUDENT_INDEX1");
    col.admin().drop();
  }

  public void testCreateIndexScalarRequiredNeg1() throws Exception {

    OracleCollection col = dbAdmin.createCollection("testCreateIndex");
    OracleCollectionAdmin colAdmin = col.admin();
    
    String docStr1 =
      "{ \"name\":\"Mike\" }";
    col.insert(db.createDocumentFromString(docStr1));
    String docStr2 =
      "{ \"name\":\"Kate\"}";
    col.insert(db.createDocumentFromString(docStr2));
    String docStr3 =
      "{ \"name\": { \"aliases\" : [\"Timothy\",\"Tim\"]}}";
    col.insert(db.createDocumentFromString(docStr3));
    
    String indexSpec1 =
      "{ \"name\":\"STUDENT_INDEX1\", " +
      "  \"scalarRequired\" : true, " +
      "  \"fields\": [" +
      "    { \"path\":\"name\", \"datatype\":\"string\", \"maxLength\":100, \"order\":\"asc\"}] " +
      "}";

    try
    {
      colAdmin.createIndex(db.createDocumentFromString(indexSpec1));
      fail("Error should have been generated because the indexed field should be a scalar");
    }
    catch (OracleException e)
    {
       Throwable c = e.getCause();
       if (isDBVersionBelow(23, 0)) {
         assertTrue(c.getMessage().contains("JSON_VALUE evaluated to non-scalar value"));
       } else {
         assertTrue(c.getMessage().contains("JSON path '$.name' evaluated to non-scalar value"));
       }
    }

    colAdmin.dropIndex("STUDENT_INDEX1");
    col.admin().drop();
  }


  public void testCreateIndexScalarRequiredNeg2() throws Exception {

    OracleCollection col = dbAdmin.createCollection("testCreateIndex");
    OracleCollectionAdmin colAdmin = col.admin();

    String docStr1 =
      "{ \"name\":\"Mike\" }";
    col.insert(db.createDocumentFromString(docStr1));
    String docStr2 =
      "{ \"name\":\"Kate\"}";
    col.insert(db.createDocumentFromString(docStr2));
    String docStr3 =
      "{ \"year\": 2020}";
    col.insert(db.createDocumentFromString(docStr3));

    String indexSpec1 =
      "{ \"name\":\"STUDENT_INDEX1\", " +
      "  \"scalarRequired\" : true," +
      "  \"fields\": [" +
      "    { \"path\":\"name\", \"datatype\":\"string\", \"maxLength\":100, \"order\":\"asc\"}]" +
      "}";

    try
    {
      colAdmin.createIndex(db.createDocumentFromString(indexSpec1));
      fail("Error should have been generated because the indexed field is missing in one document");
    }
    catch (OracleException e)
    {
       Throwable c = e.getCause();
       if (isDBVersionBelow(23, 0)) {
        assertTrue(c.getMessage().contains("JSON_VALUE evaluated to no value"));
      } else {
        assertTrue(c.getMessage().contains("JSON path '$.name' evaluated to no value"));
      }
       
    }

    colAdmin.dropIndex("STUDENT_INDEX1");
    col.admin().drop();
  }
  
  // tests about inconsistent data type between index spec and the existing doc
  public void testCreateIndexScalarRequiredNeg3() throws Exception {

    OracleCollection col = dbAdmin.createCollection("testCreateIndexScalarReqNeg3");
    OracleCollectionAdmin colAdmin = col.admin();

    String docStr1 = "{ \"name\":\"Mike\" }";
    col.insert(db.createDocumentFromString(docStr1));

    // "number" type has the conflict with the existing doc field value
    String indexSpecNeg1 =
      "{ \"name\":\"FUNC_INDEX_NEG1\", " +
      "  \"scalarRequired\" : true," +
      "  \"fields\": [" +
      "    { \"path\":\"name\", \"datatype\":\"number\"}]" +
      "}";

    try
    {
      colAdmin.createIndex(db.createDocumentFromString(indexSpecNeg1));
      colAdmin.dropIndex("FUNC_INDEX_NEG1");
      fail("Error should have been generated because the datatype of indexed field has conflict with the existing documents");
    }
    catch (OracleException e)
    {
      Throwable c = e.getCause();
      if (isDBVersionBelow(23,0)) {
        assertTrue(c.getMessage().contains("invalid number"));
      } else {
        assertTrue(c.getMessage().contains("ORA-01722: unable to convert string value"));
      }
    }

    // "date" type has the conflict with the existing doc field value
    String indexSpecNeg2 =
      "{ \"name\":\"FUNC_INDEX_NEG2\", " +
      "  \"scalarRequired\" : true," +
      "  \"fields\": [" +
      "    { \"path\":\"name\", \"datatype\":\"date\"}]" +
      "}";

    try
    {
      colAdmin.createIndex(db.createDocumentFromString(indexSpecNeg2));
      colAdmin.dropIndex("FUNC_INDEX_NEG2");
      fail("Error should have been generated because the datatype of indexed field has conflict with the existing documents");
    }
    catch (OracleException e)
    {
      Throwable c = e.getCause();

      if (!SODAUtils.sqlSyntaxBelow_18(sqlSyntaxLevel)) {
        assertTrue(c.getMessage().contains("ORA-01858"));
      }
      else {
        assertTrue(c.getMessage().contains("(full) year must be between -4713 and +9999"));
      }
    }
    
    // "timestamp" type has the conflict with the existing doc field value
    String indexSpecNeg3 =
      "{ \"name\":\"FUNC_INDEX_NEG3\", " +
      "  \"scalarRequired\" : true," +
      "  \"fields\": [" +
      "    { \"path\":\"name\", \"datatype\":\"timestamp\"}]" +
      "}";

    try
    {
      colAdmin.createIndex(db.createDocumentFromString(indexSpecNeg3));
      colAdmin.dropIndex("FUNC_INDEX_NEG3");
      fail("Error should have been generated because the datatype of indexed field has conflict with the existing documents");
    }
    catch (OracleException e)
    {
      Throwable c = e.getCause();
      if (!SODAUtils.sqlSyntaxBelow_18(sqlSyntaxLevel)) {
        assertTrue(c.getMessage().contains("ORA-01858"));
      }
      else {
        assertTrue(c.getMessage().contains("(full) year must be between -4713 and +9999"));
      }
    }
    
    col.admin().drop();
  }
  
  public void testCreateIndexNeg2() throws Exception {
    OracleCollection col = dbAdmin.createCollection("testCreateIndexNeg2");
    OracleCollectionAdmin colAdmin = col.admin();

    String docStr1 = "{ \"name\":\"Mike\" }";
    col.insert(db.createDocumentFromString(docStr1));
    
    if (!SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel)) {
      // missing the indexed value is allowed for the default mode
      String docStr2 = "{ \"orderNo\": 1001 }";
      col.insert(db.createDocumentFromString(docStr2));
    }
    
    String indexSpec1 = "{ \"name\":\"FUNC_INDEX1\", ";

    if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel)) {
      indexSpec1 += "\"scalarRequired\" : true,";
    }

    indexSpec1 += "\"fields\": [" +
                  " { \"path\":\"name\", \"datatype\":\"string\", \"maxLength\":100 }]" +
                  "}";

    colAdmin.createIndex(db.createDocumentFromString(indexSpec1));
    
    // the indexed field value is non-scalar
    String docStr3 = "{ \"name\":{\"FirstName\":\"Mike\", \"LastName\":\"Mike\" } }";
    
    try
    {
      col.insert(db.createDocumentFromString(docStr3));  
      fail("Error should have been generated because one non-scalar value is inserted");
    }
    catch (OracleException e)
    {
      Throwable c = e.getCause();
      if (isDBVersionBelow(23, 0)) {
        assertTrue(c.getMessage().contains("JSON_VALUE evaluated to non-scalar value"));
      } else {
        assertTrue(c.getMessage().contains("JSON path '$.name' evaluated to non-scalar value"));
      }
    }
    colAdmin.dropIndex("FUNC_INDEX1");
    
    // the index's "number" type has the conflict with the existing doc field value
    String indexSpecNeg1 = "{ \"name\":\"FUNC_INDEX_NEG1\", ";

    if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel))
      indexSpecNeg1 += "\"scalarRequired\" : true,";

    indexSpecNeg1 += " \"fields\": [{ \"path\":\"name\", \"datatype\":\"number\"}]}";


    try
    {
      colAdmin.createIndex(db.createDocumentFromString(indexSpecNeg1));
      colAdmin.dropIndex("FUNC_INDEX_NEG1");
      fail("Error should have been generated because the datatype of indexed field has conflict with the existing documents");
    }
    catch (OracleException e)
    {
       Throwable c = e.getCause();
       if (isDBVersionBelow(23,0)) {
        assertTrue(c.getMessage().contains("invalid number"));
      } else {
        assertTrue(c.getMessage().contains("ORA-01722: unable to convert string value"));
      }
    }
    
    // the index's "date" type has the conflict with the existing doc field value
    String indexSpecNeg2 =
      "{ \"name\":\"FUNC_INDEX_NEG2\", ";

    if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel))
      indexSpecNeg2 += "\"scalarRequired\" : true,";

    indexSpecNeg2 += "\"fields\": [{ \"path\":\"name\", \"datatype\":\"date\"}]}";


    try
    {
      colAdmin.createIndex(db.createDocumentFromString(indexSpecNeg2));
      colAdmin.dropIndex("FUNC_INDEX_NEG2");
      fail("Error should have been generated because the datatype of indexed field has conflict with the existing documents");
    }
    catch (OracleException e)
    {
      Throwable c = e.getCause();
      if (!SODAUtils.sqlSyntaxBelow_18(sqlSyntaxLevel)) {
        assertTrue(c.getMessage().contains("ORA-01858"));
      }
      else {
        assertTrue(c.getMessage().contains("(full) year must be between -4713 and +9999"));
      }
    }
    
    // the index's "timestamp" type has the conflict with the existing doc field value
    String indexSpecNeg3 = "{ \"name\":\"FUNC_INDEX_NEG3\", ";

    if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel))
      indexSpecNeg3 += "\"scalarRequired\" : true,";

    indexSpecNeg3 += " \"fields\": [{ \"path\":\"name\", \"datatype\":\"timestamp\"}]}";


    try
    {
      colAdmin.createIndex(db.createDocumentFromString(indexSpecNeg3));
      colAdmin.dropIndex("FUNC_INDEX_NEG3");
      fail("Error should have been generated because the datatype" +
           " of indexed field has conflict with the existing documents");
    }
    catch (OracleException e)
    {
      Throwable c = e.getCause();
      if (!SODAUtils.sqlSyntaxBelow_18(sqlSyntaxLevel)) {
        assertTrue(c.getMessage().contains("ORA-01858"));
      }
      else {
        assertTrue(c.getMessage().contains("(full) year must be between -4713 and +9999"));
      }
    }
    
    col.admin().drop();
  }

  public void testCreateIndexTTL() throws Exception {
    if (!isJDCSOrATPMode() && isDBVersionBelow(23, 0)) return;

    OracleCollection col = dbAdmin.createCollection("testCreateIndexTTL");
    OracleCollectionAdmin colAdmin = col.admin();

    Instant currentTime = Instant.now();
    Instant oneHourAgo = currentTime.minus(1, ChronoUnit.HOURS);
    Instant tenHoursAgo = currentTime.minus(10, ChronoUnit.HOURS);

    String docStr1 =
            "{ \"name\":\"Apple\",\"since\":\"" + currentTime + "\" }";
    col.insert(db.createDocumentFromString(docStr1));
    String docStr2 =
            "{ \"name\":\"Orange\",\"since\":\"" + tenHoursAgo.toString() + "\" }";
    col.insert(db.createDocumentFromString(docStr2));
    String docStr3 =
            "{ \"name\":\"Cheese\",\"since\":\"" + oneHourAgo.toString() + "\" }";
    col.insert(db.createDocumentFromString(docStr3));

    String indexSpec1 =
            "{ \"name\":\"TTL_INDEX1\", " +
                    "  \"ttl\" : 7200, " +
                    "  \"fields\": [ { \"path\":\"since\", \"datatype\":\"timestamp\"} ]" +
                    "}";

    colAdmin.createIndex(db.createDocumentFromString(indexSpec1));

    List<OracleDocument> indexes = ((OracleCollectionImpl) col).listIndexes();
    boolean foundIndex = false;
    for (OracleDocument index : indexes) {
      if (index.getContentAsString().contains("\"ttl\":7200")) {
        foundIndex = true;
        break;
      }
    }
    assertTrue(foundIndex);

    // Job should delete one document leaving two due to "expiration" (give it up to 5 minutes)
    for (int i = 0; i < 60; i++) {
      Thread.sleep(5000);
      if (col.find().count() == 2) break;  // already done, exit to save time
    }
    assertEquals(2, col.find().count());


    // Negative test: both "ttl" and "spatial" specified
    String indexSpec2 =
            "{ \"name\":\"TTL_INDEX2\", " +
                    "  \"ttl\" : 7200, " +
                    "  \"spatial\": \"dummy\"" +
                    "}";

    try
    {
      colAdmin.createIndex(db.createDocumentFromString(indexSpec2));
      junit.framework.Assert.fail("No error when creating an index with \"ttl\" and \"spatial\"");
    }
    catch(OracleException e) {
      Throwable t = e.getCause();
      assertTrue(t.getMessage().contains("\"spatial\" cannot be set in conjunction with \"ttl\""));
    }


    // Negative test: both "ttl" and "search_on" specified
    indexSpec2 =
            "{ \"name\":\"TTL_INDEX2\", " +
                    "  \"ttl\" : 7200, " +
                    "  \"search_on\": \"text\"" +
                    "}";

    try
    {
      colAdmin.createIndex(db.createDocumentFromString(indexSpec2));
      junit.framework.Assert.fail("No error when creating an index with \"ttl\" and \"search_on\"");
    }
    catch(OracleException e) {
      Throwable t = e.getCause();
      assertTrue(t.getMessage().contains("\"search_on\" cannot be set in conjunction with \"ttl\""));
    }


    // Negative test: both "ttl" and "dataguide" specified
    indexSpec2 =
            "{ \"name\":\"TTL_INDEX2\", " +
                    "  \"ttl\" : 7200, " +
                    "  \"dataguide\": \"on\"" +
                    "}";

    try
    {
      colAdmin.createIndex(db.createDocumentFromString(indexSpec2));
      junit.framework.Assert.fail("No error when creating an index with \"ttl\" and \"dataguide\"");
    }
    catch(OracleException e) {
      Throwable t = e.getCause();
      assertTrue(t.getMessage().contains("\"dataguide\" cannot be set in conjunction with \"ttl\""));
    }

    colAdmin.dropIndex("TTL_INDEX1");
    col.admin().drop();
  }

  public void testDropWithUncommittedWrites() throws Exception {

    boolean switchedOffAutoCommit = false;

    if (conn.getAutoCommit() == true)
    {
      conn.setAutoCommit(false);
      switchedOffAutoCommit = true;
    }

    OracleCollection col = dbAdmin.createCollection("testDropAfterInsert");
    OracleCollectionAdmin colAdmin = col.admin();

    OracleDocument doc =
      db.createDocumentFromString(new String("{ \"name\" : \"Alex\", \"friends\" : \"50\" }"));
    col.insertAndGet(doc);

    try {
      colAdmin.drop();
    }
    catch (OracleException e) {
      assertTrue(e.getMessage().contains("Error trying to drop the collection." +
                                         " Make sure all outstanding writes to the " +
                                         "collection are committed."));
    }

    // Should be able to drop after a commit
    conn.commit();
    colAdmin.drop();

    if (switchedOffAutoCommit)
      conn.setAutoCommit(true);
  }

  private void testDataGuide(boolean mixedCaseTable) throws Exception {
    
    String collName = null;
   
    if (mixedCaseTable)
      collName = "testDataGuide";
    else
      collName = "testdataguide";

    OracleCollection col = dbAdmin.createCollection(collName);
    OracleCollectionAdmin colAdmin = col.admin();

    OracleDocument index = db.createDocumentFromString(
      new String("{\"name\" : \"PERSON_METADATA_INDEX2\", \"dataguide\" : \"on\"}"));

    colAdmin.createIndex(index);

    OracleDocument doc = null;

    for (int i=0; i < 50; i++) {
      doc = db.createDocumentFromString(new String("{ \"name\" : \"Alex\", \"friends\" : \"" +
                                                    i + "\" }"));
      col.insertAndGet(doc);
    }
    
    doc = colAdmin.getDataGuide(); 
    
    String schema = "{\"type\":\"object\",\"o:length\":32,\"properties\":{\"name\":{\"type\":\"string\",\"o:length\":4," +
                     "\"o:preferred_column_name\":\"JSON_DOCUMENT$name\"}," +
                     "\"friends\":{\"type\":\"string\",\"o:length\":2," +
                     "\"o:preferred_column_name\":\"JSON_DOCUMENT$friends\"}}}";
    //Bug### is this a bug? o:length : 1 at the top level in JDCS mode 
    //(instead of o: length : 32 for JSON).
    
    if (isJDCSMode())
    {
      schema = "{\"type\":\"object\"" +
        ((!isDBVersionBelow(23, 0) && !isBug36425758()) ? "" : ",\"o:length\":1") +
        ",\"properties\":{\"name\":{\"type\":\"string\",\"o:length\":4," +
        "\"o:preferred_column_name\":\"JSON_DOCUMENT$name\"}," +
        "\"friends\":{\"type\":\"string\",\"o:length\":2," +
        "\"o:preferred_column_name\":\"JSON_DOCUMENT$friends\"}}}";

      assertEquals(schema, doc.getContentAsString());
    }
  }

  public void testDataGuide() throws Exception
  {
    if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel))
      return;

    testDataGuide(false);
  }

  public void testDataGuideMixedCaseTable() throws Exception
  {
    if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel))
      return;

    testDataGuide(true);
  }
  
  // tests about string/number functional index used in sql rewrite
  private void testFuncIndexInPlan1(String contentColumnType) throws Exception {
    if (isJDCSOrATPMode())
      if (!contentColumnType.equalsIgnoreCase("BLOB"))
        return;

    if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel)) {
      return;
    }
    
    OracleDocument mDoc = client.createMetadataBuilder()
        .contentColumnType(contentColumnType)
        .keyColumnAssignmentMethod("CLIENT").build();

    OracleCollection col;
    if (isJDCSOrATPMode())
    {
      col = db.admin().createCollection("testFuncIndexInPlan1" + contentColumnType, null);
    } else
    {
      col = db.admin().createCollection("testFuncIndexInPlan1" + contentColumnType, mDoc);
    }
    OracleDocument filterDoc = null, doc = null;
    String name = null, docStr = null;
    
    String[] key = new String[1000];
    for (int num = 0; num < 1000; num++) {
      if (num < 10) {
        name = "aaa00" + num;
      } else if (num < 100) {
        name = "aaa0" + num;
      } else {
        name = "aaa" + num;
      }

      docStr = "{\"order\" : { \"orderName\": \"" + name + "\", \"orderNum\": " + num + " } }";
      if (isJDCSOrATPMode()) 
      {
        doc = col.insertAndGet(db.createDocumentFromString(docStr));
        key[num] = doc.getKey();
      } else
      {
        doc = col.insertAndGet(db.createDocumentFromString("id" + num, docStr));
        key[num] = doc.getKey();
      }

    }
    
    final String indexName1 = "indexOnName";
    final String indexName2 = "indexOnNum";
    final String textIndexName = "textIndex";
    
    // set "order" to "asc" to bypass bug23757665
    String indexSpec1 =
      "{ \"name\":\"" + indexName1 + "\", \n" +
      "  \"scalarRequired\" : true, \n" +
      "  \"fields\": [\n" +
      "    { \"path\":\"order.orderName\", \"datatype\":\"string\", \"maxLength\" : 100, \"order\":\"asc\"} \n" +
      "] }";
    
    col.admin().createIndex(db.createDocumentFromString(indexSpec1));
    
    String indexSpec2 =
      "{ \"name\":\"" + indexName2 + "\", \n" +
      "  \"scalarRequired\" : false, \n" +
      "  \"fields\": [\n" +
      "    { \"path\":\"order.orderNum\", \"datatype\":\"number\"} \n" +
      "] }";
    
    col.admin().createIndex(db.createDocumentFromString(indexSpec2));
    
    // create full-text index
    String jsonSearchIndexSpec = createTextIndexSpec(textIndexName);
    OracleDocument d = db.createDocumentFromString(jsonSearchIndexSpec);
    col.admin().createIndex(d);

    // the "$exists" query does not use functional index, then full-text index should be picked up 
    filterDoc = db.createDocumentFromString("{ \"order.orderName\": {\"$exists\" : true} }");
    assertEquals(1000, col.find().filter(filterDoc).count());

    chkExplainPlan(col.find().filter(filterDoc), IndexType.textIndex, textIndexName);
    
    filterDoc = db.createDocumentFromString("{ \"order.orderNum\": {\"$exists\" : true} }");
    assertEquals(1000, col.find().filter(filterDoc).count());

    chkExplainPlan(col.find().filter(filterDoc), IndexType.textIndex, textIndexName);


    // test with "$eq"
    filterDoc = db.createDocumentFromString("{ \"order.orderName\": {\"$eq\" : \"aaa001\"} }");
    doc = col.find().filter(filterDoc).getOne();
    assertEquals(key[1], doc.getKey());
    chkExplainPlan(col.find().filter(filterDoc), IndexType.funcIndex, indexName1);
    
    filterDoc = db.createDocumentFromString("{ \"order.orderNum\": {\"$eq\" : 100} }");
    doc = col.find().filter(filterDoc).getOne();
    assertEquals(key[100], doc.getKey());
    chkExplainPlan(col.find().filter(filterDoc), IndexType.funcIndex, indexName2);
    
    // the index is not used for QBE containing "not" operators(such as, "$ne", "$nin", "$nor", "$not").
    filterDoc = db.createDocumentFromString("{ \"order.orderName\": {\"$ne\" : \"aaa002\"} }");
    assertEquals(999, col.find().filter(filterDoc).count());
    //chkExplainPlan(col.find().filter(filterDoc), IndexType.funcIndex, indexName1);
    
    filterDoc = db.createDocumentFromString("{ \"order.orderNum\": {\"$ne\" : 1} }");
    assertEquals(999, col.find().filter(filterDoc).count());
    //chkExplainPlan(col.find().filter(filterDoc), IndexType.funcIndex, indexName2);
    
    // test with "$gt"
    filterDoc = db.createDocumentFromString("{ \"order.orderName\": {\"$gt\" : \"aaa998\"} }");
    doc = col.find().filter(filterDoc).getOne();
    assertEquals(key[999], doc.getKey());
    chkExplainPlan(col.find().filter(filterDoc), IndexType.funcIndex, indexName1);
    
    filterDoc = db.createDocumentFromString("{ \"order.orderNum\": {\"$gt\" : 998} }");
    doc = col.find().filter(filterDoc).getOne();
    assertEquals(key[999], doc.getKey());
    chkExplainPlan(col.find().filter(filterDoc), IndexType.funcIndex, indexName2);
    
    // test with "$lt"
    filterDoc = db.createDocumentFromString("{ \"order.orderName\": {\"$lt\" : \"aaa001\"} }");
    doc = col.find().filter(filterDoc).getOne();
    assertEquals(key[0], doc.getKey());
    chkExplainPlan(col.find().filter(filterDoc), IndexType.funcIndex, indexName1);
    
    filterDoc = db.createDocumentFromString("{ \"order.orderNum\": {\"$lt\" : 1} }");
    doc = col.find().filter(filterDoc).getOne();
    assertEquals(key[0], doc.getKey());
    chkExplainPlan(col.find().filter(filterDoc), IndexType.funcIndex, indexName2);
    
    // test with $gte
    filterDoc = db.createDocumentFromString("{ \"order.orderName\": {\"$gte\" : \"aaa999\"} }");
    doc = col.find().filter(filterDoc).getOne();
    assertEquals(key[999], doc.getKey());
    chkExplainPlan(col.find().filter(filterDoc), IndexType.funcIndex, indexName1);
    
    filterDoc = db.createDocumentFromString("{ \"order.orderNum\": {\"$gte\" : 999} }");
    doc = col.find().filter(filterDoc).getOne();
    assertEquals(key[999], doc.getKey());
    chkExplainPlan(col.find().filter(filterDoc), IndexType.funcIndex, indexName2);
    
    // test with $lte
    filterDoc = db.createDocumentFromString("{ \"order.orderName\": {\"$lte\" : \"aaa000\"} }");
    doc = col.find().filter(filterDoc).getOne();
    assertEquals(key[0], doc.getKey());
    chkExplainPlan(col.find().filter(filterDoc), IndexType.funcIndex, indexName1);
    
    filterDoc = db.createDocumentFromString("{ \"order.orderNum\": {\"$lte\" : 0} }");
    doc = col.find().filter(filterDoc).getOne();
    assertEquals(key[0], doc.getKey());
    chkExplainPlan(col.find().filter(filterDoc), IndexType.funcIndex, indexName2);
    
    // the functional index is not used for "$in" query
    filterDoc = db.createDocumentFromString("{ \"order.orderName\" : {\"$in\" : [\"aaa000\", \"aaa00\", \"aaa0\"]} }");
    doc = col.find().filter(filterDoc).getOne();
    assertEquals(key[0], doc.getKey());
    //chkExplainPlan(col.find().filter(filterDoc), IndexType.funcIndex, indexName1);
    
    filterDoc = db.createDocumentFromString("{ \"order.orderNum\": {\"$in\" : [998, 999, 1000, 1001]} }");
    assertEquals(2, col.find().filter(filterDoc).count());
    // chkExplainPlan(col.find().filter(filterDoc), IndexType.funcIndex, indexName2);
   
    // test with $all
    filterDoc = db.createDocumentFromString("{ \"order.orderName\" : {\"$all\" : [\"aaa000\", \"aaa0000\"]} }");
    assertEquals(0, col.find().filter(filterDoc).count());
    if (!SODAUtils.sqlSyntaxBelow_18(sqlSyntaxLevel))
      chkExplainPlan(col.find().filter(filterDoc), IndexType.funcIndex, indexName1);
 
    filterDoc = db.createDocumentFromString("{ \"order.orderNum\": {\"$all\" : [0]} }");
    doc = col.find().filter(filterDoc).getOne();
    assertEquals(key[0], doc.getKey());
    if (!SODAUtils.sqlSyntaxBelow_18(sqlSyntaxLevel))
      chkExplainPlan(col.find().filter(filterDoc), IndexType.funcIndex, indexName2);
    
    // the index is not used for "$startsWith" query 
    filterDoc = db.createDocumentFromString("{ \"order.orderName\": {\"$startsWith\" : \"aaa998\"} }");
    assertEquals(1, col.find().filter(filterDoc).count());
    //chkExplainPlan(col.find().filter(filterDoc), IndexType.funcIndex, indexName1);
    
    // the index is not used for "$regex" query
    filterDoc = db.createDocumentFromString("{ \"order.orderName\" : {\"$regex\" : \"aaa99.*\"} }");
    assertEquals(10, col.find().filter(filterDoc).count());
    //chkExplainPlan(col.find().filter(filterDoc), IndexType.funcIndex, indexName1);
    
    // test with $and
    filterDoc = db.createDocumentFromString("{ \"$and\":[ {\"order.orderName\":{\"$gte\" : \"aaa990\"}}, {\"order.orderName\":{\"$lte\" : \"aaa990\"}} ] }");
    doc = col.find().filter(filterDoc).getOne();
    assertEquals(key[990], doc.getKey());
    if (!SODAUtils.sqlSyntaxBelow_18(sqlSyntaxLevel))
      chkExplainPlan(col.find().filter(filterDoc), IndexType.funcIndex, indexName1);
    
    filterDoc = db.createDocumentFromString("{ \"$or\":[ {\"order.orderNum\":998}, {\"order.orderNum\":999} ] }");
    assertEquals(2, col.find().filter(filterDoc).count());

    if (!SODAUtils.sqlSyntaxBelow_18(sqlSyntaxLevel))
      chkExplainPlan(col.find().filter(filterDoc), IndexType.funcIndex, indexName2);

    col.admin().dropIndex(textIndexName);
    col.admin().dropIndex(indexName1);
    col.admin().dropIndex(indexName2);
    
    col.admin().drop();
  }
  
  public void testFuncIndexInPlan1() throws Exception {
    for (String columnSqlType : columnSqlTypes) {
      testFuncIndexInPlan1(columnSqlType);
    }

    if (isCompatibleOrGreater(COMPATIBLE_20)) {
      testFuncIndexInPlan1("JSON");
    }
  }

  String[] columnSqlTypes = {
      "CLOB", "BLOB", "VARCHAR2"
  };
  
  //tests about "date"/"datetime" functional index used in sql rewrite
  private void testFuncIndexInPlan2(String contentColumnType) throws Exception {
    if (isJDCSOrATPMode())
      if (!contentColumnType.equalsIgnoreCase("BLOB"))
        return;


    if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel))
      return;

    OracleDocument mDoc = client.createMetadataBuilder()
        .contentColumnType(contentColumnType)
        .keyColumnAssignmentMethod("CLIENT").build();
    
    OracleCollection col;
    if (isJDCSOrATPMode())
    {
      col = db.admin().createCollection("testFuncIndexInPlan2" + contentColumnType, null);
    } else
    {
      col = db.admin().createCollection("testFuncIndexInPlan2" + contentColumnType, mDoc);
    }
    OracleDocument filterDoc = null, doc = null;
    String name = null, docStr = null;
    String date = "2016-01-01", date1 = "2016-07-25";
    String dateTime = "2016-01-01T00:00:00", dateTime1 = "2016-07-25T17:30:08";
    
    String[] key = new String[1000];
    for (int num = 0; num < 1000; num++) {
      if (num == 1) {
        docStr = "{\"order\" : { \"orderDate\": \"" + date1 + "\", \"orderDateTime\": \"" + dateTime1 + "\" } }";
      } else {
        docStr = "{\"order\" : { \"orderDate\": \"" + date + "\", \"orderDateTime\": \"" + dateTime + "\" } }";  
      }
      if (isJDCSOrATPMode()) 
      {
        doc = col.insertAndGet(db.createDocumentFromString(docStr));
        key[num] = doc.getKey();
      } else
      {
        doc = col.insertAndGet(db.createDocumentFromString("id" + num, docStr));
        key[num] = doc.getKey();
      }
    }
    
    final String indexName1 = "func-index1";
    final String indexName2 = "func-index2";
    final String textIndexName = "fullTextIndex";
    
    // set "order" to "asc" to bypass bug23757665
    String indexSpec1 =
      "{ \"name\":\"" + indexName1 + "\", \n" +
      "  \"scalarRequired\" : true, \n" +
      "  \"fields\": [\n" +
      "    { \"path\":\"order.orderDateTime\", \"datatype\":\"timestamp\", \"order\": \"asc\"} \n" +
      "] }";
    col.admin().createIndex(db.createDocumentFromString(indexSpec1));
    
    // the index on "date" is designed to work with "$date", which has not been added to SODA.
    /*String indexSpec2 =
      "{ \"name\":\"" + indexName2 + "\", \n" +
      "  \"fields\": [\n" +
      "    { \"path\":\"order.orderDate\", \"datatype\":\"date\"} \n" +
      "] }";
    col.admin().createIndex(db.createDocumentFromString(indexSpec2)); */
    
    // create full-text index
    String jsonSearchIndexSpec = null;

    jsonSearchIndexSpec = createTextIndexSpec(textIndexName);
    OracleDocument d = db.createDocumentFromString(jsonSearchIndexSpec);
    col.admin().createIndex(d);

    // test with "$timestamp" + "$eq"
    filterDoc = db.createDocumentFromString("{ \"order.orderDateTime\": {\"$timestamp\" : {\"$eq\" : \"" + dateTime1 + "\"} } }");
    doc = col.find().filter(filterDoc).getOne();
    assertEquals(key[1], doc.getKey());
    chkExplainPlan(col.find().filter(filterDoc), IndexType.funcIndex, indexName1);
    
    // test with "$timestamp" + "$gt"
    filterDoc = db.createDocumentFromString("{ \"order.orderDateTime\": {\"$timestamp\" : {\"$gt\" : \"2016-07-01T00:00:00\"} } }");
    doc = col.find().filter(filterDoc).getOne();
    assertEquals(key[1], doc.getKey());
    chkExplainPlan(col.find().filter(filterDoc), IndexType.funcIndex, indexName1);
    
    col.admin().dropIndex(textIndexName);
    col.admin().dropIndex(indexName1);
    
    col.admin().drop();
  }
  
  public void testFuncIndexInPlan2() throws Exception {
    for (String columnSqlType : columnSqlTypes) {
      testFuncIndexInPlan2(columnSqlType);
    }
    
    if (isCompatibleOrGreater(COMPATIBLE_20)) {
      testFuncIndexInPlan1("JSON");
    }
  }

  public void testJsonSearchIndexDefaulting() throws Exception {

    if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel))
      return;

    OracleCollection col = dbAdmin.createCollection("testJsonSearchCol");

    // Only name is specified. dataguide and search_on are defaulted.
    OracleDocument index = db.createDocumentFromString("{\"name\" : \"jsonSearchIndex1\"}");
    col.admin().createIndex(index);
    col.admin().dropIndex("jsonSearchIndex1");

    // Name and dataguide is specified. search_on is defaulted.
    index = db.createDocumentFromString("{\"name\" : \"jsonSearchIndex1\",\"dataguide\" : \"on\"}");
    col.admin().createIndex(index);
    col.admin().dropIndex("jsonSearchIndex1");

    // Name, dataguide, search_on is specified. 
    index = db.createDocumentFromString("{\"name\" : \"jsonSearchIndex1\"," +
                                        "\"dataguide\" : \"on\"," +
                                        "\"search_on\" : \"text\"}");
    col.admin().createIndex(index);
    col.admin().dropIndex("jsonSearchIndex1");

    // All fields are specified. Some letters in uppercase.
    // ### Note: language is not officially supported, do not use in production!!!
    index = db.createDocumentFromString("{\"name\" : \"jsonSearchIndex1\"," +
                                        "\"dataguiDE\" : \"On\"," +
                                        "\"sEARch_on\" : \"tEXt\"," +
                                        "\"languagE\" : \"simPlified_chinese\"}");
    col.admin().createIndex(index);
    col.admin().dropIndex("jsonSearchIndex1");

  }

  public void testJsonSearchIndexNeg() throws Exception {

    if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel))
      return;

    OracleCollection col = dbAdmin.createCollection("testJsonSearchIndexNeg");

    // Invalid value for dataguide
    OracleDocument index = db.createDocumentFromString("{\"name\" : \"jsonSearchIndex1\"," +
                                                       "\"dataguide\" : \"onn\"}");
    try {
      col.admin().createIndex(index);
      fail("No exception when index spec is invalid.");
    } catch (OracleException e) {
      Throwable t = e.getCause();

      assertEquals(t.getMessage(), "\"dataguide\" cannot be \"onn\". " +
                                   "Valid values are \"on\" or \"off\".");

      col.admin().dropIndex("jsonSearchIndex1");
    }

    // Invalid string value for search_on
    index = db.createDocumentFromString("{\"name\" : \"jsonSearchIndex1\"," +
                                        "\"search_on\" : \"blah\"}");

    try {
      col.admin().createIndex(index);
      fail("No exception when index spec is invalid.");
    }
    catch (OracleException e) {
      Throwable t = e.getCause();
      assertEquals(t.getMessage(), "\"search_on\" cannot be \"blah\". " +
                                   "Valid values are \"none\", \"text\"," +
                                   " and \"text_value\".");
      col.admin().dropIndex("jsonSearchIndex1");
    }

    // Invalid value for language
    // ### Note: language is not officially supported, do not use in production!!!
    index = db.createDocumentFromString("{\"name\" : \"jsonSearchIndex1\"," +
                                          "\"language\" : \"blah\"}");
    try {
      col.admin().createIndex(index);
      fail("No exception when index spec is invalid.");
    }
    catch (OracleException e) {
      Throwable t = e.getCause();
      assertEquals(t.getMessage(), "Language blah is not recognized.");
      col.admin().dropIndex("jsonSearchIndex1");
    }

    // Test non-string value for dataguide
    index = db.createDocumentFromString("{\"name\":\"jsonSearchIndex1\", \"dataguide\":true}");
    try {
      col.admin().createIndex(index);
      fail("No exception when index spec is invalid.");
    } catch (OracleException e) {
      Throwable t = e.getCause();
      String errMsg = "Invalid value for property dataguide: expected STRING, found TRUE.";
      assertEquals(errMsg, t.getMessage());
    }
    
    // Test non-string value for name
    index = db.createDocumentFromString("{\"name\":100}");
    try {
      col.admin().createIndex(index);
      fail("No exception when index spec is invalid.");
    } catch (OracleException e) {
      Throwable t = e.getCause();
      String errMsg = "Invalid value for property index name: expected STRING, found NUMBER.";
      assertEquals(errMsg, t.getMessage());
    }
    
    // Test non-string value for search_on
    index = db.createDocumentFromString("{\"name\":\"jsonSearchIndex1\", \"search_on\":{\"value\":\"text\"}}");
    try {
      col.admin().createIndex(index);
      fail("No exception when index spec is invalid.");
    } catch (OracleException e) {
      Throwable t = e.getCause();
      String errMsg = "Invalid value for property search_on: expected STRING, found OBJECT.";
      assertEquals(errMsg, t.getMessage());
    }
    
    // Test non-string value for language
    // ### Note: language is not officially supported, do not use in production!!!
    index = db.createDocumentFromString("{\"name\":\"jsonSearchIndex1\", \"language\":null}");
    try {
      col.admin().createIndex(index);
      fail("No exception when index spec is invalid.");
    } catch (OracleException e) {
      Throwable t = e.getCause();
      String errMsg = "Invalid value for property language: expected STRING, found NULL.";
      assertEquals(errMsg, t.getMessage());
    }
    
    // ### Note: language is not officially supported, do not use in production!!!
    index = db.createDocumentFromString("{\"name\":\"jsonSearchIndex1\", \"language\":[\"english\", \"danish\"]}");
    try {
      col.admin().createIndex(index);
      fail("No exception when index spec is invalid.");
    } catch (OracleException e) {
      Throwable t = e.getCause();
      String errMsg = "Invalid value for property language: expected STRING, found ARRAY.";
      assertEquals(errMsg, t.getMessage());
    }
    
    // Test when unknown field is presented
    String indexName1 = "jsonSearchIndex1_N1";
    index = db.createDocumentFromString("{\"name\":\"" + indexName1 + "\", \"unknown\":\"v1\"}");
    try {
      col.admin().createIndex(index);
      // bug27150687: no error when "unknown" field is presented in index spec
      //fail("No exception when unknown field is presented.");
      col.admin().dropIndex(indexName1);
    } catch (OracleException e) {
      Throwable t = e.getCause();
      String errMsg = "";
      assertEquals(errMsg, t.getMessage());
    }
    
    // Test to create search index for 1+ times 
    String indexName2 = "jsonSearchIndex2";
    index = db.createDocumentFromString("{\"name\":\"" + indexName2 + "\"}");
    col.admin().createIndex(index);
    
    String indexName3 = "jsonSearchIndex3";
    index = db.createDocumentFromString("{\"name\":\"" + indexName3 + "\", \"dataguide\":\"off\"}");
    try {
      // the second creation should cause error
      col.admin().createIndex(index);
      fail("No exception when seach index is created for 2 times on the same collection.");
    } catch (OracleException e) {
      SQLException sqlException = (SQLException) e.getCause();
      // ORA-29879: cannot create multiple domain indexes on a column list using same indextype
      assertTrue(sqlException.getMessage().contains("ORA-29879"));
    }
    
    // Test to create the duplicated name indexes on different collection
    OracleCollection col2 = dbAdmin.createCollection("testJsonSearchCol2");
    index = db.createDocumentFromString("{\"name\":\"" + indexName2 + "\"}");
    try {
      col2.admin().createIndex(index);
      // the error is suppressed by SODA Java layer
      // fail("No exception when the same name is used for 2 indexes");
    } catch (OracleException e) {
      assertTrue(e.getMessage().contains("An index with the specified name \"" + indexName2 + "\" already exists in the schema."));
    }
    
  }
  
  // more tests about search index
  public void testJsonSearchIndex2Clob() throws Exception {
    testJsonSearchIndex2("CLOB");
  }
  
  public void testJsonSearchIndex2Blob() throws Exception {
    testJsonSearchIndex2("BLOB");
  }
  
  public void testJsonSearchIndex2Varchar2() throws Exception {
    testJsonSearchIndex2("VARCHAR2");
  }

  public void testJsonSearchIndex2JSON() throws Exception {
    if (isCompatibleOrGreater(COMPATIBLE_20)) {
      testJsonSearchIndex2("JSON");
    }
  }
  
  private void testJsonSearchIndex2(String contentColumnType) throws Exception {
    if (isJDCSOrATPMode())
      if (!contentColumnType.equalsIgnoreCase("BLOB"))
        return;
    
    if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel))
      return;
    
    OracleDocument mDoc = client.createMetadataBuilder().keyColumnAssignmentMethod("CLIENT")
        .contentColumnType(contentColumnType).build();
    String colName = "testOrderby" + contentColumnType;
    OracleCollection col;
    if (isJDCSOrATPMode())
    {
      col = db.admin().createCollection(colName, null);
    } else
    {
      col = db.admin().createCollection(colName, mDoc);
    }
    OracleCollectionAdmin colAdmin = col.admin();
    final String key1="id001", key2="id002", key3="id003", key4="id004", key5="id005";
    OracleDocument doc = null, filterDoc = null;
    
    String[] key = new String[1000];
    for (int number = 0; number < 1000; number++) {
      String docStr = "{\"a\":{\"b\":{\"number\":" + number + ", \"string\": \"11." + number + "\"}}}";
      if (isJDCSOrATPMode())
      {
        doc = col.insertAndGet(db.createDocumentFromString(docStr));
        key[number] = doc.getKey();
      } else
      {
        doc = col.insertAndGet(db.createDocumentFromString("id-" + number, docStr));
        key[number] = doc.getKey();
      }
    }
    
    // Test "dataguide" property with on
    String indexName1 = "INDEX_A1";
    // ### Note: language is not officially supported, do not use in production!!!
    String indexSpec1 = "{ \"name\":\"" + indexName1 + "\", \"dataguide\":\"on\"," +
        "\"search_on\":\"text_value\", \"language\": \"english\"}";
    colAdmin.createIndex(db.createDocumentFromString(indexSpec1));
    
    filterDoc = db.createDocumentFromString("{ \"a.b.number\": {\"$eq\" : 99} }");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[99], col.find().filter(filterDoc).getOne().getKey());
    chkSearchIndexExplainPlan(col.find().filter(filterDoc), SrearchIndexType.numberIndex, indexName1);
    colAdmin.dropIndex(indexName1);
    
    // Test "dataguide" property with off
    String indexName2 = "INDEX_A2";
    // ### Note: language is not officially supported, do not use in production!!!
    String indexSpec2 = "{ \"name\":\"" + indexName2 + "\", \"dataguide\":\"off\"," +
        "\"search_on\":\"text_value\", \"language\": \"english\"}";
    colAdmin.createIndex(db.createDocumentFromString(indexSpec2));
    
    filterDoc = db.createDocumentFromString("{ \"a.b.number\": 199 }");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[199], col.find().filter(filterDoc).getOne().getKey());
    chkSearchIndexExplainPlan(col.find().filter(filterDoc), SrearchIndexType.numberIndex, indexName2);
    colAdmin.dropIndex(indexName2);
    
    // Test "search_on" property with "none"(enables data guide only and no index)
    String indexName3 = "INDEX_A3";
    // ### Note: language is not officially supported, do not use in production!!!
    String indexSpec3 = "{ \"name\":\"" + indexName3 + "\", \"dataguide\":\"on\"," +
        "\"search_on\":\"none\", \"language\": \"english\"}";
    colAdmin.createIndex(db.createDocumentFromString(indexSpec3));
    
    filterDoc = db.createDocumentFromString("{ \"a.b.number\": 299 }");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[299], col.find().filter(filterDoc).getOne().getKey());
    chkSearchIndexExplainPlan(col.find().filter(filterDoc), SrearchIndexType.noIndex, indexName1);
    colAdmin.dropIndex(indexName3);
    
    // Test "search_on" property with "text_value"
    String indexName4 = "INDEX_A4";
    // ### Note: language is not officially supported, do not use in production!!!
    String indexSpec4 = "{ \"name\":\"" + indexName4 + "\", \"dataguide\":\"on\"," +
        "\"search_on\":\"text_value\", \"language\": \"english\"}";
    colAdmin.createIndex(db.createDocumentFromString(indexSpec4));
    
    filterDoc = db.createDocumentFromString("{ \"a.b.number\": { \"$eq\" : 50} }");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[50], col.find().filter(filterDoc).getOne().getKey());
    chkSearchIndexExplainPlan(col.find().filter(filterDoc), SrearchIndexType.numberIndex, indexName4);
    colAdmin.dropIndex(indexName4);
    
    // Test "search_on" property with "text"
    String indexName5 = "INDEX_A5";
    // ### Note: language is not officially supported, do not use in production!!!
    String indexSpec5 = "{ \"name\":\"" + indexName5 + "\", \"dataguide\":\"on\"," +
        "\"search_on\":\"text\", \"language\": \"english\"}";
    colAdmin.createIndex(db.createDocumentFromString(indexSpec5));
    
    filterDoc = db.createDocumentFromString("{ \"a.b.number\": { \"$eq\" : 50} }");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[50], col.find().filter(filterDoc).getOne().getKey());
    chkSearchIndexExplainPlan(col.find().filter(filterDoc), SrearchIndexType.textIndex, indexName5);
    colAdmin.dropIndex(indexName5);
    
    // Test to cover "language" property values
    String indexName6, indexSpec6;
    // ### Note: language is not officially supported, do not use in production!!!
    String[] languages = {
        "english", "danish", "finnish", "dutch", "portuguese", "romanian", "german",
        "simplified_chinese","traditional_chinese","german_din","brazilian_portuguese",
        "french_canadian", "latin_american_spanish", "mexican_spanish",
        // bug27114976: "korean" index caused the incorrect query result 
        //"korean", 
        "swedish", "japanese", "norwegian", "catalan", "french", "spanish", "italian", 
        
    };
    
    for (String language : languages) {
      indexName6 = "INDEX_A6_" + language;
      indexSpec6 = "{ \"name\":\"" + indexName6 + "\", \"search_on\":\"text\", \"language\": \"" + language + "\"}";
      colAdmin.createIndex(db.createDocumentFromString(indexSpec6));
      // note: a match will be found for "11.10", but not for "11.100".
      filterDoc = db.createDocumentFromString("{ \"a.b.string\": {\"$contains\":\"11.10\"} }");
      assertEquals(1, col.find().filter(filterDoc).count());
      chkSearchIndexExplainPlan(col.find().filter(filterDoc), SrearchIndexType.textIndex, indexName6);
      colAdmin.dropIndex(indexName6);
    }
    
    colAdmin.drop();
  }

  public void testDBObjectNameAndSchemaGetters() throws Exception {
    OracleCollection col = db.admin().createCollection("abc");

    //Validate if is a native collection
    OracleDocument metadata = col.admin().getMetadata();
    String sMetadata = metadata.getContentAsString();
    JsonReader jsonReader = Json.createReader(new StringReader(sMetadata));
    JsonObject jsonObject = jsonReader.readObject();
    jsonReader.close();

    boolean isNative = false;
    if (jsonObject.containsKey("native")) {
      isNative = jsonObject.getBoolean("native");
    }
    String  collectionRef = (isNative) ? "abc" : "ABC";

    assertEquals(collectionRef, col.admin().getDBObjectName());
    assertEquals("SCOTT", col.admin().getDBObjectSchemaName());

    col = db.admin().createCollection("dFg");
    assertEquals("dFg", col.admin().getDBObjectName());
    assertEquals("SCOTT", col.admin().getDBObjectSchemaName());
  }
}
