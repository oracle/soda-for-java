/* Copyright (c) 2014, 2015, Oracle and/or its affiliates. 
All rights reserved.*/

/*
   DESCRIPTION
    OracleCollectionAdmin tests
 */

/**
 *  @author  Vincent Liu
 */
package oracle.json.tests.soda;

import javax.json.JsonNumber;
import javax.json.JsonString;
import javax.json.JsonValue;

import oracle.soda.OracleCollectionAdmin;
import oracle.soda.OracleException;
import oracle.soda.OracleCollection;
import oracle.soda.OracleDocument;

import oracle.json.testharness.SodaTestCase;
import oracle.json.testharness.ConnectionFactory;

public class test_OracleCollectionAdmin extends SodaTestCase {
  
  public void testGetName() throws Exception {
    OracleDocument metaDoc = client.createMetadataBuilder().build();

    String colName = "testGetName";
    OracleCollection col = dbAdmin.createCollection("testGetName", metaDoc);
    OracleCollectionAdmin colAdmin = col.admin();
    assertEquals(colName, colAdmin.getName());
  }

  public void testIsHeterogeneous() throws Exception {
    
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
    
    try {
      // Test with mediaTypeColumn, but content type = "CLOB" 
      client.createMetadataBuilder().mediaTypeColumnName("CONTENT_TYPE").contentColumnType("CLOB")
        .build();
      fail("Expected exception");
    } catch (OracleException e) {
      assertEquals(msg, e.getMessage());
    }
    
    try {
      // Test with mediaTypeColumn, but content type = "NCLOB" 
      client.createMetadataBuilder().mediaTypeColumnName("CONTENT_TYPE").contentColumnType("NCLOB")
        .build();
      fail("Expected exception");
    } catch (OracleException e) {
      assertEquals(msg, e.getMessage());
    }


    OracleCollection col6 = dbAdmin.createCollection("testIsHeterogeneous6");
    assertEquals(false, col6.admin().isHeterogeneous());
    try {
      // try to insert non-JSON document into non-heterogeneous collection
      col6.insert(db.createDocumentFromString(null, "abcd efgh ", "text/plain"));
      fail("No exception when inserting non-JSON document into non-heterogeneous collection");
    } catch (OracleException e) {
      // Expect an OracleException
      // ORA-02290: check constraint (SYS_C0010955) violated
      Throwable t = e.getCause();
      assertTrue(t.getMessage().contains("ORA-02290"));
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
    
    // Test with no mediaTypeColumn, and content type = "CLOB" 
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
    OracleDocument metaDoc = client.createMetadataBuilder().removeOptionalColumns().readOnly(false).build();
    
    OracleCollection col = dbAdmin.createCollection("testIsReadOnly", metaDoc);
    assertEquals(false, col.admin().isReadOnly());
    col.insert(db.createDocumentFromString(null, "{ \"data\" : 1001 }", null));
    assertEquals(1, col.find().count());
    
    // Test with READONLY = true
    OracleDocument metaDoc2 = client.createMetadataBuilder().removeOptionalColumns().readOnly(true).build();
    
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
    OracleDocument metaDoc = client.createMetadataBuilder().build();
    
    // Test to drop an empty collection 
    OracleCollection col = dbAdmin.createCollection("testDrop", metaDoc);
    OracleCollectionAdmin colAdmin = col.admin();
    colAdmin.drop();
    
    // Test to drop again
    colAdmin.drop();
    
    // Test to drop an non-empty collection
    OracleCollection col2 = dbAdmin.createCollection("testDrop2", metaDoc);
    OracleCollectionAdmin colAdmin2 = col2.admin();
    for (int i = 1; i <= 100; i++) {
        col2.insert(db.createDocumentFromString("{ \"value\" : \"v" + i + "\" }"));
    }
    colAdmin2.drop();
    
    // Test to drop the collection mapping to the existing table
    OracleDocument metaDoc3 = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("CLIENT")
        .versionColumnMethod("NONE").tableName("SODATBL").build();
    OracleCollection col3 = dbAdmin.createCollection("testDrop3", metaDoc3);
    OracleCollectionAdmin colAdmin3 = col3.admin();
    colAdmin3.drop();
    
  }

  public void testTruncate() throws Exception {
    OracleDocument metaDoc = client.createMetadataBuilder().build();
 
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
    OracleDocument metaDoc2 = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("CLIENT")
        .versionColumnMethod("NONE").tableName("SODATBL").build();
    OracleCollection col2 = dbAdmin.createCollection("testTruncate2", metaDoc2);
    OracleCollectionAdmin colAdmin2 = col2.admin();
    
    col2.insert(db.createDocumentFromString("id-1", "{ \"data\" : \"vvv\" }"));
    assertEquals(1, col2.find().count());
    colAdmin2.truncate();
    assertEquals(0, col2.find().count());

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
      assertEquals("ORA-00942: table or view does not exist\n", t.getMessage());
    }

  }

  public void testDropIndex() throws Exception {

    OracleDocument mDoc = client.createMetadataBuilder().build();
    OracleCollection col = dbAdmin.createCollection("testDropIndex", mDoc);
    OracleCollectionAdmin colAdmin = col.admin();

    col.insert(db.createDocumentFromString("{ \"name\": \"Andy Murray\" }"));
    col.insert(db.createDocumentFromString("{ \"name\": \"Rafael Nadal\" }"));
    col.insert(db.createDocumentFromString("{ \"name\": \"Roger Federer\" }"));

    String indexSpec =
      "{ \"name\":\"NAME_INDEX\", \"unique\":true, \n" +
      "   \"fields\": [ { \"path\":\"name\", \"datatype\":\"string\", \"order\":\"asc\" } ]\n" +
      "}";

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
    
    String fullIndexName = "indexAll-1"; 
    colAdmin.indexAll(fullIndexName);
    
    // Test to drop full text index
    colAdmin.dropIndex(fullIndexName);
    
  }

  private void testIndexAllwithCol(OracleCollection col) throws Exception {
    OracleCollectionAdmin colAdmin = col.admin();
    
    col.insert(db.createDocumentFromString("{ \"data\" : \"v1\" }"));
    col.insert(db.createDocumentFromString(null));
    if (colAdmin.isHeterogeneous()) {
      col.insert(db.createDocumentFromString(null, "abcd", "text/plain"));
      col.insert(db.createDocumentFromString(null, null, "text/plain"));
    }
    
    String indexName1 = "index1";
    
    // Call indexAll for first time
    colAdmin.indexAll(indexName1);
    
    try { 
      // Call indexAll for second time
      colAdmin.indexAll("index2");
      fail("No exception when creating full-text index for second time");
    } catch (OracleException e) {
      // Expect an OracleException
      //ORA-29879: cannot create multiple domain indexes on a column list using same indextype
      Throwable t = e.getCause();
      assertTrue(t.getMessage().contains("ORA-29879"));
    }
    
    try { 
      // Test with already used index name
      colAdmin.indexAll(indexName1);
      // we lack any ability to check if the index is the same or not 
      // so for now it's just ignored if the name matches
      // fail("No exception when the specified index name has been used");
    } catch (OracleException e) {
      // Expect an OracleException
      //assertEquals("", e.getMessage());
    }
    
    colAdmin.dropIndex(indexName1);
    
    try { 
      // Test with null collection name
      colAdmin.indexAll(null);
      fail("No exception when the index name is null");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("indexName argument cannot be null.", e.getMessage());
    }
        
    try { 
      // Test with null collection name
      colAdmin.indexAll(null, "english");
      fail("No exception when the index name is null");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("indexName argument cannot be null.", e.getMessage());
    }
   
    // Test with null for language
    // then the default language, english, will be used
    colAdmin.indexAll("index3", null);
    colAdmin.dropIndex("index3");
    
    try { 
      // Test with invalid language
      colAdmin.indexAll("index4", "invalidLanguage");
      fail("No exception when language is invalid");
    } catch (OracleException e) {
      // Expect an OracleException
      Throwable t = e.getCause();
      assertEquals("Language invalidLanguage is not recognized.", t.getMessage());
    }

    colAdmin.dropIndex("index4");
    
    // Test indexAll(indexName, language)
    colAdmin.indexAll("index5", "english");
    
    try { 
      // try to create the second full text index by indexAll(indexName, language)
      colAdmin.indexAll("index6", "english");
      fail("No exception when creating full-text index for second time");
    } catch (OracleException e) {
      // Expect an OracleException
      //ORA-29879: cannot create multiple domain indexes on a column list using same indextype
      Throwable t = e.getCause();
      assertTrue(t.getMessage().contains("ORA-29879"));
    }
    
    try { 
      // try to create the second full text index by indexAll(indexName)
      colAdmin.indexAll("index6");
      fail("No exception when creating full-text index for second time");
    } catch (OracleException e) {
      // Expect an OracleException
      //ORA-29879: cannot create multiple domain indexes on a column list using same indextype
      Throwable t = e.getCause();
      assertTrue(t.getMessage().contains("ORA-29879"));
    }
    
    colAdmin.dropIndex("index5");
    
    // Test indexAll on an empty collection
    col.find().remove();
    assertEquals(0, col.find().count());
    
    colAdmin.indexAll("Index6", "english");
    colAdmin.dropIndex("Index6");
    
    colAdmin.indexAll("Index6");
    colAdmin.dropIndex("Index6");
  }
  
  public void testIndexAll() throws Exception {
    
    // Test with contentColumnType=BLOB
    OracleDocument mDoc = client.createMetadataBuilder()
        .contentColumnType("BLOB")
        .mediaTypeColumnName("MediaTypeCol").build();
    OracleCollection col = dbAdmin.createCollection("testIndexAll1", mDoc);
    testIndexAllwithCol(col);
    
    // Test with contentColumnType=CLOB
    OracleDocument mDoc2 = client.createMetadataBuilder()
        .contentColumnType("CLOB").build();
    OracleCollection col2 = dbAdmin.createCollection("testIndexAll2", mDoc2);
    testIndexAllwithCol(col2);
    
    // Test with contentColumnType=VARCHAR2
    OracleDocument mDoc3 = client.createMetadataBuilder()
        .contentColumnType("VARCHAR2").build();
    OracleCollection col3 = dbAdmin.createCollection("testIndexAll3", mDoc3);
    testIndexAllwithCol(col3);
    
    // Test with contentColumnType=RAW
    OracleDocument mDoc5 = client.createMetadataBuilder()
        .contentColumnType("RAW").build();
    OracleCollection col5 = dbAdmin.createCollection("testIndexAll5", mDoc5);
    testIndexAllwithCol(col5);
    
    // IndexAll is NOT enabled for contentColumnType=NVARCHAR2 or NCLOB
    OracleDocument mDoc4 = client.createMetadataBuilder()
        .contentColumnType("NVARCHAR2").build();
    OracleCollection col4 = dbAdmin.createCollection("testIndexAll4", mDoc4);
    //testIndexAllwithCol(col4);
    try { 
      col4.admin().indexAll("indexAll4");
      fail("No exception when call indexAll() on content columnType NVARCHAR2 collection");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("indexAll is not implemented for content columns with type NVARCHAR2.", e.getMessage());
    }
 
    OracleDocument mDoc6 = client.createMetadataBuilder()
        .contentColumnType("NCLOB").build();
    OracleCollection col6 = dbAdmin.createCollection("testIndexAll6", mDoc6);
    //testIndexAllwithCol(col6);
    try { 
      col6.admin().indexAll("indexAll6");
      fail("No exception when call indexAll() on content columnType NCLOB collection");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("indexAll is not implemented for content columns with type NCLOB.", e.getMessage());
    }
 
  }
 
  public void testCreateIndex() throws Exception {

    OracleDocument mDoc = client.createMetadataBuilder().contentColumnType("CLOB").build();
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

    // Test when unique and language are missing(the default value will be used)
    // Test with "string" and "numer" datatypes
    // Test with "asc" and "desc" for "order" value
    String indexSpec1 =
      "{ \"name\":\"STUDENT_INDEX1\", \n" +
      "  \"fields\": [\n" +
      "    { \"path\":\"name\", \"datatype\":\"string\", \"maxLength\":100, \"order\":\"asc\"}, \n" +
      "    { \"path\":\"num\", \"datatype\":\"number\", \"order\":\"desc\"} ]\n" +
      "}";
    colAdmin.createIndex(db.createDocumentFromString(indexSpec1));

    colAdmin.dropIndex("STUDENT_INDEX1");

    // Negative test: same as above, but datatype is number and maxLength is specified
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
      "{ \"name\":\"STUDENT_INDEX1\", \"unique\":true, \"fields\": [ { \"path\":\"name\" }] }";
    colAdmin.createIndex(db.createDocumentFromString(indexSpecNeg1));
    
    colAdmin.dropIndex("STUDENT_INDEX1");

    // Test with "date" and "datetime" for datatypes
    // Test with 1 and -1 for "order" value
    String indexSpec2 =
      "{ \"name\":\"STUDENT_INDEX2\", \n" +
      "  \"unique\":true,\n" +
      "  \"fields\": [\n" +
      // createIndex() reported blocked by ORA-01858 and ORA-01861, because of the existing documents
      "    { \"path\":\"birthday\", \"datatype\":\"date\", \"order\":1 } \n" +
      "  , { \"path\":\"loggingtime\", \"datatype\":\"datetime\", \"order\":-1} \n" +
      "   ] \n" +
      "}";
    
    colAdmin.createIndex(db.createDocumentFromString(indexSpec2));
    col.insert(db.createDocumentFromString("{ \"name\":\"Candy\", \"num\":1009, \"birthday\":\"1998-08-21\"}"));

    // time zone is NOT supported
    col.insert(db.createDocumentFromString("{ \"name\":\"Jay\", \"num\":1001, \"loggingtime\":\"2004-05-03T17:30:08.45\" }"));

    colAdmin.dropIndex("STUDENT_INDEX2");

    // Negative test: same as above, but maxLength is specified with datatype datetime.
    indexSpec2 =
      "{ \"name\":\"STUDENT_INDEX2\", \n" +
      "  \"unique\":true,\n" +
      "  \"language\":\"english\", \n" +
      "  \"fields\": [\n" +
      // createIndex() reported blocked by ORA-01858 and ORA-01861, because of the existing documents
      "    { \"path\":\"birthday\", \"datatype\":\"date\", \"order\":1 } \n" +
      "  , { \"path\":\"loggingtime\", \"datatype\":\"datetime\", \"maxLength\":100, \"order\":-1} \n" +
      "   ] \n" +
      "}";

    try
    {
      colAdmin.createIndex(db.createDocumentFromString(indexSpec2));
      junit.framework.Assert.fail("No error when creating an index with datatype datetime and maxLength");
    }
    catch(OracleException e) {
      Throwable t = e.getCause();
      assertTrue(t.getMessage().contains("\"maxLength\" can only be specified if the \"datatype\" value is \"string\""));
    }

    // Negative test: both "language" and "fields" specified
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
    String indexSpec3 =
      "{ \"name\":\"STUDENT_INDEX3\", \"fields\": [ { \"path\":\"name\" }] }";

    colAdmin.createIndex(db.createDocumentFromString(indexSpec3));

    colAdmin.dropIndex("STUDENT_INDEX3");
    
    // Negative test: "unique" must only be specified with "fields".
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

    // Negative test: "singleton" must only be specified with "fields".
    indexSpec4 =
      "{ \"name\":\"STUDENT_INDEX4\", \n" +
      "  \"singleton\":true,\n" +
      "  \"language\":\"english\" \n" +
      "}";
    try {
      colAdmin.createIndex(db.createDocumentFromString(indexSpec4));
      junit.framework.Assert.fail("No error when \"singleton\" is specified with \"fields\"");
    } 
    catch(OracleException e) {
      Throwable t = e.getCause();
      assertTrue(t.getMessage().contains("In an index specification, \"singleton\" " +
                                         "cannot be specified without specifying \"fields\"."));
    }

    // Test with no fields array in index(means a full-text index)
    indexSpec4 =
      "{ \"name\":\"STUDENT_INDEX4\", \n" +
      "  \"language\":\"english\" \n" +
      "}";
    colAdmin.createIndex(db.createDocumentFromString(indexSpec4));

    String indexSpec5 = "{ \"name\":\"STUDENT_INDEX5\" }";
    try {
      // Try to create the second full text index
      colAdmin.createIndex(db.createDocumentFromString(indexSpec5));
      fail("No error when create a second full text index");
    } 
    catch(OracleException e) {
      //ORA-29879: cannot create multiple domain indexes on a column list using same indextype
      Throwable t = e.getCause();
      assertTrue(t.getMessage().contains("ORA-29879"));
    }
    colAdmin.dropIndex("STUDENT_INDEX5");

    try {
      // Try to create the second full text index by indexAll
      colAdmin.indexAll("indexAll");
      fail("No error when create a second full text index");
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
        "  \"fields\": [ { \"path\":\"num\", \"datatype\":\"number\" }] }";
      colAdmin.createIndex(db.createDocumentFromString(indexSpecN8));
      fail("No error when there is conflict between the specified index and the existing documents");
    }
    catch(OracleException e) {
      //ORA-01452: cannot CREATE UNIQUE INDEX; duplicate keys found
      Throwable t = e.getCause();
      assertTrue(t.getMessage().contains("ORA-01452"));
    }

    String indexSpecN9 =
      "{ \"name\":\"STUDENT_INDEXN9\", \"language\" : \"english\" }";
    
    try { 
      // full text index is NOT supported for NVARCHAR2 contentColumnType
      OracleDocument mDoc2 = client.createMetadataBuilder()
          .contentColumnType("NVARCHAR2").build();
      OracleCollection col2 = dbAdmin.createCollection("testCreateIndex2", mDoc2);
      col2.admin().createIndex(db.createDocumentFromString(indexSpecN9));
      fail("No exception when create text index on NVARCHAR2 collection");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("indexAll is not implemented for content columns with type NVARCHAR2.", e.getMessage());
    }
    
    try { 
      // full text index is NOT supported for NCLOB contentColumnType
      OracleDocument mDoc3 = client.createMetadataBuilder()
          .contentColumnType("NCLOB").build();
      OracleCollection col3 = dbAdmin.createCollection("testCreateIndex3", mDoc3);
      col3.admin().createIndex(db.createDocumentFromString(indexSpecN9));
      fail("No exception when create text index on NCLOB collection");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("indexAll is not implemented for content columns with type NCLOB.", e.getMessage());
    }

  } 

  public void testCreateIndexSinglenton() throws Exception {

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
      "  \"singleton\" : true, " +
      "  \"fields\": [" +
      "    { \"path\":\"name\", \"datatype\":\"string\", \"maxLength\":100, \"order\":\"asc\"}] " +
      "}";

    colAdmin.createIndex(db.createDocumentFromString(indexSpec1));
    colAdmin.dropIndex("STUDENT_INDEX1");
    col.admin().drop();
  }

  public void testCreateIndexSinglentonNeg1() throws Exception {

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
      "  \"singleton\" : true, " +
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
       assertTrue(c.getMessage().contains("JSON_VALUE evaluated to non-scalar value"));
    }

    colAdmin.dropIndex("STUDENT_INDEX1");
    col.admin().drop();
  }


  public void testCreateIndexSinglentonNeg2() throws Exception {

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
      "  \"singleton\" : true," +
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
       assertTrue(c.getMessage().contains("JSON_VALUE evaluated to no value"));
    }

    colAdmin.dropIndex("STUDENT_INDEX1");
    col.admin().drop();
  }

}
