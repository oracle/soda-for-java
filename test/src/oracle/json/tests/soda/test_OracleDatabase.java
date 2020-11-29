/* Copyright (c) 2014, 2020, Oracle and/or its affiliates. 
All rights reserved.*/

/*
   DESCRIPTION
    OracleCollection tests
 */

/**
 *  @author  Vincent Liu
 */

package oracle.json.tests.soda;

import java.io.ByteArrayInputStream;

import oracle.soda.OracleException;
import oracle.soda.OracleCollection;
import oracle.soda.OracleDocument;

import oracle.soda.rdbms.impl.OracleDocumentImpl;
import oracle.soda.rdbms.impl.OracleDatabaseImpl;

import oracle.json.testharness.SodaTestCase;

public class test_OracleDatabase extends SodaTestCase {

  // Test method for openCollection(collectionName)
  public void testOpenCollection() throws OracleException {
   
    OracleDocument metaDoc = null;
    if (isJDCSOrATPMode())
    {
      // ### replace with new builder once it becomes available
      metaDoc = db.createDocumentFromString("{\"keyColumn\":{\"name\":\"ID\",\"sqlType\":\"VARCHAR2\",\"maxLength\":255,\"assignmentMethod\":\"UUID\"},\"contentColumn\":{\"name\":\"JSON_DOCUMENT\",\"sqlType\":\"BLOB\"},\"lastModifiedColumn\":{\"name\":\"LAST_MODIFIED\"},\"versionColumn\":{\"name\":\"VERSION\",\"method\":\"UUID\"},\"creationTimeColumn\":{\"name\":\"CREATED_ON\"},\"readOnly\":false}");
    }
    else
    {
      metaDoc = client.createMetadataBuilder().build();
    }

    String colName = "testOpenCollection";
    dbAdmin.createCollection(colName, metaDoc);
    
    // Test openCollection(...) with known collectionName
    OracleCollection col = db.openCollection(colName);
    assertNotNull(col);
    assertEquals(colName, col.admin().getName());
    
    // Test openCollection(...) when the collection is created without metadata
    String colName2 = "testOpenCollectionA";
    dbAdmin.createCollection(colName2);
    OracleCollection col2 = db.openCollection(colName2);
    assertNotNull(col2);
    assertEquals(colName2, col2.admin().getName());

    // Test openCollection(...) with unknown collectionName
    OracleCollection col3 = db.openCollection("collectionUnknownA");
    assertNull(col3);

    // Test openCollection(...) with null
    try {
      db.openCollection(null);
      fail("No exception for null collection name");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("collectionName argument cannot be null.", e.getMessage());
    }

  }
  
  final static int fromString = 1;
  final static int fromStream = 2;
  final static int fromByteArray = 3;
  
  private void testCreateDocument(int from) throws Exception {
    if(from != fromString && from != fromStream && from != fromByteArray)
      throw new Exception("unknown input parameter: " + from);
    
    OracleDocument metaDoc;
    if (isJDCSOrATPMode())
    {
      metaDoc = null;
    } else
    {
      metaDoc = client.createMetadataBuilder().mediaTypeColumnName("CONTENT_TYPE").build();
    }

    OracleCollection col = dbAdmin.createCollection("testCreateDocument", metaDoc);
    OracleDocument doc = null;
    OracleDatabaseImpl dbImpl = (OracleDatabaseImpl) db;

    // Test with key=null, content=JSON data, contentType=null
    if (from == fromString)
      doc = db.createDocumentFromString(null, "{ \"value\" : \"a\" }", null);
    else if (from == fromStream)
      doc = dbImpl.createDocumentFromStream(null,
            new ByteArrayInputStream(new String("{ \"value\" : \"a\" }").getBytes()), null);
    else if (from == fromByteArray)
      doc = db.createDocumentFromByteArray(null, new String("{ \"value\" : \"a\" }").getBytes(), null);
    
    col.insertAndGet(doc);
    assertNotNull(doc);
    
    if (isJDCSOrATPMode())
      return;
    
    OracleDocument metaDoc2 = client.createMetadataBuilder().keyColumnAssignmentMethod("CLIENT").build();
    OracleCollection col2 = dbAdmin.createCollection("testCreateDocument2", metaDoc2);
    OracleDocument doc2 = null;
    // Test with key=new key, content=JSON data, contentType=null
    if (from == fromString)
      doc2 = db.createDocumentFromString("id-1", "{ \"value\" : \"a\" }", null);
    else if (from == fromStream)
      doc2 = dbImpl.createDocumentFromStream("id-1",
             new ByteArrayInputStream(new String("{ \"value\" : \"a\" }").getBytes()), null);
    else if (from == fromByteArray)
      doc2 = db.createDocumentFromByteArray("id-1", new String("{ \"value\" : \"a\" }").getBytes(), null);
    
    col2.insertAndGet(doc2);
    assertNotNull(doc2);
    
    // Test with key=known key, content=JSON data, contentType=null
    // this will not cause any error before inserting it into the collection
    if (from == fromString)
      doc2 = db.createDocumentFromString("id-1", "{ \"value\" : \"b\" }", null);
    else if (from == fromStream) {
      doc2 = dbImpl.createDocumentFromStream("id-1",
             new ByteArrayInputStream(new String("{ \"value\" : \"b\" }").getBytes()), null);
    }
    else if (from == fromByteArray)
      doc2 = db.createDocumentFromByteArray("id-1", new String("{ \"value\" : \"b\" }").getBytes(), null);
    assertNotNull(doc2);
    try {
      // "id-1" should cause conflict
      col2.insertAndGet(doc2);
      fail("No exception when there is id conflict");
    } catch (OracleException e) {
      // Expect an OracleException
      // ORA-00001: unique constraint (SYS_C0010210) violated
      Throwable t = e.getCause();
      assertEquals(true, t.getMessage().contains("ORA-00001"));
    } 
      
    OracleDocument metaDoc3 = client.createMetadataBuilder().mediaTypeColumnName("CONTENT_TYPE").keyColumnAssignmentMethod("CLIENT").build();
    OracleCollection col3 = dbAdmin.createCollection("testCreateDocument3", metaDoc3);
    OracleDocument doc3 = null;
    
    // Test with key=known key, content=JSON data, contentType=null
    if (from == fromString)
      doc3 = db.createDocumentFromString("id-1", "{ \"value\" : \"b\" }", null);
    else if (from == fromStream)
      doc3 = dbImpl.createDocumentFromStream("id-1",
             new ByteArrayInputStream(new String("{ \"value\" : \"b\" }").getBytes()), null);
    else if (from == fromByteArray)
      doc3 = db.createDocumentFromByteArray("id-1", new String("{ \"value\" : \"b\" }").getBytes(), null);
    assertNotNull(doc3);
    col3.insertAndGet(doc3);
    
    // Test with key=known key, content=JSON data, contentType="application/json"
    if (from == fromString)
      doc3 = db.createDocumentFromString("id-1", "{ \"value\" : \"b\" }", "application/json");
    else if (from == fromStream)
      doc3 = dbImpl.createDocumentFromStream("id-1",
          new ByteArrayInputStream(new String("{ \"value\" : \"b\" }").getBytes()), "application/json");
    else if (from == fromByteArray)
      doc3 = db.createDocumentFromByteArray("id-1", new String("{ \"value\" : \"b\" }").getBytes(), "application/json");
    assertNotNull(doc3);
    try {
      // "id-1" should cause conflict
      col3.insertAndGet(doc3);
      fail("No exception when there is id conflict");
    } catch (OracleException e) {
      // Expect an OracleException
      // ORA-00001: unique constraint (SYS_C0010210) violated
      Throwable t = e.getCause();
      assertEquals(true, t.getMessage().contains("ORA-00001"));
    }
    
    // Test with non-JSON data
    if (from == fromString)
      doc3 = db.createDocumentFromString("id-2", "{ 123456", "text/plain");
    else if (from == fromStream)
      doc3 = dbImpl.createDocumentFromStream("id-2",
          new ByteArrayInputStream(new String("{ 123456").getBytes()), "text/plain");
    else if (from == fromByteArray)
      doc3 = db.createDocumentFromByteArray("id-2", new String("{ 123456").getBytes(), "text/plain");
    
    assertNotNull(doc3);
    col3.insertAndGet(doc3);
      
    OracleDocument metaDoc4 = client.createMetadataBuilder().removeOptionalColumns().keyColumnAssignmentMethod("CLIENT").build();
    // Test with contentType, and but no related columns for the collection
    OracleCollection col4 = dbAdmin.createCollection("testCreateDocument4", metaDoc4);
    OracleDocument doc4 = null;
    if (from == fromString)
      doc4 = db.createDocumentFromString("id-x", "{ \"value\" : \"ddd\" }", "application/json");
    else if (from == fromStream)
      doc4 = dbImpl.createDocumentFromStream("id-x",
          new ByteArrayInputStream(new String("{ \"value\" : \"ddd\" }").getBytes()), "application/json");
    else if (from == fromByteArray)
      doc4 = db.createDocumentFromByteArray("id-x", new String("{ \"value\" : \"ddd\" }").getBytes(), "application/json");
    
    assertNotNull(doc4);
    // version and contentTyps should be ignored
    col4.insertAndGet(doc4);
    
    // Test with null for content(now null content is allowed)
    if (from == fromString)
      doc4 = db.createDocumentFromString("id-y", null, null);
    else if (from == fromStream)
      doc4 = dbImpl.createDocumentFromStream("id-y", null, null);
    else if (from == fromByteArray)
      doc4 = db.createDocumentFromByteArray("id-y", null, null);
    // null content is allowed for creating
    assertNull(doc4.getContentAsString());
    assertNull(doc4.getContentAsByteArray());
    
    // null content is allowed for inserting 
    col4.insert(doc4);
    doc4 = col4.findOne("id-y");
    OracleDocumentImpl doc4Impl = (OracleDocumentImpl) doc4;
    assertNull(doc4Impl.getContentAsStream());
    
    // Test with contentType="application/json", but content is non-JSON data
    try {
      if (from == fromString)
        doc = db.createDocumentFromString(null, "{ \"data\"", "application/json");
      else if (from == fromStream)
        doc = dbImpl.createDocumentFromStream(null,
            new ByteArrayInputStream(new String("{ \"data\"").getBytes()), "application/json");
      else if (from == fromByteArray)
        doc = db.createDocumentFromByteArray(null, new String("{ \"data\"").getBytes(), "application/json");
      
      col.insertAndGet(doc);
      fail("No exception when contentType=\"application/json\", but content is non-JSON data");
    } catch (OracleException e) {
      // Expect an OracleException
      // ORA-02290: check constraint (SYS_C0011171) violated
      Throwable t = e.getCause();
      assertEquals(true, t.getMessage().contains("ORA-02290"));
    } 
    
    // Pass contentType=null, but content is not JSON data, and then do inserting/replacing 
    try {
      if (from == fromString)
        doc = db.createDocumentFromString(null, "{ abc }", null);
      else if (from == fromStream)
        doc = dbImpl.createDocumentFromStream(null,
            new ByteArrayInputStream(new String("{ abc }").getBytes()), null);
      else if (from == fromByteArray)
        doc = db.createDocumentFromByteArray(null, new String("{ abc }").getBytes(), null);
      
      col.insertAndGet(doc);
      fail("No exception when contentType=null, but content is non-JSON data");
    } catch (OracleException e) {
      // Expect an OracleException
      // ORA-02290: check constraint (SYS_C0011171) violated
      Throwable t = e.getCause();
      assertEquals(true, t.getMessage().contains("ORA-02290"));
    } 
    
  }
  
  public void testCreateDocumentFromString() throws Exception {
    testCreateDocument(fromString);
  }
  
  public void testCreateDocumentFromStream() throws Exception {
    testCreateDocument(fromStream);
  }
  
  public void testCreateDocumentFromByteArray() throws Exception {
    testCreateDocument(fromByteArray);
  }
  
} 
