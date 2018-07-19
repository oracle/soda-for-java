/* Copyright (c) 2014, 2015, Oracle and/or its affiliates. 
All rights reserved.*/

/*
   DESCRIPTION
    OracleCollection tests
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


import oracle.soda.OracleException;
import oracle.soda.OracleCollection;
import oracle.soda.OracleCollectionAdmin;
import oracle.soda.OracleDocument;

import oracle.soda.rdbms.impl.OracleOperationBuilderImpl;
import oracle.soda.rdbms.impl.TableCollectionImpl;
import oracle.soda.rdbms.impl.OracleDocumentFragmentImpl;
import oracle.soda.rdbms.impl.OracleDocumentImpl;
import oracle.soda.rdbms.impl.OracleDatabaseImpl;

import oracle.json.testharness.SodaTestCase;

public class test_OracleCollection extends SodaTestCase {
  
  public void testFindOne() throws Exception {
    OracleDocument metaDoc = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("CLIENT").build();
    OracleCollection col = dbAdmin.createCollection("testFindOne", metaDoc);
    
    col.insert(db.createDocumentFromString("id-1", "{ \"data\" : 1 }", null));
    col.insert(db.createDocumentFromString("id-2", "{ \"data\" : 2 }", null));
    col.insert(db.createDocumentFromString("id-3", "{ \"data\" : 3 }", null));
    
    // Test with valid key
    OracleDocument doc = col.findOne("id-2");
    assertEquals("id-2", doc.getKey());
    assertEquals("{ \"data\" : 2 }", new String(doc.getContentAsByteArray(), "UTF-8"));
    
    // Test with an unknown key
    assertNull(col.findOne("id-x"));
    
    try {
      // Pass null for key
      col.findOne(null);
      fail("No exception when key is null");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("key argument cannot be null.", e.getMessage());
    }
    
  }

  public void testInsert() throws Exception {
    
    // Configured to auto-generate key
    OracleDocument metaDoc = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("UUID").build();
    OracleCollection col = dbAdmin.createCollection("testInsert", metaDoc);
    
    // Pass document without key
    col.insert(db.createDocumentFromString("{ \"data\" : 1234 }"));
    OracleDocument doc = col.find().getOne();
    assertNotNull(doc.getKey());
    assertEquals("{ \"data\" : 1234 }", new String(doc.getContentAsByteArray(), "UTF-8"));
    
    // Pass document with key
    try {
      col.insert(db.createDocumentFromString("id-x", "{ \"data\" : 2000 }"));
      fail("No exception when key is provided," +
                                  "and the collection has auto-generated keys");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals(e.getMessage(), "The collection is not configured with client-assigned keys," +
                                   " but the input document has a key.");
    }
    
    try {
      // Pass null for key
      col.insert((OracleDocument)null);
      fail("No exception when document is null");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("document argument cannot be null.", e.getMessage());
    }
    
    // Inserting document with null content is allowed
    doc = db.createDocumentFromString(null, null, null);
    assertNull(doc.getContentAsByteArray());
    col.insert(doc);
    
    // Configured to client-assigned key
    OracleDocument metaDoc2 = client.createMetadataBuilder().mediaTypeColumnName("CONTENT_TYPE")
        .keyColumnAssignmentMethod("CLIENT").build();
    
    OracleCollection col2 = dbAdmin.createCollection("testInsert2", metaDoc2);
    
    // Pass document with key
    col2.insert(db.createDocumentFromString("id-1", "{ \"data\" : \"abcd\" }", null));
    OracleDocument doc2 = col2.find().key("id-1").getOne();
    assertEquals("id-1", doc2.getKey());
    assertEquals("{ \"data\" : \"abcd\" }", new String(doc2.getContentAsByteArray(), "UTF-8"));
    
    // Pass document with non-JSON content
    col2.insert(db.createDocumentFromString("id-2", "Hello world!", "text/plain"));
    doc2 = col2.find().key("id-2").getOne();
    assertEquals("id-2", doc2.getKey());
    assertEquals("Hello world!", new String(doc2.getContentAsByteArray(), "UTF-8"));
    
    try {
      // Pass document without key (but configured to client-assigned key)
      col2.insert(db.createDocumentFromString(null, "abcd", "text/plain"));
      fail("No exception when document has not key");
    } catch (OracleException e) {
      // Expect an OracleException
      // ORA-01400: cannot insert NULL into ("testInsert2"."ID")
      Throwable cause = e.getCause();
      assertTrue(cause.getMessage().contains("ORA-01400"));
    }
    
    try {
      // Pass document with non-JSON content
      col2.insert(db.createDocumentFromString("id-3", "abcd", "application/json"));
      fail("No exception when document content is non-JSON");
    } catch (OracleException e) {
      // Expect an OracleException
      // ORA-02290: check constraint (SYS_C0023029) violated
      Throwable cause = e.getCause();
      assertTrue(cause.getMessage().contains("ORA-02290"));
    }
    
    // Test when collection is configured to read-only
    OracleDocument metaDoc3 = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("CLIENT").readOnly(true).build();
    OracleCollection col3 = dbAdmin.createCollection("testInsert3", metaDoc3);
    
    try {
      // Pass document with non-JSON content
      col3.insert(db.createDocumentFromString("id-1", "{ \"data\" : 1 }", "application/json"));
      fail("No exception when inserting document into a read-only collection");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Collection testInsert3 is read-only, insert not allowed.", e.getMessage());
    }
    
  }
 
  public void testInsertAndGet() throws Exception {
    
    // Configured to auto-generate key
    OracleDocument metaDoc = client.createMetadataBuilder().mediaTypeColumnName("CONTENT_TYPE")
        .keyColumnAssignmentMethod("GUID").build();
    
    OracleCollection col = dbAdmin.createCollection("testInsertAndGet", metaDoc);
    
    // Pass document without key
    OracleDocument doc = col.insertAndGet(db.createDocumentFromString(null, "{ \"data\" : 1234 }", null));
    verifyNullContentDocument(doc);
    
    doc = col.find().key(doc.getKey()).getOne();
    assertEquals("{ \"data\" : 1234 }", new String(doc.getContentAsByteArray(), "UTF-8"));
    
    // Pass document with key
    try {
      col.insertAndGet(db.createDocumentFromString("id-x", "{ \"data\" : 2000 }", null));
      fail("No exception when key is provided," + 
                                  "and the collection has auto-generated keys");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals(e.getMessage(), "The collection is not configured with client-assigned keys," + 
                                   " but the input document has a key.");
    }

    //  Pass document with non-JSON content
    doc = col.insertAndGet(db.createDocumentFromString(null, "non-JSON content", "text/plain"));
    verifyNullContentDocument(doc);
    
    // Configured to client-assigned key
    OracleDocument metaDoc2 = client.createMetadataBuilder().mediaTypeColumnName("CONTENT_TYPE")
        .keyColumnAssignmentMethod("CLIENT").build();
    
    OracleCollection col2 = dbAdmin.createCollection("testInsertAndGet2", metaDoc2);
    
    // Pass document with key
    OracleDocument doc2 = col2.insertAndGet(db.createDocumentFromString("id-1", "{ \"data\" : 1844 }", null));
    verifyNullContentDocument(doc2);
    doc2 = col2.find().key("id-1").getOne();
    assertEquals("id-1", doc2.getKey());
    assertEquals("{ \"data\" : 1844 }", new String(doc2.getContentAsByteArray(), "UTF-8"));
    
    try {
      // Pass null for key
      col2.insertAndGet((OracleDocument)null);
      fail("No exception when document is null");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("document argument cannot be null.", e.getMessage());
    }

    // Pass the document with null content
    doc2 = db.createDocumentFromString("id-2", null, null);
    col2.insertAndGet(doc2);
    OracleDocumentImpl docImpl = (OracleDocumentImpl) col2.findOne("id-2");
    assertNull(docImpl.getContentAsStream());
    
    try {
      // Pass document without key (but configured to client-assigned key)
      col2.insertAndGet(db.createDocumentFromString(null, "1234567890", "text/plain"));
      fail("No exception when document has not key");
    } catch (OracleException e) {
      // Expect an OracleException
      // ORA-01400: cannot insert NULL into ("testInsert2"."ID")
      Throwable cause = e.getCause();
      assertTrue(cause.getMessage().contains("ORA-01400"));
    }

    try {
      // Pass document with non-JSON content
      col2.insertAndGet(db.createDocumentFromString("id-3", "{12345678}", "application/json"));
      fail("No exception when document content is non-JSON");
    } catch (OracleException e) {
      // Expect an OracleException
      // ORA-02290: check constraint (SYS_C0023029) violated
      Throwable cause = e.getCause();
      assertTrue(cause.getMessage().contains("ORA-02290"));
    }
    
    // Test when collection is configured to read-only
    OracleDocument metaDoc3 = client.createMetadataBuilder()
        .readOnly(true).build();
    
    OracleCollection col3 = dbAdmin.createCollection("testInsertAndGet3", metaDoc3);

    try {
      // Pass document with non-JSON content
      col3.insertAndGet(db.createDocumentFromString(null, "{ \"data\" : 1 }", "test/plain"));
      fail("No exception when inserting document into a read-only collection");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Collection testInsertAndGet3 is read-only, insert not allowed.", e.getMessage());
    }

  }
  
  private void testInsert2(boolean autoCommit) throws Exception {

    conn.setAutoCommit(autoCommit);
      
    // Configured to auto-generate key
    OracleDocument metaDoc = client.createMetadataBuilder().mediaTypeColumnName("CONTENT_TYPE")
        .keyColumnAssignmentMethod("UUID").build();
    OracleCollection col = dbAdmin.createCollection("testInsert2", metaDoc);
    
    // Pass normal documents without key
    List<OracleDocument> list = new ArrayList<OracleDocument>();
    list.add(db.createDocumentFromString(null, "{ \"data\" : 1000 }", "application/json"));
    col.insert(list.iterator());
    OracleDocument doc = col.find().getOne();
    assertNotNull(doc.getKey());
    assertEquals("{ \"data\" : 1000 }", new String(doc.getContentAsByteArray(), "UTF-8"));
    
    // Pass the list containing documents with null content and non-JSON content
    list.clear();
    list.add(db.createDocumentFromString(null, "{ \"data\" : \"abcd\" }", null));
    list.add(db.createDocumentFromString(null, "Hello World", "text/plain"));
    list.add(db.createDocumentFromString(null, null, null));
    
    col.insert(list.iterator());
    assertEquals(4, col.find().count());
    if(autoCommit == false)
      conn.commit();
    
    // Pass the documents with key and without key
    list.clear();
    list.add(db.createDocumentFromString(null, "{ \"data\" : 2000 }", null));
    list.add(db.createDocumentFromString("id-1", "{ \"data\" : 3000 }", null));

    try {
      col.insert(list.iterator());
      fail("No exception when key is provided," + 
                                  "and the collection has auto-generated keys");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals(e.getMessage(), "The collection is not configured with client-assigned key," +
                                   " but documents iterator returned a document with a key, after" +
                                   " returning 1 documents.");
    }
    
    try {
      // Pass null iterator for documents
      col.insert((java.util.Iterator<OracleDocument>) null);
      fail("No exception when documents is null");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("documents argument cannot be null.", e.getMessage());
    }
    
    // Test 0-length iterator for documents
    list.clear();
    col.insert(list.iterator());
    
    // collection is configured with client assigned key
    OracleDocument metaDoc2 = client.createMetadataBuilder().mediaTypeColumnName("CONTENT_TYPE")
        .keyColumnAssignmentMethod("CLIENT").build();
    OracleCollection col2 = dbAdmin.createCollection("testInsert2-2", metaDoc2);
    
    // Test for documents with key
    List<OracleDocument> list2 = new ArrayList<OracleDocument>();
    list2.add(db.createDocumentFromString("id-1", "{ \"data\" : 1001 }", null));
    list2.add(db.createDocumentFromString("id-2", "Hello JSON", "text/plain"));
    
    col2.insert(list2.iterator());
    assertEquals(2, col2.find().count());
    
    OracleDocument doc2 = col2.findOne("id-1");
    assertEquals("id-1", doc2.getKey());
    assertEquals("{ \"data\" : 1001 }", new String(doc2.getContentAsByteArray(), "UTF-8"));
    
    doc2 = col2.find().key("id-2").getOne();
    assertEquals("Hello JSON", new String(doc2.getContentAsByteArray(), "UTF-8"));
    
    if (autoCommit == false)
      conn.commit();
    
    try {
      // Tests when there are key conflicts between newly inserted document and the existing doc
      list2.clear();
      list2.add(db.createDocumentFromString("id-3", "{ \"data\" : 1003 }", "application/json"));
      list2.add(db.createDocumentFromString("id-2", "{ \"data\" : 1002 }", "application/json"));
      col2.insert(list2.iterator());
      fail("No exception when there are key conflict");
    } catch (OracleException e) {
      // Expect an OracleException
      // ORA-00001: unique constraint (SYS_C0013526) violated
      Throwable cause = e.getCause();
      assertTrue(cause.getMessage().contains("ORA-00001"));
      
      // for autoCommit on mode, auto rollback should be made
      if (autoCommit == false)
        conn.rollback();
      
      // all the operations should be rolled back
      assertEquals(2, col2.find().count());
    }
    
    try {
      // Tests when there are key conflicts between documents in the list
      list2.clear();
      list2.add(db.createDocumentFromString("id-3", "{ \"data\" : 1003 }", null));
      list2.add(db.createDocumentFromString("id-4", "{ \"data\" : 1004 }", null));
      list2.add(db.createDocumentFromString("id-3", "{ \"data\" : 1005 }", null));
      col2.insert(list2.iterator());
      fail("No exception when there are key conflict");
    } catch (OracleException e) {
      // Expect an OracleException
      // ORA-00001: unique constraint (SYS_C0013526) violated
      Throwable cause = e.getCause();
      assertTrue(cause.getMessage().contains("ORA-00001"));
      
      // for autoCommit on mode, auto rollback should be made
      if (autoCommit == false)
        conn.rollback();
      
      // all the operations should be rolled back
      assertEquals(2, col2.find().count());
    }
    
    try {
      // Tests when there is non-JSON data but tagged as "application/json"
      list2.clear();
      list2.add(db.createDocumentFromString("id-3", "{ \"data\" : 1003 }", null));
      // contentType = null means the content is JSON data
      list2.add(db.createDocumentFromString("id-4", "{ 1004 }", null));
      list2.add(db.createDocumentFromString("id-5", "{ \"data\" : 1005 }", null));
      col2.insert(list2.iterator());
      fail("No exception when there is non-JSON data but tagged as JSON");
    } catch (OracleException e) {
      // Expect an OracleException
      // ORA-02290: check constraint (SYS_C0014190) violated
      Throwable cause = e.getCause();
      assertTrue(cause.getMessage().contains("ORA-02290"));
      
      // for autoCommit on mode, auto rollback should be made
      if (autoCommit == false)
        conn.rollback();
      
      // all the operations should be rolled back
      assertEquals(2, col2.find().count());
    }
    
    try {
      // Tests when the documents missing key
      list2.clear();
      list2.add(db.createDocumentFromString("id-3", "{ \"data\" : 1003 }", null));
      list2.add(db.createDocumentFromString(null, "{ \"data\" : 1003 }", null));
      col2.insert(list2.iterator());
      fail("No exception when the documents missing key");
    } catch (OracleException e) {
      // Expect an OracleException
      // ORA-01400: cannot insert NULL into ("testInsert2-2"."ID")
      Throwable cause = e.getCause();
      assertTrue(cause.getMessage().contains("ORA-01400"));
      
      // for autoCommit on mode, auto rollback should be made
      if (autoCommit == false)
        conn.rollback();
      
      // all the operations should be rolled back
      assertEquals(2, col2.find().count());
    }
    
    // Test when collection is configured to read-only
    OracleDocument metaDoc3 = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("CLIENT").readOnly(true).build();
    OracleCollection col3 = dbAdmin.createCollection("testInsert2-3", metaDoc3);
    
    try {
      // Try to insert documents into read-only collection
      List<OracleDocument> list3 = new ArrayList<OracleDocument>();
      list3.add(db.createDocumentFromString("id-1", "{ \"data\" : 1001 }", null));
      col3.insert(list3.iterator());
      fail("No exception when inserting document into a read-only collection");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Collection testInsert2-3 is read-only, insert not allowed.", e.getMessage());
      // for autoCommit on mode, auto rollback should be made
      if (autoCommit == false)
        conn.rollback();
      
      assertEquals(0, col3.find().count());
    }
    
    // reset connection auto-commit to the default value  
    conn.setAutoCommit(true);
  }

  //Test insert(java.util.Iterator documents) with AutoCommit On mode
  public void testInsert2AutoCommitOn() throws Exception {
    testInsert2(true);
  }
  
  //Test insert(java.util.Iterator documents) with AutoCommit Off mode
  public void testInsert2AutoCommitOff() throws Exception {
    testInsert2(false);
  }
  
  private void verifyNullContentDocumentList(List<OracleDocument> list) throws Exception {
    Iterator<OracleDocument> it = list.iterator();
    while (it.hasNext()) {
      OracleDocument doc = it.next();
      verifyNullContentDocument(doc);
    }
    
  }
  
  private void testInsertAndGet2(boolean autoCommit) throws Exception {
    
    conn.setAutoCommit(autoCommit);
      
    // Configured to auto-generate key
    OracleDocument metaDoc = client.createMetadataBuilder().mediaTypeColumnName("CONTENT_TYPE")
        .keyColumnAssignmentMethod("UUID").build();
    OracleCollection col = dbAdmin.createCollection("testInsertAndGet2", metaDoc);
    
    // Pass normal documents without key
    List<OracleDocument> list = new ArrayList<OracleDocument>();
    list.add(db.createDocumentFromString(null, "{ \"data\" : 1000 }", "application/json"));
    List<OracleDocument> resultList = col.insertAndGet(list.iterator());
    verifyNullContentDocumentList(resultList);
    
    OracleDocument doc = col.find().getOne();
    assertNotNull(doc.getKey());
    assertEquals("{ \"data\" : 1000 }", new String(doc.getContentAsByteArray(), "UTF-8"));
    
    // Pass the list containing documents with null content and non-JSON content
    list.clear();
    list.add(db.createDocumentFromString(null, "{ \"data\" : \"abcd\" }", null));
    list.add(db.createDocumentFromString(null, "Hello World", "text/plain"));
    list.add(db.createDocumentFromString(null, null, null));
    
    resultList = col.insertAndGet(list.iterator());
    verifyNullContentDocumentList(resultList);
    doc = col.findOne(resultList.get(0).getKey());
    assertEquals("{ \"data\" : \"abcd\" }", new String(doc.getContentAsByteArray(), "UTF-8"));
   
    String lastModified = resultList.get(1).getLastModified();
    doc = ((OracleOperationBuilderImpl)col.find().key(resultList.get(1).getKey())).lastModified(lastModified).getOne();
    assertNotNull(doc);
    assertEquals("Hello World", new String(doc.getContentAsByteArray(), "UTF-8"));  
 
    String version = resultList.get(1).getVersion();
    doc = col.find().key(resultList.get(1).getKey()).version(version).getOne();
    assertEquals("Hello World", new String(doc.getContentAsByteArray(), "UTF-8"));
 
    doc = col.findOne(resultList.get(2).getKey());
    assertEquals(null, ((OracleDocumentImpl) doc).getContentAsStream());
    
    assertEquals(4, col.find().count());
    
    if(autoCommit == false)
      conn.commit();
    
    // Pass the documents with key and without key
    list.clear();
    list.add(db.createDocumentFromString(null, "{ \"data\" : 2000 }", null));
    list.add(db.createDocumentFromString("id-1", "{ \"data\" : 3000 }", null));
    
    try {
      col.insertAndGet(list.iterator());
      fail("No exception when key is provided," +
                                  "and the collection has auto-generated keys");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals(e.getMessage(), "The collection is not configured with client-assigned key," +
                                   " but documents iterator returned a document with a key, after" +
                                   " returning 1 documents.");
    }

    try {
      // Pass null iterator for documents
      col.insertAndGet((java.util.Iterator<OracleDocument>) null);
      fail("No exception when list count is 0");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("documents argument cannot be null.", e.getMessage());
    }
    
    // Test with 0-length iterator for documents
    list.clear();
    resultList = col.insertAndGet(list.iterator());
    assertEquals(0, resultList.size());
 
    // collection is configured with client assigned key
    OracleDocument metaDoc2 = client.createMetadataBuilder().mediaTypeColumnName("CONTENT_TYPE")
        .keyColumnAssignmentMethod("CLIENT").build();
    OracleCollection col2 = dbAdmin.createCollection("testInsertAndGet2-2", metaDoc2);
    
    // Test for documents with key
    List<OracleDocument> list2 = new ArrayList<OracleDocument>();
    list2.add(db.createDocumentFromString("id-1", "{ \"data\" : 1001 }", null));
    list2.add(db.createDocumentFromString("id-2", "Hello JSON", "text/plain"));
    
    List<OracleDocument> resultlist2 = col2.insertAndGet(list2.iterator());
    assertEquals(2, col2.find().count());
    verifyNullContentDocumentList(resultlist2);
    
    assertEquals("id-1", resultlist2.get(0).getKey());
    String version2 = resultlist2.get(0).getVersion();
    
    OracleDocument doc2 = col2.find().key("id-1").version(version2).getOne();
    assertEquals("id-1", doc2.getKey());
    assertEquals("{ \"data\" : 1001 }", new String(doc2.getContentAsByteArray(), "UTF-8"));
    
    assertEquals("id-2", resultlist2.get(1).getKey());
    version2 = resultlist2.get(1).getVersion();
    doc2 = col2.find().key("id-2").version(version2).getOne();
    assertEquals("Hello JSON", new String(doc2.getContentAsByteArray(), "UTF-8"));
    
    if (autoCommit == false)
      conn.commit();
    
    try {
      // Tests when there are key conflicts between newly inserted document and the existing doc
      list2.clear();
      list2.add(db.createDocumentFromString("id-3", "{ \"data\" : 1003 }", "application/json"));
      list2.add(db.createDocumentFromString("id-2", "{ \"data\" : 1002 }", "application/json"));
      col2.insertAndGet(list2.iterator());
      fail("No exception when there are key conflict");
    } catch (OracleException e) {
      // Expect an OracleException
      // ORA-00001: unique constraint (SYS_C0013526) violated
      Throwable cause = e.getCause();
      assertTrue(cause.getMessage().contains("ORA-00001"));
      
      // for autoCommit on mode, auto rollback should be made
      if (autoCommit == false)
        conn.rollback();
      
      // all the operations should be rolled back
      assertEquals(2, col2.find().count());
    }
    
    try {
      // Tests when there are key conflicts between documents in the list
      list2.clear();
      list2.add(db.createDocumentFromString("id-3", "{ \"data\" : 1003 }", null));
      list2.add(db.createDocumentFromString("id-4", "{ \"data\" : 1004 }", null));
      list2.add(db.createDocumentFromString("id-3", "{ \"data\" : 1005 }", null));
      col2.insertAndGet(list2.iterator());
      fail("No exception when there are key conflict");
    } catch (OracleException e) {
      // Expect an OracleException
      // ORA-00001: unique constraint (SYS_C0013526) violated
      Throwable cause = e.getCause();
      assertTrue(cause.getMessage().contains("ORA-00001"));
      
      // for autoCommit on mode, auto rollback should be made
      if (autoCommit == false)
        conn.rollback();
      
      // all the operations should be rolled back
      assertEquals(2, col2.find().count());
    }
    
    try {
      // Tests when there is non-JSON data but tagged as "application/json"
      list2.clear();
      list2.add(db.createDocumentFromString("id-3", "{ \"data\" : 1003 }", null));
      // contentType = null means the content is JSON data
      list2.add(db.createDocumentFromString("id-4", "{ 1004 }", null));
      list2.add(db.createDocumentFromString("id-5", "{ \"data\" : 1005 }", null));
      col2.insertAndGet(list2.iterator());
      fail("No exception when there is non-JSON data but tagged as JSON");
    } catch (OracleException e) {
      // Expect an OracleException
      // ORA-02290: check constraint (SYS_C0014190) violated
      Throwable cause = e.getCause();
      assertTrue(cause.getMessage().contains("ORA-02290"));
      
      // for autoCommit on mode, auto rollback should be made
      if (autoCommit == false)
        conn.rollback();
      
      // all the operations should be rolled back
      assertEquals(2, col2.find().count());
    }
    
    try {
      // Tests when the documents missing key
      list2.clear();
      list2.add(db.createDocumentFromString("id-3", "{ \"data\" : 1003 }", null));
      list2.add(db.createDocumentFromString( null, "{ \"data\" : 1003 }", null));
      col2.insertAndGet(list2.iterator());
      fail("No exception when the documents missing key");
    } catch (OracleException e) {
      // Expect an OracleException
      // ORA-01400: cannot insert NULL into ("testInsert2-2"."ID")
      Throwable cause = e.getCause();
      assertTrue(cause.getMessage().contains("ORA-01400"));
      
      // for autoCommit on mode, auto rollback should be made
      if (autoCommit == false)
        conn.rollback();
      
      // all the operations should be rolled back
      assertEquals(2, col2.find().count());
    }
    
    // Test when collection is configured to read-only
    OracleDocument metaDoc3 = client.createMetadataBuilder().mediaTypeColumnName("CONTENT_TYPE")
        .keyColumnAssignmentMethod("CLIENT").readOnly(true).build();
    OracleCollection col3 = dbAdmin.createCollection("testInsertAndGet2-3", metaDoc3);
    
    try {
      // Try to insert documents into read-only collection
      List<OracleDocument> list3 = new ArrayList<OracleDocument>();
      list3.add(db.createDocumentFromString("id-1", "{ \"data\" : 1001 }", null));
      col3.insertAndGet(list3.iterator());
      fail("No exception when inserting document into a read-only collection");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Collection testInsertAndGet2-3 is read-only, insert not allowed.", e.getMessage());
      // for autoCommit on mode, auto rollback should be made
      if (autoCommit == false)
        conn.rollback();
      
      assertEquals(0, col3.find().count());
    }
    
    // reset connection auto-commit to the default value  
    conn.setAutoCommit(true);
  }

  //Test insertAndGet(java.util.Iterator documents) with AutoCommit On mode
  public void testInsertAndGet2AutoCommitOn() throws Exception {
    testInsertAndGet2(true);
  }
  
  //Test insertAndGet(java.util.Iterator documents) with AutoCommit Off mode
  public void testInsertAndGet2AutoCommitOff() throws Exception {
    testInsertAndGet2(false);
  }

  private void testSaveWithCol(OracleCollection col, boolean clientAssignedKey) throws Exception{
    OracleDocument doc = null;
    OracleCollectionAdmin colAdmin = col.admin();
    
    if(clientAssignedKey) {
      // Test with specified key(not used)
      doc = db.createDocumentFromString("id-1", "{ \"data\" : \"v1\" }", null);
      col.save(doc);
      doc = col.find().getOne();
      assertEquals("{ \"data\" : \"v1\" }", new String(doc.getContentAsByteArray(), "UTF-8"));
      
      // Test with specified key(already used)
      doc = db.createDocumentFromString("id-1", "{ \"data\" : \"new-v1\" }", null);
      col.save(doc);
      doc = col.find().getOne();
      assertEquals("{ \"data\" : \"new-v1\" }", new String(doc.getContentAsByteArray(), "UTF-8"));
    } else {
      // Test with document without key
      doc = db.createDocumentFromString("{ \"data\" : \"v1\" }");
      col.save(doc);
      doc = col.find().getOne();
      assertEquals("{ \"data\" : \"v1\" }", new String(doc.getContentAsByteArray(), "UTF-8"));
    }
    
    if(colAdmin.isHeterogeneous()) {
      // Test with non-JSON content
      if(clientAssignedKey) {
        doc = db.createDocumentFromString("id-2", "abcd1234", "text/plain");
        col.save(doc);
        doc = col.findOne("id-2");
        assertEquals("abcd1234", new String(doc.getContentAsByteArray(), "UTF-8"));
      } else {
        doc = db.createDocumentFromString(null, "abcd1234", "text/plain");
        col.save(doc);
        assertEquals(2, col.find().count());
      }
    } else {
      // Test to save non-JSON content to JSON-Only collection
      if(clientAssignedKey) 
        doc = db.createDocumentFromString("id-2", "abcd1234", "text/plain");
      else
        doc = db.createDocumentFromString(null, "abcd1234", "text/plain");
      
      try { 
        col.save(doc);
        fail("No exception when saving non-JSON to JSON-Only collection");
      } catch (OracleException e) {
        // Expect an OracleException
        // ORA-02290: check constraint (SYS_C0040145) violated
        Throwable cause = e.getCause();
        assertTrue(cause.getMessage().contains("ORA-02290"));
      }
    }
      
    if (clientAssignedKey) {
      try {
        // Test for document without key
        doc = db.createDocumentFromString("{ \"value\" : \"a1\" }");
        col.save(doc);
        junit.framework.Assert
            .fail("No exception when saving document without key to client-assigned key collection");
      } catch (OracleException e) {
        // Expect an OracleException
        // ORA-01400: cannot insert NULL into ("testSave2"."ID")
        Throwable cause = e.getCause();
        assertTrue(cause.getMessage().contains("ORA-01400"));
      }
    } else {
      try {
        // Test for document with key
        doc = db.createDocumentFromString("id-3", "{ \"value\" : \"a1\" }", null);
        col.save(doc);
        fail("No exception when saving document with key to auto-generate key collection");
      } catch (OracleException e) {
        // Expect an OracleException
        assertEquals("The collection is not configured with client-assigned keys, but the input document has a key.", e.getMessage());
      }
    }

    try 
    {
      // Test with null document
      col.save(null);
      fail("No exception when document is null");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("document argument cannot be null.", e.getMessage());
    }

    // Test to save document with null content
    if (clientAssignedKey) {
      // Test with specified key(not used)
      doc = db.createDocumentFromString("id-1", null, null);
      col.save(doc);
      doc = col.findOne("id-1");
      assertEquals(null, doc.getContentAsByteArray());
    } else {
      col.find().remove();
      // Test with document without key
      doc = db.createDocumentFromString(null, null, null);
      col.save(doc);
      assertEquals(1, col.find().count());
    }

  }
  
  public void testSave() throws Exception {

    // Configured to auto-generate key and heterogeneous collection
    OracleDocument mDoc = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("GUID").mediaTypeColumnName("Media_Type")
        .keyColumnType("VARCHAR2")
        .build();
    OracleCollection col = dbAdmin.createCollection("testSave", mDoc);
    testSaveWithCol(col, false);
    
    // Configured to client-generate key and heterogeneous collection
    OracleDocument mDoc2 = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("CLIENT").mediaTypeColumnName("Media_Type")
        .keyColumnType("NVARCHAR2")
        .build();
    OracleCollection col2 = dbAdmin.createCollection("testSave2", mDoc2);
    testSaveWithCol(col2, true);
    
    // Configured to client-generate key and non-heterogeneous collection
    OracleDocument mDoc3 = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("CLIENT")
        .build();
    OracleCollection col3 = dbAdmin.createCollection("testSave3", mDoc3);
    testSaveWithCol(col3, true);
    
    // Configured to auto-generate key and non-heterogeneous collection
    OracleDocument mDoc4 = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("UUID")
        .keyColumnType("RAW")
        .build();
    OracleCollection col4 = dbAdmin.createCollection("testSave4", mDoc4);
    testSaveWithCol(col4, false);

    // Test to save document to read-only collection
    OracleDocument mDoc5 = client.createMetadataBuilder().readOnly(true).build();
    OracleCollection col5 = dbAdmin.createCollection("testSave5", mDoc5);
    try { 
      OracleDocument doc = db.createDocumentFromString("{\"data\":\"v1\"}");
      col5.save(doc);
      fail("No exception when saving document to an readOnly collection");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Collection testSave5 is read-only, save not allowed.", e.getMessage());
    }
    
  }

  private void testSaveAndGetWithCol(OracleCollection col, boolean clientAssignedKey) throws Exception{

    OracleDocument doc = null;
    OracleCollectionAdmin colAdmin = col.admin();

    if(clientAssignedKey) {
      // Test with specified key(not used)
      doc = db.createDocumentFromString("id-a1", "{ \"data\" : 1000 }", null);
      doc = col.saveAndGet(doc);
      verifyNullContentDocument(doc);
      doc = col.find().key("id-a1").version(doc.getVersion()).getOne();
      assertEquals("{ \"data\" : 1000 }", new String(doc.getContentAsByteArray(), "UTF-8"));
      
      // Test with specified key(already used)
      doc = db.createDocumentFromString("id-a1", "{ \"data\" : 1001 }", null);
      doc = col.saveAndGet(doc);
      verifyNullContentDocument(doc);
      doc = ((OracleOperationBuilderImpl)col.find().key(doc.getKey())).lastModified(doc.getLastModified()).getOne();
      assertEquals("{ \"data\" : 1001 }", new String(doc.getContentAsByteArray(), "UTF-8"));
    } else {
      // Test with document without key
      doc = db.createDocumentFromString(null, "{ \"data\" : 1000 }", null);
      doc = col.saveAndGet(doc);
      verifyNullContentDocument(doc);
      doc = col.find().key(doc.getKey()).version(doc.getVersion()).getOne();
      assertEquals("{ \"data\" : 1000 }", new String(doc.getContentAsByteArray(), "UTF-8"));
    }
    
    if(colAdmin.isHeterogeneous()) {
      // Test with non-JSON content
      if(clientAssignedKey) {
        doc = db.createDocumentFromString("id-a1", "Hello Word", "text/plain");
      } else {
        doc = db.createDocumentFromString(null, "Hello Word", "text/plain");
      }
      doc = col.saveAndGet(doc);
      verifyNullContentDocument(doc);
      doc = ((OracleOperationBuilderImpl)col.find().key(doc.getKey())).lastModified(doc.getLastModified()).getOne();
      assertEquals("Hello Word", new String(doc.getContentAsByteArray(), "UTF-8"));
      assertEquals("text/plain", doc.getMediaType());
    } else {
      // Test to save non-JSON content to JSON-Only collection
      if(clientAssignedKey) 
        doc = db.createDocumentFromString("id-2", "abcd1234", "text/plain");
      else
        doc = db.createDocumentFromString(null, "abcd1234", "text/plain");
      
      try { 
        col.saveAndGet(doc);
        fail("No exception when saving non-JSON to JSON-Only collection");
      } catch (OracleException e) {
        // Expect an OracleException
        // ORA-02290: check constraint (SYS_C0040145) violated
        Throwable cause = e.getCause();
        assertTrue(cause.getMessage().contains("ORA-02290"));
      }
    }
    
    // Test with non-JSON content but content type is null(means to JSON)
    if(clientAssignedKey) 
      doc = db.createDocumentFromString("id-2", "abcd1234", null);
    else
      doc = db.createDocumentFromString(null, "abcd1234", null);
    
    try { 
      col.saveAndGet(doc);
      fail("No exception when content is non-JSON, and but content type is JSON");
    } catch (OracleException e) {
      // Expect an OracleException
      // ORA-02290: check constraint (SYS_C0040145) violated
      Throwable cause = e.getCause();
      assertTrue(cause.getMessage().contains("ORA-02290"));
    }
    
    if (clientAssignedKey) {
      try {
        // Test for document without key
        doc = db.createDocumentFromString(null, "{ \"data\" : 2000 }", null);
        col.saveAndGet(doc);
        fail("No exception when saving document without key to client-assigned key collection");
      } catch (OracleException e) {
        // Expect an OracleException
        // ORA-01400: cannot insert NULL into ("testSave2"."ID")
        Throwable cause = e.getCause();
        assertTrue(cause.getMessage().contains("ORA-01400"));
      }
    } else {
      try {
        // Test for document with key
        doc = db.createDocumentFromString("id-a3", "{ \"data\" : 2000 }", null);
        col.saveAndGet(doc);
        fail("No exception when saving document with key to auto-generate key collection");
      } catch (OracleException e) {
        // Expect an OracleException
        assertEquals("The collection is not configured with client-assigned keys, but the input document has a key.", e.getMessage());
      }
    }
    
    try {
        // Test with null document
        col.saveAndGet(null);
        fail("No exception when document is null");
    } catch (OracleException e) {
        // Expect an OracleException
        assertEquals("document argument cannot be null.", e.getMessage());
    }
    
    // Test to save document with null content
    if (clientAssignedKey) {
      // Test with specified key(used)
      doc = db.createDocumentFromString("id-a1", null, null);
    } else {
      // Test with document without key
      doc = db.createDocumentFromString(null);
    }
    doc = col.saveAndGet(doc);
    verifyNullContentDocument(doc);
    assertEquals(1, col.find().key(doc.getKey()).version(doc.getVersion()).count());

    assertEquals(1, ((OracleOperationBuilderImpl)col.find().key(doc.getKey())).lastModified(doc.getLastModified()).count());
    doc = col.findOne(doc.getKey());
    assertEquals(null, doc.getContentAsByteArray());
    
  }
  
  public void testSaveAndGet() throws Exception{
    
    // Configured to auto-generate key and heterogeneous collection
    OracleDocument mDoc = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("GUID").mediaTypeColumnName("Media_Type")
        .keyColumnType("RAW")
        .build();
    OracleCollection col = dbAdmin.createCollection("testSaveAndGet", mDoc);
    testSaveAndGetWithCol(col, false);
   
    // Configured to client-generate key and heterogeneous collection
    OracleDocument mDoc2 = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("CLIENT").mediaTypeColumnName("Media_Type")
        .keyColumnType("NVARCHAR2")
        .build();
    OracleCollection col2 = dbAdmin.createCollection("testSaveAndGet2", mDoc2);
    testSaveAndGetWithCol(col2, true);
    
    // Configured to client-generate key and non-heterogeneous collection
    OracleDocument mDoc3 = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("CLIENT")
        .keyColumnType("VARCHAR2")
        .build();
    OracleCollection col3 = dbAdmin.createCollection("testSaveAndGet3", mDoc3);
    testSaveAndGetWithCol(col3, true);
    
    // Configured to auto-generate key and non-heterogeneous collection
    OracleDocument mDoc4 = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("UUID")
        .build();
    OracleCollection col4 = dbAdmin.createCollection("testSaveAndGet4", mDoc4);
    testSaveAndGetWithCol(col4, false);
    
    // Test to save document to read-only collection
    OracleDocument mDoc5 = client.createMetadataBuilder().readOnly(true).build();
    OracleCollection col5 = dbAdmin.createCollection("testSaveAndGet5", mDoc5);
    try { 
      OracleDocument doc = db.createDocumentFromString("{\"data\":\"v1\"}");
      col5.saveAndGet(doc);
      fail("No exception when saving document to an readOnly collection");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Collection testSaveAndGet5 is read-only, save not allowed.", e.getMessage());
    }
  }

  public void testFindFragment() throws Exception {
    
    // findFragment only work for non-JSON data
    OracleDocument mDoc = client.createMetadataBuilder().
        contentColumnType("BLOB").mediaTypeColumnName("MediaType")
        .keyColumnAssignmentMethod("CLIENT").build();
    OracleCollection col = dbAdmin.createCollection("testFindFragment", mDoc);
    
    OracleDocumentFragmentImpl frag = null;
    FileInputStream fileStream = new FileInputStream(new File("data/oracle.jpg"));
    
    String key = "id-1";
    OracleDatabaseImpl dbImpl = (OracleDatabaseImpl) db;
    col.insert(dbImpl.createDocumentFromStream("id-1", fileStream, "image/jpeg"));
    fileStream.close();
  
    long fileLength = (new File("data/oracle.jpg")).length();
    // read first 20k
    frag = (OracleDocumentFragmentImpl)((TableCollectionImpl)col).findFragment(key, 0, 20000);
    
    assertEquals(fileLength , frag.getTotalDatabaseObjectLength());
    assertEquals(0, frag.getOffset());
    assertEquals(20000, frag.getContentLength());
    assertEquals("image/jpeg", frag.getMediaType());
    File file = new File("data/oracle.jpg");
    FileInputStream in = new FileInputStream(file);
    byte[] bytes = new byte[20000];
    in.read(bytes);
    assertEquals(new String(bytes, "UTF-8"), new String(frag.getContentAsByteArray(), "UTF-8"));
    
    // read second 20k
    frag = (OracleDocumentFragmentImpl)((TableCollectionImpl)col).findFragment(key, 20000, 20000);
    
    assertEquals(fileLength, frag.getTotalDatabaseObjectLength());
    assertEquals(20000, frag.getOffset());
    assertEquals(20000, frag.getContentLength());
    assertEquals("image/jpeg", frag.getMediaType());
    in.read(bytes);
    assertEquals(new String(bytes, "UTF-8"), new String(frag.getContentAsByteArray(), "UTF-8"));
    
    // read left data
    frag = (OracleDocumentFragmentImpl)((TableCollectionImpl)col).findFragment(key, 40000, 20000);
    
    assertEquals(fileLength, frag.getTotalDatabaseObjectLength());
    assertEquals(40000, frag.getOffset());
    assertEquals(fileLength-40000, frag.getContentLength());
    assertEquals("image/jpeg", frag.getMediaType());
    
    int leftLength = (int)fileLength - 40000;
    byte[] finalBytes = new byte[leftLength];
    in.read(finalBytes);
    assertEquals(new String(finalBytes, "UTF-8"), new String(frag.getContentAsByteArray(), "UTF-8"));

    // Test with unknown key
    frag = (OracleDocumentFragmentImpl)((TableCollectionImpl)col).findFragment("id-x", 0, 20000);
    assertNull(frag);
  
    // Negative Test
    try {
      // Test with offset > document length
      frag = (OracleDocumentFragmentImpl)((TableCollectionImpl)col).findFragment("id-1", fileLength+10000, 20000);
      fail("No exception when there is id conflict");
    } catch (OracleException e) {
      // Expect an OracleException
      // bug19601645: when offset is invalid, no error message was generated, just throw SODAUtils$1OracleSQLException
      //assertEquals("", e.getMessage());
    }
   
    try {
      // Test with offset is negative
      frag = (OracleDocumentFragmentImpl)((TableCollectionImpl)col).findFragment("id-1", -1, 20000);
      fail("No exception when offset is negative");
    } catch (OracleException e) {
      // Expect an OracleException
      // bug19601645: when offset is invalid, no error message was generated, just throw SODAUtils$1OracleSQLException
      //assertEquals("", e.getMessage());
    }

    try {
      // Test with length is negative
      frag = (OracleDocumentFragmentImpl)((TableCollectionImpl)col).findFragment("id-1", 0, -1);
      //fail("No exception when length is negative");
    } catch (OracleException e) {
      // Expect an OracleException
      // bug19601645: No exception when length is negative  
      //assertEquals("", e.getMessage());
    }
    
    try {
      //Test with null for key
      frag = (OracleDocumentFragmentImpl)((TableCollectionImpl)col).findFragment(null, 0, 20000);
      //fail("No exception when key is null");
    } catch (OracleException e) {
      // Expect an OracleException
      // bug19601645: No exception when key is null
      //assertEquals("", e.getMessage());
    }
 
  }
}
