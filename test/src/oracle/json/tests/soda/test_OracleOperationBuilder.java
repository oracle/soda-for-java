/* Copyright (c) 2014, 2020, Oracle and/or its affiliates. 
All rights reserved.*/

/*
   DESCRIPTION
    OracleOperationBuilder read operations
 */

/**
 *  @author  Vincent Liu
 */
package oracle.json.tests.soda;

import java.util.HashSet;

import oracle.soda.OracleCursor;
import oracle.soda.OracleException;
import oracle.soda.OracleCollection;
import oracle.soda.OracleDocument;
import oracle.soda.OracleOperationBuilder;

import oracle.soda.rdbms.OracleRDBMSMetadataBuilder;
import oracle.soda.rdbms.impl.OracleOperationBuilderImpl;
import oracle.soda.rdbms.impl.OracleDocumentImpl;

import oracle.json.testharness.SodaTestCase;

public class test_OracleOperationBuilder extends SodaTestCase {

  public void testKeys() throws Exception {
    OracleDocument mDoc = client.createMetadataBuilder().keyColumnAssignmentMethod("CLIENT").build();  
    
    OracleCollection col;
    if (isJDCSMode())
    {
      col = db.admin().createCollection("testkeys", null);
    } else
    {
      col = db.admin().createCollection("testkeys" , mDoc);
    }
    
    String[] key = new String[10];
    OracleDocument doc = null;
    for (int i = 1; i <= 10; i++) 
    {
      if (isJDCSMode()) 
      {
        doc = col.insertAndGet(db.createDocumentFromString("{\"d\":" + i + "}"));  
        key[i-1] = doc.getKey();
      } else
      {
        doc = col.insertAndGet(db.createDocumentFromString("id-" + i, "{\"d\":" + i + "}"));  
        key[i-1] = doc.getKey();   
      }   
    }

    // Test keys(...) with known and unknown keys
    HashSet<String> keySet = new HashSet<String>();
    keySet.add(key[1]); // known key
    if (isJDCSMode()) 
    {
      keySet.add("7BA79AB96D4C44F8A37DF7FD138EC0F7"); // unknown key
    } else
    {
      keySet.add("id-x"); // unknown key
    }
    OracleOperationBuilder builder = col.find().keys(keySet);
    assertEquals(1, builder.count());
    assertEquals("{\"d\":" + 2 + "}", new String(builder.getOne().getContentAsByteArray(), "UTF-8"));

    

    // Test keys(...) with 1+ known and unknown keys
    keySet.clear();
    keySet.add(key[0]); // known key
    keySet.add("7BA79AB96D4C44F8A37DF7FD138EC0F7"); // unknown key
    keySet.add(key[4]); // known key
    keySet.add("7BA79AB96D4C44F8A37DF7FD138EC0F8"); // unknown key
    keySet.add(key[9]); // known key
    builder = col.find().keys(keySet);
    // there should be 3 documents matching
    assertEquals(3, builder.count());
    OracleCursor cursor = builder.getCursor();
    while (cursor.hasNext()) {
      doc = cursor.next();
      if (doc.getKey().equals(key[0])) {
        assertEquals("{\"d\":" + 1 + "}", new String(doc.getContentAsByteArray(), "UTF-8"));
        keySet.remove(key[0]);
      }
      if (doc.getKey().equals(key[4])) {
        assertEquals("{\"d\":" + 5 + "}", doc.getContentAsString());
        keySet.remove(key[4]);
      }
      if (doc.getKey().equals(key[9])) {
        assertEquals("{\"d\":" + 10 + "}", doc.getContentAsString());
        keySet.remove(key[9]);
      }
    }
    cursor.close();
    // there should be only 2 unmatched keys left
    assertEquals(2, keySet.size()); 

    
      // Test with other key(), startKey(), and keys() calls
      keySet.clear();
      keySet.add(key[2]);
      keySet.add(key[3]);
      keySet.add(key[4]);
      HashSet<String> keySet0 = new HashSet<String>();
      keySet0.add("7BA79AB96D4C44F8A37DF7FD138EC0F7");
      builder = ((OracleOperationBuilderImpl) col.find().keys(keySet0).key(key[0])).startKey(key[0], true, true).keys(keySet);
      // the last keys(...) call will overwrite other
      assertEquals(3, builder.count());

      // Test when no matched key in keys iterator()
      keySet0.clear();
      keySet0.add("7BA79AB96D4C44F8A37DF7FD138EC0F7");
      assertEquals(0, col.find().keys(keySet0).count());

    if (!isJDCSMode()) // client assigned keys are not used in JDCS mode
    {
      // Test when the length of keys is more than 16
      for (int i = 24; i > 0; i--) {
        keySet0.add("id-" + i);
      }
      
      assertEquals(10, col.find().keys(keySet0).count());
    }

    // Negative tests
    try {
      // Pass null for keys iterator
      col.find().keys(null).getCursor();
      fail("No exception when keys set is null");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("keys argument cannot be null.", e.getMessage());
    }

    try {
      // Pass an empty iterator for keys iterator
      col.find().keys(new HashSet<String>()).getCursor();
      fail("No exception when keys set is empty");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("keys set is empty.", e.getMessage());
    }

    try {
      // Pass the keys list containing null item
      keySet.clear();
      keySet.add(key[0]);
      keySet.add(null);
      keySet.add(key[2]);
      col.find().keys(keySet).getCursor();
      fail("No exception when keys set containing null item");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("keys set contains null.", e.getMessage());
    }

  }

  public void testStartKey() throws Exception {
    OracleDocument mDoc; 
    if (isJDCSMode()) {
      mDoc = db.createDocumentFromString("{\"keyColumn\":{\"name\":\"ID\",\"sqlType\":\"VARCHAR2\",\"maxLength\":255,\"assignmentMethod\":\"CLIENT\"},\"contentColumn\":{\"name\":\"JSON_DOCUMENT\",\"sqlType\":\"BLOB\"},\"lastModifiedColumn\":{\"name\":\"LAST_MODIFIED\"},\"versionColumn\":{\"name\":\"VERSION\",\"method\":\"UUID\"},\"creationTimeColumn\":{\"name\":\"CREATED_ON\"},\"readOnly\":false}"); 
    } else {
      mDoc = client.createMetadataBuilder().keyColumnAssignmentMethod("CLIENT").build();  
    }
    
    OracleCollection col = dbAdmin.createCollection("testStartKey", mDoc);
    for (int i = 1; i <= 5; i++) {
      col.insertAndGet(db.createDocumentFromString("id-" + i, "{ \"d\" : " + i + " }"));
    }
    for (int i = 11; i <= 15; i++) {
      col.insertAndGet(db.createDocumentFromString("id-" + i, "{ \"d\" : " + i + " }"));
    }

    // Test startKey(...) with known key, ascending=true, inclusive=true
    OracleOperationBuilder builder = ((OracleOperationBuilderImpl) col.find()).startKey("id-2", true, true);
    assertEquals(4, builder.count());
    // Test startKey(...) with known key, ascending=true, inclusive=false
    builder = ((OracleOperationBuilderImpl) col.find()).startKey("id-2", true, false);
    assertEquals(3, builder.count());
    
    // Test null for inclusive(equals to inclusive=true)
    builder = ((OracleOperationBuilderImpl) col.find()).startKey("id-4", true, null);
    assertEquals(2, builder.count());
    builder = ((OracleOperationBuilderImpl) col.find()).startKey("id-4", false, null);
    assertEquals(9, builder.count());

    // Test startKey(...) with known key, ascending=false, inclusive=true
    builder = ((OracleOperationBuilderImpl) col.find()).startKey("id-11", false, true);
    assertEquals(2, builder.count());
    // Test startKey(...) with known key, ascending=false, inclusive=false
    builder = ((OracleOperationBuilderImpl) col.find()).startKey("id-11", false, false);
    assertEquals(1, builder.count());
    if (isJDCSMode()) {
      assertEquals("{\"d\":" + 1 + "}", new String(builder.getOne()
          .getContentAsByteArray(), "UTF-8"));
    } else {
      assertEquals("{ \"d\" : " + 1 + " }", new String(builder.getOne()
          .getContentAsByteArray(), "UTF-8"));
    }
    

    // Test startKey(...) with unknown key and true
    builder = ((OracleOperationBuilderImpl) col.find()).startKey("id-18", true, false);
    assertEquals(4, builder.count());
    OracleCursor cursor = builder.getCursor();
    if (isJDCSMode()) {
      // 1st document should be "id-2"
      assertEquals("{\"d\":" + 2 + "}", new String(cursor.next().getContentAsByteArray(), "UTF-8"));
      // 2rd document should be "id-3"
      assertEquals("{\"d\":" + 3 + "}", new String(cursor.next().getContentAsByteArray(), "UTF-8"));
    } else {
      // 1st document should be "id-2"
      assertEquals("{ \"d\" : " + 2 + " }", new String(cursor.next()
          .getContentAsByteArray(), "UTF-8"));
      // 2rd document should be "id-3"
      assertEquals("{ \"d\" : " + 3 + " }", new String(cursor.next()
          .getContentAsByteArray(), "UTF-8"));
    }
    
    // 3rd document should be "id-4"
    assertEquals("id-4", cursor.next().getKey());
    // 4th document should be "id-5"
    assertEquals("id-5", cursor.next().getKey());

    // Test startKey(...) with unknown key and false
    builder = ((OracleOperationBuilderImpl) col.find()).startKey("iw", false, true);
    assertEquals(10, builder.count());

    // Test with other key(), startKey(), and keys() calls
    HashSet<String> keySet = new HashSet<String>();
    keySet.clear();
    keySet.add("id-3");
    keySet.add("id-4");
    keySet.add("id-5");
    HashSet<String> keySet0 = new HashSet<String>();
    keySet0.add("id-a");
    builder = ((OracleOperationBuilderImpl) col.find()).startKey("id-13", false, false).key("id-1").keys(keySet);
    builder = ((OracleOperationBuilderImpl) builder).startKey("id-3", null, false);
    
    // the last startKey("id-14", null) should overwrite other
    assertEquals(2, builder.count());

    // Test with an empty string for key
    assertEquals(0, ((OracleOperationBuilderImpl) col.find()).startKey("", false, false).count());

    // Negative tests
    try {
      // Pass null for keys iterator
      ((OracleOperationBuilderImpl) col.find()).startKey(null, true, true).getCursor();
      fail("No exception when key is null");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("startKey argument cannot be null.", e.getMessage());
    }

  }

  public void testKey() throws Exception {
    OracleDocument mDoc = client.createMetadataBuilder().keyColumnAssignmentMethod("CLIENT").build();  
    
    OracleCollection col;
    if (isJDCSMode())
    {
      col = db.admin().createCollection("testKey", null);
    } else
    {
      col = db.admin().createCollection("testKey" , mDoc);
    }

    String[] key = new String[10];
    OracleDocument doc = null;
    for (int i = 1; i <= 10; i++) 
    {
      if (isJDCSMode()) 
      {
        doc = col.insertAndGet(db.createDocumentFromString("{\"v\":" + i + "}"));  
        key[i-1] = doc.getKey();
      } else
      {
        doc = col.insertAndGet(db.createDocumentFromString("id-" + i, "{\"v\":" + i + "}")); 
        key[i-1] = doc.getKey();   
      }         
    }

    // Test key(...) with known key
    doc = col.find().key(key[2]).getOne();
    String ver = doc.getVersion();
    String lastModified = doc.getLastModified();
    assertEquals("{\"v\":" + 3 + "}",
        new String(doc.getContentAsByteArray(), "UTF-8"));

    // Test key(...) with known key and version()
    assertEquals("{\"v\":" + 3 + "}", new String(col.find().key(key[2])
        .version(ver).getOne().getContentAsByteArray(), "UTF-8"));

    // Test key(...) with known key and lastModified()
    OracleCursor cursor = ((OracleOperationBuilderImpl) col.find().key(key[2])).lastModified(lastModified)
        .getCursor();
    assertEquals(key[2], cursor.next().getKey());

    // Test key(...) with unknown key
    cursor = col.find().key("7BA79AB96D4C44F8A37DF7FD138EC0F7").getCursor();
    assertEquals(false, cursor.hasNext());

    // Test key(...) with unknown key and version()
    assertEquals(0, col.find().key("7BA79AB96D4C44F8A37DF7FD138EC0F7").version(ver).count());

    // Test with other key(), startKey(), and keys() calls

    HashSet<String> keySet = new HashSet<String>();
    keySet.add(key[6]);
    keySet.add(key[7]);
    keySet.add(key[8]);
    OracleOperationBuilder builder = ((OracleOperationBuilderImpl) col.find().key(key[0]).keys(keySet)).startKey(key[2], null, false).key(key[9]);
    // the last key("id-10") should overwrite other
    assertEquals(1, builder.count());
    assertEquals(key[9], builder.getOne().getKey());

    // Test with an empty string for key
    assertEquals(0, col.find().key("").count());


    // Negative tests
    try {
      // Pass null for key
      col.find().key(null).getCursor();
      fail("No exception when key is null");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("key argument cannot be null.", e.getMessage());
    }

  }

  public void testTimeRange() throws Exception {
    OracleDocument mDoc = client.createMetadataBuilder().keyColumnAssignmentMethod("SEQUENCE").keyColumnSequenceName("SeqOnKeyColumn").build();  
    
    OracleCollection col;
    if (isJDCSMode())
    {
      col = dbAdmin.createCollection("testTimeRange", null);
    } else
    {
      col = dbAdmin.createCollection("testTimeRange", mDoc);
    }

    OracleDocument doc = null;
    String lastModified1 = null, lastModified3 = null, lastModified4 = null, lastModified5 = null, lastModified9 = null, lastModified10 = null;
    for (int i = 1; i <= 10; i++) {
      doc = col.insertAndGet(db.createDocumentFromString("{ \"v\" : " + i + " }"));
      if (i == 1)
        lastModified1 = doc.getLastModified();
      if (i == 3)
        lastModified3 = doc.getLastModified();
      if (i == 4)
        lastModified4 = doc.getLastModified();
      if (i == 5)
        lastModified5 = doc.getLastModified();
      if (i == 9)
        lastModified9 = doc.getLastModified();
      if (i == 10)
        lastModified10 = doc.getLastModified();
    }

    // Test with valid time stamp for since and until
    OracleOperationBuilderImpl builderImpl = (OracleOperationBuilderImpl) col.find();
    doc = builderImpl.timeRange(lastModified10, lastModified10, true).getOne();
    if (isJDCSMode())
    {
      assertEquals("{\"v\":" + 10 + "}",
        new String(doc.getContentAsByteArray(), "UTF-8"));
    } else
    {
      assertEquals("{ \"v\" : " + 10 + " }",
        new String(doc.getContentAsByteArray(), "UTF-8"));
    }
    

    // Test with null since and valid time stamp for until
    OracleCursor cursor = builderImpl.timeRange(null, lastModified1, true).getCursor();
    if (isJDCSMode())
    {
      assertEquals("{\"v\":" + 1 + "}", new String(cursor.next()
        .getContentAsByteArray(), "UTF-8"));
    } else
    {
      assertEquals("{ \"v\" : " + 1 + " }", new String(cursor.next()
        .getContentAsByteArray(), "UTF-8"));
    }    

    // Test with valid time stamp for since and null for until
    assertEquals(5, builderImpl.timeRange(lastModified5, null, false).count());

    // Test when since is equal to until
    assertEquals(1, builderImpl.timeRange(lastModified5, lastModified5, true).count());

    // Test when since is greater than until
    assertEquals(0, builderImpl.timeRange(lastModified10, lastModified1, true).count());
    
    // Test null for inclusive(equals to inclusive=true)
    assertEquals(5, builderImpl.timeRange(lastModified1, lastModified5, null).count());
    assertEquals(3, builderImpl.timeRange(lastModified1, lastModified3, null).count());
    
    cursor = builderImpl.timeRange(lastModified4, lastModified5, false).getCursor();
    assertEquals(1, builderImpl.timeRange(lastModified4, lastModified5, false).count());

    // call timeRange for 1+ times
    OracleOperationBuilder builder = builderImpl.timeRange(lastModified1, lastModified1, true);
    assertEquals(7, ((OracleOperationBuilderImpl) builder).timeRange(lastModified3, null, false).count());

    // Negative tests
    try {
      // Pass null for both since and until
      builderImpl.timeRange(null, null, true).getCursor();
      fail("No exception when since and until are both null");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("'since' and 'until' arguments cannot both be null.", e.getMessage());
    }

    try {
      // Pass invalid time stamp for since
      cursor = builderImpl.timeRange("abc", lastModified1, false).getCursor();
      cursor.hasNext(); // cause exception
      fail("No exception when since time stamp is invalid");
    } catch (OracleException e) {
      // Expect an OracleException
      Throwable t = e.getCause();
      // ORA-01858: a non-numeric character was found where a numeric was expected
      assertTrue(t.getMessage().contains("ORA-01858"));
    }

    try {
      // Pass invalid time stamp for until
      ((OracleOperationBuilderImpl) col.find()).timeRange(lastModified1, "123", true).count();
      fail("No exception when until time stamp is invalid");
    } catch (OracleException e) {
      // Expect an OracleException
      Throwable t = e.getCause();
      // ORA-01840: input value not long enough for date format
      assertTrue(t.getMessage().contains("ORA-01840"));
    }
    
    if (isJDCSMode())
      return;

    // test when no lastModified column
    OracleDocument mDoc2;
    if (isJDCSMode()) {
      mDoc2 = db.createDocumentFromString("{\"keyColumn\":{\"name\":\"ID\",\"sqlType\":\"VARCHAR2\",\"maxLength\":255,\"assignmentMethod\":\"CLIENT\"},\"contentColumn\":{\"name\":\"JSON_DOCUMENT\",\"sqlType\":\"BLOB\"},\"lastModifiedColumn\":{\"name\":\"LAST_MODIFIED\"},\"versionColumn\":{\"name\":\"VERSION\",\"method\":\"UUID\"},\"creationTimeColumn\":{\"name\":\"CREATED_ON\"},\"readOnly\":false}");
    } else {
      mDoc2 = client.createMetadataBuilder().removeOptionalColumns()
          .keyColumnAssignmentMethod("CLIENT").build();
    }
    
    OracleCollection col2 = dbAdmin.createCollection("testTimeRange2", mDoc2);
    col2.insertAndGet(db.createDocumentFromString("id:1", "{ \"data\" : " + 2 + " }"));
    
    try {
      // should report exception about no lastModified column
      ((OracleOperationBuilderImpl) col2.find()).timeRange(null, "2014-07-08T03:30:32.000001", true).getOne(); 
      fail("No exception when timeRange() is called, and but there is no lastModified column");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Collection testTimeRange2 has no time stamp indicating when last modified.", e.getMessage());
    }

  }

  public void testVersion() throws Exception {
    OracleDocument mDoc = client.createMetadataBuilder().keyColumnAssignmentMethod("CLIENT").build();  
    
    OracleCollection col;
    if (isJDCSMode())
    {
      col = db.admin().createCollection("testVersion", null);
    } else
    {
      col = db.admin().createCollection("testVersion", mDoc);
    }
 

    OracleDocument doc = null;
    String version1 = null, version7 = null, version10 = null;
    String[] key = new String[10];
    for (int i = 1; i <= 10; i++) {
      if (isJDCSMode()) 
      {
        doc = col.insertAndGet(db.createDocumentFromString("{ \"v\" : " + i + " }"));
        key[i-1] = doc.getKey();
      } else
      {
        doc = col.insertAndGet(db.createDocumentFromString("id." + i,
          "{ \"v\" : " + i + " }"));
        key[i-1] = doc.getKey();
      }
      
      if (i == 1)
        version1 = doc.getVersion();
      if (i == 7)
        version7 = doc.getVersion();
      if (i == 10)
        version10 = doc.getVersion();
    }

    // Test with known version
    doc = col.find().version(version1).getOne();
    if (isJDCSMode())
    {
        assertEquals("{\"v\":" + 1 + "}", new String(doc.getContentAsByteArray(), "UTF-8"));
    } else
    {
        assertEquals("{ \"v\" : " + 1 + " }", new String(doc.getContentAsByteArray(), "UTF-8"));
    }
    
    // Test with known version and key
    doc = col.find().version(version7).key(key[6]).getOne();
    if (isJDCSMode())
    {
        assertEquals("{\"v\":" + 7 + "}", new String(doc.getContentAsByteArray(), "UTF-8"));
    } else
    {
        assertEquals("{ \"v\" : " + 7 + " }", new String(doc.getContentAsByteArray(), "UTF-8"));
    }

    // Test with unknown version
    assertEquals(0, col.find().version("xyz").count());

    // Test with unmatched version and key
    OracleCursor cursor = col.find().version(version1).key(key[4]).getCursor();
    assertEquals(false, cursor.hasNext());

    // Test with empty string for version
    cursor = col.find().version("").getCursor();
    assertEquals(false, cursor.hasNext());

    // Call version() for 1+ times
    assertEquals(1, col.find().version(version1).version(version10)
      .count());
    cursor = col.find().version(version1).version(version10)
        .getCursor();
    assertEquals(key[9], cursor.next().getKey());

    // Negative tests
    try {
      // Pass null for version
      col.find().version(null).count();
      fail("No exception when version is null");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("version argument cannot be null.", e.getMessage());
    }
    
    // test when no version column
    OracleDocument mDoc2 = client.createMetadataBuilder().removeOptionalColumns()
        .keyColumnAssignmentMethod("CLIENT").build();
    
    OracleCollection col2;
    if (isJDCSMode())
    {
      col2 = dbAdmin.createCollection("testVersion2", null);
      doc = col2.insertAndGet(db.createDocumentFromString("{ \"data\" : " + 1 + " }"));
      key[0] = doc.getKey();
    } else
    {
      col2 = dbAdmin.createCollection("testVersion2", mDoc2);
      col2.insertAndGet(db.createDocumentFromString("id:1", "{ \"data\" : " + 1 + " }"));
      key[0] = doc.getKey();
    }

    try {
      // should report exception about no lastModified column
      col2.find().key(key[0]).version("000001").getCursor(); 
    } catch (OracleException e) {
      //Expect an OracleException
      assertEquals("Collection testVersion2 has no version.", e.getMessage());
    }

  }

  public void testLastModified() throws Exception {
    OracleDocument mDoc = client.createMetadataBuilder().keyColumnAssignmentMethod("GUID").lastModifiedColumnIndex("index_lastModified").build();
    
    OracleCollection col;
    if (isJDCSMode())
    {
      col = dbAdmin.createCollection("testLastModified", null);
    } else
    {
      col = dbAdmin.createCollection("testLastModified", mDoc);
    }     

    OracleDocument doc = null;
    String lastModified1 = null, lastModified5 = null, key1 = null;
    for (int i = 1; i <= 10; i++) {
      doc = col.insertAndGet(db.createDocumentFromString("{ \"v\" : " + i + " }"));
      if (i == 1) {
        lastModified1 = doc.getLastModified();
        key1 = doc.getKey();
      }
      if (i == 5)
        lastModified5 = doc.getLastModified();
    }

    // Test with valid time stamp
    assertEquals(1, ((OracleOperationBuilderImpl) col.find()).lastModified(lastModified5).count());
    doc = ((OracleOperationBuilderImpl) col.find()).lastModified(lastModified5).getOne();
    if (isJDCSMode())
    {
      assertEquals("{\"v\":" + 5 + "}",
        new String(doc.getContentAsByteArray(), "UTF-8"));
    } else
    {
      assertEquals("{ \"v\" : " + 5 + " }",
        new String(doc.getContentAsByteArray(), "UTF-8"));
    }    

    // Test with valid time stamp and key
    OracleCursor cursor = ((OracleOperationBuilderImpl) col.find()).lastModified(lastModified1).key(key1)
        .getCursor();
    if (isJDCSMode())
    {
      assertEquals("{\"v\":" + 1 + "}", new String(cursor.next()
        .getContentAsByteArray(), "UTF-8"));
    } else
    {
      assertEquals("{ \"v\" : " + 1 + " }", new String(cursor.next()
        .getContentAsByteArray(), "UTF-8"));
    }        

    // Test with valid but inexistent time stamp
    assertEquals(0, ((OracleOperationBuilderImpl) col.find()).lastModified("2014-07-08T03:30:32.000001")
        .count());

    // call lastModified for 1+ times
    OracleOperationBuilder builder = ((OracleOperationBuilderImpl) col.find()).lastModified(lastModified5);
    assertEquals(key1, ((OracleOperationBuilderImpl) builder).lastModified(lastModified1).getOne().getKey());

    // call lastModified and timeRange
    builder = ((OracleOperationBuilderImpl) col.find()).lastModified(lastModified1);
    assertEquals(10, ((OracleOperationBuilderImpl) builder).timeRange(lastModified1,null,null).count());
    builder = ((OracleOperationBuilderImpl) col.find()).timeRange(lastModified1,null,null);
    assertEquals(key1, ((OracleOperationBuilderImpl) builder).lastModified(lastModified1).getOne().getKey());
     

    // Negative tests
    try {
      // Pass null for time stamp
      ((OracleOperationBuilderImpl) col.find()).lastModified(null).getCursor(); 
      fail("No exception when lastModified time stamp is null");
       
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("lastModified argument cannot be null.", e.getMessage());
    }

    try {
      // Pass invalid time stamp
      ((OracleOperationBuilderImpl) col.find()).lastModified("abc").getOne();
      junit.framework.Assert
          .fail("No exception when lastModified time stamp is invalid");
    } catch (OracleException e) {
      // Expect an OracleException
      Throwable t = e.getCause();
      // ORA-01858: a non-numeric character was found where a numeric was expected
      assertTrue(t.getMessage().contains("ORA-01858"));
    }

    if (isJDCSMode()) {
      // JDCS mode expected lastModifiedColumn.name to be LAST_MODIFIED
      // ORA-40774: Metadata component lastModifiedColumn.name has value NULL which differs from expected value LAST_MODIFIED.
      return;
    }
    // test when no lastModified column
    OracleDocument mDoc2 = client.createMetadataBuilder().removeOptionalColumns().keyColumnAssignmentMethod("CLIENT").build();
    
    String colName2 = "testLastModified2";
    OracleCollection col2 = dbAdmin.createCollection(colName2, mDoc2); 
    doc = col2.insertAndGet(db.createDocumentFromString("id:1", "{ \"data\" : " + 2 + " }"));
    assertNull(doc.getLastModified());
    
    try {
      builder = ((OracleOperationBuilderImpl) col2.find().key("id:1")).lastModified("2014-07-08T03:30:32.000001");
      builder.count(); 
      fail("No exception when lastModified() is called, and but there is no lastModified column");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Collection " + colName2 + " has no time stamp indicating when last modified.", e.getMessage());
    }

    // Test with the specified index on lastModified column
    OracleDocument mDoc3; 
    if (isJDCSMode()) {
      mDoc3 = db.createDocumentFromString("{\"keyColumn\":{\"name\":\"ID\",\"sqlType\":\"VARCHAR2\",\"maxLength\":255,\"assignmentMethod\":\"CLIENT\"},\"contentColumn\":{\"name\":\"JSON_DOCUMENT\",\"sqlType\":\"BLOB\"},\"lastModifiedColumn\":{\"name\":\"LAST_MODIFIED\"},\"versionColumn\":{\"name\":\"VERSION\",\"method\":\"UUID\"},\"creationTimeColumn\":{\"name\":\"CREATED_ON\"},\"readOnly\":false}");
    } else {
      mDoc3 = client.createMetadataBuilder().keyColumnAssignmentMethod("CLIENT")
          .lastModifiedColumnName("LAST_MODIFIED").lastModifiedColumnIndex("IndexLastModifiedCol").build();
    }

    OracleCollection col3 = dbAdmin.createCollection("testLastModified3", mDoc3);

    String lastModified2 = null, lastModified3 = null;
    OracleDocument doc3 = null;
    for (int n = 1; n <= 5; n++) {
      doc3 = col3.insertAndGet(db.createDocumentFromString("id-"+n,
          "{ \"data\" : " + n + " }"));
      if (n == 2) {
        lastModified2 = doc3.getLastModified();
      }
      if (n == 3)
        lastModified3 = doc3.getLastModified();
    }

    // Test with valid time stamp
    assertEquals(1, ((OracleOperationBuilderImpl) col3.find()).lastModified(lastModified2).count());
    doc3 = ((OracleOperationBuilderImpl) col3.find()).lastModified(lastModified2).getOne();
    assertEquals("{ \"data\" : 2 }", new String(doc3.getContentAsByteArray(), "UTF-8"));

    // Test with valid time stamp and key
    OracleCursor cursor3 = ((OracleOperationBuilderImpl) col3.find()).lastModified(lastModified3).key("id-3").getCursor();
    assertEquals("{ \"data\" : 3 }", new String(cursor3.next().getContentAsByteArray(), "UTF-8"));
    
    // Test with timeRange()
    doc3 = ((OracleOperationBuilderImpl) col3.find()).timeRange(lastModified3, lastModified3, true).getOne();
    assertEquals("id-3", doc3.getKey());
  }
  
  public void testGetCursor () throws Exception {
    OracleDocument mDoc; 
    if (isJDCSMode()) {
      mDoc = db.createDocumentFromString("{\"keyColumn\":{\"name\":\"ID\",\"sqlType\":\"VARCHAR2\",\"maxLength\":255,\"assignmentMethod\":\"CLIENT\"},\"contentColumn\":{\"name\":\"JSON_DOCUMENT\",\"sqlType\":\"BLOB\"},\"lastModifiedColumn\":{\"name\":\"LAST_MODIFIED\"},\"versionColumn\":{\"name\":\"VERSION\",\"method\":\"UUID\"},\"creationTimeColumn\":{\"name\":\"CREATED_ON\"},\"readOnly\":false}");
    } else {
      mDoc = client.createMetadataBuilder().keyColumnAssignmentMethod("CLIENT").build();
    }
    
    OracleCollection col = dbAdmin.createCollection("testGetCursor", mDoc);

    for (int i = 1; i <= 9; i++) {
      col.insertAndGet(db.createDocumentFromString("id-" + i, "{ \"value\" : " + i + " }"));
    }

    // Test when the operation returns 1+ documents
    OracleCursor cursor = ((OracleOperationBuilderImpl) col.find()).startKey("id", true, true).getCursor();
    int i = 1;
    assertEquals(true, cursor.hasNext());
    while(cursor.hasNext())
    {
      if (isJDCSMode()) {
        assertEquals("{\"value\":" + i + "}", new String(cursor.next().getContentAsByteArray(), "UTF-8"));
      } else {
        assertEquals("{ \"value\" : " + i + " }", new String(cursor.next().getContentAsByteArray(), "UTF-8"));
      }
      i++;
    }
    
    // Test when the operation returns 0 documents
    cursor = col.find().key("id-x").getCursor();
    assertEquals(false, cursor.hasNext());
    
    // Test when the operation returns 1 document
    cursor = col.find().key("id-5").getCursor();
    assertEquals("id-5", cursor.next().getKey());
    assertEquals(false, cursor.hasNext());
    
    // Negative tests
    try {
      // Call next() when hasNext() return false
      cursor.next();
      fail("No exception when calling next() after hasNext() returns false");
       
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("next() cannot be called on a closed cursor.", e.getMessage());
    }

  }

  public void testLimit() throws Exception {
    OracleDocument mDoc = null;
    if (isJDCSMode()) {
      // ### replace with new builder once it becomes available
      mDoc = db.createDocumentFromString("{\"keyColumn\":{\"name\":\"ID\",\"sqlType\":\"VARCHAR2\",\"maxLength\":255,\"assignmentMethod\":\"UUID\"},\"contentColumn\":{\"name\":\"JSON_DOCUMENT\",\"sqlType\":\"BLOB\"},\"lastModifiedColumn\":{\"name\":\"LAST_MODIFIED\"},\"versionColumn\":{\"name\":\"VERSION\",\"method\":\"UUID\"},\"creationTimeColumn\":{\"name\":\"CREATED_ON\"},\"readOnly\":false}");
    } else {
      mDoc = client.createMetadataBuilder().versionColumnMethod("UUID").build();
    }

    OracleCollection col = dbAdmin.createCollection("testLimit", mDoc);


    for (int i = 1; i <= 10; i++) {
      col.insertAndGet(db.createDocumentFromString(null, "{ \"value\" : " + i + " }"));
    }

    // Test when limit >= collection count
    OracleCursor cursor = col.find().limit(20).getCursor();
    int counter = 0;
    while(cursor.hasNext())
    {
      cursor.next();
      counter++;
    }
    assertEquals(10, counter);
    
    // Test when 0 < limit < collection count
    cursor = col.find().limit(1).getCursor();
    assertEquals(true, cursor.hasNext());
    cursor.next();
    assertEquals(false, cursor.hasNext());
    
    // Test with other limit() call
    cursor = col.find().limit(8).limit(5).limit(2).getCursor();
    assertEquals(true, cursor.hasNext());
    cursor.next();
    assertEquals(true, cursor.hasNext());
    cursor.next();
    assertEquals(false, cursor.hasNext());

    // Negative tests
    try
    {
      col.find().limit(5).count();
      fail("No exception when count() is used with limit().");
    }
    catch (OracleException e)
    {
      assertEquals(e.getMessage(), "skip or limit cannot be set for a count operation.");
    }

    try {
      // Pass 0 for limit when collection count > 0
      cursor = col.find().skip(2).limit(0).getCursor();
      fail("No exception when limit is 0");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("limit argument must be positive.", e.getMessage());
    }
    
    try {
      // Pass 0 for limit when collection count = 0
      cursor = col.find().key("ABC").limit(0).getCursor();
      fail("No exception when limit is 0");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("limit argument must be positive.", e.getMessage());
    }

    try {
      // Pass negative for limit
      cursor = col.find().limit(-2).getCursor();
      fail("No exception when limit is negative");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("limit argument must be positive.", e.getMessage());
    }
    
  }
  
  public void testSkip() throws Exception {
    OracleDocument mDoc; 
    if (isJDCSMode()) {
      mDoc = db.createDocumentFromString("{\"keyColumn\":{\"name\":\"ID\",\"sqlType\":\"VARCHAR2\",\"maxLength\":255,\"assignmentMethod\":\"CLIENT\"},\"contentColumn\":{\"name\":\"JSON_DOCUMENT\",\"sqlType\":\"BLOB\"},\"lastModifiedColumn\":{\"name\":\"LAST_MODIFIED\"},\"versionColumn\":{\"name\":\"VERSION\",\"method\":\"UUID\"},\"creationTimeColumn\":{\"name\":\"CREATED_ON\"},\"readOnly\":false}");
    } else {
      mDoc = client.createMetadataBuilder().keyColumnAssignmentMethod("CLIENT").build();
    }

    OracleCollection col = dbAdmin.createCollection("testSkip", mDoc);

    for (int i = 1; i <= 10; i++) {
      col.insertAndGet(db.createDocumentFromString("id-" + i, "{ \"value\" : " + i + " }"));
    }

    // Test with skip >= collection count
    OracleCursor cursor = col.find().limit(10).skip(20).getCursor();
    assertEquals(false, cursor.hasNext());

    cursor = col.find().limit(10).skip(10).getCursor();
    assertEquals(false, cursor.hasNext());

    // Test with 0 < skip < collection count
    cursor = ((OracleOperationBuilderImpl) col.find().skip(8)).startKey("id", true, false).limit(10).getCursor();
    assertEquals(true, cursor.hasNext());
    // after skip and order,  the left document should be id-8 and id-9
    assertEquals("id-8", cursor.next().getKey());
    if (isJDCSMode()) {
      assertEquals("{\"value\":9}", new String(cursor.next().getContentAsByteArray(), "UTF-8"));
    } else {
      assertEquals("{ \"value\" : 9 }", new String(cursor.next().getContentAsByteArray(), "UTF-8"));
    }
    assertEquals(false, cursor.hasNext());

    // Test with positive for skip when collection count is 0
    cursor = ((OracleOperationBuilderImpl) col.find()).startKey("id-999", true, true).skip(5).limit(5).getCursor();
    assertEquals(false, cursor.hasNext());

    // Test with other skip() call
    cursor = col.find().skip(6).skip(4).skip(8).getCursor();
    assertEquals(true, cursor.hasNext());
    cursor.next();
    assertEquals(true, cursor.hasNext());
    cursor.next();
    assertEquals(false, cursor.hasNext());

    // Test with limit()(pagination function)
    cursor = ((OracleOperationBuilderImpl) col.find().limit(3)).startKey("id", true, true).getCursor();
    assertEquals("id-1", cursor.next().getKey());
    assertEquals("id-10", cursor.next().getKey());
    assertEquals("id-2", cursor.next().getKey());

    cursor = ((OracleOperationBuilderImpl) col.find().skip(3).limit(3)).startKey("id", true, false).getCursor();
    assertEquals("id-3", cursor.next().getKey());
    assertEquals("id-4", cursor.next().getKey());
    assertEquals("id-5", cursor.next().getKey());

    cursor = col.find().skip(9).limit(3).getCursor();
    assertEquals(true, cursor.hasNext());
    cursor.next();
    assertEquals(false, cursor.hasNext());

    cursor = ((OracleOperationBuilderImpl) col.find().skip(0).limit(10)).startKey("id", true, true).getCursor();
    assertEquals("id-1", cursor.next().getKey());

    // Pass 0 for skip when collection count = 0
    cursor = ((OracleOperationBuilderImpl) col.find()).startKey("id-999", true, false).skip(0).getCursor();
    assertFalse(cursor.hasNext());
    assertEquals(0, ((OracleOperationBuilderImpl) col.find()).startKey("id-999", true, false).skip(0).count());

    try {
      // Pass negative for skip
      cursor = col.find().skip(-2).getCursor();
      fail("No exception when skip is negative");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("skip argument must be nonnegative.", e.getMessage());
    }

    // Test skip() with count() (error)
    try
    {
      col.find().skip(3).count();
      fail("No exception when skip() and limit() are used with count().");
    }
    catch (OracleException e)
    {
      assertEquals(e.getMessage(), "skip or limit cannot be set for a count operation.");
    }
  }


  /**
   * Make sure basic pagination plan (i.e. whole table pagination
   * only, no where clause, and no filer spec orderby).
   * has the following form. This should be produced
   * by the pagination workaround (see OracleOperationBuilderImpl.
   * paginationWorkaround() method for more info).
   *
   * SELECT STATEMENT
   * NESTED LOOPS
   *   VIEW
   *    WINDOW NOSORT STOPKEY
   *     INDEX FULL SCAN
   *   TABLE ACCESS BY USER ROWID
   *
   * The index full scan/window no sort stopkey/view is used to
   * get the rowids of the rows we are interested in. This become
   * the inner (left) side of the join. The outer (right) side
   * of the join is the table. Thus table access is only for
   * the rows with rowids we're interested in.
   */
  public void testSkipAndLimitPlan() throws Exception {
    OracleCollection col = dbAdmin.createCollection("testSkipAndLimitPlan");
    OracleDocument d;

    for (int i = 0; i < 50; i++)
    {
      d = db.createDocumentFromString("{\"num\" : " + i + "}");
      col.insert(d);
    }

    OracleOperationBuilderImpl obuilder = (OracleOperationBuilderImpl) col.
                                          find().skip(10).limit(10);

    String plan = obuilder.explainPlan("basic");

    checkPaginationPlan(plan);
  }

  // Similar to testSkipAndLimitPlan(), but for skip only.
  public void testSkipPlan() throws Exception {
    OracleCollection col = dbAdmin.createCollection("testSkipPlan");
    OracleDocument d;

    for (int i = 0; i < 30; i++)
    {
      d = db.createDocumentFromString("{\"num\" : " + i + "}");
      col.insert(d);
    }

    OracleOperationBuilderImpl obuilder = (OracleOperationBuilderImpl) col.
            find().skip(10);

    String plan = obuilder.explainPlan("basic");

    checkPaginationPlan(plan);
  }

  // Similar to testSkipAndLimitPlan(), but for limit only.
  public void testLimitPlan() throws Exception {
    OracleCollection col = dbAdmin.createCollection("testLimitPlan");
    OracleDocument d;

    for (int i = 0; i < 30; i++)
    {
      d = db.createDocumentFromString("{\"num\" : " + i + "}");
      col.insert(d);
    }

    OracleOperationBuilderImpl obuilder = (OracleOperationBuilderImpl) col.
                                          find().limit(20);

    String plan = obuilder.explainPlan("basic");

    checkPaginationPlan(plan);
  }

  private void checkPaginationPlan(String plan)
  {
    // Note: (?s) allows matching across return lines

    if (!plan.matches("(?s).*NESTED LOOPS.*"))
    {
      fail("Nested loops join is not found in pagination plan.");
    }

    if (!plan.matches("(?s).*INDEX FULL SCAN.*"))
    {
      fail("Index full scan is not found in pagination plan.");
    }

    if (!plan.matches("(?s).*TABLE ACCESS BY USER ROWID.*"))
    {
      fail("Table access by rowid is not found in pagination plan.");
    }
  }


  // For a basic start key query, make sure
  // index is used if FIRST_ROWS hint is supplied.
  public void testStartKeyPlan() throws Exception
  {
    OracleCollection col = dbAdmin.createCollection("testStartKeyPlan");
    OracleDocument d;

    for (int i = 0; i < 30; i++)
    {
      d = db.createDocumentFromString("{\"num\" : " + i + "}");
      col.insert(d);
    }

    OracleOperationBuilderImpl obuilder = (OracleOperationBuilderImpl) col.
                                          find();
    obuilder = (OracleOperationBuilderImpl)obuilder.startKey("0",true,true);
    obuilder = (OracleOperationBuilderImpl)obuilder.firstRowsHint(10);

    String plan = obuilder.explainPlan("basic");

    // Note: (?s) allows matching across return lines

    if (!plan.matches("(?s).*INDEX RANGE SCAN.*"))
    {
      fail("Index range scan is not found.");
    }
  }

  public void testHeaderOnly() throws Exception {

    OracleDocument mDoc = null;

    if (isJDCSMode()) {
      // ### replace with new builder once it becomes available
      mDoc = db.createDocumentFromString("{\"keyColumn\":{\"name\":\"ID\",\"sqlType\":\"VARCHAR2\",\"maxLength\":255,\"assignmentMethod\":\"UUID\"},\"contentColumn\":{\"name\":\"JSON_DOCUMENT\",\"sqlType\":\"BLOB\"},\"lastModifiedColumn\":{\"name\":\"LAST_MODIFIED\"},\"versionColumn\":{\"name\":\"VERSION\",\"method\":\"UUID\"},\"creationTimeColumn\":{\"name\":\"CREATED_ON\"},\"readOnly\":false}");
    }
    else {
      mDoc = client.createMetadataBuilder().keyColumnAssignmentMethod("UUID").build();
    }
    
    OracleCollection col = dbAdmin.createCollection("testHeaderOnly", mDoc);
    String key2 = null;
    for (int i = 1; i <= 10; i++) {
      OracleDocument doc1 = col.insertAndGet(db.createDocumentFromString(null, "{ \"content\" : " + i + " }"));
      if (i == 2)
        key2 = doc1.getKey();
    }

    // call headerOnly() on getOne()
    OracleDocument doc = col.find().skip(2).limit(10).headerOnly().getOne();
    assertNotNull(doc.getKey());
    assertNotNull(doc.getLastModified());
    assertNotNull(doc.getVersion());
    assertNotNull(doc.getCreatedOn());
    
    assertNull(doc.getContentAsByteArray());
    assertNull(((OracleDocumentImpl) doc).getContentAsStream());
    assertNull(doc.getContentAsString());
    
    // call headerOnly() on getCursor()
    OracleCursor cursor = col.find().key(key2).headerOnly().getCursor();
    doc = cursor.next();
    assertEquals(key2, doc.getKey());
    assertNotNull(doc.getLastModified());
    assertNotNull(doc.getVersion());
    assertNotNull(doc.getCreatedOn());
    
    assertNull(doc.getContentAsByteArray());
    assertNull(((OracleDocumentImpl) doc).getContentAsStream());
    assertNull(doc.getContentAsString());

    assertEquals(10, col.find().headerOnly().count());

    // Negative test (skip and limit not allowed with count).
    try {
      col.find().skip(8).limit(8).headerOnly().count();
      fail("No exception when skip() and limit() are specified with count().");
    }
    catch (OracleException e) {
      assertEquals(e.getMessage(), "skip or limit cannot be set for a count operation.");
    }

    // Test when no LastModified, CreatedOn and Version
    OracleDocument mDoc2 = client.createMetadataBuilder().removeOptionalColumns().build();
    
    OracleCollection col2;
    if (isJDCSMode())
    {
      col2 = dbAdmin.createCollection("testHeaderOnly2", null);
    } else
    {
      col2 = dbAdmin.createCollection("testHeaderOnly2", mDoc2);
    }

    for (int i = 1; i <= 10; i++) {
      OracleDocument doc2 = col2.insertAndGet(db.createDocumentFromString(null, "{ \"content2\" : " + i + " }"));
      if (i == 2)
        key2 = doc2.getKey();
    }
    cursor = col2.find().key(key2).headerOnly().getCursor();
    doc = cursor.next();
    assertEquals(key2, doc.getKey());
    if (!isJDCSMode()) // no need to assert it in jdcs mode
    {
      assertNull(doc.getLastModified());
      assertNull(((OracleDocumentImpl) doc).getContentAsStream());
      assertNull(doc.getVersion());
      assertNull(doc.getCreatedOn());
    }
    
    assertNull(doc.getContentAsByteArray());
  }
  
  public void testCount() throws Exception {
    OracleDocument mDoc = client.createMetadataBuilder().keyColumnAssignmentMethod("CLIENT").build();
    
    OracleCollection col;
    if (isJDCSMode())
    {
      col = dbAdmin.createCollection("testCount", null);
    } else
    {
      col = dbAdmin.createCollection("testCount", mDoc);
    }

    String version2 = null, lastModified5 = null;
    String[] key = new String[10];
    for (int i = 1; i <= 10; i++) {
      OracleDocument doc;
      if (isJDCSMode()) 
      {
        doc = col.insertAndGet(db.createDocumentFromString("{ \"content\" : " + i + " }"));
        key[i-1] = doc.getKey();
      } else
      {
        doc = col.insertAndGet(db.createDocumentFromString("id:"+i, "{ \"content\" : " + i + " }"));
        key[i-1] = doc.getKey();
      }

      if (i==2)
        version2 = doc.getVersion();
      if (i==5)
        lastModified5 = doc.getLastModified();
    }

    // Call it when the operation returns 0 document
    if (isJDCSMode()) 
    {
      assertEquals(0, col.find().key("7BA79AB96D4C44F8A37DF7FD138EC0F7").count());
    }
    else
    {
      assertEquals(0, col.find().key("id:x").count());
    }
    
    assertEquals(0, col.find().key(key[0]).version("xyz").count());
    
    // Call it when the operation returns 1 document
    assertEquals(1, col.find().key(key[1]).version(version2).count());
    assertEquals(0, ((OracleOperationBuilderImpl) col.find().key(key[1])).lastModified(lastModified5).count());
    
    // Call it when the operation returns 1+ document
    assertEquals(10, col.find().count());
    if (!isJDCSMode()) 
    {
      // startKey("id:3", false) should return "id:1", "id:10", "id:2"
      assertEquals(3, ((OracleOperationBuilderImpl) col.find()).startKey(key[2], false, false).count());
      // startKey("id:3", false) should return "id:6", "id:7", "id:8", "id:9"
      assertEquals(4, ((OracleOperationBuilderImpl) col.find()).startKey(key[5], true, true).count());
    }
    try {
      col.find().skip(3).limit(3).count();
      fail("No exception when count() is specified with skip() and limit()");
    }
    catch (OracleException e) {
     assertEquals(e.getMessage(), "skip or limit cannot be set for a count operation.");
    }
  }
  
  public void testGetOne() throws Exception {
    OracleDocument mDoc = client.createMetadataBuilder().keyColumnAssignmentMethod("CLIENT").build();

    OracleCollection col;
    if (isJDCSMode())
    {
      col = dbAdmin.createCollection("testGetOne", null);
    } else
    {
      col = dbAdmin.createCollection("testGetOne", mDoc);
    }

    String[] key = new String[10];
    for (int i = 1; i <= 10; i++) {
      OracleDocument doc;
      if (isJDCSMode()) 
      {
        doc = col.insertAndGet(db.createDocumentFromString("{\"content\":" + i + "}"));
        key[i-1] = doc.getKey();
      } else
      {
        doc = col.insertAndGet(db.createDocumentFromString("id:"+i, "{\"content\":" + i + "}"));
        key[i-1] = doc.getKey();
      }
    }

    // call getOne() when collection count is 1 
    OracleDocument doc = col.find().key(key[4]).getOne();
    assertEquals("{\"content\":5}", new String(doc.getContentAsByteArray(), "UTF-8"));
    
    // Find the lowest key (since on AJD keys are UUID, and those won't be ordered).
    String lowestKey = key[0];
    for (int i = 1; i < 10; ++i)
      if (lowestKey.compareTo(key[i]) > 0)
        lowestKey = key[i];

    // call getOne() when collection count is 1+
    doc = ((OracleOperationBuilderImpl) col.find()).startKey(lowestKey, true, false).getOne();
    assertNotNull(doc);
    
    // Negative tests
    //Call it when operation returns 0 result document
    doc = col.find().skip(10).limit(10).getOne();
    assertNull(doc);
    
  }

  //test method for read operations
  public void testReadOperations() throws Exception {
    OracleDocument mDoc = client.createMetadataBuilder().keyColumnAssignmentMethod("CLIENT").build();
    
    OracleCollection col;
    if (isJDCSMode())
    {
      col = dbAdmin.createCollection("testReadOperations", null);
    } else
    {
      col = dbAdmin.createCollection("testReadOperations", mDoc);
    }

    String version2 = null, version12 = null;
    String lastModified3 = null, lastModified7 = null, lastModified12 = null, lastModified20 = null;
    String[] key = new String[20];
    for (int i = 1; i <= 20; i++) {
      OracleDocument doc;
      if (isJDCSMode()) 
      {
        doc = col.insertAndGet(db.createDocumentFromString("{\"data\":" + i + "}"));
        key[i-1] = doc.getKey();
      } else
      {
        doc = col.insertAndGet(db.createDocumentFromString("id:"+i, "{\"data\":" + i + "}"));
        key[i-1] = doc.getKey();
      }

      switch (i)
      {
      case 2:
        version2 = doc.getVersion();
        break;
      case 3:
        lastModified3 = doc.getLastModified();
        break;
      case 7:
        lastModified7 = doc.getLastModified();
        break;
      case 12:
        lastModified12 = doc.getLastModified();
        version12 = doc.getVersion();;
        break;
      case 20:
        lastModified20 = doc.getLastModified();;
        break;
      }
    }
      
    // col.find().getCursor()
    OracleCursor cursor = col.find().getCursor();
    int counter=0;
    while(cursor.hasNext()) {
      cursor.next();
      counter++;
    }
    assertEquals(20, counter);

    // test with col.find().key(k1).version(v1).getOne()
    OracleDocument doc = col.find().key(key[1]).version(version2).getOne();
    assertEquals(key[1], doc.getKey());
    assertEquals("{\"data\":2}", new String(doc.getContentAsByteArray(), "UTF-8"));

    doc = col.find().key(key[2]).version(version2).getOne();
    assertNull(doc);

    // test with col.find().key(k1).version(v1).lastModified(t1).getOne()
    doc = ((OracleOperationBuilderImpl) col.find().key(key[6])).lastModified(lastModified7).getOne();
    assertEquals(key[6], doc.getKey());
    assertEquals("{\"data\":7}", new String(doc.getContentAsByteArray(), "UTF-8"));

    doc = ((OracleOperationBuilderImpl) col.find().key(key[16])).lastModified(lastModified7).getOne();
    assertNull(doc);

    // test with col.find().keys(keys).lastModified(t1).getOne()
    HashSet<String> keySet = new HashSet<String>();
    keySet.add(key[1]);
    
    keySet.add(key[2]);
    keySet.add(key[4]);
    keySet.add(key[6]);
    
    if (isJDCSMode())
    {
      keySet.add("7BA79AB96D4C44F8A37DF7FD138EC0F7");
      keySet.add("7BA79AB96D4C44F8A37DF7FD138EC0F8");
    }
    else
    {
      keySet.add("id:x");
      keySet.add("id:71");
    }
    doc = ((OracleOperationBuilderImpl) col.find().keys(keySet)).lastModified(lastModified7).getOne();
    assertEquals("{\"data\":7}", new String(doc.getContentAsByteArray(), "UTF-8"));

    keySet.clear();
    keySet.add(key[0]);
    keySet.add(key[1]);
    keySet.add(key[2]);
    doc = ((OracleOperationBuilderImpl) col.find().keys(keySet)).lastModified(lastModified7).getOne();
    assertNull(doc);

    // col.find().headerOnly().keys(keys).limit(5).getCursor()
    keySet.clear();
    if (isJDCSMode())
    {
      keySet.add("7BA79AB96D4C44F8A37DF7FD138EC0F7");
    }
    else
    {
      keySet.add("id:71");
      keySet.add("id-1");
      keySet.add("id-11");
    }
    
    keySet.add(key[6]);
    
    cursor = col.find().headerOnly().keys(keySet).limit(5).getCursor();
    assertEquals(true, cursor.hasNext());
    doc = cursor.next();
    assertEquals(false, cursor.hasNext());
    assertEquals(lastModified7, doc.getLastModified());
    assertNotNull(doc.getKey());
    assertNotNull(doc.getVersion());
    assertNotNull(doc.getCreatedOn());
    assertNotNull(doc.getKey());
    assertNotNull(doc.getMediaType());
    assertNull(doc.getContentAsByteArray());
    assertNull(((OracleDocumentImpl) doc).getContentAsStream());

    keySet.clear();
    if (isJDCSMode())
    {
      keySet.add("7BA79AB96D4C44F8A37DF7FD138EC0F7");
    }
    else
    {
      keySet.add("id:71");
      keySet.add("id-1");
      keySet.add("id-11");
    }
    cursor = col.find().headerOnly().keys(keySet).limit(1).getCursor();
    assertEquals(false, cursor.hasNext());

    // test with col.find().key(k1).keys(keys). count();
    keySet.clear();
    if (isJDCSMode())
    {
      keySet.add("7BA79AB96D4C44F8A37DF7FD138EC0F7");
    }
    else
    {
      keySet.add("id:71");
      keySet.add("id-1");
      keySet.add("id-11");
      keySet.add("id:21");
      keySet.add("id:123");
    }
    keySet.add(key[11]); // matched
    keySet.add(key[12]); // matched
    keySet.add(key[17]); // matched
    
    assertEquals(3, col.find().key(key[0]).keys(keySet).count());

    // test col.find().timeRange(since, until).keys(keys).limit(n).getCursor()
    keySet.clear();
    if (isJDCSMode())
    {
      keySet.add("7BA79AB96D4C44F8A37DF7FD138EC0F7");
    }
    else
    {
      keySet.add("id:71");
    }
    keySet.add(key[0]);
    keySet.add(key[6]);
    keySet.add(key[7]);
    keySet.add(key[11]);
    keySet.add(key[12]);
    OracleOperationBuilderImpl builderImpl = (OracleOperationBuilderImpl) col.find();
    cursor = builderImpl.timeRange(lastModified7, lastModified12, false).keys(keySet).limit(5).getCursor();
    assertEquals(true, cursor.hasNext());
    doc = cursor.next();
    assertEquals("{\"data\":8}", new String(doc.getContentAsByteArray(), "UTF-8"));

    keySet.clear();
    keySet.add(key[9]);
    builderImpl = (OracleOperationBuilderImpl) col.find();
    cursor = builderImpl.timeRange(lastModified7, lastModified12, false).keys(keySet).limit(5).getCursor();
    assertEquals(true, cursor.hasNext());
    doc = cursor.next();
    assertEquals("{\"data\":10}", new String(doc.getContentAsByteArray(), "UTF-8"));

    // test with col.find().timeRange(since, until). version(v1).getCursor()
    builderImpl = (OracleOperationBuilderImpl) col.find();
    cursor = builderImpl.timeRange(lastModified3, lastModified12, true). version(version12).getCursor();
    assertEquals(true, cursor.hasNext());
    doc = cursor.next();
    assertEquals(key[11], doc.getKey());

    cursor = ((OracleOperationBuilderImpl) col.find()).timeRange(lastModified12, lastModified12, false). version(version12).getCursor();
    assertEquals(false, cursor.hasNext());

    // test with col.find().keys(keys).limit(10).getCursor()
    keySet.clear();
    if (isJDCSMode())
    {
      keySet.add("7BA79AB96D4C44F8A37DF7FD138EC0F7");
    }
    else
    {
      keySet.add("id:191");
    }
    keySet.add(key[17]);
    
    cursor = col.find().keys(keySet).limit(10).getCursor();
    assertEquals(true, cursor.hasNext());
    doc = cursor.next();
    assertEquals(key[17], doc.getKey());
    assertEquals(false, cursor.hasNext());

    // test with col.find().keys(keys).limit(10).skip(10).getCursor()
    keySet.clear();
    keySet.add(key[14]);
    keySet.add(key[15]);
    keySet.add(key[16]);
    cursor = col.find().keys(keySet).limit(10).skip(10).getCursor();
    assertEquals(false, cursor.hasNext());
    
    // col.find().startKey(k1).count()
    if (!isJDCSMode())
    {
      assertEquals(8, ((OracleOperationBuilderImpl) col.find()).startKey("id:2", true, false).count());
      assertEquals(6, ((OracleOperationBuilderImpl) col.find()).startKey("id:3", true, false).count());
      assertEquals(2, ((OracleOperationBuilderImpl) col.find()).startKey("id:10", false, true).count());
      assertEquals(13, ((OracleOperationBuilderImpl) col.find()).startKey("id:20", false, true).count());
    }

    // col.find().keys(keys).timeRange(since, until).count()
    keySet.clear();
    keySet.add(key[0]);
    keySet.add(key[1]);
    keySet.add(key[14]);
    keySet.add(key[15]);
    keySet.add(key[16]);
    assertEquals(3, ((OracleOperationBuilderImpl) col.find().keys(keySet)).timeRange(lastModified3, lastModified20, true).count());

    // test with col.find().key(k1).keys(keys).skip(5). count();
    // Negative test.
    keySet.clear();
    if (isJDCSMode())
    {
      keySet.add("7BA79AB96D4C44F8A37DF7FD138EC0F7");
    }
    else
    {
      keySet.add("id:71");
      keySet.add("id-1");
      keySet.add("id:12"); // matched
      keySet.add("id:13"); // matched
      keySet.add("id:21");
      keySet.add("id:18"); // matched
      keySet.add("id:123");
    }
    
    try
    {
      col.find().key(key[0]).keys(keySet).skip(5).count();
      fail("No exception when count() is used with skip().");
    }
    catch (OracleException e)
    {
      assertEquals(e.getMessage(), "skip or limit cannot be set for a count operation.");
    }

    // lock() is not provided yet
    //col.find().startKey(key).skip(0).limit(5).lock().getCursor()
    
  }
  
  public void testReadOperations2() throws Exception {
    OracleDocument mDoc = client.createMetadataBuilder().keyColumnAssignmentMethod("SEQUENCE").keyColumnSequenceName("seq_key_column").build();
    
    OracleCollection col;
    if (isJDCSMode())
    {
      col = dbAdmin.createCollection("testReadOperations2", null);
    } else
    {
      col = dbAdmin.createCollection("testReadOperations2", mDoc);
    }
    
    String key4 = null, key5 = null, key9 = null;
    String lastModified4 = null, lastModified5 = null, lastModified14 = null, lastModified20 = null;
    String version5 = null, version9 = null;
    String createdOn5 = null;
    final String contentTypeStr = "application/json";
    for (int i = 1; i <= 20; i++) {
      OracleDocument doc = col.insertAndGet(db.createDocumentFromString(null, "{ \"data\" : " + i + " }", contentTypeStr));
      switch (i)
      {
      case 4:
        key4 = doc.getKey();
        lastModified4 = doc.getLastModified();
        break;
      case 5:
        key5 = doc.getKey();
        version5 = doc.getVersion();
        lastModified5 = doc.getLastModified();
        createdOn5 = doc.getCreatedOn();
        break;
      case 9:
        key9 = doc.getKey();
        version9 = doc.getVersion();
        break;
      case 14:
        lastModified14 = doc.getLastModified();
        break;
      case 20:
        lastModified20 = doc.getLastModified();;
        break;
      }
    }
    
    // test with col.find().key(k1).lastModified(t1).count()
    assertEquals(1, ((OracleOperationBuilderImpl) col.find().key(key5)).lastModified(lastModified5).count());
    assertEquals(0, ((OracleOperationBuilderImpl) col.find().key(key4)).lastModified(lastModified5).count());
    
    // Test for reusing OracleOperationBuilder object
    HashSet<String> keySet = new HashSet<String>();
    keySet.add("111");
    keySet.add(key4);
    keySet.add("222");
    keySet.add(key5);
    
    OracleOperationBuilder builder = col.find().keys(keySet);
    assertEquals(2, builder.count());
    
    assertEquals(1, builder.version(version5).count());
    OracleCursor cursor = ((OracleOperationBuilderImpl) builder.version(version9)).lastModified(lastModified5).getCursor();
    assertEquals(false, cursor.hasNext());
    
    OracleOperationBuilder builder2 = ((OracleOperationBuilderImpl) col.find()).timeRange(lastModified4, lastModified14, true);
    assertEquals(11, builder2.count());
    assertEquals(key5, builder2.key(key5).getOne().getKey());
    assertEquals(lastModified5, builder2.getOne().getLastModified());

    // test with col.find().key().version().getOne();
    OracleDocument doc = col.find().key(key5).version(version5).getOne();
    assertEquals(lastModified5, doc.getLastModified());
    assertNull(col.find().key(key5).version(version9).getOne());
    
    // test with col.find().key().version().headerOnly().getOne();
    doc = col.find().key(key9).version(version9).headerOnly().getOne();
    assertNotNull(doc);
    assertNull(doc.getContentAsString());
    assertNull(doc.getContentAsByteArray());
    assertNull(((OracleDocumentImpl) doc).getContentAsStream());
    
    // test with col.find().key().lastModified().getOne();
    doc = ((OracleOperationBuilderImpl) col.find().key(key4)).lastModified(lastModified4).getOne();
    if (isJDCSMode())
    {
      assertEquals("{\"data\":4}", new String(doc.getContentAsByteArray(), "UTF-8"));
    } else
    {
      assertEquals("{ \"data\" : 4 }", new String(doc.getContentAsByteArray(), "UTF-8"));
    }    
    assertNull(((OracleOperationBuilderImpl) col.find().key(key9)).lastModified(lastModified5).getOne());
    
    // test with col.find().key().lastModified().headerOnly().getOne();
    doc = ((OracleOperationBuilderImpl) col.find().key(key5)).lastModified(lastModified5).headerOnly().getOne();
    assertEquals(version5, doc.getVersion());
    assertEquals(createdOn5, doc.getCreatedOn());
    assertEquals(lastModified5, doc.getCreatedOn());
    assertEquals(contentTypeStr, doc.getMediaType());
    assertNull(doc.getContentAsString());
    assertNull(doc.getContentAsByteArray());
    assertNull(((OracleDocumentImpl) doc).getContentAsStream());
    
    assertNull(((OracleOperationBuilderImpl) col.find().key(key9)).lastModified(lastModified4).getOne());
 
    // lock() is not provided yet
    // col.find().lastModified(t1).lock().getOne()
    // col.find().version(v1).lock().getCursor()
    // col.find().key(k1).lock().getOne()
    
    // col.find().filter(fstr).lock().getOne()
    // col.find().filter().timeRange().getCursor() 
    // col.find().filter(fstr).limit(10).getCursor()
    // col. find().keys().filter()
    
  }

  private void testKeyLikeWithColumnType(String columnType) throws Exception {
    if (isJDCSMode())
        return;

    OracleDocument mDoc = client.createMetadataBuilder()
      .keyColumnAssignmentMethod("CLIENT").keyColumnType(columnType).build();

    OracleCollection col = dbAdmin.createCollection("testKeyLike" + columnType, mDoc);

    OracleDocument doc = db.createDocumentFromString("1mykeyA", "{\"name\" : \"Alex\"}");
    col.insert(doc);

    doc = db.createDocumentFromString("2mykeyB", "{\"name\" : \"Mark\"}");
    col.insert(doc);

    assertEquals(col.find().keyLike("_mykey_", null).count(), 2);

    assertEquals(col.find().keyLike("_mykeyA", null).count(), 1);

    doc = col.find().keyLike("_mykeyA", null).getOne();

    assertEquals(doc.getContentAsString(), "{\"name\" : \"Alex\"}");

    doc = db.createDocumentFromString("3mykey_C", "{\"name\" : \"Zev\"}");
    col.insert(doc);

    assertEquals(col.find().keyLike("3mykey!_C", "!").count(), 1);

    doc = col.find().keyLike("3mykey!_C", "!").getOne();

    assertEquals(doc.getContentAsString(), "{\"name\" : \"Zev\"}");
  }

  public void testKeyLike() throws Exception {
    testKeyLikeWithColumnType("VARCHAR2");
    testKeyLikeWithColumnType("NVARCHAR2");
  }

  private void testKeyLikeWithColumnTypeNeg(String assignMethod,
                                            String columnType) throws Exception {
    OracleRDBMSMetadataBuilder b;
    OracleDocument mDoc;
    OracleCollection col;
    if (isJDCSMode())
    {
      col = dbAdmin.createCollection("testKeyLike", null);
    } else
    {
      b = client.createMetadataBuilder()
      .keyColumnAssignmentMethod(assignMethod).keyColumnType(columnType);

      if (assignMethod.equals("SEQUENCE"))
      {
        b.keyColumnSequenceName("testKeyLike" + assignMethod + columnType + "Seq");
      }

      mDoc = b.build();
      col = dbAdmin.createCollection("testKeyLike" + assignMethod + columnType,
                                                     mDoc);
    }
    

    try {
      col.find().keyLike("_mykey_", null).getCursor();
      fail("No exception for unsupported kyeLike");
    }
    catch(OracleException e) {
      assertEquals(e.getMessage(), "keyLike() can only be specified" +
        " for a collection with client-assigned keys and a varchar2 key column type.");
    }
  }

  public void testKeyLikeNeg() throws Exception {
    if (isJDCSMode()) // only one test needed in jdcs mode
    {
      testKeyLikeWithColumnTypeNeg(null, null);
      return;
    }
    testKeyLikeWithColumnTypeNeg("UUID", "RAW");
    testKeyLikeWithColumnTypeNeg("UUID", "NUMBER");
    testKeyLikeWithColumnTypeNeg("GUID", "RAW");
    testKeyLikeWithColumnTypeNeg("GUID", "NUMBER");
    testKeyLikeWithColumnTypeNeg("SEQUENCE", "RAW");
    testKeyLikeWithColumnTypeNeg("SEQUENCE", "NUMBER");
  }  

  private void testLockWithColumnType(String columnType) throws Exception {
    try {

      OracleDocument mDoc = null;
 
      if (isJDCSMode()) {
        // ### replace with new builder once it becomes available
        mDoc = db.createDocumentFromString("{\"keyColumn\":{\"name\":\"ID\",\"sqlType\":\"VARCHAR2\",\"maxLength\":255,\"assignmentMethod\":\"UUID\"},\"contentColumn\":{\"name\":\"JSON_DOCUMENT\",\"sqlType\":\"BLOB\"},\"lastModifiedColumn\":{\"name\":\"LAST_MODIFIED\"},\"versionColumn\":{\"name\":\"VERSION\",\"method\":\"UUID\"},\"creationTimeColumn\":{\"name\":\"CREATED_ON\"},\"readOnly\":false}");
      }
      else {
        mDoc = client.createMetadataBuilder().contentColumnType(columnType).build();
      }

      OracleCollection col = db.admin().createCollection("lockCollection", mDoc);

      // Auto commit should not be on for testing lock()
      conn.setAutoCommit(false);

      OracleDocument doc1 = db.createDocumentFromString("{\"name\" : \"raphael\"}");
      OracleDocument doc2 = db.createDocumentFromString("{\"name\" : \"leonardo\"}");

      OracleDocument retDoc = col.insertAndGet(doc1);
      col.insert(doc2);

      // Lock all documents
      col.find().lock().getCursor();
      conn.commit();

      // Lock documents matching a QBE
      OracleDocument f = db.createDocumentFromString("{\"name\" : {\"$exists\" : true}}");
      col.find().filter(f).lock().getCursor();
      conn.commit();

      // Lock a single document
      OracleDocument lockedDoc = col.find().key(retDoc.getKey()).lock().getOne();
      assertTrue(lockedDoc.getContentAsString().contains("raphael"));
      conn.commit();

      // Negative tests

      try {
        col.find().lock().count();
        fail("No exception when lock() is specified with count()");
      }
      catch (OracleException e) {
        assertEquals(e.getMessage(), "Method lock() cannot be specified in conjunction with method count().");
      }

      try {
        col.find().lock().skip(10).getCursor();
        fail("No exception when lock() is specified with skip()");
      }
      catch (OracleException e) {
        assertEquals(e.getMessage(), "Method lock() cannot be specified in conjunction with method skip().");
      }

      try {
        col.find().lock().limit(10).getCursor();
        fail("No exception when lock() is specified with limit()");
      }
      catch (OracleException e) {
        assertEquals(e.getMessage(), "Method lock() cannot be specified in conjunction with method limit().");
      }

      try {
        // Put lock after limit (in the test above lock was before limit).
        // This order should be tested as well.
        col.find().limit(10).lock().getCursor();
        fail("No exception when lock() is specified with limit()");
      }
      catch (OracleException e) {
        assertEquals(e.getMessage(), "Method lock() cannot be specified in conjunction with method limit().");
      }

      try {
        // Put lock after skip  (in the test above lock was before skip).
        // This order should be tested as well.
        col.find().skip(10).lock().getCursor();
        fail("No exception when lock() is specified with skip()");
      }
      catch (OracleException e) {
        assertEquals(e.getMessage(), "Method lock() cannot be specified in conjunction with method skip().");
      }


    }
    finally {
      conn.setAutoCommit(true);
    }
  }

  public void testLockBLOB() throws Exception {
    testLockWithColumnType("BLOB");
  }

  public void testLockVARCHAR2() throws Exception {
    if (isJDCSMode())
      return;

    testLockWithColumnType("VARCHAR2");
  }
    

  public void testLockCLOB() throws Exception {
    if (isJDCSMode())
      return;

    testLockWithColumnType("CLOB");
  }

}
