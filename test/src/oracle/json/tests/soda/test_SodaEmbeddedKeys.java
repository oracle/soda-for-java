/* $Header: xdk/test/txjjson/src/oracle/json/tests/soda/test_SodaEmbeddedKeys.java /st_xdk_soda1/17 2024/09/16 17:30:12 vemahaja Exp $ */

/* Copyright (c) 2021, 2024, Oracle and/or its affiliates.*/

/*
   MODIFIED    (MM/DD/YY)
    jspiegel    04/29/21 - Creation
 */
package oracle.json.tests.soda;

import java.util.Map.Entry;

import oracle.json.testharness.SodaTestCase;
import oracle.json.util.ByteArray;

import oracle.soda.OracleCollection;
import oracle.soda.OracleDocument;
import oracle.soda.OracleCursor;

import oracle.sql.json.OracleJsonBinary;
import oracle.sql.json.OracleJsonFactory;
import oracle.sql.json.OracleJsonObject;
import oracle.sql.json.OracleJsonValue;
import oracle.sql.json.OracleJsonValue.OracleJsonType;

import java.util.ArrayList;
import java.util.List;

/**
 *  @since release specific (what release of product did this appear in)
 */
public class test_SodaEmbeddedKeys extends SodaTestCase {
  
    public final static String METADATA_REG_VARCHAR_EMBEDDED = "{\n"
        + "  \"keyColumn\" : { \n"
        + "    \"name\" : \"ID\", \n"
        + "    \"assignmentMethod\" : \"EMBEDDED_OID\",\n"
        + "    \"path\" : \"_id\" \n"
        + "  },  \n"
        + "  \"contentColumn\" : { \n"
        + "      \"sqlType\" : \"BLOB\", \n"
        + "      \"jsonFormat\": \"OSON\""
        + "  }, \n"
        + "  \"versionColumn\" : { \n"
        + "     \"name\" : \"VERSION\", \n"
        + "     \"method\" : \"UUID\" \n"
        + "  }, \n"
        + "  \"lastModifiedColumn\" : { \n"
        + "     \"name\" : \"LAST_MODIFIED\" \n"
        + "  },\n"
        + "  \"creationTimeColumn\" : { \n"
        + "     \"name\" : \"CREATED_ON\"\n"
        + "  }\n"
        + "}";
    
    public final static String METADATA_21_VARCHAR_EMBEDDED = "{\n"
        + "  \"keyColumn\" : { \n"
        + "    \"name\" : \"ID\", \n"
        + "    \"assignmentMethod\" : \"EMBEDDED_OID\",\n"
        + "    \"path\" : \"_id\" \n"
        + "  },  \n"
        + "  \"contentColumn\" : { \n"
        + "      \"sqlType\" : \"JSON\" \n"
        + "  }, \n"
        + "  \"versionColumn\" : { \n"
        + "     \"name\" : \"VERSION\", \n"
        + "     \"method\" : \"UUID\" \n"
        + "  }, \n"
        + "  \"lastModifiedColumn\" : { \n"
        + "     \"name\" : \"LAST_MODIFIED\" \n"
        + "  },\n"
        + "  \"creationTimeColumn\" : { \n"
        + "     \"name\" : \"CREATED_ON\"\n"
        + "  }\n"
        + "}";
    

    public void testMergeOneAndGetWithMatColumnEmbeddedID() throws Exception {
      if (!isCompatibleOrGreater(COMPATIBLE_23))
        return;

      //Default collection
      testMergeOneAndGetWithMatColumnEmbeddedID(null);

      ///////////////////////////////////////////////////////////
      // Sha256 version (less optimized mergepatch code path). //
      ///////////////////////////////////////////////////////////
      OracleDocument doc = client.createMetadataBuilder().keyColumnAssignmentMethod("EMBEDDED_OID").
                                                          keyColumnType("RAW").
                                                          contentColumnType("JSON").
                                                          build();
      testMergeOneAndGetWithMatColumnEmbeddedID(doc);

      ////////////////////////////////////////////////////////////////////////////////////////
      // Repeat the above tests, but now for UUID version (optimized mergepatch code path). //
      ////////////////////////////////////////////////////////////////////////////////////////
      doc = client.createMetadataBuilder().keyColumnAssignmentMethod("EMBEDDED_OID").keyColumnType("RAW").
                                           contentColumnType("JSON").versionColumnMethod("UUID").build();
      testMergeOneAndGetWithMatColumnEmbeddedID(doc);
    }

    private void testMergeOneAndGetWithMatColumnEmbeddedID(OracleDocument meta) throws Exception {

      OracleDocument doc = null;

      OracleCollection col = null;

      if (meta != null)
        col = db.admin().createCollection("myColMergeOne", meta);
      else
        col = db.admin().createCollection("myColMergeOne");

      OracleDocument ret = col.insertAndGet(db.createDocumentFromString("{ \"_id\" : 1, \"name\" : \"alex\"}"));

      // Use mergeOneAndGet to change the value of "name" field
      ret = col.find().key(ret.getKey()).mergeOneAndGet(db.createDocumentFromString("{ \"name\" : \"alexander\"}"));

      assertNotNull(ret.getKey());
      assertNotNull(ret.getVersion());
      if (isNative(col)) {
        assertNull(ret.getLastModified());
        assertNull(ret.getCreatedOn());
      }
      else {
        assertNotNull(ret.getLastModified());
        assertNotNull(ret.getCreatedOn());
      }
      assertNotNull(ret.getMediaType());

      doc  = col.find().key(ret.getKey()).filter("{\"_id\" : 1, \"name\" : \"alexander\"}").getOne();
      OracleJsonObject obj = doc.getContentAs(OracleJsonObject.class);
      OracleJsonValue id = obj.get("_id");
      assertEquals(OracleJsonType.DECIMAL, id.getOracleJsonType());
      assertEquals(1, id.asJsonNumber().intValue());

      // Use mergeOneAndGet to change the value of both "name" and "_id" fields
      ret = col.find().key(ret.getKey()).mergeOneAndGet(db.createDocumentFromString("{ \"name\" : \"Alexander\"}"));

      assertNotNull(ret.getKey());
      assertNotNull(ret.getVersion());
      if (isNative(col)) {
        assertNull(ret.getLastModified());
        assertNull(ret.getCreatedOn());
      }
      else {
        assertNotNull(ret.getLastModified());
        assertNotNull(ret.getCreatedOn());
      }
      assertNotNull(ret.getMediaType());

      doc  = col.find().key(ret.getKey()).filter("{\"name\" : \"Alexander\"}").getOne();
      obj = doc.getContentAs(OracleJsonObject.class);
      id = obj.get("_id");
      assertEquals(OracleJsonType.DECIMAL, id.getOracleJsonType());
      assertEquals(1, id.asJsonNumber().intValue());

      try
      {
        ret = col.find().key(ret.getKey()).mergeOneAndGet(db.createDocumentFromString("{ \"_id\" : null, \"name\" : \"Alexander II\"}"));
        fail("No exception when _id is nullified");
      }
      catch (Exception e) 
      {
        if (isNative(col))
          assertTrue(e.getMessage().contains("_id field cannot be removed"));
        else
          assertTrue(e.getMessage().contains("Modifying _id is not allowed for merge operation."));
      }
 
      col.admin().drop();
    }
   
    public void testInsertWithoutKey() throws Exception {
      if (!isAutonomousShared())
        if (isDBVersionBelow(19, 19))
          return;

      //Default collection 
      if (isCompatibleOrGreater(COMPATIBLE_23) && !isDBVersionBelow(23,4))
        testInsertWithoutKey(null);

      String meta = (isDBMajorVersion(21) && isCompatibleOrGreater(20)) ? METADATA_21_VARCHAR_EMBEDDED : METADATA_REG_VARCHAR_EMBEDDED;
      testInsertWithoutKey(db.createDocumentFromString(meta));

    }

    private void testInsertWithoutKey(OracleDocument meta) throws Exception {
      OracleCollection col = null;
      if (meta != null)
        col = db.admin().createCollection("testFindOne", meta);
      else
        col = db.admin().createCollection("testFindOne");

      col.insert(db.createDocumentFrom("{\"hello\":123}"));
      OracleDocument doc = col.find().getCursor().next();
      OracleJsonObject obj = doc.getContentAs(OracleJsonObject.class);
      OracleJsonValue id = obj.get("_id");
        
      assertEquals(OracleJsonType.BINARY, id.getOracleJsonType());
      OracleJsonBinary bid = id.asJsonBinary();
      assertTrue(bid.isId());

      col.admin().drop();
    }
    
    public void testInsertWithMatColumnEmbeddedID() throws Exception {
      if (!isCompatibleOrGreater(COMPATIBLE_23))
        return;

      // Default collection
      testInsertWithMatColumnEmbeddedID(null);

      OracleDocument meta = client.createMetadataBuilder().keyColumnAssignmentMethod("EMBEDDED_OID").
                                                           keyColumnType("RAW").
                                                           contentColumnType("JSON").build();
      testInsertWithMatColumnEmbeddedID(meta);
    }

    private void testInsertWithMatColumnEmbeddedID(OracleDocument meta) throws Exception {

       // Generated OID
       OracleDocument doc = null;
       OracleCollection col = null;

       if (meta != null)
         col = db.admin().createCollection("myCol");
       else
         col = db.admin().createCollection("myCol", meta);

       OracleDocument ret = col.insertAndGet(db.createDocumentFromString("{ \"name\" : \"alex\"}"));
       assertEquals(ret.getKey().length(), 26);
       assertTrue(ByteArray.isHex(ret.getKey()));

       doc = col.find().getCursor().next();
       OracleJsonObject obj = doc.getContentAs(OracleJsonObject.class);
       OracleJsonValue id = obj.get("_id");
       assertEquals(OracleJsonType.BINARY, id.getOracleJsonType());
       OracleJsonBinary bid = id.asJsonBinary();
       assertTrue(bid.isId());

       col.admin().truncate();

       // Provided numeric id
       ret = col.insertAndGet(db.createDocumentFromString("{ \"_id\" : 1, \"name\" : \"alex\"}"));
       assertEquals(ret.getKey().length(), 6);
       assertTrue(ByteArray.isHex(ret.getKey()));

       doc = col.find().getCursor().next();
       obj = doc.getContentAs(OracleJsonObject.class);
       id = obj.get("_id");
       assertEquals(id.asJsonNumber().intValue(), 1);

       col.admin().truncate();

       // Provided string id
       ret = col.insertAndGet(db.createDocumentFromString("{ \"_id\" : \"abc\", \"name\" : \"alex\"}"));
       assertEquals(ret.getKey().length(), 8);
       assertTrue(ByteArray.isHex(ret.getKey()));

       doc = col.find().getCursor().next();
       obj = doc.getContentAs(OracleJsonObject.class);
       id = obj.get("_id");
       assertEquals(id.asJsonString().getString(), "abc");

       col.admin().truncate();

       ret = col.insertAndGet(db.createDocumentFromString("{ \"_id\" : {\"first\" : \"les\", \"last\" : \"paul\"}, \"occupation\" : \"guitarist\"}"));
       assertTrue(ByteArray.isHex(ret.getKey()));

       doc = col.find().getCursor().next();
       obj = doc.getContentAs(OracleJsonObject.class);
       id = obj.get("_id");
       assertEquals("les", id.asJsonObject().getString("first"));
       assertEquals("paul", id.asJsonObject().getString("last"));

       col.admin().drop();
    }

    public void testBulkInsertWithMatColumnEmbeddedID() throws Exception {
      if (!isCompatibleOrGreater(COMPATIBLE_23))
        return;
 
      //Default collection
      testBulkInsertWithMatColumnEmbeddedID(null);

      OracleDocument meta = client.createMetadataBuilder().keyColumnAssignmentMethod("EMBEDDED_OID").
                                                           keyColumnType("RAW").
                                                           contentColumnType("JSON").build();
       
      testBulkInsertWithMatColumnEmbeddedID(meta);
    }

    private void testBulkInsertWithMatColumnEmbeddedID(OracleDocument meta) throws Exception {

      OracleCollection col = null;

      if (meta != null)
        col = db.admin().createCollection("myColBulk", meta);
      else
        col = db.admin().createCollection("myColBulk");

      ArrayList<OracleDocument> docs = new ArrayList<OracleDocument>();

      for (int i=0; i<500; i++)
      {
        docs.add(db.createDocumentFromString("{\"status\" : " + i + "}"));
      }

      for (int i=0; i<100; i++)
      {
        docs.add(db.createDocumentFromString("{\"_id\" : \"abc" + i + "\", \"status\" : " + i + "}"));
      }

      List<OracleDocument> retDocs = col.insertAndGet(docs.iterator());
      for (int i = 0; i < retDocs.size(); i++) {
        OracleDocument retDoc = retDocs.get(i);
	assertNotNull(retDoc.getKey());
      }

      assertEquals(600, col.find().filter("{\"_id\" : {\"$exists\" : true}}").count());
      assertEquals(100, col.find().filter("{\"_id\" : {\"$regex\" : \".*abc.*\"}}").count());
      col.admin().drop();
    }    

    public void testReplaceWithMatColumnEmbeddedID() throws Exception {
      if (!isCompatibleOrGreater(COMPATIBLE_23))
        return;

      //Default
      testReplaceWithMatColumnEmbeddedID(null);

      OracleDocument meta = client.createMetadataBuilder().keyColumnAssignmentMethod("EMBEDDED_OID").
                                                           keyColumnType("RAW").
                                                           contentColumnType("JSON").build();
      testReplaceWithMatColumnEmbeddedID(meta);
    }

    private void testReplaceWithMatColumnEmbeddedID(OracleDocument meta) throws Exception {

      OracleDocument doc = null;
      OracleCollection col = null;
      
      if (meta != null)
        col = db.admin().createCollection("myColReplace", meta);
      else
        col = db.admin().createCollection("myColReplace");

      OracleDocument ret = col.insertAndGet(db.createDocumentFromString("{ \"_id\" : 1, \"name\" : \"alex\"}"));

      // "name" field value changes with this replace
      ret = col.find().key(ret.getKey()).replaceOneAndGet(db.createDocumentFromString("{ \"_id\" : 1, \"name\" : \"alexander\"}"));

      assertNotNull(ret.getKey());
      assertNotNull(ret.getVersion());
      if (!isNative(col)) {
        assertNotNull(ret.getLastModified());
        assertNotNull(ret.getCreatedOn());
      }
      else {
        assertNull(ret.getLastModified());
        assertNull(ret.getCreatedOn());
      }
      assertNotNull(ret.getMediaType());

      doc  = col.find().key(ret.getKey()).filter("{\"_id\" : 1, \"name\" : \"alexander\"}").getOne();
      OracleJsonObject obj = doc.getContentAs(OracleJsonObject.class);
      OracleJsonValue id = obj.get("_id");
      assertEquals(OracleJsonType.DECIMAL, id.getOracleJsonType());
      assertEquals(1, id.asJsonNumber().intValue());

      try {
        // _id value changes with this replace
        ret = col.find().key(ret.getKey()).replaceOneAndGet(db.createDocumentFromString("{ \"_id\" : 2, \"name\" : \"alexander\"}"));
        fail("Expected exception when trying to update an immutable column");
      } catch (Exception e) {
        assertTrue(e.getCause().getMessage().contains("ORA-54059"));
      }
      
      // no _id value provided in the replacement doc. In this case,
      // previous _id should be injected into the content of the final replace document.
      ret = col.find().key(ret.getKey()).replaceOneAndGet(db.createDocumentFromString("{ \"name\" : \"alexander\"}"));

      assertNotNull(ret.getKey());
      assertNotNull(ret.getVersion());
      if (!isNative(col)) {
        assertNotNull(ret.getLastModified());
        assertNotNull(ret.getCreatedOn());
      }
      else {
        assertNull(ret.getLastModified());
        assertNull(ret.getCreatedOn());
      }
      assertNotNull(ret.getMediaType());

      doc  = col.find().key(ret.getKey()).filter("{\"_id\" : 1, \"name\" : \"alexander\"}").getOne();
      obj = doc.getContentAs(OracleJsonObject.class);
      id = obj.get("_id");
      assertEquals(OracleJsonType.DECIMAL, id.getOracleJsonType());
      assertEquals(1, id.asJsonNumber().intValue());

      col.admin().drop();
    }

    
    /*
     *  This test ensures that after the document is inserted with embedded_id method
     *  its content does not change (the _id is injected in the persisted document, but
     *  the client-side document does not change). In other words, it ensures that
     *  documents are immutable.
     * */
    public void testDocumentImmutability() throws Exception {
      if (!isAutonomousShared())
        if (isDBVersionBelow(19, 19))
          return;

      //Default collection
      if (isCompatibleOrGreater(COMPATIBLE_23) && !isDBVersionBelow(23,4))
        testDocumentImmutability(null);

      String meta = (isDBMajorVersion(21) && isCompatibleOrGreater(20)) ? METADATA_21_VARCHAR_EMBEDDED : METADATA_REG_VARCHAR_EMBEDDED;
      testDocumentImmutability(db.createDocumentFromString(meta));
    }

    private void testDocumentImmutability(OracleDocument meta) throws Exception {
      OracleCollection col = null;

      if (meta != null)
        col = db.admin().createCollection("testDocumentImmutability", meta);
      else
        col = db.admin().createCollection("testDocumentImmutability");
      
      OracleDocument doc = db.createDocumentFrom("{\"field1\":\"testDocument\"}");
      col.insert(doc);
      assertEquals("{\"field1\":\"testDocument\"}", doc.getContentAsString());
      
      OracleJsonFactory factory = new OracleJsonFactory();
      OracleJsonObject obj = factory.createObject();
      obj.put("field1", "testDocument2");
      doc = db.createDocumentFrom(obj);
      assertEquals("{\"field1\":\"testDocument2\"}", doc.getContentAsString());
      col.admin().drop();
    }
    
    public void testUpdateOpWithEmbeddedID() throws Exception {
      if (!isAutonomousShared())
        if (isDBVersionBelow(19, 19))
          return;
        
      //Default collection
      if (isCompatibleOrGreater(COMPATIBLE_23) && !isDBVersionBelow(23,4))
        testUpdateOpWithEmbeddedID(null);

      String meta = (isDBMajorVersion(21) && isCompatibleOrGreater(20)) ? METADATA_21_VARCHAR_EMBEDDED : METADATA_REG_VARCHAR_EMBEDDED;
      testUpdateOpWithEmbeddedID(db.createDocumentFromString(meta));
    }

    private void testUpdateOpWithEmbeddedID(OracleDocument meta) throws Exception {
      OracleCollection col = null;

      if (meta != null) 
        col = db.admin().createCollection("testUpdateOpWithEmbeddedID", meta);
      else
        col = db.admin().createCollection("testUpdateOpWithEmbeddedID");
      
      OracleDocument doc = db.createDocumentFromString("{\"field2\" : \"test\", \"field1\":1, \"test\":1 }"),
                     updateDoc = db.createDocumentFromString("{\"_id\" : 1}"),
                     updateDoc3 = db.createDocumentFromString("{\"_id\" : 3, \"a\": 1}");
      
      doc = col.insertAndGet(doc);
      
      /* REPLACE */
      try {
        doc = col.find().key(doc.getKey()).replaceOneAndGet(updateDoc);
        assertEquals("1", doc.getKey());
        if (isNative(col))
          fail("Expected exception");
      } catch (Exception e) {
        if (!isNative(col)) 
          fail("Unexpected exception, replacement failed");
        else
        {
          assertTrue(e.getCause().getMessage().contains("ORA-54059"));
        }
      }
      
      try {
        col.find().key(doc.getKey()).replaceOne(updateDoc3);
        if (isNative(col))
          fail("Expected exception");
        else {
          OracleDocument repDoc = col.findOne("3");
          assertEquals("{\"_id\":3,\"a\":1}", repDoc.getContentAsString());
        }
      } catch (Exception e) {
        if (!isNative(col)) 
          fail("Unexpected exception, replacement failed");
        else
        {
          assertTrue(e.getCause().getMessage().contains("ORA-54059"));
        }
      }
      col.admin().drop();
    }
    
    public void testNegUpdateOpWithEmbeddedID() throws Exception {
      if (!isAutonomousShared())
        if (isDBVersionBelow(19, 19))
          return;

      //Default collection
      if (isCompatibleOrGreater(COMPATIBLE_23) && !isDBVersionBelow(23,4))
        testNegUpdateOpWithEmbeddedID(null);

      String meta = (isDBMajorVersion(21) && isCompatibleOrGreater(20)) ? METADATA_21_VARCHAR_EMBEDDED : METADATA_REG_VARCHAR_EMBEDDED;
      testNegUpdateOpWithEmbeddedID(db.createDocumentFromString(meta));
    }

    private void testNegUpdateOpWithEmbeddedID(OracleDocument meta) throws Exception {
      
      OracleCollection col = null;

      if (meta != null)
        col = db.admin().createCollection("testUpdateOpWithEmbeddedID", meta);
      else
        col = db.admin().createCollection("testUpdateOpWithEmbeddedID");
      
      OracleDocument doc = db.createDocumentFromString("{\"field2\" : \"test\", \"field1\":1, \"test\":1 }"),
                     doc2 = db.createDocumentFromString("{\"field3\" : 969, \"field1\":1 }"),
                     doc3 = db.createDocumentFromString("{\"field2\":1 }"),
                     updateDoc = db.createDocumentFromString("{\"_id\" : 1}"),
                     updateDoc2 = db.createDocumentFromString("{\"test\" : 1}"),
                     patch = db.createDocumentFromString("[ { \"op\": \"remove\", \"path\": \"/test\", \"value\": \"boo\" }, \"test\"]");
      
      doc = col.insertAndGet(doc);
      doc2 = col.insertAndGet(doc2);
      doc3 = col.insertAndGet(doc3);
      
      /* MERGE */
      try {
        col.find().key(doc.getKey()).mergeOneAndGet(patch);
        fail("Expected exception since merge spec is invalid");
      } catch (Exception e) {
        assertTrue(e.getMessage().contains("JSON merge patch document must be an object."));
      }
      
      try {
        col.find().key(doc.getKey()).mergeOne(patch);
        fail("Expected exception since merge spec is invalid");
      } catch (Exception e) {
        assertTrue(e.getMessage().contains("JSON merge patch document must be an object."));
      }
      
      try {
        col.find().key(doc.getKey()).mergeOneAndGet(updateDoc);
        fail("Expected exception since replacement document contains _id");
      } catch (Exception e) {
        if (isNative(col))
          assertTrue(e.getCause().getMessage().contains("ORA-54059"));
        else
          assertTrue(e.getMessage().contains("Modifying _id is not allowed for merge operation."));
      }
      
      try {
        col.find().key(doc.getKey()).mergeOne(updateDoc);
        fail("Expected exception since replacement document contains _id");
      } catch (Exception e) {
        if (isNative(col))
          assertTrue(e.getCause().getMessage().contains("ORA-54059"));
        else
          assertTrue(e.getMessage().contains("Modifying _id is not allowed for merge operation."));
      }
      
      /* REPLACE */
      try {
        col.find().key(doc3.getKey()).replaceOne(updateDoc2);
        if (!isNative(col))
          fail("Expected exception since replacement document doesn't contain _id");
      } catch (Exception e) {
        assertTrue(e.getMessage().contains("The _id field is missing in replacement document."));
        if (isNative(col))
          fail("Unexpected exception");
      }
      
      col.admin().drop(); 
    }
    
}


