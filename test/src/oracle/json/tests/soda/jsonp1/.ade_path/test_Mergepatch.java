

/* $Header: xdk/test/txjjson/src/oracle/json/tests/soda/jsonp1/test_Mergepatch.java /st_xdk_soda1/1 2021/08/25 00:17:26 morgiyan Exp $ */

/* Copyright (c) 2019, 2021, Oracle and/or its affiliates. */
/* All rights reserved.*/
package oracle.json.tests.soda.jsonp1;

import javax.json.Json;
import javax.json.JsonObject;

import oracle.json.testharness.SodaTestCase;
import oracle.soda.OracleCollection;
import oracle.soda.OracleDocument;
import oracle.soda.OracleException;
import oracle.soda.rdbms.OracleRDBMSMetadataBuilder;
import oracle.soda.rdbms.impl.OracleDatabaseImpl;

public class test_Mergepatch extends SodaTestCase {
  
  public void testUUIDVersion() throws OracleException {
    if (isJDCSOrATPMode()) { 
      return; 
    }
    doMergeTest("UUID", null);
  }
  
  public void testHashVersion() throws OracleException {
    if (isJDCSOrATPMode()) { 
      return; 
    }
    doMergeTest("SHA256", null);
  }
  
  public void testUUIDVersionWithClob() throws OracleException {
    if (isJDCSOrATPMode()) {
      return; 
    }
    
    doMergeTest("UUID", "CLOB");
  }
  
  public void testUUIDVersionWithVarchar2() throws OracleException {
    if (isJDCSOrATPMode()) {
      return; 
    }
    
    doMergeTest("UUID", "VARCHAR2");
  }
  
  public void testUUIDVersionWithBLOB() throws OracleException {
    if (isJDCSOrATPMode()) {
      return; 
    }
    
    doMergeTest("UUID", "BLOB");
  }

  public void testHashVersionWithClob() throws OracleException {
    if (isJDCSOrATPMode()) {
      return; 
    }
    
    doMergeTest("SHA256", "CLOB");
  }
  
  public void testDefaultMetadata() throws OracleException {

    OracleCollection col = db.admin().createCollection("uuidcol");
    
    mergeTest(col);
  }
  
  public void testNullMerge() throws OracleException {
    OracleCollection col = db.admin().createCollection("uuidcol");
    
    JsonObject obj = Json.createObjectBuilder()
        .add("name", "pear")
        .add("count", 47)
        .build();

    OracleDocument doc = null;
    try {
      doc = db.createDocumentFrom(obj);
    }
    catch (OracleException e) {
      if (e.getMessage().contains("oracle.sql.json.OracleJsonFactory class is not available. Ensure the JDBC jar includes oracle.sql.json support.") && !OracleDatabaseImpl.isOracleJsonAvailable())
        return;
      else
        throw e;
    }

    doc = col.insertAndGet(doc);
    
    String key = doc.getKey();

    OracleDocument patch = db.createDocumentFromString(null);
    
    try {
      col.find().key(key).mergeOne(patch);
      fail();
    } catch (OracleException e) {

    }
  }
  
  
  private void doMergeTest(String versionColumnMethod, String contentColumnType) throws OracleException {
    OracleRDBMSMetadataBuilder md = client.createMetadataBuilder()
                              .versionColumnMethod(versionColumnMethod);
                         
    if (contentColumnType != null) {
      md.contentColumnType(contentColumnType);
    }

    OracleCollection col = db.admin().createCollection("uuidcol", md.build());
    
    mergeTest(col);
  }

  private void mergeTest(OracleCollection col) throws OracleException {
    JsonObject obj = Json.createObjectBuilder()
                         .add("name", "pear")
                         .add("count", 47)
                         .build();
    
    OracleDocument doc = null;
    try {
      doc = db.createDocumentFrom(obj);
    }
    catch (OracleException e) {
      if (e.getMessage().contains("oracle.sql.json.OracleJsonFactory class is not available. Ensure the JDBC jar includes oracle.sql.json support.") && !OracleDatabaseImpl.isOracleJsonAvailable())
        return;
      else
        throw e;
    }
    
    doc = col.insertAndGet(doc);
    
    String key = doc.getKey();
    
    
    OracleDocument patch = db.createDocumentFrom(
        Json.createObjectBuilder().add("count", 10).build()
    );

    // mergeOne
    boolean result = col.find().key(key).mergeOne(patch);
    assertTrue(result);
    
    JsonObject obj2 = col.findOne(key).getContentAs(JsonObject.class);
    assertEquals("pear", obj2.getString("name"));
    assertEquals(10, obj2.getInt("count"));
    
    
    // mergeOneAndGet
    patch = db.createDocumentFrom(
        Json.createObjectBuilder().add("count", 20).build()
    );
    OracleDocument d = col.find().key(key).mergeOneAndGet(patch);
    assertNotNull(d);
    
    JsonObject obj3 = col.findOne(key).getContentAs(JsonObject.class);
    assertEquals("pear", obj3.getString("name"));
    assertEquals(20, obj3.getInt("count"));
    
    // not found
    result = col.find()
                .key("0000000000000000")
                .mergeOne(patch);
    assertFalse(result);
    

  }

  
}
