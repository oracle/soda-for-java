/* Copyright (c) 2014, 2019, Oracle and/or its affiliates. 
All rights reserved.*/

/*
   DESCRIPTION
    SODA security tests
 */

/**
 *  @author  Vincent Liu
 */

package oracle.json.tests.soda;

import java.sql.SQLDataException;
import java.sql.SQLException;

import oracle.soda.OracleCollection;
import oracle.soda.OracleDocument;
import oracle.soda.OracleException;
import oracle.soda.rdbms.impl.OracleOperationBuilderImpl;
import oracle.json.parser.QueryException;
import oracle.json.testharness.SodaTestCase;
import oracle.soda.rdbms.impl.SODAUtils;

public class test_OracleSodaSecurity extends SodaTestCase {
  
  public void testSQLInjection() throws Exception {
    OracleDocument metaDoc = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("CLIENT")
        .mediaTypeColumnName("MediaType")
        .build();
    OracleCollection col;
    if (isJDCSOrATPMode())
    {
      col = db.admin().createCollection("testSQLInjection", null);
    } else
    {
      col = db.admin().createCollection("testSQLInjection", metaDoc);
    }

    String version1 = null;
    OracleDocument doc = null;
    for (int i = 1; i <= 10; i++) {
      if (isJDCSOrATPMode()) 
      {
        doc = col.insertAndGet(db.createDocumentFromString("{ \"d\" : " + i + " }"));
      } else
      {
        doc = col.insertAndGet(db.createDocumentFromString("id-" + i, "{ \"d\" : " + i + " }"));
      }

      if(i==1)
        version1 = doc.getVersion();
    }
    
    // Tests about query operations
    if (!isJDCSOrATPMode())
    {
      assertEquals(0, col.find().key("xxx or 1=1 ").count());
      assertEquals(0, col.find().key("xxx ) or ( 1=1 ").count());
      
      assertNull(col.findOne("id\" or 1=1 or 1=\""));
      assertNull(col.findOne("id\" ) or ( 1=1 or 1=\""));
      
      assertEquals(0, col.find().key("yyy' or 1=1 or 1='zzz").count());
      assertEquals(0, col.find().key("yyy' ) or ( 1=1 or 1='zzz").count());
      
      assertNull(col.find().key("yyy' or 1=1 or 1='zzz").version(version1).getOne());
      assertNull(col.find().key("yyy' ) or ( 1=1 or 1='zzz").version(version1).getOne());
      
      // Tests about remove operations
      assertEquals(0, col.find().key("xxx or 1=1 ").remove());
      assertEquals(0, col.find().key("xxx') or (1=1) --").remove());
      
      // Tests about replace operations
      doc = db.createDocumentFromString("id-xxx", "{ \"d\" : \"new value\" }");
      
      assertEquals(false, col.find().key("xxx or 1=1 ").replaceOne(doc));
      assertEquals(false, col.find().key("xxx') or (1=1) --").replaceOne(doc));
    }
    col.find().remove();
    assertEquals(0, col.find().count());
        
    final String createTblStr =
      "declare pragma autonomous_transaction; "
    + "begin execute immediate 'create table tbl_by_SQLInjection ( c1 number )' end;";
    
    String[] key = new String[10];
    if (isJDCSOrATPMode()) 
    {
      try 
      {
        doc = col.insertAndGet(db.createDocumentFromString(createTblStr));
        fail("No Exception when doc's content is not json.");
      }
      catch (Exception e) 
      {
        String msg = e.getMessage();
        if (msg == null)
          fail(e.toString());
        assertTrue(msg.contains("Error occurred during conversion of the input"));
      }
    } 

    if (isJDCSOrATPMode()) 
    {
      doc = col.insertAndGet(db.createDocumentFromString("{\"originalField\" :\"" + createTblStr + "\"}"));
      key[0]  = doc.getKey();
    }
    else
    {
      doc = col.insertAndGet(db.createDocumentFromString("id-1", createTblStr, "text/plain"));
      key[0]  = doc.getKey();
      assertEquals(createTblStr, new String(col.findOne(key[0]).getContentAsByteArray(), "UTF-8"));

      doc = col.insertAndGet(db.createDocumentFromString("id-2", createTblStr, "text/plain"));
      key[1]  = doc.getKey();
      assertEquals(createTblStr, new String(col.findOne(key[1]).getContentAsByteArray(), "UTF-8"));
    }
    
    if (isJDCSOrATPMode()) 
    {
      doc = col.find().key(key[0]).replaceOneAndGet(db.createDocumentFromString("{\"sql\" :\"" + createTblStr + "\"}"));
      key[0]  = doc.getKey();
    }
    else
    {
      col.find().key(key[0]).replaceOne(db.createDocumentFromString("{\"sql\":\"" + createTblStr + "\"}"));
    }
    doc = col.findOne(key[0]);
    assertEquals("{\"sql\":\"" + createTblStr + "\"}", new String(doc.getContentAsByteArray(), "UTF-8"));
    assertEquals("application/json", doc.getMediaType());
    
    if (!isJDCSOrATPMode()) 
    {
      col.save(db.createDocumentFromString("id-1", createTblStr, "text/plain"));
      doc = col.findOne("id-1");
      assertEquals(createTblStr, new String(doc.getContentAsByteArray(), "UTF-8"));
      assertEquals("text/plain", doc.getMediaType());
      
      col.save(db.createDocumentFromString("id-3", createTblStr, "text/plain"));
      doc = col.findOne("id-3");
      assertEquals(createTblStr, new String(doc.getContentAsByteArray(), "UTF-8"));
      assertEquals("text/plain", doc.getMediaType());
      assertEquals(3, col.find().count());
    }
    else
    {
      assertEquals(1, col.find().count());
    }
    col.find().remove();

    OracleDocument metaDoc2 = client.createMetadataBuilder()
      .keyColumnAssignmentMethod("CLIENT")
      .build();
    OracleCollection col2;
    if (isJDCSOrATPMode())
    {
      col2 = dbAdmin.createCollection("testSQLInjection2", null);
    } else
    {
      col2 = dbAdmin.createCollection("testSQLInjection2", metaDoc2);
    }

    for (int i = 1; i <= 10; i++) {
      if (isJDCSOrATPMode()) 
      {
        doc = col2.insertAndGet(db.createDocumentFromString("{ \"dataValue\" : " + i + " }"));
        key[i-1] = doc.getKey();
      } else
      {
         doc = col2.insertAndGet(db.createDocumentFromString("id-" + i, "{ \"dataValue\" : " + i + " }", null));
        key[i-1] = doc.getKey();
      }     
    }
    
    String fStr = "{ \"$query\" : {\"dataValue\" : {\"$gt\" : \"" + createTblStr + "\" }}, \"$orderby\" : {\"d\" : -1} }";
    OracleDocument filterDoc = db.createDocumentFromString(fStr);
    assertEquals(0, col2.find().filter(filterDoc).count());
    
    // Test with attack via collection name
    OracleDocument metaDoc3 = client.createMetadataBuilder().keyColumnAssignmentMethod("CLIENT").build();
    final String dropTbl = 
      "declare pragma autonomous_transaction; " +
      "begin execute immediate 'drop table unknown_tbl'; end;";
    String colName3 = dropTbl.toString();
    
    // if 'drop table unknown_tbl' is really executed, ORA-00942 should be reported.
    if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel)) {
      try {
        db.admin().createCollection(colName3.toUpperCase(), metaDoc3);
        fail("No exception with long collection name");
      }
      catch (OracleException e) {
        Throwable cause = e.getCause();
        assertTrue(cause.getMessage().contains("ORA-00972"));
      }
    }
    else {
      OracleCollection col3;
      if (isJDCSOrATPMode()) 
      {
        col3 = db.admin().createCollection(colName3.toUpperCase(), null);
        doc = col3.insertAndGet(db.createDocumentFromString("{ \"dataValue\" : 1001 }"));
        key[2] = doc.getKey();
      } else
      {
        col3 = db.admin().createCollection(colName3.toUpperCase(), metaDoc3);
        doc = col3.insertAndGet(db.createDocumentFromString("id001", "{ \"dataValue\" : 1001 }", null));
        key[2] = doc.getKey();
      }
      assertEquals(1, col3.find().key(key[2]).count());
      col3.admin().drop();
    }
    
    // Test with attacks via key column name
    if (!isJDCSOrATPMode()) 
    {
      final String keyColumnStr = "CREATED_ON\" is not null) or 1=1 or (\"VERSION";
      OracleDocument metaDoc4 = client.createMetadataBuilder().keyColumnSequenceName(keyColumnStr)
        .keyColumnAssignmentMethod("CLIENT").build();
      OracleCollection col4 = db.admin().createCollection("testSQLInjection4", metaDoc4);
      col4.insertAndGet(db.createDocumentFromString("id001", "{ \"dataValue\" : 1001 }", null));
      assertEquals(0, col4.find().key("idxxx").count());
      col4.admin().drop();
    }
    
    // Test with attacks via version column name
    final String versionColumnStr = "ID\" is not null) or 1=1 or (\"CREATED_ON";
    OracleDocument metaDoc5 = client.createMetadataBuilder().versionColumnName(versionColumnStr).build();
    OracleCollection col5 = null;

    if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel)) {
      try {
        col5 = db.admin().createCollection("testSQLInjection5", metaDoc5);
        fail("No exception with long collection name");
      }
      catch (OracleException e) {
        Throwable cause = e.getCause();
        assertTrue(cause.getMessage().contains("ORA-00972"));
      }
    }
    else {
      if (isJDCSOrATPMode())
      {
        col5 = db.admin().createCollection("testSQLInjection5", null);
      } else
      {
        col5 = db.admin().createCollection("testSQLInjection5", metaDoc5);
      }
      
      doc = col5.insertAndGet(db.createDocumentFromString("{ \"dataValue\" : 1001 }"));
      assertEquals(0, col5.find().version("unknown version value").count());
      col5.admin().drop();
    }
    
    // Test with attacks via lastModified column name
    final String lastModifiedColumnStr = "ID'' is not null) or 'a'='a' or (''CREATED_ON";
    OracleDocument metaDoc6 = client.createMetadataBuilder().lastModifiedColumnName(lastModifiedColumnStr)
      .keyColumnAssignmentMethod("CLIENT").build();
    OracleCollection col6 = null;
    if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel)) {
      try {
        col6 = db.admin().createCollection("testSQLInjection6", metaDoc6);
        fail("No exception with long collection name");
      }
      catch (OracleException e) {
        Throwable cause = e.getCause();
        assertTrue(cause.getMessage().contains("ORA-00972"));
      }
    }
    else {
      if (isJDCSOrATPMode())
      {
        col6 = db.admin().createCollection("testSQLInjection6", null);
        doc = col6.insertAndGet(db.createDocumentFromString("{ \"dataValue\" : 1001 }"));
        key[0] = doc.getKey();
      } else
      {
        col6 = db.admin().createCollection("testSQLInjection6", metaDoc6);
        doc = col6.insertAndGet(db.createDocumentFromString("key1", "{ \"dataValue\" : 1001 }"));
        key[0] = doc.getKey();
      }     
      
      assertEquals(0, ((OracleOperationBuilderImpl) col6.find().key(key[0]))
        .lastModified("2017-05-17T07:29:56.825900Z").count());
      col6.admin().drop();
    }
    
    // Tests with attacks via version parameter
    if (!isJDCSOrATPMode())
    {
      OracleDocument metaDoc7 = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("CLIENT")
        .versionColumnMethod("NONE").tableName("SODATBL")
        .build();
      OracleCollection col7 = db.admin().createCollection("testSQLInjection7", metaDoc7);
      
      for (int i = 1; i <= 10; i++) {
        col7.insertAndGet(db.createDocumentFromString("id-" + i, "{ \"value\" : " + i + " }", null));
      }
      
      assertEquals(0, col7.find().key("id-xxx").version("1 or 1=1 ").count());
      assertEquals(0, col7.find().key("id-xxx").version("1') or (1=1) --").count());
      
      assertNull(col7.find().key("id-xxx").version("1 or 1=1 ").getOne());
      assertNull(col7.find().key("id-xxx").version("1') or (1=1) --").getOne());
      
      assertEquals(0, col7.find().key("id-xxx").version("1') or 1=1").remove());
      assertEquals(0, col7.find().key("id-xxx").version("1') or (1=1) --").remove());
      
      doc = db.createDocumentFromString("id-xxx", "{ \"value\" : \"replaced value\" }");
      assertEquals(false, col7.find().key("id-xxx").version("1') or 1=1").replaceOne(doc));
      assertEquals(false, col7.find().key("id-xxx").version("1') or (1=1) --").replaceOne(doc));
    }
    
    // Test with sql injection attack via filter spec
    
    // Tests when sql injection attack happen in path filed
    OracleDocument metaDoc8 = client.createMetadataBuilder().keyColumnAssignmentMethod("CLIENT").build();        
    OracleCollection col8;
    if (isJDCSOrATPMode())
    {
      col8 = db.admin().createCollection("testSQLInjection8", null);
    } else
    {
      col8 = db.admin().createCollection("testSQLInjection8", metaDoc8);
    }   
    String version2 = null;

    for (int i = 1; i <= 10; i++) {
      if (isJDCSOrATPMode())
      {
        doc = col8.insertAndGet(db.createDocumentFromString("{ \"d\" : " + i + " }"));
        key[0] = doc.getKey();
      } else
      {
        doc = col8.insertAndGet(db.createDocumentFromString("id-" + i, "{ \"d\" : " + i + " }"));
        key[0] = doc.getKey();
      }     
      if(i == 2)
        version2 = doc.getVersion();    
    }


    OracleDocument filter = db.createDocumentFromString("{\"d==1||1==1||@.d\" : {\"$gt\" : 5}}");
    assertEquals(0, col8.find().filter(filter).count());
    
    filter = db.createDocumentFromString("{\"d==1||1==1||1\" : {\"$eq\" : 5}}");
    assertEquals(0, col8.find().filter(filter).count());
    
    filter = db.createDocumentFromString("{\"d>=100)') or 1=1\" : {\"$exists\" : true}}");
    assertEquals(0, col8.find().filter(filter).count());
    
    filter = db.createDocumentFromString("{\"d>=100)')or(1=1)\" : {\"$startsWith\" : \"aaa\"}}");
    assertEquals(0, col8.find().filter(filter).count());
    
    // Tests when sql injection attack happen in argument value
    filter = db.createDocumentFromString("{\"d\": {\"$startsWith\":\"aaa')')or(1=1)\"} }");
    assertEquals(0, col8.find().filter(filter).count());

    if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel)) {
      try {
        filter = db.createDocumentFromString("{\"d\":{\"$hasSubstring\": \"abc))')or(1=1)\"} }");
      }
      catch (OracleException e) {
        Throwable cause = e.getCause();
        assertTrue(cause.getMessage().contains("ORA-40442"));
      }
    }
    else {
      filter = db.createDocumentFromString("{\"d\":{\"$hasSubstring\": \"abc))')or(1=1)\"} }");
      assertEquals(0, col8.find().filter(filter).count());
    }

    // Tests when sql injection attack happen in "id" values
    if (!isJDCSOrATPMode())
    {
      filter = db.createDocumentFromString("{\"$id\" : [\"id-0)or(1=1\"]}");
      assertEquals(0, col8.find().filter(filter).count());
      
      filter = db.createDocumentFromString("{\"$id\" : [\"id-0')or(1=1\"]}");
      assertEquals(0, col8.find().filter(filter).count());
      
      filter = db.createDocumentFromString("{\"$id\" : [\"id-0')or('1'='1\"]}");
      assertEquals(0, col8.find().filter(filter).count());
      
      filter = db.createDocumentFromString("{\"$id\" : [\"id-1'))--\"]}");
      assertEquals(0, col8.find().filter(filter).version(version2).count());
    }
    
    // Test with sql injection attack via spatial query
    OracleDocument metaDoc9 = client.createMetadataBuilder().keyColumnAssignmentMethod("CLIENT").build();
    String docStr1 = "{\"location\" : {\"type\": \"Point\", \"coordinates\": [33.7243,-118.1579]} }";
    OracleCollection col9;
    if (isJDCSOrATPMode())
    {
      col9 = db.admin().createCollection("testSQLInjection9", null);
      doc = col9.insertAndGet(db.createDocumentFromString(docStr1));
      key[0] = doc.getKey();
    } else
    {
      col9 = db.admin().createCollection("testSQLInjection9", metaDoc9);
      doc = col9.insertAndGet(db.createDocumentFromString("id001", docStr1));
      key[0] = doc.getKey();
    }  
    
    OracleDocument sptialSpec = db.createDocumentFromString(
        "{ \"location\" : { \"$near\" : {\n" +
        "    \"$geometry\" : { \"type\" : \"Point\", \"coordinates\" : [34.0162,-118.2019] },\n" +
        "    \"$distance\" : 10, \n" +
        "    \"$unit\"     : \"mile')='TRUE')or(1=1) --\"} \n" +
        "}}");
    
    try {
      col9.find().filter(sptialSpec).count();
      fail("No exception when invalid unit value is specified");
    } catch (OracleException e) {
      SQLException sqlException = (SQLException) e.getCause();
      if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel)) {
        // ORA-40449: invalid data type for return value
        // (because there are no spatial operation in 12.2.0.1
        assertTrue(sqlException.getMessage().contains("ORA-40449"));
      }
      else {
        // ORA-13205: internal error while parsing spatial parameters
        assertTrue(sqlException.getMessage().contains("ORA-13205"));
      }
    }
    
    sptialSpec = db.createDocumentFromString(
        "{ \"location\" : { \"$near\" : {\n" +
        "    \"$geometry\" : { \"type\" : \"Point\", \"coordinates\" : [34.0162,-118.2019] },\n" +
        "    \"$distance\" : \"10')='TRUE')or(1=1) --\"} \n" +
        "}}");
    
    try {
      col9.find().filter(sptialSpec).count();
      fail("No exception when $distance value is invalid");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      assertEquals("The operand for $distance must be a number.", queryException.getMessage());
    }
    
  }

}
