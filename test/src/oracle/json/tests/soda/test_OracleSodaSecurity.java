/* Copyright (c) 2014, 2015, Oracle and/or its affiliates. 
All rights reserved.*/

/*
   DESCRIPTION
    SODA security tests
 */

/**
 *  @author  Vincent Liu
 */

package oracle.json.tests.soda;

import oracle.soda.OracleCollection;
import oracle.soda.OracleDocument;
import oracle.json.testharness.SodaTestCase;

public class test_OracleSodaSecurity extends SodaTestCase {
  
  public void testSQLInjection() throws Exception {
    
    OracleDocument metaDoc = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("CLIENT")
        .mediaTypeColumnName("MediaType")
        .build();
    OracleCollection col = dbAdmin.createCollection("testSQLInjection", metaDoc);
    
    String version1 = null;
    OracleDocument doc = null;
    for (int i = 1; i <= 10; i++) {
      doc = col.insertAndGet(db.createDocumentFromString("id-" + i, "{ \"d\" : " + i + " }"));
      
      if(i==1)
        version1 = doc.getVersion();
    }
    
    assertEquals(0, col.find().key("xxx or 1=1 ").count());
    assertEquals(0, col.find().key("xxx ) or ( 1=1 ").count());
    
    assertNull(col.findOne("id\" or 1=1 or 1=\""));
    assertNull(col.findOne("id\" ) or ( 1=1 or 1=\""));
    
    assertEquals(0, col.find().key("yyy' or 1=1 or 1='zzz").count());
    assertEquals(0, col.find().key("yyy' ) or ( 1=1 or 1='zzz").count());
    
    assertNull(col.find().key("yyy' or 1=1 or 1='zzz").version(version1).getOne());
    assertNull(col.find().key("yyy' ) or ( 1=1 or 1='zzz").version(version1).getOne());
    
    col.find().remove();
    assertEquals(0, col.find().count());
        
    final String createTblStr =
      "declare pragma autonomous_transaction; "
    + "begin execute immediate 'create table tbl_by_SQLInjection ( c1 number )' end;";
    
    col.insert(db.createDocumentFromString("id-1", createTblStr, "text/plain"));
    assertEquals(createTblStr, new String(col.findOne("id-1").getContentAsByteArray(), "UTF-8"));
    
    col.insert(db.createDocumentFromString("id-2", createTblStr, "text/plain"));
    assertEquals(createTblStr, new String(col.findOne("id-2").getContentAsByteArray(), "UTF-8"));
    
    col.find().key("id-1").replaceOne(db.createDocumentFromString("{ \"sql\" : \"" + createTblStr + "\" }"));
    doc = col.findOne("id-1");
    assertEquals("{ \"sql\" : \"" + createTblStr + "\" }", new String(doc.getContentAsByteArray(), "UTF-8"));
    assertEquals("application/json", doc.getMediaType());
    
    col.save(db.createDocumentFromString("id-1", createTblStr, "text/plain"));
    doc = col.findOne("id-1");
    assertEquals(createTblStr, new String(doc.getContentAsByteArray(), "UTF-8"));
    assertEquals("text/plain", doc.getMediaType());
    
    col.save(db.createDocumentFromString("id-3", createTblStr, "text/plain"));
    doc = col.findOne("id-3");
    assertEquals(createTblStr, new String(doc.getContentAsByteArray(), "UTF-8"));
    assertEquals("text/plain", doc.getMediaType());
    
    assertEquals(3, col.find().count());
    col.find().remove();
    
    for (int i = 1; i <= 10; i++) {
      col.insertAndGet(db.createDocumentFromString("id-" + i, "{ \"dataValue\" : " + i + " }", null));
    }
    
    String fStr = "{ \"$query\" : {\"dataValue\" : {\"$gt\" : \"" + createTblStr + "\" }}, \"$orderby\" : {\"d\" : -1} }";
    OracleDocument filterDoc = db.createDocumentFromString(fStr);
    assertEquals(0, col.find().filter(filterDoc).count());
      
  }

}
