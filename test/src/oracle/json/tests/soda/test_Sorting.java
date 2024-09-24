/* $Header: xdk/test/txjjson/src/oracle/json/tests/soda/test_Sorting.java /st_xdk_soda1/1 2023/02/10 16:15:52 migsilva Exp $ */

/* Copyright (c) 2023, Oracle and/or its affiliates. */

/*
   DESCRIPTION
    <short description of component this file declares/defines>

   PRIVATE CLASSES
    <list of private classes defined - with one-line descriptions>

   NOTES
    <other useful comments, qualifications, etc.>

   MODIFIED    (MM/DD/YY)
    migsilva    01/27/23 - Creation
 */

package oracle.json.tests.soda;

import java.util.ArrayList;
import java.util.Arrays;

import oracle.soda.OracleCollection;
import oracle.soda.OracleCursor;
import oracle.soda.OracleDocument;
import oracle.sql.json.OracleJsonObject;
import oracle.sql.json.OracleJsonArray;
import oracle.sql.json.OracleJsonTimestampTZ;
import oracle.sql.json.OracleJsonFactory;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import oracle.json.testharness.SodaTestCase;

/**
 *  @version $Header: xdk/test/txjjson/src/oracle/json/tests/soda/test_Sorting.java /st_xdk_soda1/1 2023/02/10 16:15:52 migsilva Exp $
 *  @author  migsilva
 *  @since   release specific (what release of product did this appear in)
 */

public class test_Sorting extends SodaTestCase {
  public void testOrderByWithString() throws Exception {
    if(isDBVersionBelow(23,0) || !compatible().equals("20"))
      return;
    OracleDocument metaDoc = client.createMetadataBuilder().contentColumnType("JSON").versionColumnMethod("UUID").build();
    
    OracleCollection col = db.admin().createCollection("testOrderByWithString", metaDoc);
    OracleDocument doc = db.createDocumentFromString("{\"xxx\" : [1,2,4,-1],  \"ddd\": [1,2] }");
    col.insert(doc);
    doc = db.createDocumentFromString("{\"xxx\" : [5,0], \"ddd\": [5,2]}");
    col.insert(doc);
    
    OracleDocument filter = db.createDocumentFromString(""
      + "{ \"$query\": {}, \"$orderby\": [{\"path\": \"xxx\" , \"order\" :  \"asc\", \"sortByMinMax\" : true}] }");
    
    OracleCursor c1 = col.find().filter(filter).getCursor();
    doc = c1.next();
    assertEquals("{\"xxx\":[1,2,4,-1],\"ddd\":[1,2]}", doc.getContentAsString());
    doc = c1.next();
    assertEquals("{\"xxx\":[5,0],\"ddd\":[5,2]}", doc.getContentAsString());
    
    filter = db.createDocumentFromString(""
        + "{ \"$query\": {}, \"$orderby\": [{\"path\": \"xxx\" , \"order\" :  \"desc\", \"sortByMinMax\" : true}, {\"path\": \"ddd\" , \"order\" :  \"desc\", \"sortByMinMax\" : true}] }");
    c1 = col.find().filter(filter).getCursor();
    doc = c1.next();
    assertEquals("{\"xxx\":[5,0],\"ddd\":[5,2]}", doc.getContentAsString());
    doc = c1.next();
    assertEquals("{\"xxx\":[1,2,4,-1],\"ddd\":[1,2]}", doc.getContentAsString());
    
    filter = db.createDocumentFromString(""
        + "{ \"$query\": {},\"$orderby\": { \"$fields\" : [{\"path\": \"xxx\" , \"order\" :  \"asc\", \"sortByMinMax\" : true}, {\"path\": \"ddd\" , \"order\" :  \"desc\", \"sortByMinMax\" : true}]}}");
    c1 = col.find().filter(filter).getCursor();
    doc = c1.next();
    assertEquals("{\"xxx\":[1,2,4,-1],\"ddd\":[1,2]}", doc.getContentAsString());
    doc = c1.next();
    assertEquals("{\"xxx\":[5,0],\"ddd\":[5,2]}", doc.getContentAsString());
  }
  
  public void testOrderByWithJsonObject() throws Exception {
    if(isDBVersionBelow(23,0) || !compatible().equals("20"))
      return;
    OracleDocument metaDoc = client.createMetadataBuilder().contentColumnType("JSON").versionColumnMethod("UUID").build();
    
    OracleCollection col = db.admin().createCollection("testOrderByWithJsonObject", metaDoc);
    
    OracleJsonFactory factory = new OracleJsonFactory();
    
    OracleJsonArray arr1 = factory.createArray(), arr2 = factory.createArray(), 
                    arr3 = factory.createArray(), arr4 = factory.createArray();
    arr1.add(5);
    arr1.add(0);
    
    arr2.add(5);
    arr2.add(2);
    
    arr3.add(1);
    arr3.add(2);
    arr3.add(4);
    arr3.add(-1);
    
    arr4.add(1);
    arr4.add(2);
    
    OracleJsonObject obj1 = factory.createObject(), obj2 = factory.createObject();
    obj1.put("xxx", arr1);
    obj1.put("ddd", arr2);
    
    obj2.put("xxx", arr3);
    obj2.put("ddd", arr4);
    
    col.insert(db.createDocumentFrom(obj1));
    col.insert(db.createDocumentFrom(obj2));
    
    OracleDocument filter = db.createDocumentFromString(""
        + "{ \"$query\": {}, \"$orderby\": [{\"path\": \"xxx\" , \"order\" :  \"asc\", \"sortByMinMax\" : true}] }");
    OracleCursor c1 = col.find().filter(filter).getCursor();
    OracleDocument doc = c1.next();
    assertEquals("{\"xxx\":[1,2,4,-1],\"ddd\":[1,2]}", doc.getContentAs(OracleJsonObject.class).toString());
    doc = c1.next();
    assertEquals("{\"xxx\":[5,0],\"ddd\":[5,2]}", doc.getContentAs(OracleJsonObject.class).toString());
    
    filter = db.createDocumentFromString(""
        + "{ \"$query\": {}, \"$orderby\": [{\"path\": \"xxx\" , \"order\" :  \"desc\", \"sortByMinMax\" : true}, {\"path\": \"ddd\" , \"order\" :  \"desc\", \"sortByMinMax\" : true}] }");
    c1 = col.find().filter(filter).getCursor();
    doc = c1.next();
    assertEquals("{\"xxx\":[5,0],\"ddd\":[5,2]}", doc.getContentAs(OracleJsonObject.class).toString());
    doc = c1.next();
    assertEquals("{\"xxx\":[1,2,4,-1],\"ddd\":[1,2]}", doc.getContentAs(OracleJsonObject.class).toString());
    
    filter = db.createDocumentFromString(""
        + "{ \"$query\": {},\"$orderby\": { \"$fields\" : [{\"path\": \"xxx\" , \"order\" :  \"asc\", \"sortByMinMax\" : true}, {\"path\": \"ddd\" , \"order\" :  \"desc\", \"sortByMinMax\" : true}]}}");
    c1 = col.find().filter(filter).getCursor();
    doc = c1.next();
    assertEquals("{\"xxx\":[1,2,4,-1],\"ddd\":[1,2]}", doc.getContentAs(OracleJsonObject.class).toString());
    doc = c1.next();
    assertEquals("{\"xxx\":[5,0],\"ddd\":[5,2]}", doc.getContentAs(OracleJsonObject.class).toString());
  }
  
  //Even with the sortByMinMax flag, the $orderby should work the same for not Array types
  public void testOrderByWithNoArrayType() throws Exception {
    if(isDBVersionBelow(23,0) || !compatible().equals("20"))
      return;
    OracleDocument metaDoc = client.createMetadataBuilder().contentColumnType("JSON").versionColumnMethod("UUID").build();
    
    OracleCollection col = db.admin().createCollection("testOrderByWithNoArrayType", metaDoc);
    
    OracleJsonFactory factory = new OracleJsonFactory();
    OracleJsonObject obj1 = factory.createObject(), obj2 = factory.createObject();
    OracleJsonTimestampTZ ts1 = factory.createTimestampTZ(OffsetDateTime.now(ZoneOffset.UTC)),
                          ts2 = factory.createTimestampTZ(OffsetDateTime.now(ZoneOffset.UTC));
    obj1.put("xxx", ts1);
    col.insert(db.createDocumentFrom(obj1));
    obj2.put("xxx", ts2);
    col.insert(db.createDocumentFrom(obj2));
    
    OracleDocument filter = db.createDocumentFromString(""
        + "{ \"$query\": {}, \"$orderby\": [{\"path\": \"xxx\" , \"order\" :  \"desc\", \"sortByMinMax\" : true}] }");
    OracleCursor c1 = col.find().filter(filter).getCursor();
    OracleDocument doc = c1.next();
    assertEquals(obj2.toString(), doc.getContentAs(OracleJsonObject.class).toString());
    doc = c1.next();
    assertEquals(obj1.toString(), doc.getContentAs(OracleJsonObject.class).toString());
  }
  
  public void testOrderByWithEmptyArrays() throws Exception {
    if(isDBVersionBelow(23,0) || !compatible().equals("20"))
      return;
    OracleDocument metaDoc = client.createMetadataBuilder().contentColumnType("JSON").versionColumnMethod("UUID").build();
    
    OracleCollection col = db.admin().createCollection("testOrderByWithEmptyArrays", metaDoc);
    OracleDocument doc = db.createDocumentFromString("{\"xxx\" : [1,2,4,-1],  \"ddd\": [1,2] }");
    col.insert(doc);
    doc = db.createDocumentFromString("{\"xxx\" : [5,0], \"ddd\": [5,2]}");
    col.insert(doc);
    doc = db.createDocumentFromString("{\"xxx\" : [], \"ddd\": []}");
    col.insert(doc);
    
    OracleDocument filter = db.createDocumentFromString(""
      + "{ \"$query\": {}, \"$orderby\": [{\"path\": \"xxx\" , \"order\" :  \"asc\", \"sortByMinMax\" : true}] }");
    
    OracleCursor c1 = col.find().filter(filter).getCursor();
    doc = c1.next();
    assertEquals("{\"xxx\":[],\"ddd\":[]}", doc.getContentAsString());
    doc = c1.next();
    assertEquals("{\"xxx\":[1,2,4,-1],\"ddd\":[1,2]}", doc.getContentAsString());
    doc = c1.next();
    assertEquals("{\"xxx\":[5,0],\"ddd\":[5,2]}", doc.getContentAsString());
    
    filter = db.createDocumentFromString(""
        + "{ \"$query\": {}, \"$orderby\": [{\"path\": \"xxx\" , \"order\" :  \"desc\", \"sortByMinMax\" : true}, {\"path\": \"ddd\" , \"order\" :  \"desc\", \"sortByMinMax\" : true}] }");
    c1 = col.find().filter(filter).getCursor();
    doc = c1.next();
    assertEquals("{\"xxx\":[5,0],\"ddd\":[5,2]}", doc.getContentAsString());
    doc = c1.next();
    assertEquals("{\"xxx\":[1,2,4,-1],\"ddd\":[1,2]}", doc.getContentAsString());
    doc = c1.next();
    assertEquals("{\"xxx\":[],\"ddd\":[]}", doc.getContentAsString());
    
    filter = db.createDocumentFromString(""
        + "{ \"$query\": {},\"$orderby\": { \"$fields\" : [{\"path\": \"xxx\" , \"order\" :  \"asc\", \"sortByMinMax\" : true}, {\"path\": \"ddd\" , \"order\" :  \"desc\", \"sortByMinMax\" : true}]}}");
    c1 = col.find().filter(filter).getCursor();
    doc = c1.next();
    assertEquals("{\"xxx\":[],\"ddd\":[]}", doc.getContentAsString());
    doc = c1.next();
    assertEquals("{\"xxx\":[1,2,4,-1],\"ddd\":[1,2]}", doc.getContentAsString());
    doc = c1.next();
    assertEquals("{\"xxx\":[5,0],\"ddd\":[5,2]}", doc.getContentAsString());
  }

}

