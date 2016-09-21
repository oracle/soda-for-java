/* Copyright (c) 2014, 2016, Oracle and/or its affiliates. 
All rights reserved.*/

/*
   DESCRIPTION
    OracleOperationBuilder write operations (such as remove, replace, etc)
 */

/**
 *  @author  Vincent Liu
 */
package oracle.json.tests.soda;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import java.util.HashSet;

import javax.json.stream.JsonParsingException;

import oracle.jdbc.OraclePreparedStatement;

import oracle.soda.OracleCursor;
import oracle.soda.OracleException;
import oracle.soda.OracleCollection;
import oracle.soda.OracleDocument;
import oracle.soda.OracleOperationBuilder;
import oracle.soda.rdbms.impl.OracleOperationBuilderImpl;

import oracle.json.testharness.SodaTestCase;

public class test_OracleOperationBuilder2 extends SodaTestCase {

  public void testFilterWithEmptyStep() throws Exception          {
     testFilterWithEmptyStep(false);
  }

  public void testFilterWithEmptyStepWithJsonTextIndex() throws Exception
  {
     testFilterWithEmptyStep(true);
  }

  private void testFilterWithEmptyStep(boolean withJTextIndex) throws Exception {
    OracleCollection col = db.admin().createCollection("testFilter");

    if (withJTextIndex)
    {
       col.admin().indexAll("testFilterIndex");
    }

    OracleDocument doc = db.createDocumentFromString("{\"\" : 1}");
    col.insert(doc);
    doc = db.createDocumentFromString("{\"\" : {\"\" : { \"\" : 1}}}");
    col.insert(doc);
    doc = db.createDocumentFromString("{\"\" : { \"name\" : \"alex\"} }");
    col.insert(doc);
    doc = db.createDocumentFromString("{\"name\" : { \"\" : \"kumiko\" } }");
    col.insert(doc);
    doc = db.createDocumentFromString("{\"\" : [10, 11]}");
    col.insert(doc);
    doc = db.createDocumentFromString("{\"items\" : { \"\" : [ 10, 11 ] }}");
    col.insert(doc);
    doc = db.createDocumentFromString("{ \"\" : { \"\" : [ 10, 11 ] }}");
    col.insert(doc);
    doc = db.createDocumentFromString("{\"items\" : { \"\" : { \"\" : [ 10, 11 ] }}}");
    col.insert(doc);

    OracleDocument filter;

    // Single top-level empty step
    filter = db.createDocumentFromString("{\"\" : 1}");
    assertEquals(1, col.find().filter(filter).count());

    // Top level empty step with another non-empty step
    filter = db.createDocumentFromString("{\".name\" : \"alex\"}");
    assertEquals(1, col.find().filter(filter).count());

    // Same thing with backquotes
    filter = db.createDocumentFromString("{\"``.name\" : \"alex\"}");
    assertEquals(1, col.find().filter(filter).count());

    // Three empty steps
    filter = db.createDocumentFromString("{\"..\" : 1}");
    assertEquals(1, col.find().filter(filter).count());

    // Same thing with backquotes in some steps instead
    filter = db.createDocumentFromString("{\"``..``\" : 1}");
    assertEquals(1, col.find().filter(filter).count());

    // Empty step below the top level non-empty step
    filter = db.createDocumentFromString("{\"name.\" : \"kumiko\"}");
    assertEquals(1, col.find().filter(filter).count());

    // Same thing with backquotes
    filter = db.createDocumentFromString("{\"name.``\" : \"kumiko\"}");
    assertEquals(1, col.find().filter(filter).count());

    // Single top-level empty step with array value
    filter = db.createDocumentFromString("{\"``[1]\" : 11}");
    assertEquals(1, col.find().filter(filter).count());

    // Empty step below the top level with array value
    filter = db.createDocumentFromString("{\"items.[1]\" : 11}");
    assertEquals(1, col.find().filter(filter).count());

    // Same thing with backquotes
    filter = db.createDocumentFromString("{\"items.``[1]\" : 11}");
    assertEquals(1, col.find().filter(filter).count());

    // Two empty steps with array value
    filter = db.createDocumentFromString("{\".[1]\" : 11}");
    assertEquals(1, col.find().filter(filter).count());

    // Same thing with backquotes
    filter = db.createDocumentFromString("{\"``.[1]\" : 11}");
    assertEquals(1, col.find().filter(filter).count());

    // Two empty steps with array value
    filter = db.createDocumentFromString("{\".[1]\" : 11}");
    assertEquals(1, col.find().filter(filter).count());

    // Same thing with backquotes
    filter = db.createDocumentFromString("{\"``.[1]\" : 11}");
    assertEquals(1, col.find().filter(filter).count());

    if (withJTextIndex)
    {
      col.admin().dropIndex("testFilterIndex");
    }
  }

  private void testFilter(String contentColumnType) throws Exception {

    OracleDocument mDoc = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("CLIENT").contentColumnType(contentColumnType).build();
    
    OracleCollection col = db.admin().createCollection("testFilter", mDoc);
    for (int i = 1; i <= 10; i++) {
      col.insertAndGet(db.createDocumentFromString("id-" + i, "{ \"d\" : " + i + " }"));
    }
    
    OracleDocument filterDoc =db.createDocumentFromString("{ \"$id\" : [\"id-3\", \"id-5\", \"id-7\"] }"); 
    
    OracleOperationBuilder builder = col.find().filter(filterDoc);
    assertEquals(3, builder.count());
    OracleCursor cursor = builder.getCursor();
    assertEquals("id-3", cursor.next().getKey());
    assertEquals("{ \"d\" : 5 }", new String(cursor.next().getContentAsByteArray(), "UTF-8"));
    assertEquals("id-7", cursor.next().getKey());
    assertFalse(cursor.hasNext());
    
    HashSet<String> keySet = new HashSet<String>();
    keySet.add("id-2");
    keySet.add("id-3");
    keySet.add("id-9"); 
    builder.keys(keySet);
    assertEquals(5, builder.count());
    cursor = builder.getCursor();
    assertEquals("id-2", cursor.next().getKey());
    cursor.next(); // skip id-3
    cursor.next(); // skip id-5
    cursor.next(); // skip id-7
    assertEquals("{ \"d\" : 9 }", new String(cursor.next().getContentAsByteArray(), "UTF-8"));
    assertFalse(cursor.hasNext());
    
    filterDoc = db.createDocumentFromString("{ \"$id\" : [\"id-1\", \"id-7\"] }");
    builder = col.find().key("id-3").filter(filterDoc);
    assertEquals(3, builder.count());
    cursor = builder.getCursor();
    assertEquals("id-1", cursor.next().getKey());
    assertEquals("{ \"d\" : 3 }", new String(cursor.next().getContentAsByteArray(), "UTF-8"));
    assertEquals("{ \"d\" : 7 }", new String(cursor.next().getContentAsByteArray(), "UTF-8"));
    assertFalse(cursor.hasNext());
    
    // SODA filter might not support the following key array syntax
    /*filterDoc =db.createDocumentFromString(" [\"id-2\", \"id-5\", \"id-8\"] ");
    builder = col.find().key("id-4").filter(filterDoc);
    assertEquals(4, builder.count());
    cursor = builder.getCursor();
    assertEquals("id-2", cursor.next().getKey());
    assertEquals("{ \"d\" : 4 }", new String(cursor.next().getContentAsByteArray(), "UTF-8"));
    assertEquals("{ \"d\" : 5 }", new String(cursor.next().getContentAsByteArray(), "UTF-8"));
    assertEquals("id-8", cursor.next().getKey());*/
    
    String fStr = "{ \"$query\" : {\"d\" : {\"$gt\" : 5 }}, \"$orderby\" : {\"d\" : -1} }";
    filterDoc = db.createDocumentFromString(fStr);
    builder = ((OracleOperationBuilderImpl) col.find()).filter(filterDoc);
    cursor = builder.getCursor();
    assertEquals(1, ((OracleOperationBuilderImpl) builder).startKey("id-3", false, false).count());
    
    builder = ((OracleOperationBuilderImpl) builder).startKey("id-7", false, false);
    assertEquals(2, builder.count());
    cursor = builder.getCursor();
    assertEquals("id-6", cursor.next().getKey());
    assertEquals("{ \"d\" : 10 }", new String(cursor.next().getContentAsByteArray(), "UTF-8"));
    assertFalse(cursor.hasNext());

    fStr = "{ \"d\" : 1 } blah blah blah";
    // Test with "{ \"d\" : 1 }"
    filterDoc = db.createDocumentFromString("{ \"d\" : 1 }");
    OracleDocument doc =  col.find().filter(filterDoc).getOne();
    assertEquals("id-1", doc.getKey());
  
    try {
      // Test with "{ \"d\" : 1 } 2 3 4"
      filterDoc = db.createDocumentFromString("{ \"d\" : 1 } 2 3 4");
      cursor = col.find().filter(filterDoc).getCursor();
      fail("No exception when filter string is non-JSON format");
    } catch (OracleException e) {
      // Expect OracleException
      assertEquals("Invalid filter condition.", e.getMessage());
    }
    
    try {
      // Test with "1 2 3 4"
      filterDoc = db.createDocumentFromString("1 2 3 4");
      cursor = col.find().filter(filterDoc).getCursor();
      fail("No exception when filter string is non-JSON format");
    } catch (OracleException e) {
      // Expect OracleException
      assertEquals("Invalid filter condition.", e.getMessage());
    }

    col.admin().drop(); 
  }
 
 private void testFilter2(String contentColumnType) throws Exception {
    
    OracleDocument doc = null, mDoc = null; 
    
    if (contentColumnType.equalsIgnoreCase("BLOB")) {
      mDoc = client.createMetadataBuilder().contentColumnType("BLOB").mediaTypeColumnName("MediaType").build();
    } else {
      mDoc = client.createMetadataBuilder().contentColumnType(contentColumnType).build();
    }
    OracleCollection col = db.admin().createCollection("testFilter2", mDoc);
    
    String docStr1 = "{ \n" +
        "\"seq\": 101, \n" +
        "\"type\": \"t1\", \n" +
        "\"term1\": [\"v1\"],\n" +
        "\"term2\": [{\"value\": \"v2\", \"type\": \"t2\"}], \n" +
        "\"term3\": [{\"value\": \"v3\", \"language\": \"en\"}] }";

    String docStr2 = "{ \n" +
        "\"seq\": 102, \n" +
        "\"type\": \"t2\", \n" +
        "\"term1\": 1, \n" +
        "\"term2\": [20, 21], \n" +
        "\"term3\": [{\"value\": \"v3\", \"language\": \"ch\"}] }";

    String docStr3 = "{ \n" +
        "\"seq\": 103, \n" +
        "\"type\": \"t3\", \n" +
        "\"term1\": [30, 31, 32], \n" +
        "\"term2\": [20, 21], \n" +
        "\"term3\": {\"value\": \"v3\", \"language\": \"ch\"} }";

    String key1, key2, key3, key4;
    doc = col.insertAndGet(db.createDocumentFromString(docStr1));
    key1 = doc.getKey();
    doc = col.insertAndGet(db.createDocumentFromString(docStr2));
    key2 = doc.getKey();
    doc = col.insertAndGet(db.createDocumentFromString(docStr3));
    key3 = doc.getKey();
    
    if (col.admin().isHeterogeneous()) {
      doc = col.insertAndGet(db.createDocumentFromString(null, "data4", "text/plain"));
      key4 = doc.getKey();
    } else {
      doc = col.insertAndGet(db.createDocumentFromString("{ \"data\" : 4 }"));
      key4 = doc.getKey();
    }
 
    // match doc2
    OracleDocument filterDoc = db.createDocumentFromString(
        "{ \"seq\" : { \"$gt\" : 101, \"$lte\" : 102 } }");
    assertEquals(1, col.find().filter(filterDoc).count());
    doc = col.find().filter(filterDoc).getOne();
    assertEquals(key2, doc.getKey());
    
    // match doc1, doc3, doc5
    filterDoc = db.createDocumentFromString(
        "{ \"$id\" : [ \"" + key1 + "\", \"" + key3 + "\", \"" + key4 + "\" ] }");
    assertEquals(3, col.find().filter(filterDoc).count());

    // match doc1
    filterDoc = db.createDocumentFromString(
        "{ \"seq\" : { \"$gte\" : 101, \"$lt\" : 102 } }");
    assertEquals(1, col.find().filter(filterDoc).count());
    doc = col.find().filter(filterDoc).getOne();
    assertEquals(key1, doc.getKey());

    // match doc2, doc3
    filterDoc = db.createDocumentFromString(
        "{ \"$or\" : [ { \"type\" : { \"$eq\" : \"t3\" } }," +
                      "{ \"term3.language\" : \"ch\"  } ]," +
                      "\"$orderby\" : {\"seq\":1} }");
    assertEquals(2, col.find().filter(filterDoc).count());
    OracleCursor cursor = col.find().filter(filterDoc).getCursor();
    assertEquals(true, cursor.hasNext());
    assertEquals(key2, cursor.next().getKey());
    assertEquals(key3, cursor.next().getKey());
    assertEquals(false, cursor.hasNext());
    cursor.close();

    // match doc3
    filterDoc = db.createDocumentFromString(
        "{ \"type\" : {\"$in\": [\"t1\", \"t3\"]}, \"seq\" : {\"$gt\" : 101}}");
    assertEquals(1, col.find().filter(filterDoc).count());
    doc = col.find().filter(filterDoc).getOne();
    assertEquals(key3, doc.getKey());

    // match doc2, doc3 if doc4 is non-JSON. 
    // If doc4 is JSON, match it as well.
    filterDoc = db.createDocumentFromString(
        "{ \"type\" : {\"$nin\": [\"t1\"]}, \"$orderby\" : {\"seq\":1}  }");

    if (col.admin().isHeterogeneous()) {
      assertEquals(2, col.find().filter(filterDoc).count());
    }
    else {
      assertEquals(3, col.find().filter(filterDoc).count());
    }

    cursor = col.find().filter(filterDoc).getCursor();
    assertEquals(true, cursor.hasNext());
    assertEquals(key2, cursor.next().getKey());
    assertEquals(key3, cursor.next().getKey());

    if (!col.admin().isHeterogeneous())
    {
      assertEquals(key4, cursor.next().getKey());

    }
    assertEquals(false, cursor.hasNext());
    cursor.close();

    // match doc3
    filterDoc = db.createDocumentFromString("{\"term1[0]\" : 30 }");
    assertEquals(1, col.find().filter(filterDoc).count());
    doc = col.find().filter(filterDoc).getOne();
    assertEquals(key3, doc.getKey());

    // match doc1
    filterDoc = db.createDocumentFromString("{ \"term3[0].language\" : {\"$eq\":\"en\"} }");
    assertEquals(1, col.find().filter(filterDoc).count());
    doc = col.find().filter(filterDoc).getOne();
    assertEquals(key1, doc.getKey());

    // match doc3
    filterDoc = db.createDocumentFromString("{ \"type\" : {\"$in\": [\"t2\", \"t3\"]}, \"term1[1]\" : {\"$gt\" : 20}}");
    assertEquals(1, col.find().filter(filterDoc).count());
    doc = col.find().filter(filterDoc).getOne();
    assertEquals(key3, doc.getKey());

    // match doc1
    filterDoc = db.createDocumentFromString(
        "{ \"$and\" : [ { \"term1[0]\" : \"v1\" }, { \"term2[0].type\" : \"t2\"}, {\"term3[0].value\" : \"v3\"} ]}");
    assertEquals(1, col.find().filter(filterDoc).count());
    cursor = col.find().filter(filterDoc).getCursor();
    assertEquals(true, cursor.hasNext());
    assertEquals(key1, cursor.next().getKey());
    assertEquals(false, cursor.hasNext());
    cursor.close();

    // Tests about "$all" operator 
    // match doc3
    filterDoc = db.createDocumentFromString(" {\"term1\" : {\"$all\":[30, 31, 32]} }");
    assertEquals(1, col.find().filter(filterDoc).count());
    doc = col.find().filter(filterDoc).getOne();
    assertEquals(key3, doc.getKey());

    // match doc3
    filterDoc = db.createDocumentFromString(" {\"term1\" : {\"$all\":[32, 31, 30]} }");
    // the different order should not matter 
    assertEquals(1, col.find().filter(filterDoc).count());
    doc = col.find().filter(filterDoc).getOne();
    assertEquals(key3, doc.getKey());
    
    // match doc3, 
    // for "$all", all the constants in query condition must match the values in the instance doc
    // but does not need to cover all the values in the instance doc
    filterDoc = db.createDocumentFromString(" {\"term1\" : {\"$all\":[30, 31]} }");
    // since 30 and 31 are found in the instance doc, it's a match.
    doc = col.find().filter(filterDoc).getOne();
    assertEquals(key3, doc.getKey());

    // match none, since 33 does not match any value in the instance doc
    filterDoc = db.createDocumentFromString(" {\"term1\" : {\"$all\":[30, 31, 33]} }");
    assertEquals(0, col.find().filter(filterDoc).count());
    assertNull(col.find().filter(filterDoc).getOne());

    //
    // Tests with $not
    //

    // match doc1 only if doc4 is not JSON.
    // If doc4 is JSON, match it as well.
    filterDoc = db.createDocumentFromString("{\"term3[0].language\" :" +
                                            "{\"$not\" : {\"$all\":[\"ch\"]}}, " +
                                            "\"$orderby\" : {\"seq\":1}}");
    matchUpToTwoDocs(filterDoc, col, key1, key4);

    // match doc3 only if doc4 is not JSON. 
    // If doc4 is JSON, match it as well.
    filterDoc = db.createDocumentFromString("{\"seq\" : {\"$not\" : {\"$lt\":102}}," +
                                            "\"type\": {\"$not\" : {\"$eq\":\"t2\"}}," +
                                            "\"$orderby\" : {\"seq\":1}}");
    matchUpToTwoDocs(filterDoc, col, key3, key4);

    // match doc3 only if doc4 is not JSON.
    // If doc4 is JSON, match it as well.
    filterDoc = db.createDocumentFromString("{\"seq\" : {\"$not\":{\"$lte\":102}}," +
                                            "\"$orderby\" : {\"seq\":1}}");

    matchUpToTwoDocs(filterDoc, col, key3, key4);

    // match doc1 only if doc4 is not JSON.
    // If doc4 is JSON, match it as well.
    filterDoc = db.createDocumentFromString("{\"seq\" : {\"$not\":{\"$gte\":102}}," +
                                            "\"$orderby\" : {\"seq\":1}}");
    matchUpToTwoDocs(filterDoc, col, key1, key4);

    // match doc1, doc2,and doc3 only doc4 is not JSON.
    // If doc4 is JSON, match it as well.
    filterDoc = db.createDocumentFromString("{\"type\" : {\"$not\": {\"$startsWith\":\"n\"}} }");
    if (col.admin().isHeterogeneous()) {
      assertEquals(3, col.find().filter(filterDoc).count());
    }
    else {
      assertEquals(4, col.find().filter(filterDoc).count());
    }

    //
    // Tests with $nor
    //
 
    // match doc1 only is doc4 is not JSON.
    // If doc4 is JSON, match it as well.
    filterDoc = db.createDocumentFromString("{\"$nor\" : [{\"term3[0].language\" : \"ch\"}]," +
                                            "\"$orderby\" : {\"seq\":1}}");
    matchUpToTwoDocs(filterDoc, col, key1, key4);

    // match doc1 if mixed with non-JSON. Also doc4 otherwise.
    filterDoc = db.createDocumentFromString(
        "{\"$nor\" : [{\"seq\": {\"$gt\": 102}}, {\"type\":\"t2\"}," +
                     "{\"term1[2]\": {\"$exists\":true}} ]," +
                     "\"$orderby\" : {\"seq\" :1}}");
    matchUpToTwoDocs(filterDoc, col, key1, key4);

    // match doc1
    filterDoc = db.createDocumentFromString(
        "{\"$nor\" : [{\"term2\" : {\"$all\":[20, 21]}}," +
                     "{\"term2[0].value\":{\"$exists\":false}} ]," +
                     "\"$orderby\" : {\"seq\" :1}}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key1, col.find().filter(filterDoc).getOne().getKey());

    col.admin().drop();
  } 

  private void matchUpToTwoDocs(OracleDocument filterDoc,
                                OracleCollection col,
                                String firstKey, String secondKey)
                                throws OracleException,
                                IOException {

    // Make sure the count is correct
    if (col.admin().isHeterogeneous()) {
      assertEquals(1, col.find().filter(filterDoc).count());
    }
    else {
      assertEquals(2, col.find().filter(filterDoc).count());
    }

    // Get a cursor and find one or two docs
    // depending on whether the second doc is JSON or not.
    OracleCursor cursor = col.find().filter(filterDoc).getCursor();

    assertEquals(true, cursor.hasNext());
    assertEquals(firstKey, cursor.next().getKey());

    if (!col.admin().isHeterogeneous()) {
      assertEquals(true, cursor.hasNext());
      assertEquals(secondKey, cursor.next().getKey());
    }

    assertEquals(false, cursor.hasNext());

    cursor.close();
  }

  private void testFilter3(String contentColumnType) throws Exception {
   
   OracleDocument mDoc = null; 
   
   if (contentColumnType.equalsIgnoreCase("BLOB")) {
     mDoc = client.createMetadataBuilder().contentColumnType("BLOB").mediaTypeColumnName("MediaType").build();
   } else {
     mDoc = client.createMetadataBuilder().contentColumnType(contentColumnType).build();
   }
   OracleCollection col = db.admin().createCollection("testFilter3", mDoc);
   
   if(col.admin().isHeterogeneous()) {
     col.insert(db.createDocumentFromString(null, "{orderno}", "text/plain"));
     col.insert(db.createDocumentFromString(null, "{orderno}", "text/plain"));
     col.insert(db.createDocumentFromString(null, "{orderno}", "text/plain"));
   }
   
   String docStr1 = "{ \"order\" : [ \n" + 
       "{ \"orderno\":301, \"items\": [ { \"name\": \"Wicked Bugs\", \"price\": 11.37, \"quantity\": 1 }, \n" + 
           "{ \"name\": \"How to Lie with Statistics\", \"price\": 9.56, \"quantity\": 1 } ] }, \n" + 
       "{ \"orderno\":302, \"items\": [ { \"name\": \"Discovering Statistics\", \"price\": 15.00, \"quantity\": 2 }, \n" + 
           "{ \"name\": \"Integrated Algebra\", \"price\": 8.99, \"quantity\": 1 } ] }\n" + 
       "] }";

   String docStr2 = "{ \"order\" : [ \n" + 
       "{ \"orderno\":303, \"items\": [ { \"name\": \"Envisioning Information\", \"price\": 25.50, \"quantity\": 1 } ] }, \n" + 
       "{ \"orderno\":304, \"items\": [ { \"name\": \"Calculus For Dummies\", \"price\": 14.00, \"quantity\": 2 } ] }, \n" +
       "{ \"orderno\":305, \"items\": [ { \"name\": \"Business Law\", \"price\": 18.00, \"quantity\": 2 } ] } \n" + 
       "] }";

   String docStr3 = " { \"order\" : { \"orderno\":306, \"items\": [\n" + 
       "{ \"name\": \"Tart and Sweet\", \"price\": 16.49, \"quantity\": 1 }, \n" +
       "{ \"name\": \"Young Men and Fire\", \"price\": 10.88, \"quantity\": 2}, \n" + 
       "{ \"name\": \"Barbara Pleasant\", \"price\": 13.57, \"quantity\": 3} \n" +
       "] } }";
 
   String key1, key2, key3;
   OracleDocument filterDoc = null, doc = null;
   OracleCursor cursor = null;

   doc = col.insertAndGet(db.createDocumentFromString(docStr1));
   key1 = doc.getKey();
   doc = col.insertAndGet(db.createDocumentFromString(docStr2));
   key2 = doc.getKey();
   doc = col.insertAndGet(db.createDocumentFromString(docStr3));
   key3 = doc.getKey();

   // match doc1, doc3
   filterDoc = db.createDocumentFromString(
       "{ \"order[0].items[0].quantity\": 1, \"order[0].items[*].price\": {\"$gt\" : 9.0, \"$lte\" : 12.0} }");
   assertEquals(2, col.find().filter(filterDoc).count());

   //match doc1
   filterDoc = db.createDocumentFromString("{ \"order[0].orderno\": 301 }");
   assertEquals(key1, col.find().filter(filterDoc).getOne().getKey());
   
   filterDoc = db.createDocumentFromString("{ \"order[1].items[0].name\": \"Discovering Statistics\" }");
   assertEquals(key1, col.find().filter(filterDoc).getOne().getKey());
   
   filterDoc = db.createDocumentFromString("{ \"order[1].items[0].name\": {\"$startsWith\": \"Discovering\"} }");
   assertEquals(key1, col.find().filter(filterDoc).getOne().getKey());

   filterDoc = db.createDocumentFromString(
       "{ \"order[0].orderno\": {\"$gte\" : 301}, \"order[1].items[0].name\": {\"$startsWith\": \"Discovering\"} }");
   assertEquals(1, col.find().filter(filterDoc).count());
   cursor = col.find().filter(filterDoc).getCursor();
   assertEquals(true, cursor.hasNext());
   assertEquals(key1, cursor.next().getKey());
   assertEquals(false, cursor.hasNext());
   cursor.close();

   //match doc3
   filterDoc = db.createDocumentFromString("{ \"order[0].items[0].name\": {\"$startsWith\": \"Tart\"} }");
   assertEquals(key3, col.find().filter(filterDoc).getOne().getKey());
   
   filterDoc = db.createDocumentFromString("{ \"order[0].items[1].price\": 10.88 }");
   assertEquals(key3, col.find().filter(filterDoc).getOne().getKey());
   
   filterDoc = db.createDocumentFromString("{ \"order[0].items[2].quantity\": {\"$exists\" : true} }");
   assertEquals(key3, col.find().filter(filterDoc).getOne().getKey());

   filterDoc = db.createDocumentFromString(
       "{ \"$and\" : [{\"order[0].items[0].name\": {\"$startsWith\": \"Tart\"}}, " +
       "    {\"order[0].items[1].price\": 10.88}, {\"order[0].items[2].quantity\": {\"$exists\" : true} } ] }");
   assertEquals(1, col.find().filter(filterDoc).count());
   cursor = col.find().filter(filterDoc).getCursor();
   assertEquals(true, cursor.hasNext());
   assertEquals(key3, cursor.next().getKey());
   assertEquals(false, cursor.hasNext());
   cursor.close();

   // match doc2
   filterDoc = db.createDocumentFromString(
       "{\"order[0].orderno\" : {\"$gt\" : 301, \"$lt\" : 306} }");
   assertEquals(1, col.find().filter(filterDoc).count());
   assertEquals(key2, col.find().filter(filterDoc).getOne().getKey());
   
   // also match doc2
   filterDoc = db.createDocumentFromString(
       "{\"order[2].orderno\" : {\"$gt\" : 301, \"$lt\" : 306} }");
   assertEquals(1, col.find().filter(filterDoc).count());
   assertEquals(key2, col.find().filter(filterDoc).getOne().getKey());
   
   filterDoc = db.createDocumentFromString(
        "{ \"$or\" : [ {\"order[0].orderno\" : {\"$gt\" : 301, \"$lt\" : 306}}, " +
        "              {\"order[2].orderno\" : {\"$gt\" : 301, \"$lt\" : 306}} ] }");
   assertEquals(1, col.find().filter(filterDoc).count());

   col.admin().drop();
 }
 
  private void testFilter3OrderBy(String contentColumnType, boolean withIndex) throws Exception {

   OracleDocument mDoc = null;

   if (contentColumnType.equalsIgnoreCase("BLOB")) {
     mDoc = client.createMetadataBuilder().contentColumnType("BLOB").mediaTypeColumnName("MediaType").build();
   } else {
     mDoc = client.createMetadataBuilder().contentColumnType(contentColumnType).build();
   }
   OracleCollection col = db.admin().createCollection("testFilter3", mDoc);

   if (withIndex) {
     col.admin().indexAll("indexAll-3");
   }

   if(col.admin().isHeterogeneous()) {
     col.insert(db.createDocumentFromString(null, "{orderno}", "text/plain"));
     col.insert(db.createDocumentFromString(null, "{orderno}", "text/plain"));
     col.insert(db.createDocumentFromString(null, "{orderno}", "text/plain"));
   }

   String docStr1 = "{ \"orderno\":301, \"order\" : [ \n" +
       "{ \"items\": [ { \"name\": \"Wicked Bugs\", \"price\": 11.37, \"quantity\": 1 }, \n" +
           "{ \"name\": \"How to Lie with Statistics\", \"price\": 9.56, \"quantity\": 1 } ] }, \n" +
       "{ \"items\": [ { \"name\": \"Discovering Statistics\", \"price\": 15.00, \"quantity\": 2 }, \n" +
           "{ \"name\": \"Integrated Algebra\", \"price\": 8.99, \"quantity\": 1 } ] }\n" +
       "] }";

   String docStr2 = "{ \"orderno\":303, \"order\" : [ \n" +
       "{ \"items\": [ { \"name\": \"Envisioning Information\", \"price\": 25.50, \"quantity\": 1 } ] }, \n" +
       "{ \"items\": [ { \"name\": \"Calculus For Dummies\", \"price\": 14.00, \"quantity\": 2 } ] }, \n" +
       "{ \"items\": [ { \"name\": \"Business Law\", \"price\": 18.00, \"quantity\": 2 } ] } \n" +
       "] }";

   String docStr3 = " { \"orderno\":306, \"order\" : { \"items\": [\n" +
       "{ \"name\": \"Tart and Sweet\", \"price\": 16.49, \"quantity\": 1 }, \n" +
       "{ \"name\": \"Young Men and Fire\", \"price\": 10.88, \"quantity\": 2}, \n" +
       "{ \"name\": \"Barbara Pleasant\", \"price\": 13.57, \"quantity\": 3} \n" +
       "] } }";

   String key1, key2, key3;
   OracleDocument filterDoc = null, doc = null;
   OracleCursor cursor = null;

   doc = col.insertAndGet(db.createDocumentFromString(docStr1));
   key1 = doc.getKey();
   doc = col.insertAndGet(db.createDocumentFromString(docStr2));
   key2 = doc.getKey();
   doc = col.insertAndGet(db.createDocumentFromString(docStr3));
   key3 = doc.getKey();

   // match doc1, doc3
   filterDoc = db.createDocumentFromString(
       "{ \"order[0].items[0].quantity\": 1, \"order[0].items[*].price\": {\"$gt\" : 9.0, \"$lte\" : 12.0} }");
   assertEquals(2, col.find().filter(filterDoc).count());
   filterDoc = db.createDocumentFromString(
       "{ \"order[0].items[0].quantity\": 1, \"order[0].items[*].price\": {\"$gt\" : 9.0, \"$lte\" : 12.0}," +
       "  \"$orderby\" : {\"orderno\":1} }");
   cursor = col.find().filter(filterDoc).getCursor();
   assertEquals(true, cursor.hasNext());
   assertEquals(key1, cursor.next().getKey());
   assertEquals(key3, cursor.next().getKey());
   assertEquals(false, cursor.hasNext());
   cursor.close();

   //match doc2
   filterDoc = db.createDocumentFromString(
       "{ \"orderno\" : {\"$gt\" : 301, \"$lt\" : 306}, \"$orderby\" : {\"orderno\":1} }");
   assertEquals(1, col.find().filter(filterDoc).count());
   cursor = col.find().filter(filterDoc).getCursor();
   assertEquals(true, cursor.hasNext());
   assertEquals(key2, cursor.next().getKey());
   assertEquals(false, cursor.hasNext());
   cursor.close();

   //match doc1, 2, 3
   filterDoc = db.createDocumentFromString(
       "{ \"orderno\" : {\"$gte\" : 301, \"$lte\" : 306}, \"$orderby\" : {\"orderno\":1} }");
   assertEquals(3, col.find().filter(filterDoc).count());
   cursor = col.find().filter(filterDoc).getCursor();
   assertEquals(true, cursor.hasNext());
   assertEquals(key1, cursor.next().getKey());
   assertEquals(key2, cursor.next().getKey());
   assertEquals(key3, cursor.next().getKey());
   assertEquals(false, cursor.hasNext());
   cursor.close();

   //match doc1, 2, 3
   filterDoc = db.createDocumentFromString(
       "{ \"orderno\" : {\"$gte\" : 301, \"$lte\" : 306}, \"$orderby\" : {\"orderno\":-1} }");
   assertEquals(3, col.find().filter(filterDoc).count());
   cursor = col.find().filter(filterDoc).getCursor();
   assertEquals(true, cursor.hasNext());
   assertEquals(key3, cursor.next().getKey());
   assertEquals(key2, cursor.next().getKey());
   assertEquals(key1, cursor.next().getKey());
   assertEquals(false, cursor.hasNext());
   cursor.close();

   //match doc1, 2, 3
   filterDoc = db.createDocumentFromString(
       "{ \"orderno\" : {\"$gte\" : 301, \"$lte\" : 306}, \"$orderby\" : {\"orderno\":-1} }");
   assertEquals(3, col.find().filter(filterDoc).count());

   col.admin().drop();
 }

  String[] columnSqlTypes = {
      // ### Oracle Database does not support NVARCHAR2, NCLOB, and RAW storage for JSON 
      //"CLOB", "NCLOB", "BLOB", "VARCHAR2", "NVARCHAR2", "RAW"
      "CLOB", "BLOB", "VARCHAR2"
  };
  
  public void testFilter() throws Exception {
    for (String columnSqlType : columnSqlTypes) {
      testFilter(columnSqlType);
    }
  }
  
  public void testFilter2() throws Exception {
    for (String columnSqlType : columnSqlTypes) {
      testFilter2(columnSqlType);
    }
  }
  
  public void testFilter3() throws Exception {
    for (String columnSqlType : columnSqlTypes) {
      testFilter3(columnSqlType);
    }
  }

  public void testFilter3OrderBy() throws Exception {
    for (String columnSqlType : columnSqlTypes) {
      testFilter3OrderBy(columnSqlType, false);
      testFilter3OrderBy(columnSqlType, true);
    }
  }

  private void testFilterNeg(String contentColumnType) throws Exception {
    OracleDocument mDoc = null, filterDoc = null; 
    
    if (contentColumnType.equalsIgnoreCase("BLOB")) {
      mDoc = client.createMetadataBuilder().contentColumnType("BLOB").mediaTypeColumnName("MediaType").build();
    } else {
      mDoc = client.createMetadataBuilder().contentColumnType(contentColumnType).build();
    }
    OracleCollection col = db.admin().createCollection("testFilter4" + contentColumnType, mDoc);
    OracleCursor cursor = null;
    
    String[] invalidArrayPaths = {
        "order[*,].orderno",
        "order[*, 1].orderno",
        "order[0, *].orderno",
        "order[0,*,2].orderno",
        "order[0 to *].orderno",
        "order[2, 0 to *].orderno",
        "order[* to 2].orderno",
        "order[, 1].orderno",
        "order[0, ].orderno",
        "order[0 to ].orderno",
        "order[to 1].orderno",
        "order[ to 1].orderno",
        "order[2, 0 to ].orderno",
        "order[0, to 2].orderno",
        "order[0 to , 2].orderno",
        "order[abc].orderno",
        "order[abc to 2].orderno",
        "order[abc, 1].orderno",
        "order[0, abc].orderno",
        "order[0, abc, 2].orderno",
        "order[0 to abc].orderno",
        "order[0to2].orderno",
        "order[0-2].orderno",
        "order[0.2].orderno",
        "order[0+2].orderno",
        
        "order[].orderno",
    };
  
    for (String invalidPath : invalidArrayPaths) {
      try {
        filterDoc = db.createDocumentFromString("{ \"" + invalidPath + "\" : 301}");
        col.find().filter(filterDoc).getOne();
        fail("No exception when invalid path, (" + invalidPath + "), is used");
      } catch (OracleException e) {
        // Expect an OracleException
        Throwable t = e.getCause();

        String expectResponse =
               "Path (" + invalidPath + ") has invalid array subscript ";
        assertEquals(expectResponse,
                     t.getMessage().substring(0,expectResponse.length()));
      }
    }

    String[] invalidArrayPaths2 = {
        "order.[1]orderno",
    };
    for (String invalidPath : invalidArrayPaths2) {
      try {
        filterDoc = db.createDocumentFromString("{ \"" + invalidPath + "\" : 301}");
        cursor = col.find().filter(filterDoc).getCursor();
        cursor.hasNext();
        fail("No exception when invalid path, (" + invalidPath + "), is used");
      } catch (OracleException e) {
        // Expect an OracleException
        String expectResponse =
               "Missing dot separator in path (" + invalidPath + ")";
        Throwable t = e.getCause();
        assertEquals(expectResponse,
                     t.getMessage().substring(0,expectResponse.length()));
      }
    }

    String[] invalidArrayPaths4 = {
        "order[0]orderno",
        "order[0][1]orderno"
    };
    for (String invalidPath : invalidArrayPaths4) {

      try {
        filterDoc = db.createDocumentFromString("{ \"" + invalidPath + "\" : 301}");
        col.find().filter(filterDoc).count();
        fail("No exception when invalid path, (" + invalidPath + "), is used");
      } catch (OracleException e) {
        // Expect an OracleException
        String expectResponse =
               "Missing dot separator in path (" + invalidPath + ")";
        Throwable t = e.getCause();
        assertEquals(expectResponse,
                     t.getMessage().substring(0,expectResponse.length()));
      }
    }

    // negative test for empty key list
    String[] emptyKeyListExprs = {
        "{ \"$id\" : []}",
        "{\"order[0].orderno\" : {\"$in\":[]} }",
        "{\"order[0].orderno\" : {\"$nin\":[]} }",
        "{\"order[0].orderno\" : {\"$all\":[]} }",
        "{ \"$or\" : [ ] }",
        "{ \"$and\" : [ ] }",
        "{ \"$nor\" : [ ] }"
    };
    String[] testedOperators = {
        "$id", "$in", "$nin", "$all", 
        "$or", "$and", "$nor"
    };
  
    int counter = 0;
    for (String emptyKeyListExpr : emptyKeyListExprs) {
      try {
        filterDoc = db.createDocumentFromString(emptyKeyListExpr);
        col.find().filter(filterDoc).count();
        fail("No exception when empty key list is presented");
      } catch (OracleException e) {
        // Expect an OracleException
        Throwable t = e.getCause();
        assertEquals("Operator " + testedOperators[counter] + " must have at least one target.", t.getMessage());
      }
      counter++;
    }
    
    col.admin().drop();
  }
  
  public void testFilterNeg() throws Exception {
    for (String columnSqlType : columnSqlTypes) {
      testFilterNeg(columnSqlType);
    }
  }

  // QBE Tests about path beginning with array step
  private void testFilter4(String contentColumnType) throws Exception {
    OracleDocument mDoc = null; 
    
    if (contentColumnType.equalsIgnoreCase("BLOB")) {
      mDoc = client.createMetadataBuilder().contentColumnType("BLOB").mediaTypeColumnName("MediaType").build();
    } else {
      mDoc = client.createMetadataBuilder().contentColumnType(contentColumnType).build();
    }
    OracleCollection col = db.admin().createCollection("testFilter4", mDoc);
    
    String docStr1 = 
            "[ { \"name\":\"Angelia\", \"gender\":\"F\", \"age\":20}, \n" +
            "  { \"name\":\"Amanda\", \"age\":25, \"gender\":\"F\"}, \n" +
            "  { \"name\":\"Anne\", \"gender\":\"F\", \"age\":21} ]";

    String docStr2 = 
            "[ [ { \"name\":\"Carrie\", \"gender\":\"F\", \"age\":21}, \n" +
            "    { \"name\":\"Candy\", \"gender\":\"F\", \"age\":22} ],\n" + 
            "  { \"name\":\"Christina\", \"gender\":\"F\", \"age\":23}, \n" +
            "  [ { \"name\":\"Daisy\", \"gender\":\"F\", \"age\":22}, \n" + 
            "    { \"name\":\"Diana\", \"gender\":\"F\", \"age\":25}, \n" + 
            "    { \"name\":\"Demi\", \"gender\":\"F\", \"age\":24} ]\n" +
            "]";

    String docStr3 = 
            "[ [ { \"name\":\"Bob\", \"gender\":\"M\", \"age\":21}, \n" + 
            "    { \"name\":\"Brian\", \"gender\":\"M\", \"age\":22} ], \n" +
            "  [ { \"name\":\"Carter\", \"gender\":\"M\", \"age\":23}, \n" + 
            "    { \"name\":\"Carl\", \"gender\":\"M\", \"age\":22} ], \n" +
            "  [ { \"name\":\"Colin\", \"gender\":\"M\", \"age\":20}, \n" + 
            "    { \"name\":\"Dave\", \"gender\":\"M\", \"age\":24} ] \n" +
            "]";

    String key1, key2, key3;
    OracleDocument doc, filterDoc;
    doc = col.insertAndGet(db.createDocumentFromString(docStr1));
    key1 = doc.getKey();
    doc = col.insertAndGet(db.createDocumentFromString(docStr2));
    key2 = doc.getKey();
    doc = col.insertAndGet(db.createDocumentFromString(docStr3));
    key3 = doc.getKey();
    assertEquals(3, col.find().count());
 
    // match doc1
    filterDoc = db.createDocumentFromString("{ \"[0].name\": \"Angelia\" }");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key1, col.find().filter(filterDoc).getOne().getKey());
   
    if (isPatch(PATCH1)) {
      // the later tests can not work on 12102 db, see bug20061399
      col.find().remove();
      col.admin().drop();
      return;
    }

    filterDoc = db.createDocumentFromString("{ \"[1].name\": \"Amanda\" }");
    assertEquals(key1, col.find().filter(filterDoc).getOne().getKey());
 
    // match doc2
    filterDoc = db.createDocumentFromString("{ \"[0][0].name\": \"Carrie\" }");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key2, col.find().filter(filterDoc).getOne().getKey());

    filterDoc = db.createDocumentFromString("{ \"[0][1].name\": \"Candy\" }");
    assertEquals(key2, col.find().filter(filterDoc).getOne().getKey());
    filterDoc = db.createDocumentFromString("{ \"[1].name\": \"Christina\" }");
    assertEquals(key2, col.find().filter(filterDoc).getOne().getKey());
    filterDoc = db.createDocumentFromString("{ \"[2][0].name\": \"Daisy\" }");
    assertEquals(key2, col.find().filter(filterDoc).getOne().getKey());
    filterDoc = db.createDocumentFromString("{ \"[2][1].name\": \"Diana\" }");
    assertEquals(key2, col.find().filter(filterDoc).getOne().getKey());
    filterDoc = db.createDocumentFromString("{ \"[2][2].name\": \"Demi\" }");
    assertEquals(key2, col.find().filter(filterDoc).getOne().getKey());
 
    // match doc3
    filterDoc = db.createDocumentFromString("{ \"[2][1].name\": {\"$startsWith\": \"D\"},  \"[2][1].gender\": \"M\" }");
    assertEquals(1, col.find().filter(filterDoc).count());
    OracleCursor cursor = col.find().filter(filterDoc).getCursor();
    assertEquals(true, cursor.hasNext());
    assertEquals(key3, cursor.next().getKey());
    assertEquals(false, cursor.hasNext());

    // match doc2
    filterDoc = db.createDocumentFromString("{ \"[2][2]\": {\"$exists\" : true} }");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key2, col.find().filter(filterDoc).getOne().getKey());

    // match doc1, 2, 3
    filterDoc = db.createDocumentFromString("{ \"[2]\": {\"$exists\" : true} }");
    assertEquals(3, col.find().filter(filterDoc).count());

    // match doc1
    filterDoc = db.createDocumentFromString("{ \"[2][0]\": {\"$exists\" : false} }");
    // blocked by bug20407304
    /*assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key1, col.find().filter(filterDoc).getOne().getKey());*/

    // match doc3
    filterDoc = db.createDocumentFromString("{ \"[0][1].name\": \"Brian\" }");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key3, col.find().filter(filterDoc).getOne().getKey());

    // match doc3
    // blocked by bug20407304
    /*filterDoc = db.createDocumentFromString("{ \"[*][0].age\": {\"$lte\" : 20 } }");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key3, col.find().filter(filterDoc).getOne().getKey());

    // match doc2
    filterDoc = db.createDocumentFromString("{ \"[0, 1, 2][0 to 2].age\": {\"$gt\" : 24 } }");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key2, col.find().filter(filterDoc).getOne().getKey());*/

    // match doc1
    filterDoc = db.createDocumentFromString("{ \"[0 to 2].name\": \"Angelia\" }");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key1, col.find().filter(filterDoc).getOne().getKey());

    // match doc2
    // blocked by bug20407304
    /*filterDoc = db.createDocumentFromString("{ \"[0,1].age\": {\"$eq\" : 23 } }");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key2, col.find().filter(filterDoc).getOne().getKey());*/

    // match doc1
    filterDoc = db.createDocumentFromString("{ \"[0 to 1, 2].age\": {\"$gt\" : 23 } }");
    assertEquals(3, col.find().filter(filterDoc).count());
    HashSet<String> keys = new HashSet<String>();
    keys.add(key1);
    keys.add(key2);
    keys.add(key3);
    assertTrue(keys.contains(col.find().filter(filterDoc).getOne().getKey()));

    col.find().remove();
    col.admin().drop();
  }
  
  public void testFilter4() throws Exception {
    for (String columnSqlType : columnSqlTypes) {
      testFilter4(columnSqlType);
    }
  }
  
  //QBE Tests about path containing multiple level array steps 
  private void testFilter5(String contentColumnType) throws Exception {
    OracleDocument mDoc = null; 
    
    if (contentColumnType.equalsIgnoreCase("BLOB")) {
      mDoc = client.createMetadataBuilder().contentColumnType("BLOB").mediaTypeColumnName("MediaType").build();
    } else {
      mDoc = client.createMetadataBuilder().contentColumnType(contentColumnType).build();
    }
    OracleCollection col = db.admin().createCollection("testFilter5", mDoc);
    
    String docStr1 = 
            "{\"matrix\": [ \n" +
            "   [{\"id\":\"00\"}, {\"id\":\"01\"}, {\"id\":\"02\"}], \n" +
            "   {\"id\":\"1\"}, \n" +
            "   [{\"id\":\"20\"}, {\"id\":\"21\"}, {\"id\":\"22\"}] " +
            "]}";

    String docStr2 =  
            "{\"matrix\": [ \n" +
            "   [{\"id\":\"2.00\"}, [{\"id\":\"2.010\"}, {\"id\":\"2.011\"}, {\"id\":\"2.012\"}] ], \n" +
            "   [{\"id\":\"2.10\"}, {\"id\":\"2.11\"}, [{\"value\":99}, {\"value\":101}] ], \n" +
            "   {\"id\":\"2.2\"} \n" + 
            "] }";

    String docStr3 = 
            "{\"matrix\": [ \n" +
            "   {\"id\":\"3.0\"}, \n" +
            "   [{\"id\":\"3.10\"}, {\"id\":\"3.11\", \"value\":[[{\"id\":\"00\"}, {\"id\":\"01\"}, {\"id\":\"02\"}]] }]\n" +
            "] }";

    String key1, key2, key3;
    OracleDocument doc, filterDoc;
    doc = col.insertAndGet(db.createDocumentFromString(docStr1));
    key1 = doc.getKey();
    doc = col.insertAndGet(db.createDocumentFromString(docStr2));
    key2 = doc.getKey();
    doc = col.insertAndGet(db.createDocumentFromString(docStr3));
    key3 = doc.getKey();
    assertEquals(3, col.find().count());
   
    // match doc1
    filterDoc = db.createDocumentFromString("{ \"matrix[0][0].id\": \"00\" }");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key1, col.find().filter(filterDoc).getOne().getKey());
    filterDoc = db.createDocumentFromString("{ \"matrix[0][1].id\": \"01\" }");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key1, col.find().filter(filterDoc).getOne().getKey());
    
    filterDoc = db.createDocumentFromString("{ \"matrix[0][2].id\": \"02\" }");
    assertEquals(key1, col.find().filter(filterDoc).getOne().getKey());
    filterDoc = db.createDocumentFromString("{ \"matrix[1].id\": \"1\" }");
    assertEquals(key1, col.find().filter(filterDoc).getOne().getKey());
    filterDoc = db.createDocumentFromString("{ \"matrix[2][0].id\": \"20\" }");
    assertEquals(key1, col.find().filter(filterDoc).getOne().getKey());
    filterDoc = db.createDocumentFromString("{ \"matrix[2][1].id\": \"21\" }");
    assertEquals(key1, col.find().filter(filterDoc).getOne().getKey());
    filterDoc = db.createDocumentFromString("{ \"matrix[2][2].id\": \"22\" }");
    assertEquals(key1, col.find().filter(filterDoc).getOne().getKey());

    //match doc2
    filterDoc = db.createDocumentFromString("{ \"matrix[0][0].id\": \"2.00\" }");
    assertEquals(key2, col.find().filter(filterDoc).getOne().getKey());
    filterDoc = db.createDocumentFromString("{ \"matrix[0][1][0].id\": \"2.010\" }");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key2, col.find().filter(filterDoc).getOne().getKey());
    filterDoc = db.createDocumentFromString("{ \"matrix[0][1][1].id\": \"2.011\" }");
    assertEquals(key2, col.find().filter(filterDoc).getOne().getKey());
    filterDoc = db.createDocumentFromString("{ \"matrix[0][1][2].id\": \"2.012\" }");
    assertEquals(key2, col.find().filter(filterDoc).getOne().getKey());
    
    filterDoc = db.createDocumentFromString("{ \"matrix[1][0].id\": \"2.10\" }");
    assertEquals(key2, col.find().filter(filterDoc).getOne().getKey());
    filterDoc = db.createDocumentFromString("{ \"matrix[1][1].id\": \"2.11\" }");
    assertEquals(key2, col.find().filter(filterDoc).getOne().getKey());
    
    filterDoc = db.createDocumentFromString("{ \"matrix[2].id\": \"2.2\" }");
    assertEquals(key2, col.find().filter(filterDoc).getOne().getKey());

    //match doc3
    filterDoc = db.createDocumentFromString("{ \"matrix[0].id\": \"3.0\" }");
    assertEquals(key3, col.find().filter(filterDoc).getOne().getKey());
    filterDoc = db.createDocumentFromString("{ \"matrix[1][0].id\": \"3.10\" }");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key3, col.find().filter(filterDoc).getOne().getKey());
    filterDoc = db.createDocumentFromString("{ \"matrix[1][1].id\": \"3.11\" }");
    assertEquals(key3, col.find().filter(filterDoc).getOne().getKey());
    filterDoc = db.createDocumentFromString("{ \"matrix[1][1].value[0][0].id\": \"00\" }");
    assertEquals(key3, col.find().filter(filterDoc).getOne().getKey());
    filterDoc = db.createDocumentFromString("{ \"matrix[1][1].value[0][1].id\": \"01\" }");
    assertEquals(key3, col.find().filter(filterDoc).getOne().getKey());
    filterDoc = db.createDocumentFromString("{ \"matrix[1][1].value[0][2].id\": \"02\" }");
    assertEquals(key3, col.find().filter(filterDoc).getOne().getKey());

    //match doc2
    filterDoc = db.createDocumentFromString("{ \"matrix[1][2][0].value\": {\"$lte\":99} }");
    OracleCursor cursor = col.find().filter(filterDoc).getCursor();
    assertEquals(true, cursor.hasNext());
    assertEquals(key2, cursor.next().getKey());
    assertEquals(false, cursor.hasNext());
    
    filterDoc = db.createDocumentFromString("{ \"matrix[1][2][1].value\": {\"$gt\":100, \"$lt\":102} }");
    assertEquals(key2, col.find().filter(filterDoc).getOne().getKey());

    filterDoc = db.createDocumentFromString("{ \"matrix[1][2][1]\": {\"$exists\":true} }");
    assertEquals(key2, col.find().filter(filterDoc).getOne().getKey());
    filterDoc = db.createDocumentFromString("{ \"matrix[1][2][1].value\": {\"$exists\":true} }");
    assertEquals(key2, col.find().filter(filterDoc).getOne().getKey());

    filterDoc = db.createDocumentFromString("{ \"matrix[*][*][*].id\": \"2.011\"}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key2, col.find().filter(filterDoc).getOne().getKey());
    filterDoc = db.createDocumentFromString("{ \"matrix[0,1,3][0 to 5][*].id\": {\"$startsWith\" : \"2.\"}}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key2, col.find().filter(filterDoc).getOne().getKey());

    // match doc3
    filterDoc = db.createDocumentFromString("{ \"matrix[*][0, 1, 2].value[*][*].id\": \"01\" }");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key3, col.find().filter(filterDoc).getOne().getKey());
    filterDoc = db.createDocumentFromString("{ \"matrix[0 to 2][*].value[0, 1 to 3][0 to 1, 2].id\": \"02\" }");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key3, col.find().filter(filterDoc).getOne().getKey());

    col.find().remove();
    col.admin().drop();
    
  }

  public void testFilter5() throws Exception {
    for (String columnSqlType : columnSqlTypes) {
      testFilter5(columnSqlType);
    }
  }

  // Test $orderby with $query or other top-level query
  // operators (before and after $orderby).
  private void testFilter6(String contentColumnType) throws Exception {

    OracleCollection col = db.admin().createCollection("testFilter6",
                                                       getClientAssignedKeyMetadata(
                                                       contentColumnType));

    OracleDocument d;

    d = db.createDocumentFromString("6", "{\"b\" : 6}");
    col.insert(d);
    d = db.createDocumentFromString("4", "{\"b\" : 4}");
    col.insert(d);
    d = db.createDocumentFromString("1", "{\"b\" : 1}");
    col.insert(d);
    d = db.createDocumentFromString("3", "{\"b\" : 3}");
    col.insert(d);

    OracleDocument f;

    // Test mixing $orderby with other top-level
    // query operators. We support this for convenience,
    // without requiring an explicit $query operator.
    f = db.createDocumentFromString( "{\"$or\" : [{\"b\" : 1}," +
                                                 "{\"b\" : 3}," +
                                                 "{\"b\" : 4}]," +
                                     "\"$orderby\" : { \"b\" : 1 }}");

    OracleCursor c = col.find().filter(f).getCursor();

    checkKey("1", c);
    checkKey("3", c);
    checkKey("4", c);

    c.close();

    // Same, but the $orderby is positioned first.
    f = db.createDocumentFromString("{\"$orderby\" : { \"b\" : 1 }," +
                                     "\"$or\" : [{\"b\" : 1}," +
                                                "{\"b\" : 3}," +
                                                "{\"b\" : 4}]}");

    c = col.find().filter(f).getCursor();

    checkKey("1", c);
    checkKey("3", c);
    checkKey("4", c);

    c.close();

    // Same as above, but using an explicit $query
    f = db.createDocumentFromString("{\"$query\" : {\"$or\" : [{\"b\" : 1}," +
                                                              "{\"b\" : 3}," +
                                                              "{\"b\" : 4}]}," +
                                      "\"$orderby\" : { \"b\" : 1 }}");

    c = col.find().filter(f).getCursor();

    checkKey("1", c);
    checkKey("3", c);
    checkKey("4", c);

    c.close();

    col.admin().drop();
  }

  public void testFilter6() throws Exception {
    for (String columnSqlType : columnSqlTypes) {
      testFilter6(columnSqlType);
    }
  }

  // Test $ge and $le (synonyms for $gte and $lte)
  private void testFilter7(String contentColumnType) throws Exception {

    OracleCollection col = db.admin().createCollection("testFilter7",
                                                       getClientAssignedKeyMetadata(
                                                       contentColumnType));


    OracleDocument d;

    d = db.createDocumentFromString("6", "{\"b\" : 6}");
    col.insert(d);
    d = db.createDocumentFromString("4", "{\"b\" : 4}");
    col.insert(d);
    d = db.createDocumentFromString("1", "{\"b\" : 1}");
    col.insert(d);
    d = db.createDocumentFromString("3", "{\"b\" : 3}");
    col.insert(d);

    OracleDocument f;

    // Test $le (a synonym for $lte)
    f = db.createDocumentFromString( "{\"b\" : { \"$le\" : 3 }," +
                                     "\"$orderby\" : { \"b\" : 1}}");

    OracleCursor c = col.find().filter(f).getCursor();

    checkKey("1", c);
    checkKey("3", c);

    c.close();

    // Test $ge (a synonym for $gte)
    f = db.createDocumentFromString( "{\"b\" : { \"$ge\" : 3 }," +
                                     "\"$orderby\" : { \"b\" : 1}}");

    c = col.find().filter(f).getCursor();

    checkKey("3", c);
    checkKey("4", c);
    checkKey("6", c);

    c.close();

    col.admin().drop();
  }

  public void testFilter7() throws Exception {
    for (String columnSqlType : columnSqlTypes) {
      testFilter7(columnSqlType);
    }
  }

  // Test $ge and $le (synonyms for $gte and $lte)
  private void testFilter8(String contentColumnType) throws Exception {

    OracleCollection col = db.admin().createCollection("testFilter8",
                                                       getClientAssignedKeyMetadata(
                                                       contentColumnType));

    OracleDocument d;

    d = db.createDocumentFromString("1", "{\"b\" : \"true\"}");
    col.insert(d);
    d = db.createDocumentFromString("2", "{\"b\" : \"false\"}");
    col.insert(d);
    d = db.createDocumentFromString("3", "{\"b\" : \"null\"}");
    col.insert(d);
    d = db.createDocumentFromString("4", "{\"b\" : true}");
    col.insert(d);
    d = db.createDocumentFromString("5", "{\"b\" : false}");
    col.insert(d);
    d = db.createDocumentFromString("6", "{\"b\" : null}");
    col.insert(d);

    OracleDocument f;

    // "true" string constant
    checkSingleResult("{\"b\" : \"true\"}", "1", col);

    // "false" string constant
    checkSingleResult("{\"b\" : \"false\"}", "2", col);

    // "null" string constant
    checkSingleResult("{\"b\" : \"null\"}", "3", col);

    // true constant
    checkSingleResult("{\"b\" : true}", "4", col);

    // false constant
    checkSingleResult("{\"b\" : false}", "5", col);

    // null constant
    checkSingleResult("{\"b\" : null}", "6", col);

    col.admin().drop();
  }

  private void checkSingleResult(String filter,
                                 String expectedKey,
                                 OracleCollection col)
                                 throws OracleException, IOException
  {
    OracleDocument f = db.createDocumentFromString(filter);
    OracleCursor c = col.find().filter(f).getCursor();

    checkKey(expectedKey, c);

    if (c.hasNext()) {
      fail ("Only one document expected to match.");
    }

    c.close();
  }

  private void checkKey(String expectedKey, OracleCursor c) 
                        throws OracleException
  {
    OracleDocument r = c.next();

    if (!r.getKey().equals(expectedKey)) {
      fail ("Expected key was " + expectedKey +
            ", but " + r.getKey() + " was returned.");
    } 
  }

  private OracleDocument getClientAssignedKeyMetadata(String contentColumnType)
                                                      throws OracleException
  {
    OracleDocument mDoc = null;

    if (contentColumnType.equalsIgnoreCase("BLOB")) {
      mDoc = client.createMetadataBuilder().
                    keyColumnAssignmentMethod("CLIENT").
                    contentColumnType("BLOB").
                    mediaTypeColumnName("MediaType").build();
    } else {
      mDoc = client.createMetadataBuilder().
                    keyColumnAssignmentMethod("CLIENT").
                    contentColumnType(contentColumnType).build();
    }

    return mDoc;
  }

  public void testFilter8() throws Exception {
    for (String columnSqlType : columnSqlTypes) {
      testFilter8(columnSqlType);
    }
  }

  public void testFilterWithSkipAndLimit() throws Exception {

    OracleCollection col = db.admin().createCollection("testFilterSL",
            getClientAssignedKeyMetadata("BLOB"));

    OracleDocument d;

    for (int i = 0; i < 50; i++)
    {
      d = db.createDocumentFromString(String.valueOf(i),
                                      "{\"num\" : " + i + "}");
      col.insert(d);
    }

    OracleDocument f = db.createDocumentFromString(
                          "{\"num\" : {\"$gte\" : 12}}");

    OracleCursor c = col.find().filter(f).skip(10).limit(10).getCursor();

    OracleDocument r;

    // 22 is the starting key (we skipped 10 items starting with the item with
    // key 12).
    int i = 22;
    while (c.hasNext())
    {
      // 31 should be the last key, since the limit is 10.
      if (i > 31)
        fail("Limit was 10, so the last should be the one with key equals to 31");
      r = c.next();
      assertEquals(r.getKey(), String.valueOf(i));
      i++;
    }

    c.close();
  }

  public void testFilterNeg2() throws Exception {

    OracleCollection col = db.admin().createCollection("testFilterNeg2");

    OracleDocument f;

    // Multiple query operators are not allowed
    try {
      f = db.createDocumentFromString(
        "{\"$query\" : { \"a\" : 1 }, \"$query\" : { \"b\" : 2 }}");
      col.find().filter(f).getCursor();
      fail("No exception when $query is used twice");
    }
    catch (OracleException e) {
      Throwable t = e.getCause();
      assertTrue(t.getMessage().
                   equals("Multiple $query operators are not allowed."));
    }

    // Multiple order-by operators are not allowed
    try {
      f = db.createDocumentFromString(
        "{\"$orderby\" : { \"a\" : 1 }, \"$orderby\" : { \"b\" : 2 }}");
      col.find().filter(f).getCursor();
      fail("No exception when $orderby is used twice");
    }
    catch (OracleException e) {
      Throwable t = e.getCause();
      assertTrue(t.getMessage().
                   equals("Multiple $orderby operators are not allowed."));
    }

    // $query value cannot be an array (must be an object).
    try {
      f = db.createDocumentFromString(
        "{\"$query\" : [{ \"a\" : 1 }]}");
      col.find().filter(f).getCursor();
      fail("No exception when $query value is an array");
    }
    catch (OracleException e) {
      Throwable t = e.getCause();
      assertTrue(t.getMessage().
                   equals("The value of the $query operator must be a JSON object."));
    }

    // $query value cannot be an number (must be an object).
    try {
      f = db.createDocumentFromString("{\"$query\" : 1 }");
      col.find().filter(f).getCursor();
      fail("No exception when $query value is a number");
    }
    catch (OracleException e) {
      Throwable t = e.getCause();
      assertTrue(t.getMessage().
                   equals("The value of the $query operator must be a JSON object."));
    }

    // $query value cannot be an boolean (must be an object).
    try {
      f = db.createDocumentFromString("{\"$query\" : true }");
      col.find().filter(f).getCursor();
      fail("No exception when $query value is a boolean");
    }
    catch (OracleException e) {
      Throwable t = e.getCause();
      assertTrue(t.getMessage().
                   equals("The value of the $query operator must be a JSON object."));
    }

    // $query value cannot be null (must be an object).
    try {
      f = db.createDocumentFromString("{\"$query\" : null }");
      col.find().filter(f).getCursor();
      fail("No exception when $query value is null");
    }
    catch (OracleException e) {
      Throwable t = e.getCause();
      assertTrue(t.getMessage().
                   equals("The value of the $query operator must be a JSON object."));
    }

    // $orderby value cannot be an array (must be an object).
    try {
      f = db.createDocumentFromString(
        "{\"$orderby\" : [{ \"a\" : 1 }]}");
      col.find().filter(f).getCursor();
      fail("No exception when $orderby value is an array");
    }
    catch (OracleException e) {
      Throwable t = e.getCause();
      assertTrue(t.getMessage().
              equals("The value of the $orderby operator must be a JSON object."));
    }

    // $orderby value cannot be an number (must be an object).
    try {
      f = db.createDocumentFromString(
        "{\"$orderby\" : 1 }");
      col.find().filter(f).getCursor();
      fail("No exception when $orderby value is a number");
    }
    catch (OracleException e) {
      Throwable t = e.getCause();
      assertTrue(t.getMessage().
              equals("The value of the $orderby operator must be a JSON object."));
    }

    // $orderby value cannot be an boolean (must be an object).
    try {
      f = db.createDocumentFromString(
        "{\"$orderby\" : true }");
      col.find().filter(f).getCursor();
      fail("No exception when $orderby value is a boolean");
    }
    catch (OracleException e) {
      Throwable t = e.getCause();
      assertTrue(t.getMessage().
              equals("The value of the $orderby operator must be a JSON object."));
    }

    // $orderby value cannot be null (must be an object).
    try {
      f = db.createDocumentFromString(
        "{\"$orderby\" : null }");
      col.find().filter(f).getCursor();
      fail("No exception when $orderby value is null");
    }
    catch (OracleException e) {
      Throwable t = e.getCause();
      assertTrue(t.getMessage().
                   equals("The value of the $orderby operator must be a JSON object."));
    }

    // $query shouldn't be used after another top-level query operator
    try {
      f = db.createDocumentFromString(
        "{\"a\" : 1 , \"$query\" : { \"b\" : 2 }}");
      col.find().filter(f).getCursor();
      fail("No exception when $query is used after another query operator");
    }
    catch (OracleException e) {
      Throwable t = e.getCause();
      assertTrue(t.getMessage().
                   equals("If $query is used, other query operators cannot " +
                          "appear at the top level of the filter specification."));
    }

    // $query shouldn't be used before another top-level query operators
    try {
      f = db.createDocumentFromString(
              "{\"$query\" : { \"b\" : 2 }, \"a\" : 1}");
      col.find().filter(f).getCursor();
      fail("No exception when $query is used before another query operator");
    }
    catch (OracleException e) {
      Throwable t = e.getCause();
      assertTrue(t.getMessage().
              equals("If $query is used, other query operators cannot " +
                      "appear at the top level of the filter specification."));
    }

  }

  private void basicRemoveTest(OracleCollection col, boolean clientAssignedKey) throws Exception{
    
    OracleDocument doc;
    String key1 = null, key3 = null, key5 = null;
    String version1 = null, version3 = null, version5 = null;
    for (int i = 1; i <= 5; i++) {
      if (clientAssignedKey)
        doc = col.insertAndGet(db.createDocumentFromString("id-" + i, "{ \"value\" : \"v" + i + "\" }"));
      else   
        doc = col.insertAndGet(db.createDocumentFromString(null,"{ \"value\" : \"v" + i + "\" }"));
      
      if (i == 1) {
        key1 = doc.getKey();
        version1 = doc.getVersion();
      }
        
      if (i == 3) {
        key3 = doc.getKey();
        version3 = doc.getVersion();
      }
      
      if (i == 5) {
        key5 = doc.getKey();
        version5 = doc.getVersion();
      }
    }
    
    assertEquals("{ \"value\" : \"v3\" }", col.find().key(key3).version(version3).getOne().getContentAsString());
    assertEquals(1, col.find().key(key3).version(version3).remove());
    assertNull(col.find().key(key3).version(version3).getOne());
    
    assertEquals("{ \"value\" : \"v5\" }", col.find().key(key5).version(version5).getOne().getContentAsString());
    assertEquals(1, col.find().key(key5).version(version5).remove());
    assertNull(col.find().key(key5).version(version5).getOne());
    
    assertEquals("{ \"value\" : \"v1\" }", col.find().key(key1).version(version1).getOne().getContentAsString());
    assertEquals(1, col.find().key(key1).version(version1).remove());
    assertNull(col.find().key(key1).version(version1).getOne());
    
    assertEquals(2, col.find().count());
    assertEquals(2, col.find().remove());
    
    assertEquals(0, col.find().count());
  }
  
  public void testRemove() throws Exception {
    
   // Test with KeyAssignmentMethod="CLIENT"
   OracleDocument mDoc = client.createMetadataBuilder().keyColumnAssignmentMethod("CLIENT").build();

   OracleCollection col = dbAdmin.createCollection("testRemove", mDoc);
   String lastModified1=null, lastModified10=null, lastModified4=null;
   OracleDocument doc;
   for (int i = 1; i <= 10; i++) {
     doc = col.insertAndGet(db.createDocumentFromString("id-" + i, "{ \"d\" : " + i + " }"));
     
     if (i == 1)
       lastModified1 = doc.getLastModified();
     if (i == 10)
       lastModified10 = doc.getLastModified();
     if (i == 4)
       lastModified4 = doc.getLastModified();
   }
   
   assertEquals(10, col.find().count());
  
   // Test with key()
   assertEquals(1, col.find().key("id-5").remove());
   assertNull(col.findOne("id-5"));
   // remove again
   assertEquals(0, col.find().key("id-5").remove());
   assertEquals(9, col.find().count());
  
   // Test with keys()
   HashSet<String> keySet = new HashSet<String>();
   keySet.add("id-2");
   keySet.add("id-5"); // already removed
   keySet.add("id-8");
   
   assertEquals(2, col.find().keys(keySet).remove());
   assertNull(col.findOne("id-2"));
   assertNull(col.findOne("id-8"));
   assertEquals(7, col.find().count());
   
   // Test with startKey()
   assertEquals(3, ((OracleOperationBuilderImpl)col.find()).startKey("id-5", true, true).remove());
   assertNull(col.findOne("id-6"));
   assertNull(col.findOne("id-7"));
   assertNull(col.findOne("id-9"));
   assertEquals(0, ((OracleOperationBuilderImpl)col.find()).startKey("id-7", true, true).remove());
   
   assertEquals(4, col.find().count());
   
   // Test with timeRange()
   assertEquals(1, ((OracleOperationBuilderImpl)col.find()).timeRange(null, lastModified1, true).remove());
   assertEquals(0, ((OracleOperationBuilderImpl)col.find()).timeRange(null, lastModified1, true).remove());
   assertEquals(2, ((OracleOperationBuilderImpl)col.find()).timeRange(lastModified1, lastModified4, true).remove());
   
   assertEquals(1, col.find().count());
   // only id-10 is left
   assertEquals("id-10", col.find().getOne().getKey());
   
   //key().lastModified()
   assertEquals(0, ((OracleOperationBuilderImpl)col.find().key("id-10")).lastModified(lastModified1).remove());
   assertEquals(1, ((OracleOperationBuilderImpl)col.find().key("id-10")).lastModified(lastModified10).remove());
   
   assertEquals(0, col.find().count());
   
   // Test with KeyAssignmentMethod="SEQUENCE" and versionMethod = "TIMESTAMP"
   OracleDocument mDoc2 = client.createMetadataBuilder().versionColumnMethod("TIMESTAMP")
       .keyColumnAssignmentMethod("SEQUENCE").keyColumnSequenceName("keysqlname").build();
   OracleCollection col2 = dbAdmin.createCollection("testRemove2", mDoc2);
   basicRemoveTest(col2, false);
   
   // Test with KeyAssignmentMethod="GUID" and versionMethod = "SEQUENTIAL"
   OracleDocument mDoc3 = client.createMetadataBuilder().versionColumnMethod("SEQUENTIAL")
       .keyColumnAssignmentMethod("GUID").build();
   OracleCollection col3 = dbAdmin.createCollection("testRemove3", mDoc3);
   basicRemoveTest(col3, false);
   
   // Test with KeyAssignmentMethod="UUID" and versionMethod = "UUID"
   OracleDocument mDoc4 = client.createMetadataBuilder().versionColumnMethod("UUID")
       .keyColumnAssignmentMethod("UUID").build();
   OracleCollection col4 = dbAdmin.createCollection("testRemove4", mDoc4);
   basicRemoveTest(col4, false);

   // Test with KeyAssignmentMethod="SEQUENCE" and versionMethod = "SHA256"
   OracleDocument mDoc5 = client.createMetadataBuilder().versionColumnMethod("SHA256")
       .keyColumnAssignmentMethod("SEQUENCE").keyColumnSequenceName("key_sql_name").build();
   OracleCollection col5 = dbAdmin.createCollection("testRemove5", mDoc5);
   basicRemoveTest(col5, false);

   // Test with KeyAssignmentMethod="GUID" and versionMethod = "MD5"
   OracleDocument mDoc6 = client.createMetadataBuilder().versionColumnMethod("MD5")
       .keyColumnAssignmentMethod("GUID").build();
   OracleCollection col6 = dbAdmin.createCollection("testRemove6", mDoc6);
   basicRemoveTest(col6, false);
   
   // Test with versionMethod = "NONE"
   OracleDocument mDoc7 = client.createMetadataBuilder()
       .keyColumnAssignmentMethod("CLIENT")
       .versionColumnMethod("NONE").tableName("SODATBL").build();

   OracleCollection col7 = dbAdmin.createCollection("testRemove7", mDoc7);
   basicRemoveTest(col7, true);
   
  }
  
  private void testReplaceOneWithCol(OracleCollection col, boolean clientAssignedKey, boolean replaceAndGet) throws Exception {
    
    OracleDocument doc;
    String key2 = null, key4 = null, key5 = null;
    String version2 = null, version4 = null;
    String createdOn2 = null, lastModified2 = null, lastModified5 = null;
    for (int i = 1; i <= 5; i++) {
      if (clientAssignedKey)
        doc = col.insertAndGet(db.createDocumentFromString("id-" + i, "{ \"data\" : " + i + " }"));
      else   
        doc = col.insertAndGet(db.createDocumentFromString(null, "{ \"data\" : " + i + " }"));
        
      if (i == 2) {
        key2 = doc.getKey();
        version2 = doc.getVersion();
        createdOn2 = doc.getCreatedOn();
        lastModified2 = doc.getLastModified();
      }
      
      if (i == 4) {
        key4 = doc.getKey();
        version4 = doc.getVersion();
      }
      
      if (i == 5) {
        key5 = doc.getKey();
        lastModified5 = doc.getLastModified();
      }
    }
    
    // Test with key()
    // key in document will be ignored
    doc = db.createDocumentFromString("id-x", "{ \"data\" : \"v2\" }");
    if (replaceAndGet) {
      doc = col.find().key(key2).version(version2).replaceOneAndGet(doc);
      verifyNullContentDocument(doc);
      assertEquals(key2, doc.getKey());
      assertEquals(1, col.find().key(key2).version(doc.getVersion()).count());
      assertEquals(1, ((OracleOperationBuilderImpl)col.find().key(key2)).lastModified(doc.getLastModified()).count());
    }
    else
      assertTrue(col.find().key(key2).version(version2).replaceOne(doc));
    
    doc = col.findOne(key2);
    assertEquals("{ \"data\" : \"v2\" }", doc.getContentAsString());
    // an new version should be generated
    assertFalse(doc.getVersion().equals("version-v2"));
    assertFalse(doc.getVersion().contentEquals(version2));

    // createdOn should not be changed
    assertEquals(createdOn2, doc.getCreatedOn());
    
    // lastModified should be updated
    assertEquals(1, ((OracleOperationBuilderImpl)col.find().key(key2)).lastModified(doc.getLastModified()).count());
    assertEquals(0, ((OracleOperationBuilderImpl)col.find().key(key2)).lastModified(lastModified2).count());
    
    // Test with document not including key
    doc = db.createDocumentFromString("{ \"data\" : \"v2-2\" }");
    if (replaceAndGet) {
      doc = col.find().key(key2).replaceOneAndGet(doc);
      verifyNullContentDocument(doc);
      assertEquals(key2, doc.getKey());
      assertEquals(1, col.find().key(key2).version(doc.getVersion()).count());
      assertEquals(1, ((OracleOperationBuilderImpl)col.find().key(key2)).lastModified(doc.getLastModified()).count());
    } else {
      assertTrue(col.find().key(key2).replaceOne(doc));
    }
    
    doc = col.findOne(key2);
    assertEquals("{ \"data\" : \"v2-2\" }", doc.getContentAsString());
    
    // Test with document having non-JSON data
    if(col.admin().isHeterogeneous()) {
      // updated JSON data to non-JSON data
      doc = db.createDocumentFromString(null, "new data v4", "text/plain");
      if (replaceAndGet) {
        doc = col.find().key(key4).version(version4).replaceOneAndGet(doc);
        verifyNullContentDocument(doc);
        assertEquals(key4, doc.getKey());
        assertEquals(1, col.find().key(key4).version(doc.getVersion()).count());
        assertEquals(1, ((OracleOperationBuilderImpl)col.find().key(key4)).lastModified(doc.getLastModified()).count());
      } else {
        assertTrue(col.find().key(key4).version(version4).replaceOne(doc));
      }
      
      doc = col.findOne(key4);
      assertEquals("new data v4", new String(doc.getContentAsByteArray(), "UTF-8"));
      assertEquals("text/plain", doc.getMediaType());
      
      // updated non-JSON data to JSON data
      doc = db.createDocumentFromString("{ \"data\" : \"v4-2\" }");
      if (replaceAndGet) {
        doc = col.find().key(key4).replaceOneAndGet(doc);
        verifyNullContentDocument(doc);
        assertEquals(key4, doc.getKey());
        assertEquals(1, col.find().key(key4).version(doc.getVersion()).count());
        assertEquals(1, ((OracleOperationBuilderImpl)col.find().key(key4)).lastModified(doc.getLastModified()).count());
      } else {
        assertTrue(col.find().key(key4).replaceOne(doc));
      }
      
      doc = col.findOne(key4);
      assertEquals("{ \"data\" : \"v4-2\" }", new String(doc.getContentAsByteArray(), "UTF-8"));
      assertEquals("application/json", doc.getMediaType());
    } else {
      try {
        // Test with non-JSON data for non-Heterogeneous collection 
        doc = db.createDocumentFromString(null, "new data v4", "text/plain");
        if (replaceAndGet) 
          col.find().key(key4).replaceOneAndGet(doc);
        else
          col.find().key(key4).replaceOne(doc);
        
        fail("No exception when replacing with non-JSON in non-Heterogeneous collection");
      } catch (OracleException e) {
        // Expect an OracleException
        // ORA-02290: check constraint (SYS_C0012008) violated
        Throwable t = e.getCause();
        assertTrue(t.getMessage().contains("ORA-02290"));
      }
    }
    
    // Test with key() + lastModified
    doc = db.createDocumentFromString("{ \"data\" : \"v5\" }");
    if (replaceAndGet) {
      doc = ((OracleOperationBuilderImpl)col.find().key(key5)).lastModified(lastModified5).replaceOneAndGet(doc);
      verifyNullContentDocument(doc);
      assertEquals(key5, doc.getKey());
      assertEquals(1, col.find().key(key5).version(doc.getVersion()).count());
      assertEquals(1, ((OracleOperationBuilderImpl)col.find().key(key5)).lastModified(doc.getLastModified()).count());
    } else {
      assertTrue(((OracleOperationBuilderImpl)col.find().key(key5)).lastModified(lastModified5).replaceOne(doc));
    }
    
    doc = col.findOne(key5);
    assertEquals("{ \"data\" : \"v5\" }", new String(doc.getContentAsByteArray(), "UTF-8"));
    
    // Test with null content
    doc = db.createDocumentFromString(null);
    if (replaceAndGet) {
      doc = col.find().key(key5).replaceOneAndGet(doc);
      verifyNullContentDocument(doc);
      assertEquals(key5, doc.getKey());
      assertEquals(1, col.find().key(key5).version(doc.getVersion()).count());
    } else {
      assertTrue(col.find().key(key5).replaceOne(doc));
    }
    
    doc = col.findOne(key5);
    assertNull(doc.getContentAsString());
    assertNull(doc.getContentAsByteArray());
    
    doc = db.createDocumentFromString("id-x", "{ \"data\" : \"x\" }");
    assertFalse(col.find().key("1001").replaceOne(doc));
    
    try {
      // Test when no document meeting the condition
      HashSet<String> keySet = new HashSet<String>();
      keySet.add(key2);
      keySet.add(key5); 
      doc = db.createDocumentFromString("{ \"data\" : \"x\" }");
      if (replaceAndGet) 
        col.find().key(key2).keys(keySet).replaceOneAndGet(doc);
      else
        col.find().key(key2).keys(keySet).replaceOne(doc);
      
      fail("No exception when key() is not using with replaceOne()");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("The key for the document to replace must be specified using the key() method.", e.getMessage());
    }
    
    try { 
      // Pass null for document
      if (replaceAndGet) 
        col.find().key(key2).replaceOneAndGet(null);
      else
        col.find().key(key2).replaceOne(null);
      
      fail("No exception when document is null");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("document argument cannot be null.", e.getMessage());
    }
   
    //Test when no document matching the specified key
    //Firstly removed the doc with key5
    assertEquals(1, col.find().key(key5).remove());
    
    doc = db.createDocumentFromString("{ \"data\" : \"abc\" }");
    if (replaceAndGet) 
      assertEquals(null, col.find().key(key5).replaceOneAndGet(doc));
    else
      assertEquals(false, col.find().key(key5).replaceOne(doc));
 
    // Remove all the documents in the collection
    col.find().remove();
    // Test with the empty collection
    if (replaceAndGet) 
      assertEquals(null, col.find().key(key5).replaceOneAndGet(doc));
    else
      assertEquals(false, col.find().key(key5).replaceOne(doc));

  }
  
  public void testReplaceOne() throws Exception {
    
    // Test with keyColumnAssignmentMethod="SEQUENCE", versionColumnMethod="SEQUENTIAL" and contentColumnType="BLOB"
    OracleDocument mDoc = client.createMetadataBuilder().mediaTypeColumnName("CONTENT_TYPE")
        .keyColumnAssignmentMethod("SEQUENCE").keyColumnSequenceName("keysql1")
        .versionColumnMethod("SEQUENTIAL")
        .contentColumnType("BLOB").build();
    OracleCollection col = dbAdmin.createCollection("testReplaceOne", mDoc);
    testReplaceOneWithCol(col, false, false);
    
    // Test with keyColumnAssignmentMethod="GUID", versionColumnMethod="TIMESTAMP" and contentColumnType="CLOB"
    OracleDocument mDoc2 = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("GUID")
        .versionColumnMethod("TIMESTAMP")
        .contentColumnType("CLOB").build();
    OracleCollection col2 = dbAdmin.createCollection("testReplaceOne2", mDoc2);
    testReplaceOneWithCol(col2, false, false);
    
    // Test with keyColumnAssignmentMethod="UUID", versionColumnMethod="UUID", and contentColumnType="VARCHAR2"
    OracleDocument mDoc3 = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("UUID")
        .versionColumnMethod("UUID")
        .contentColumnType("VARCHAR2").build();
    OracleCollection col3 = dbAdmin.createCollection("testReplaceOne3", mDoc3);
    testReplaceOneWithCol(col3, false, false);
    
    // Test with keyColumnAssignmentMethod="CLIENT", versionColumnMethod="SHA256", and contentColumnType="NVARCHAR2"
    OracleDocument mDoc4 = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("CLIENT")
        .versionColumnMethod("SHA256")
        // ### Oracle Database does not support NVARCHAR2 storage for JSON
        //.contentColumnType("NVARCHAR2").build();
        .contentColumnType("VARCHAR2").build();
    OracleCollection col4 = dbAdmin.createCollection("testReplaceOne4", mDoc4);
    testReplaceOneWithCol(col4, true, false);
    
    // Test with keyColumnAssignmentMethod="CLIENT", versionColumnMethod="MD5", and contentColumnType="RAW"
    OracleDocument mDoc5 = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("CLIENT")
        .versionColumnMethod("MD5")
        // ### Oracle Database does not support RAW storage for JSON
        //.contentColumnType("RAW").build();
        .contentColumnType("VARCHAR2").build();
    OracleCollection col5 = dbAdmin.createCollection("testReplaceOne5", mDoc5);
    testReplaceOneWithCol(col5, true, false);
    
    // Test with keyColumnAssignmentMethod="CLIENT", versionColumnMethod="NONE"
    OracleDocument mDoc6 = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("CLIENT")
        .mediaTypeColumnName("CONTENT_TYPE")
        .versionColumnMethod("NONE").tableName("SODATBL").build();
    OracleCollection col6 = dbAdmin.createCollection("testReplaceOne6", mDoc6);
    testReplaceOneWithCol(col6, true, false);
    
    // Test with readOnly=true
    OracleDocument mDoc7 = client.createMetadataBuilder()
        .readOnly(true).tableName("SODA_TABLE").build();
   
    String uuidKey = "D362D2C133DB4F20A5AFEC15929DFEF9";
    String insertSql = 
      "insert into soda_table values('" + uuidKey +"', 'application/json', SYSTIMESTAMP, SYSTIMESTAMP, 'ver000001', c2b('{ \"data\" : \"1\" }'))"; 
    PreparedStatement stmt = conn.prepareStatement(insertSql);
    stmt.execute();
    // auto-commit set on
    //conn.commit(); 
    stmt = (OraclePreparedStatement) conn.prepareStatement("select count(*) from soda_table");
    ResultSet rs = stmt.executeQuery();

    OracleCollection col7 = dbAdmin.createCollection("testReplaceOne7", mDoc7);
    assertTrue(col7.admin().isReadOnly());
    
    String key = col7.find().getOne().getKey();
    assertEquals(1, col7.find().key(key).count());
 
    try { 
      OracleDocument doc = db.createDocumentFromString("{ \"data\" : \"v1\" }");
      col7.find().key(key).replaceOne(doc);
      fail("No exception when do replace in readOnly collection");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Collection testReplaceOne7 is read-only, replaceOne not allowed.", e.getMessage());
    }
 
    try { 
      col7.find().remove();
      fail("No exception when do remove in readOnly collection");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Collection testReplaceOne7 is read-only, remove not allowed.", e.getMessage());
    }
    
    // clean up
    stmt = (OraclePreparedStatement) conn.prepareStatement("delete from soda_table");
    stmt.execute();
    
  }
 
  public void testReplaceOneAndGet() throws Exception {
    
    // Test with keyColumnAssignmentMethod="SEQUENCE", versionColumnMethod="MD5" and keyColumnType="NUMBER"
    OracleDocument mDoc = client.createMetadataBuilder().mediaTypeColumnName("CONTENT_TYPE")
        .keyColumnAssignmentMethod("SEQUENCE").keyColumnSequenceName("keysql1")
        .versionColumnMethod("MD5")
        .keyColumnType("NUMBER").build();
    OracleCollection col = dbAdmin.createCollection("testReplaceOneAndGet", mDoc);
    testReplaceOneWithCol(col, false, true);

    // Test with keyColumnAssignmentMethod="GUID", versionColumnMethod="SHA256" and keyColumnType="VARCHAR2"
    OracleDocument mDoc2 = client.createMetadataBuilder().mediaTypeColumnName("CONTENT_TYPE")
        .keyColumnAssignmentMethod("GUID")
        .versionColumnMethod("SHA256")
        .keyColumnType("VARCHAR2").build();
    OracleCollection col2 = dbAdmin.createCollection("testReplaceOneAndGet2", mDoc2);
    testReplaceOneWithCol(col2, false, true);
    
    // Test with keyColumnAssignmentMethod="UUID", versionColumnMethod="TIMESTAMP", and keyColumnType="RAW"
    OracleDocument mDoc3 = client.createMetadataBuilder().mediaTypeColumnName("CONTENT_TYPE")
        .keyColumnAssignmentMethod("UUID")
        .versionColumnMethod("TIMESTAMP")
        .keyColumnType("RAW").build();
    OracleCollection col3 = dbAdmin.createCollection("testReplaceOneAndGet3", mDoc3);
    testReplaceOneWithCol(col3, false, true);
    
    // Test with keyColumnAssignmentMethod="CLIENT", versionColumnMethod="TIMESTAMP", and keyColumnType="NVARCHAR2"
    OracleDocument mDoc4 = client.createMetadataBuilder().mediaTypeColumnName("CONTENT_TYPE")
        .keyColumnAssignmentMethod("CLIENT")
        .versionColumnMethod("TIMESTAMP")
        .keyColumnType("NVARCHAR2").build();
    OracleCollection col4 = dbAdmin.createCollection("testReplaceOneAndGet4", mDoc4);
    testReplaceOneWithCol(col4, true, true);
    
    // Test with keyColumnAssignmentMethod="CLIENT", versionColumnMethod="SEQUENTIAL", and contentColumnType="NCLOB"
    OracleDocument mDoc5 = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("CLIENT")
        .versionColumnMethod("SEQUENTIAL")
        // ### Oracle Database does not support NVARCHAR2, NCLOB, and RAW storage for JSON    
        //.contentColumnType("NCLOB").build();
        .contentColumnType("CLOB").build();
    OracleCollection col5 = dbAdmin.createCollection("testReplaceOneAndGet5", mDoc5);
    testReplaceOneWithCol(col5, true, true);
    
    // Test with keyColumnAssignmentMethod="UUID", versionColumnMethod="NONE"
    OracleDocument mDoc6 = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("UUID")
        .mediaTypeColumnName("CONTENT_TYPE")
        .versionColumnMethod("NONE").tableName("SODATBL").build();
    OracleCollection col6 = dbAdmin.createCollection("testReplaceOneAndGet6", mDoc6);
    testReplaceOneWithCol(col6, false, true);
    
    // Test with readOnly=true
    OracleDocument mDoc7 = client.createMetadataBuilder().readOnly(true).build();
    OracleCollection col7 = dbAdmin.createCollection("testReplaceOneAndGet7", mDoc7);
    try { 
      OracleDocument doc = db.createDocumentFromString(null, "abcd efgh", "text/plain");
      col7.find().key("id-1").replaceOneAndGet(doc);
      fail("No exception when do replace in readOnly collection");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Collection testReplaceOneAndGet7 is read-only, replaceOneAndGet not allowed.", e.getMessage());
    }
    
  }

} 
