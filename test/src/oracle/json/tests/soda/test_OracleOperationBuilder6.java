/* Copyright (c) 2017, 2023, Oracle and/or its affiliates. */
/* All rights reserved.*/

/*
   DESCRIPTION
   Tests for $orderby
 */

/**
 *  @author  Vincent Liu
 */

package oracle.json.tests.soda;

import java.util.HashMap;
import java.util.HashSet;
import java.sql.SQLException;



import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Date;

import oracle.json.parser.QueryException;
import oracle.json.testharness.SodaTestCase.IndexType;
import oracle.json.testharness.SodaTestCase;
import oracle.soda.OracleCollection;
import oracle.soda.OracleCollection;
import oracle.soda.OracleCursor;
import oracle.soda.OracleDatabase;
import oracle.soda.OracleDocument;
import oracle.soda.OracleException;
import oracle.soda.rdbms.OracleRDBMSClient;
import oracle.soda.rdbms.OracleRDBMSMetadataBuilder;
import oracle.soda.rdbms.impl.OracleDatabaseImpl;
import oracle.soda.rdbms.impl.OracleOperationBuilderImpl;
import oracle.soda.rdbms.impl.SODAUtils;
import oracle.sql.json.OracleJsonDate;
import oracle.sql.json.OracleJsonFactory;
import oracle.sql.json.OracleJsonIntervalDS;
import oracle.sql.json.OracleJsonIntervalYM;
import oracle.sql.json.OracleJsonObject;
import oracle.sql.json.OracleJsonValue;
import oracle.sql.json.OracleJsonTimestamp;
import oracle.sql.json.OracleJsonDecimal;
import oracle.sql.json.OracleJsonBinary;
import oracle.sql.json.OracleJsonString;
import oracle.sql.json.OracleJsonDouble;
import oracle.sql.json.OracleJsonTimestampTZ;


public class test_OracleOperationBuilder6 extends SodaTestCase {
  
  private void checkKeys(OracleCollection col, OracleDocument filterDoc,
      HashSet<String> expectedKeys) throws Exception {
    OracleCursor c = col.find().filter(filterDoc).getCursor();

    HashSet<String> keys = new HashSet<String>();

    while (c.hasNext())
      keys.add(c.next().getKey());

    c.close();

    assertEquals(keys, expectedKeys);
  }
  
  public void testNestedCondition() throws Exception {
    OracleCollection col = db.admin().createCollection("testNestedCondition");
    OracleDocument r1 = col.insertAndGet(db.createDocumentFromString("{\"f\" : [1, 10, 20]}"));
    col.insert(db.createDocumentFromString("{\"f[*]\" : [ 10, 8]}"));
    assertEquals(1, col.find().filter("{\"f[*]\" : {\"$gt\" : 9, \"$lt\" : 11}}").count());
    assertEquals(r1.getKey(), col.find().filter("{\"f\" : {\"$gt\" : 9, \"$lt\" : 11}}").getOne().getKey());
  }
  
  // Test with $not operator
  public void testNotClob() throws Exception {
    testNot("CLOB");
  }
  
  public void testNotBlob() throws Exception {
    testNot("BLOB");
  }
  
  public void testNotVarchar2() throws Exception {
    testNot("VARCHAR2");
  }
  
  public void testNotJSON() throws Exception {
    if (isCompatibleOrGreater(COMPATIBLE_20)) {
      testNot("JSON");
    }
  }
  
  private void testNot(String contentColumnType) throws Exception {
    if (isJDCSOrATPMode())
      if (!contentColumnType.equalsIgnoreCase("BLOB"))
        return;

    if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel))
      return;
    
    OracleDocument mDoc = client.createMetadataBuilder().keyColumnAssignmentMethod("CLIENT")
      .contentColumnType(contentColumnType).build();
    
    String colName = "testNot" + contentColumnType;
    OracleCollection col;
    if (isJDCSOrATPMode())
    {
      col = db.admin().createCollection(colName, null);
    } else
    {
      col = db.admin().createCollection(colName, mDoc);
    }
    
    String key1="id001", key2="id002", key3="id003";
    OracleDocument doc = null, filterDoc = null;
    HashSet<String> expectedKeys = new HashSet<String>();
    
    String docStr1 = "{ \"orderno\":101, \"date\":\"2017-08-01\", \"time\":\"2017-08-01T11:30:00\", " +
        "\"order\" : [ \n" + 
        "{ \"items\": [ { \"name\": \"Essentials of Business Law\", \"price\": 63.54, \"quantity\":1}, \n" + 
                        "{ \"name\": \"Bossypants\", \"price\": 12.99, \"quantity\": 1, \"paid\":true } ] }, \n" + 
        "{ \"items\": [ { \"name\": \"Discovering Statistics\", \"price\": 15.00, \"quantity\": 2, \"paid\":true }, \n" + 
                        "{ \"name\": \"Integrated Algebra\", \"price\": 8.99, \"quantity\": 1, \"paid\":true } ] }\n" + 
        "] }";

    String docStr2 = "{ \"orderno\":102, \"date\":\"2017-08-01\", \"time\":\"2017-08-01T06:00:00\"," +
        "\"order\" : [ \n" + 
        "{ \"items\": [ { \"name\": \"Tina Fey\", \"price\": 12.99, \"quantity\": 1 } ] }, \n" + 
        "{ \"items\": [ { \"name\": \"Calculus For Dummies\", \"price\": 14.00, \"quantity\": 2 } ] }, \n" +
        "{ \"items\": [ { \"name\": \"Business Law\", \"price\": 18.00, \"quantity\": 2 } ] } \n" + 
        "] }";

    String docStr3 = " { \"orderno\":103, \"date\":\"2017-01-01\", \"time\":\"2017-01-01T09:00:00\"," +
        "\"order\" : { \"items\": [\n" + 
        "{ \"name\": \"Understanding Property Law\", \"price\": 54.49, \"quantity\": 1 }, \n" +
        "{ \"name\": \"Young Men and Fire\", \"price\": 10.88, \"quantity\": 2}, \n" + 
        "{ \"name\": \"Tina Fey\", \"price\": 13.57, \"quantity\": 3} \n" +
        "] } }";
    if (isJDCSOrATPMode()) 
    {
      doc = col.insertAndGet(db.createDocumentFromString(docStr1));
      key1 = doc.getKey();
      doc = col.insertAndGet(db.createDocumentFromString(docStr2));
      key2 = doc.getKey();
      doc = col.insertAndGet(db.createDocumentFromString(docStr3));
      key3 = doc.getKey();
    } else
    {
      col.insertAndGet(db.createDocumentFromString(key1, docStr1));
      col.insertAndGet(db.createDocumentFromString(key2, docStr2));
      col.insertAndGet(db.createDocumentFromString(key3, docStr3));
    }
    
    // Test $not with multiple comparison operators
    filterDoc = db.createDocumentFromString("{\"order[0].items[0].price\":{\"$not\": {\"$gt\":40, \"$lt\":80 }}}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key2, col.find().filter(filterDoc).getOne().getKey());
    
    filterDoc = db.createDocumentFromString("{\"order[*].items[*].quantity\":{\"$not\": {\"$eq\":1, \"$eq\":3 }}}");
    assertEquals(2, col.find().filter(filterDoc).count());
    expectedKeys.clear();
    expectedKeys.add(key1);
    expectedKeys.add(key2);
    checkKeys(col, filterDoc, expectedKeys);
    
    filterDoc = db.createDocumentFromString("{\"order[*].items[*].quantity\":{\"$not\": {\"$ne\":0, \"$lte\":100 }}}");
    assertEquals(0, col.find().filter(filterDoc).count());
    
    // test about modifier operator wrapping $not
    filterDoc = db.createDocumentFromString("{\"order[0].items[0].name\": {\"$length\":{\"$not\": " +
    		"{\"$gt\":6, \"$lt\":20 }}}}");
    assertEquals(2, col.find().filter(filterDoc).count());
    expectedKeys.clear();
    expectedKeys.add(key1);
    expectedKeys.add(key3);
    checkKeys(col, filterDoc, expectedKeys);
    
    filterDoc = db.createDocumentFromString("{\"order\": {\"$size\":{\"$not\": {\"$gte\":1, \"$lte\":2 }}}}");
    // ### Bug on 12.2.0.1: $size doesn't work correctly,
    // else branch shows wrong result.
    if (!SODAUtils.sqlSyntaxBelow_18(sqlSyntaxLevel)) {
      assertEquals(1, col.find().filter(filterDoc).count());
      assertEquals(key2, col.find().filter(filterDoc).getOne().getKey());
    }
    else {
      assertEquals(3, col.find().filter(filterDoc).count());
    }
    
    filterDoc = db.createDocumentFromString("{\"$or\": [" +
        "{\"order[0].items[0].name\": {\"$upper\":{\"$not\": {\"$startsWith\":\"ESS\" }}}}," +
        "{\"order[0].items[0].name\": {\"$not\": {\"$hasSubstring\" : \"Law\" }}}  ]}");
    assertEquals(2, col.find().filter(filterDoc).count());
    expectedKeys.clear();
    expectedKeys.add(key2);
    expectedKeys.add(key3);
    checkKeys(col, filterDoc, expectedKeys);
    
    // Negative tests
    // Test about $not wrapping modifier
    filterDoc = db.createDocumentFromString("{\"order[0].items[0].name\":{\"$not\":" +
        "{\"$upper\":{\"$startsWith\":\"ESS\"},\"$hasSubstring\":\"Law\"}}}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $not wraps $upper");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      String expMsg = "$not operator cannot wrap $upper operator.";
      assertEquals(expMsg, queryException.getMessage());
    }
    
    // Test $not with array operand
    filterDoc = db.createDocumentFromString("{\"order[0].items[0].price\":{\"$not\":" +
        "[{\"$gt\":100},{\"$lt\":150}]}}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $not's operand is an array.");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      String expMsg = "The components of the $not operator must be objects.";
      assertEquals(expMsg, queryException.getMessage());
    }
    
    // Test $not with array operand
    filterDoc = db.createDocumentFromString("{\"order[0].items[0].price\":{\"$not\":null}}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $not's operand is null.");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      String expMsg = "The components of the $not operator must be objects.";
      assertEquals(expMsg, queryException.getMessage());
    }
   
    col.admin().drop();
  }
  
  // Test with $orderby operator
  public void testOrderbyClob() throws Exception {
    testOrderby("CLOB");
  }
  
  public void testOrderbyBlob() throws Exception {
    testOrderby("BLOB");
  }
  
  public void testOrderbyVarchar2() throws Exception {
    testOrderby("VARCHAR2");
  }
  
  public void testOrderbyJSON() throws Exception {
    if (isCompatibleOrGreater(COMPATIBLE_20)) {
      testNot("JSON");
    }
  }
  
  private void testOrderby(String contentColumnType) throws Exception {
    if (isJDCSOrATPMode())
      if (!contentColumnType.equalsIgnoreCase("BLOB"))
        return;

    if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel))
      return;
    
    OracleDocument mDoc = client.createMetadataBuilder().keyColumnAssignmentMethod("CLIENT")
      .contentColumnType(contentColumnType).build();
    
    String colName = "testOrderby" + contentColumnType;
    OracleCollection col;
    if (isJDCSOrATPMode())
    {
      col = db.admin().createCollection(colName, null);
    } else
    {
      col = db.admin().createCollection(colName, mDoc);
    }
    String key1="id001", key2="id002", key3="id003", key4="id004", key5="id005";
    OracleDocument doc = null, filterDoc = null;
    
    String docStr1 = "{\"orderno\":1, \"date\":\"2017-08-01\", \"time\":\"2017-08-01T11:30:00\", \"string\": \"aaa\"}";
    String docStr2 = "{\"orderno\":2, \"date\":\"2017-08-01\", \"time\":\"2017-08-01T06:00:00\", \"string\": \"abc002\"}";
    String docStr3 = "{\"orderno\":101, \"date\":\"2017-01-01\", \"time\":\"2017-01-01T09:00:00\", \"string\": \"Aaa\"}";
    String docStr4 = "{\"orderno\":102, \"date\":\"2017-05-01\", \"time\":\"2017-05-01T09:00:00\", \"string\": \"abd002\"}";
    String docStr5 = "{\"orderno\":103, \"date\":\"2017-07-15\", \"time\":\"2017-07-15T09:18:00\", \"string\": \"abc001\"}";
    
    if (isJDCSOrATPMode()) 
    {
      doc = col.insertAndGet(db.createDocumentFromString(docStr1));
      key1 = doc.getKey();
      doc = col.insertAndGet(db.createDocumentFromString(docStr2));
      key2 = doc.getKey();
      doc = col.insertAndGet(db.createDocumentFromString(docStr3));
      key3 = doc.getKey();
      doc = col.insertAndGet(db.createDocumentFromString(docStr4));
      key4 = doc.getKey();
      doc = col.insertAndGet(db.createDocumentFromString(docStr5));
      key5 = doc.getKey();
    } else
    {
      col.insert(db.createDocumentFromString(key1, docStr1));
      col.insert(db.createDocumentFromString(key2, docStr2));
      col.insert(db.createDocumentFromString(key3, docStr3));
      col.insert(db.createDocumentFromString(key4, docStr4));
      col.insert(db.createDocumentFromString(key5, docStr5));
    }

    
    
    // Tests $orderby with array object
    filterDoc = db.createDocumentFromString("{\"$orderby\": [" +
        "{\"path\":\"date\",\"datatype\":\"date\",\"order\":\"asc\"}, " +
        "{\"path\":\"orderno\",\"datatype\":\"number\",\"order\":\"asc\"} ]}");
    OracleCursor cursor = col.find().filter(filterDoc).getCursor();
    assertEquals(key3, cursor.next().getKey());
    assertEquals(key4, cursor.next().getKey());
    assertEquals(key5, cursor.next().getKey());
    assertEquals(key1, cursor.next().getKey());
    assertEquals(key2, cursor.next().getKey());
    assertEquals(false, cursor.hasNext());
    cursor.close();
    
    // Tests to cover "date", "datetime", "string", "number" datatypes
    filterDoc = db.createDocumentFromString("{\"$orderby\": [" +
        "{\"path\":\"date\",\"datatype\":\"date\",\"order\":\"asc\"}, "+
        "{\"path\":\"string\",\"datatype\":\"string\",\"order\":\"desc\"} ]}");
    cursor = col.find().filter(filterDoc).getCursor();
    assertEquals(key3, cursor.next().getKey());
    assertEquals(key4, cursor.next().getKey());
    assertEquals(key5, cursor.next().getKey());
    assertEquals(key2, cursor.next().getKey());
    assertEquals(key1, cursor.next().getKey());
    assertEquals(false, cursor.hasNext());
    cursor.close();
    
    filterDoc = db.createDocumentFromString("{\"$orderby\": [" +
        "{\"path\":\"time\",\"datatype\":\"datetime\",\"order\":\"desc\"}]}");
    cursor = col.find().filter(filterDoc).getCursor();
    assertEquals(key1, cursor.next().getKey());
    assertEquals(key2, cursor.next().getKey());
    assertEquals(key5, cursor.next().getKey());
    assertEquals(key4, cursor.next().getKey());
    assertEquals(key3, cursor.next().getKey());
    assertEquals(false, cursor.hasNext());
    cursor.close();
    
    // when no "order" field is supplied, the default "asc" should be applied.
    filterDoc = db.createDocumentFromString("{\"$orderby\": [" +
        "{\"path\":\"string\",\"datatype\":\"string\",\"order\":\"desc\"}, "+
        "{\"path\":\"orderno\",\"datatype\":\"number\"} ]}");
    cursor = col.find().filter(filterDoc).getCursor();
    assertEquals(key4, cursor.next().getKey());
    assertEquals(key2, cursor.next().getKey());
    assertEquals(key5, cursor.next().getKey());
    assertEquals(key1, cursor.next().getKey());
    assertEquals(key3, cursor.next().getKey());
    assertEquals(false, cursor.hasNext());
    cursor.close();
    
    // Test $orderby with maxLength field
    filterDoc = db.createDocumentFromString("{\"$orderby\": [" +
        "{\"path\":\"string\", \"datatype\":\"string\", \"maxLength\":6, \"order\":\"desc\"}, "+
        "{\"path\":\"orderno\",\"datatype\":\"number\",\"order\":\"asc\"} ]}");
    // bug27093614: when "maxLength" is supplied, "truncate" key needs to be added
    // then the result should be changed to: key4, key2, key5,key1, key3 
    cursor = col.find().filter(filterDoc).getCursor();
    assertEquals(key4, cursor.next().getKey());
    assertEquals(key2, cursor.next().getKey());
    assertEquals(key5, cursor.next().getKey());
    assertEquals(key1, cursor.next().getKey());
    assertEquals(key3, cursor.next().getKey());
    assertEquals(false, cursor.hasNext());
    cursor.close();
    
    // when "datatype" field is absent, the default "string" should be applied.
    filterDoc = db.createDocumentFromString("{\"$orderby\": [" +
        "{\"path\":\"string\", \"maxLength\":3, \"order\":\"desc\"}, "+
        "{\"path\":\"orderno\",\"datatype\":\"number\",\"order\":\"asc\"} ]}");
    // bug27093614: "maxLength" field is ignored when "datatype" is absent
    cursor = col.find().filter(filterDoc).getCursor();
    /*assertEquals(key2, cursor.next().getKey());
    assertEquals(key4, cursor.next().getKey());
    assertEquals(key5, cursor.next().getKey());
    assertEquals(key1, cursor.next().getKey());
    assertEquals(key3, cursor.next().getKey());
    assertEquals(false, cursor.hasNext());
    cursor.close();*/
    
    // Test when datatype in QBE spec does not match document's datatype
    filterDoc = db.createDocumentFromString("{\"$orderby\":[ " +
        "{\"path\":\"orderno\", \"order\":\"asc\"} ]}");
    cursor = col.find().filter(filterDoc).getCursor();
    assertEquals(key1, cursor.next().getKey());
    assertEquals(key3, cursor.next().getKey());
    assertEquals(key4, cursor.next().getKey());
    assertEquals(key5, cursor.next().getKey());
    assertEquals(key2, cursor.next().getKey());
    assertEquals(false, cursor.hasNext());
    cursor.close();
    
    // Negative tests for $orderby
    
    // Test with missing path field
    filterDoc = db.createDocumentFromString("{\"$orderby\":[ " +
        "{\"datatype\":\"string\", \"order\":\"asc\"} ]}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when \"path\" field is missing");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      String expMsg = "An $orderby field must specify a path.";
      assertEquals(expMsg, queryException.getMessage());
    }
    
    // Test null/object/array value for path field
    filterDoc = db.createDocumentFromString("{\"$orderby\":[ " +
        "{\"path\":null, \"datatype\":\"string\", \"order\":\"asc\"} ]}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when the value of \"path\" is invalid");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      String expMsg = "The \"$orderby\" property \"path\" must be of type \"string\".";
      assertEquals(expMsg, queryException.getMessage());
    }

    filterDoc = db.createDocumentFromString("{\"$orderby\":[ " +
        "{\"path\":{\"value\":\"orderno\"}, \"datatype\":\"string\", \"order\":\"asc\"} ]}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when the value of \"path\" is invalid");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      String expMsg = "The \"$orderby\" property \"path\" must be of type \"string\".";
      assertEquals(expMsg, queryException.getMessage());
    }
    
    filterDoc = db.createDocumentFromString("{\"$orderby\":[ " +
        "{\"path\":[\"date\",\"orderno\"], \"datatype\":\"string\", \"order\":\"asc\"} ]}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when the value of \"path\" is invalid");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      String expMsg = "The \"$orderby\" property \"path\" must be of type \"string\".";
      assertEquals(expMsg, queryException.getMessage());
    }

    // Test invalid value for "datatype"
    filterDoc = db.createDocumentFromString("{\"$orderby\":[{\"path\":\"date\", \"datatype\":\"boolean\"}]}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when the value of \"datatype\" is invalid");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      String expMsg = "The $orderby property \"datatype\" cannot have value \"boolean\".";
      assertEquals(expMsg, queryException.getMessage());
    }
    
    filterDoc = db.createDocumentFromString("{\"$orderby\":[{\"path\":\"date\", \"datatype\":\"array\"}]}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when the value of \"datatype\" is invalid");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      String expMsg = "The $orderby property \"datatype\" cannot have value \"array\".";
      assertEquals(expMsg, queryException.getMessage());
    }
    
    // Test to specify maxLength when "datatype" is non-string
    filterDoc = db.createDocumentFromString("{\"$orderby\":[{\"path\":\"date\", \"datatype\":\"date\", \"maxLength\":10}]}");
    // bug27093614: when datatype is non-string, but "maxLength" is supplied, no error is reported.
    /*try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when \"maxLength\" is supplied for non-string datatype");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      String expMsg = "";
      assertEquals(expMsg, queryException.getMessage());
    }*/
    
    // Test with invalid "maxLength" value 
    filterDoc = db.createDocumentFromString("{\"$orderby\":[{\"path\":\"string\", " +
        "\"datatype\":\"string\", \"maxLength\":0}]}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when the value of \"maxLength\" is invalid");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      String expMsg = "The $orderby property \"maxLength\" cannot have value \"0\".";
      assertEquals(expMsg, queryException.getMessage());
    }
    
    filterDoc = db.createDocumentFromString("{\"$orderby\":[{\"path\":\"string\", " +
        "\"datatype\":\"string\", \"maxLength\":-10}]}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when the value of \"maxLength\" is invalid");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      String expMsg = "The $orderby property \"maxLength\" cannot have value \"-10\".";
      assertEquals(expMsg, queryException.getMessage());
    }
    
    filterDoc = db.createDocumentFromString("{\"$orderby\":[{\"path\":\"string\", "
            + "\"datatype\":\"string\", \"maxLength\":null}]}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when the value of \"maxLength\" is invalid");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      String expMsg = "The \"$orderby\" property \"maxLength\" must be of type \"number\".";
      assertEquals(expMsg, queryException.getMessage());
    }

    filterDoc = db.createDocumentFromString("{\"$orderby\":[{\"path\":\"string\", "
        + "\"datatype\":\"string\", \"maxLength\":\"100\"}]}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when the value of \"maxLength\" is invalid");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      String expMsg = "The \"$orderby\" property \"maxLength\" must be of type \"number\".";
      assertEquals(expMsg, queryException.getMessage());
    }
    
    // Test with invalid "order" value
    filterDoc = db.createDocumentFromString("{\"$orderby\":[{\"path\":\"string\", "
        + "\"datatype\":\"string\", \"order\":\"abc\"}]}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when the value of \"order\" is invalid");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      String expMsg = "The $orderby property \"order\" cannot have value \"abc\".";
      assertEquals(expMsg, queryException.getMessage());
    }
    
    filterDoc = db.createDocumentFromString("{\"$orderby\":[{\"path\":\"string\", "
        + "\"datatype\":\"string\", \"order\":1}]}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when the value of \"order\" is invalid");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      String expMsg = "The \"$orderby\" property \"order\" must be of type \"string\".";
      assertEquals(expMsg, queryException.getMessage());
    }
    
    filterDoc = db.createDocumentFromString("{\"$orderby\":[{\"path\":\"string\", "
        + "\"datatype\":\"string\", \"order\":null}]}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when the value of \"order\" is invalid");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      String expMsg = "The \"$orderby\" property \"order\" must be of type \"string\".";
      assertEquals(expMsg, queryException.getMessage());
    }
    
    // Test with unknown field in $orderby spec
    filterDoc = db.createDocumentFromString("{\"$orderby\":[{\"path\":\"string\", "
        + "\"value\":\"abc\", \"datatype\":\"string\"}]}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when the unknown field is presented");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      String expMsg = "An $orderby field cannot have property \"value\".";
      assertEquals(expMsg, queryException.getMessage());
    }
    
    col.admin().drop();
  }

  //////////////////////////////////////////////////////////////
  // $orderby error modes tests (default, scalarRequired, lax //
  //////////////////////////////////////////////////////////////

  private void testOrderbyLax(String contentColumnType,
                                boolean withIndex,
                                boolean withFilter)
    throws Exception
  {
    if (isJDCSOrATPMode())
      if (!contentColumnType.equalsIgnoreCase("BLOB"))
        return;
      
    OracleDocument mDoc = client.createMetadataBuilder().keyColumnAssignmentMethod("CLIENT")
      .contentColumnType(contentColumnType).build();

    OracleCollection col;
    if (isJDCSOrATPMode())
    {
      col = db.admin().createCollection("orderByCol" + contentColumnType, null);
    } else
    {
      col = db.admin().createCollection("orderByCol" + contentColumnType, mDoc);
    }
    String key1="k1", key2="k2", key3="k3";

    String docStr1 = "{\"sfield\" : \"aac\", \"nfield\" : 21}";
    String docStr2 = "{\"sfield\": \"aaa\", \"nfield\": 20}";
    String docStr3 = "{\"newfield\": [\"aab\"]}";

    OracleDocument doc = null;

    if (isJDCSOrATPMode()) 
    {
      doc = col.insertAndGet(db.createDocumentFromString(docStr1));
      key1 = doc.getKey();
      doc = col.insertAndGet(db.createDocumentFromString(docStr2));
      key2 = doc.getKey();
      doc = col.insertAndGet(db.createDocumentFromString(docStr3));
      key3 = doc.getKey();
    } else
    {
      col.insert(db.createDocumentFromString(key1, docStr1));
      col.insert(db.createDocumentFromString(key2, docStr2));
      col.insert(db.createDocumentFromString(key3, docStr3));
    }
    
    String indexName = "\"orderByIndex" + contentColumnType + "\"";

    if (withIndex)
    {
      col.admin().createIndex(db.createDocumentFromString("{\"name\" :" + indexName + "," +
        "\"indexNulls\" : true," +
        "\"lax\" : true," +
        "\"fields\" : [ {\"path\" : \"sfield\", \"datatype\" : \"varchar2\"}," +
        "  {\"path\" : \"nfield\", \"datatype\" : \"number\"}]}"));
    }

    OracleDocument fDoc;

    if (withFilter)
    {
      fDoc = db.createDocumentFromString(
        "{\"$query\" : {\"sfield\" : {\"$gt\" : \"a\"}}," +
          "\"$orderby\" : {\"$lax\" : true, \"$fields\" :" +
          "[ {\"path\" : \"sfield\", \"datatype\" : \"varchar2\"}," +
          "  {\"path\" : \"nfield\", \"datatype\" : \"number\"}]}}");
    }
    else
    {
      fDoc = db.createDocumentFromString(
        "{\"$orderby\" : {\"$lax\" : true, \"$fields\" :" +
          "[ {\"path\" : \"sfield\", \"datatype\" : \"varchar2\"}," +
          "  {\"path\" : \"nfield\", \"datatype\" : \"number\"}]}}");
    }

    OracleCursor c = col.find().filter(fDoc).getCursor();

    OracleDocument d;

    if (isJDCSOrATPMode()) //// Blocked by bug 28996376 since 20181130 (remove this if() line once the bug is fixed).
        return;

    if (withFilter) {
      d = c.next();
      assertEquals(d.getKey(), key2);

      d = c.next();
      assertEquals(d.getKey(), key1);

      d = c.next();

      assertEquals(d, null);
    }
    else
    {
      d = c.next();
      assertEquals(d.getKey(), key2);

      d = c.next();
      assertEquals(d.getKey(), key1);

      d = c.next();
      assertEquals(d.getKey(), key3);

      d = c.next();

      assertEquals(d, null);
    }

    if (withIndex)
    {
      /*
      String plan = ((OracleOperationBuilderImpl) col.find().filter(fDoc)).
        explainPlan("basic");
      if (withFilter)
      {
        assertTrue(plan.contains("INDEX RANGE SCAN"));
      }
      else
      {
        assertTrue(plan.contains("INDEX FULL SCAN"));
      }
      */
      col.admin().dropIndex(indexName);
    }
    col.admin().truncate();

  }


  private void testOrderbyDefaultNeg (String contentColumnType,
                                      boolean withIndex,
                                      boolean withFilter)
    throws Exception
  {
    if (isJDCSOrATPMode())
      if (!contentColumnType.equalsIgnoreCase("BLOB"))
        return;

    if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel))
      return;

    OracleDocument mDoc = client.createMetadataBuilder().keyColumnAssignmentMethod("CLIENT")
      .contentColumnType(contentColumnType).build();

    OracleCollection col;
    if (isJDCSOrATPMode())
    {
      col = db.admin().createCollection("orderByCol" + contentColumnType, null);
    } else
    {
      col = db.admin().createCollection("orderByCol" + contentColumnType, mDoc);
    }

    String docStr1 = "{\"sfield\" : \"aac\", \"nfield\" : 21}";
    String docStr2 = "{\"sfield\": \"aaa\", \"nfield\": 20}";
    String docStr3 = "{\"sfield\": [\"aab\"]}";

    String key1="k1", key2="k2", key3="k3";
    OracleDocument doc = null;
    if (isJDCSOrATPMode()) 
    {
      doc = col.insertAndGet(db.createDocumentFromString(docStr1));
      key1 = doc.getKey();
      doc = col.insertAndGet(db.createDocumentFromString(docStr2));
      key2 = doc.getKey();
      doc = col.insertAndGet(db.createDocumentFromString(docStr3));
      key3 = doc.getKey();
    } else
    {
      col.insertAndGet(db.createDocumentFromString(key1, docStr1));
      col.insertAndGet(db.createDocumentFromString(key2, docStr2));
      col.insertAndGet(db.createDocumentFromString(key3, docStr3));
    }

    String indexName = "\"orderByIndex" + contentColumnType + "\"";

    if (withIndex)
    {
      try
      {
        col.admin().createIndex(db.createDocumentFromString("{\"name\" :" + indexName + "," +
          "\"indexNulls\" : true," +
          "\"fields\" : [ {\"path\" : \"sfield\", \"datatype\" : \"varchar2\"}," +
          "  {\"path\" : \"nfield\", \"datatype\" : \"number\"}]}"));

        fail("No exception when bad value is indexed\n");
      }
      catch (OracleException e)
      {
        Throwable c = e.getCause();
        assertTrue(c.getMessage().contains("ORA-40456"));
      }
    }

    OracleDocument fDoc;

    if (withFilter)
    {
      fDoc = db.createDocumentFromString(
        "{\"$query\" : {\"sfield\" : {\"$gt\" : \"a\"}}," +
          "\"$orderby\" : {\"$fields\" :" +
          "[ {\"path\" : \"sfield\", \"datatype\" : \"varchar2\"}," +
          "  {\"path\" : \"nfield\", \"datatype\" : \"number\"}]}}");
    }
    else
    {
      fDoc = db.createDocumentFromString(
        "{\"$orderby\" : {\"$fields\" :" +
          "[ {\"path\" : \"sfield\", \"datatype\" : \"varchar2\"}," +
          "  {\"path\" : \"nfield\", \"datatype\" : \"number\"}]}}");
    }

    try
    {
      OracleCursor c = col.find().filter(fDoc).getCursor();
      fail("No exception when bad value is ordered\n");
    }
    catch (OracleException e)
    {
      Throwable c = e.getCause();
      assertTrue(c.getMessage().contains("ORA-40456"));
    }

    col.admin().truncate();

    if (withIndex)
    {
      col.admin().dropIndex(indexName);
    }
  }

  private void testOrderbyDefault (String contentColumnType,
                                   boolean withIndex,
                                   boolean withFilter)
    throws Exception
  {
    if (isJDCSOrATPMode())
      if (!contentColumnType.equalsIgnoreCase("BLOB"))
        return;

    if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel))
      return;

    OracleDocument mDoc = client.createMetadataBuilder().keyColumnAssignmentMethod("CLIENT")
      .contentColumnType(contentColumnType).build();

    OracleCollection col;
    if (isJDCSOrATPMode())
    {
      col = db.admin().createCollection("orderByCol" + contentColumnType, null);
    } else
    {
      col = db.admin().createCollection("orderByCol" + contentColumnType, mDoc);
    }

    String docStr1 = "{\"sfield\" : \"aac\", \"nfield\" : 21}";
    String docStr2 = "{\"sfield\": \"aaa\", \"nfield\": 20}";
    // The fields we are ordering by and indexing by are
    // missing in this document. With default orderby/index modes,
    // that's allowed.
    String docStr3 = "{\"newfield\": \"aab\"}";

    String key1="k1", key2="k2", key3="k3";
    OracleDocument doc = null;

    if (isJDCSOrATPMode()) 
    {
      doc = col.insertAndGet(db.createDocumentFromString(docStr1));
      key1 = doc.getKey();
      doc = col.insertAndGet(db.createDocumentFromString(docStr2));
      key2 = doc.getKey();
      doc = col.insertAndGet(db.createDocumentFromString(docStr3));
      key3 = doc.getKey();
    } else
    {
      col.insertAndGet(db.createDocumentFromString(key1, docStr1));
      col.insertAndGet(db.createDocumentFromString(key2, docStr2));
      col.insertAndGet(db.createDocumentFromString(key3, docStr3));
    }

    String indexName = "\"orderByIndex" + contentColumnType + "\"";

    if (withIndex)
    {
      col.admin().createIndex(db.createDocumentFromString("{\"name\" :" + indexName + "," +
        "\"indexNulls\" : true," +
        "\"fields\" : [ {\"path\" : \"sfield\", \"datatype\" : \"varchar2\"}," +
        "  {\"path\" : \"nfield\", \"datatype\" : \"number\"}]}"));
    }

    OracleDocument fDoc;

    if (withFilter)
    {
      fDoc = db.createDocumentFromString(
        "{\"$query\" : {\"sfield\" : {\"$gt\" : \"a\"}}," +
          "\"$orderby\" : {\"$fields\" :" +
          "[ {\"path\" : \"sfield\", \"datatype\" : \"varchar2\"}," +
          "  {\"path\" : \"nfield\", \"datatype\" : \"number\"}]}}");
    }
    else
    {
      fDoc = db.createDocumentFromString(
        "{\"$orderby\" : {\"$fields\" :" +
          // Force maxLength to be the same as the maxLength defaulted
          // in the index. This shouldn't be required, and
          // the index used to be picked up without specifying maxLength.
          // Looks like it regressed at some point, bug 28013818 filed.
          // Once bug is fixed, maxLength : 2000 can be deleted.
          "[ {\"path\" : \"sfield\", \"datatype\" : \"varchar2\", \"maxLength\" : 2000}," +
          "  {\"path\" : \"nfield\", \"datatype\" : \"number\"}]}}");
    }

    OracleCursor c = col.find().filter(fDoc).getCursor();

    OracleDocument d;

    if (withFilter) {
      d = c.next();
      assertEquals(d.getKey(), key2);

      d = c.next();
      assertEquals(d.getKey(), key1);

      d = c.next();

      assertEquals(d, null);
    }
    else
    {
      d = c.next();
      assertEquals(d.getKey(), key2);

      d = c.next();
      assertEquals(d.getKey(), key1);

      d = c.next();
      assertEquals(d.getKey(), key3);

      d = c.next();

      assertEquals(d, null);
    }

    if (withIndex)
    {
      String plan = ((OracleOperationBuilderImpl) col.find().filter(fDoc)).
        explainPlan("basic");
      if (withFilter)
      {
        // ### Bug with index pickup on 12.2.0.1
        if (!SODAUtils.sqlSyntaxBelow_18(sqlSyntaxLevel)) {
          assertTrue(plan.contains("INDEX RANGE SCAN"));
        }
        else {
          assertTrue(plan.contains("TABLE ACCESS FULL"));
        }
      }
      else
      {
        assertTrue(plan.contains("INDEX FULL SCAN"));
      }
      col.admin().dropIndex(indexName);
    }
    col.admin().truncate();
  }


  private void testOrderbyScalarReq (String contentColumnType,
                                     boolean withIndex,
                                     boolean withFilter)
    throws Exception
  {
    if (isJDCSOrATPMode())
      if (!contentColumnType.equalsIgnoreCase("BLOB"))
        return;

    OracleDocument mDoc = client.createMetadataBuilder().keyColumnAssignmentMethod("CLIENT")
      .contentColumnType(contentColumnType).build();

    OracleCollection col;
    if (isJDCSOrATPMode())
    {
      col = db.admin().createCollection("orderByCol" + contentColumnType, null);
    } else
    {
      col = db.admin().createCollection("orderByCol" + contentColumnType, mDoc);
    }
    String docStr1 = "{\"sfield\" : \"aac\", \"nfield\" : 21}";
    String docStr2 = "{\"sfield\": \"aaa\", \"nfield\": 20}";
    String docStr3 = "{\"sfield\": \"aab\", \"nfield\" : 20}";
    String key1="k1", key2="k2", key3="k3";
    OracleDocument doc = null;

    if (isJDCSOrATPMode()) 
    {
      doc = col.insertAndGet(db.createDocumentFromString(docStr1));
      key1 = doc.getKey();
      doc = col.insertAndGet(db.createDocumentFromString(docStr2));
      key2 = doc.getKey();
      doc = col.insertAndGet(db.createDocumentFromString(docStr3));
      key3 = doc.getKey();
    } else
    {
      col.insertAndGet(db.createDocumentFromString(key1, docStr1));
      col.insertAndGet(db.createDocumentFromString(key2, docStr2));
      col.insertAndGet(db.createDocumentFromString(key3, docStr3));
    }

    String indexName = "\"orderByIndex" + contentColumnType + "\"";
    if (withIndex)
    {
      col.admin().createIndex(db.createDocumentFromString("{\"name\" :" + indexName + "," +
                                "\"indexNulls\" : true," +
                                "\"scalarRequired\" : true," +
                                "\"fields\" : [ {\"path\" : \"sfield\", \"datatype\" : \"varchar2\"}," +
                                             "  {\"path\" : \"nfield\", \"datatype\" : \"number\"}]}"));
    }

    OracleDocument fDoc;

    if (withFilter)
    {
      fDoc = db.createDocumentFromString(
        "{\"$query\" : {\"sfield\" : {\"$gt\" : \"a\"}}," +
        "\"$orderby\" : {\"$scalarRequired\" : true, \"$fields\" :" +
        "[ {\"path\" : \"sfield\", \"datatype\" : \"varchar2\"}," +
        "  {\"path\" : \"nfield\", \"datatype\" : \"number\"}]}}");
    }
    else {
      fDoc = db.createDocumentFromString(
        "{\"$orderby\" : {\"$scalarRequired\" : true, \"$fields\" :" +
        // Force maxLength to be the same as the maxLength defaulted
        // in the index. This shouldn't be required, and
        // the index used to be picked up without specifying maxLength.
        // Looks like it regressed at some point, bug 28013818 filed.
        // Once bug is fixed, maxLength : 2000 can be deleted.
        "[ {\"path\" : \"sfield\", \"datatype\" : \"varchar2\", \"maxLength\" : 2000}," +
        "  {\"path\" : \"nfield\", \"datatype\" : \"number\"}]}}");
    }

    OracleCursor c = col.find().filter(fDoc).getCursor();

    OracleDocument d;

    d = c.next();
    assertEquals(d.getKey(), key2);

    d = c.next();
    assertEquals(d.getKey(), key3);

    d = c.next();
    assertEquals(d.getKey(), key1);

    d = c.next();

    assertEquals(d, null);

    if (withIndex)
    {
      String plan = ((OracleOperationBuilderImpl) col.find().filter(fDoc)).
                    explainPlan("basic");
      if (withFilter)
      {
        // ### Bug on 12.2.0.1: index doesn't get picked up
        if (!SODAUtils.sqlSyntaxBelow_18(sqlSyntaxLevel))
        {
          assertTrue(plan.contains("INDEX RANGE SCAN"));
        }
        else
        {
          assertTrue(plan.contains("TABLE ACCESS FULL"));
        }
      } else {
        assertTrue(plan.contains("INDEX FULL SCAN"));
      }
      col.admin().dropIndex(indexName);
    }
    col.admin().truncate();
  }


  //////////////////////
  // OrderByScalarReq //
  //////////////////////

  public void testOrderByScalarReqBLOB() throws Exception
  {
    testOrderbyScalarReq("BLOB", false, false);
  }

  public void testOrderByScalarReqBLOBWithIndex() throws Exception
  {
    testOrderbyScalarReq("BLOB", true, false);
  }

  public void testOrderByScalarReqBLOBWithFilter() throws Exception
  {
    testOrderbyScalarReq("BLOB", false, true);
  }

  public void testOrderByScalarReqBLOBWithIndexAndFilter() throws Exception
  {
    testOrderbyScalarReq("BLOB", true, true);
  }
  public void testOrderByScalarReqCLOB() throws Exception
  {
    testOrderbyScalarReq("CLOB", false, false);
  }

  public void testOrderByScalarReqCLOBWithIndex() throws Exception
  {
    testOrderbyScalarReq("CLOB", true, false);
  }

  public void testOrderByScalarReqCLOBWithFilter() throws Exception
  {
    testOrderbyScalarReq("CLOB", false, true);
  }

  public void testOrderByScalarReqCLOBWithIndexAndFilter() throws Exception
  {
    testOrderbyScalarReq("CLOB", true, true);
  }

  public void testOrderByScalarReqVARCHAR2() throws Exception
  {
    testOrderbyScalarReq("VARCHAR2", false, false);
  }

  public void testOrderByScalarReqVARCHAR2WithIndex() throws Exception
  {
    testOrderbyScalarReq("VARCHAR2", true, false);
  }

  public void testOrderByScalarReqVARCHAR2WithFilter() throws Exception
  {
    testOrderbyScalarReq("VARCHAR2", false, true);
  }

  public void testOrderByScalarReqVARCHAR2WithIndexAndFilter() throws Exception
  {
    testOrderbyScalarReq("VARCHAR2", true, true);
  }

  public void testOrderByScalarReqJSON() throws Exception
  {
    if (isCompatibleOrGreater(COMPATIBLE_20)) {
      testOrderbyScalarReq("JSON", false, false);
    }
  }

  public void testOrderByScalarReqJSONWithIndex() throws Exception
  {
    if (isCompatibleOrGreater(COMPATIBLE_20)) {
      testOrderbyScalarReq("JSON", true, false);
    }
  }

  public void testOrderByScalarReqJSONWithFilter() throws Exception
  {
    if (isCompatibleOrGreater(COMPATIBLE_20)) {
      testOrderbyScalarReq("JSON", false, true);
    }
  }

  public void testOrderByScalarReqJSONWithIndexAndFilter() throws Exception
  {
    if (isCompatibleOrGreater(COMPATIBLE_20)) {
      testOrderbyScalarReq("JSON", true, true);
    }
  }

  ////////////////////
  // OrderByDefault //
  ////////////////////

  public void testOrderByDefaultBLOB() throws Exception
  {
    testOrderbyDefault("BLOB", false, false);
  }

  public void testOrderByDefaultBLOBWithIndex() throws Exception
  {
    testOrderbyDefault("BLOB", true, false);
  }

  public void testOrderByDefaultBLOBWithFilter() throws Exception
  {
    testOrderbyDefault("BLOB", false, true);
  }

  public void testOrderByDefaultBLOBWithIndexAndFilter() throws Exception
  {
    testOrderbyDefault("BLOB", true, true);
  }
  public void testOrderByDefaultCLOB() throws Exception
  {
    testOrderbyScalarReq("CLOB", false, false);
  }

  public void testOrderByDefaultCLOBWithIndex() throws Exception
  {
    testOrderbyScalarReq("CLOB", true, false);
  }

  public void testOrderByDefaultCLOB2WithFilter() throws Exception
  {
    testOrderbyDefault("CLOB", false, true);
  }

  public void testOrderByDefaultCLOBWithIndexAndFilter() throws Exception
  {
    testOrderbyDefault("CLOB", true, true);
  }

  public void testOrderByDefaultVARCHAR2() throws Exception
  {
    testOrderbyDefault("VARCHAR2", false, false);
  }

  public void testOrderByDefaultVARCHAR2WithIndex() throws Exception
  {
    testOrderbyDefault("VARCHAR2", true, false);
  }

  public void testOrderByDefaultVARCHAR2WithFilter() throws Exception
  {
    testOrderbyDefault("VARCHAR2", false, true);
  }

  public void testOrderByDefaultVARCHAR2WithIndexAndFilter() throws Exception
  {
    testOrderbyDefault("VARCHAR2", true, true);
  }

  public void testOrderByDefaultJSON() throws Exception
  {
    if (isCompatibleOrGreater(COMPATIBLE_20)) {
      testOrderbyDefault("JSON", false, false);
    }
  }

  public void testOrderByDefaultJSONWithIndex() throws Exception
  {
    if (isCompatibleOrGreater(COMPATIBLE_20)) {
      testOrderbyDefault("JSON", true, false);
    }
  }

  public void testOrderByDefaultJSONWithFilter() throws Exception
  {
    if (isCompatibleOrGreater(COMPATIBLE_20)) {
      testOrderbyDefault("JSON", false, true);
    }
  }

  public void testOrderByDefaultJSONWithIndexAndFilter() throws Exception
  {
    if (isCompatibleOrGreater(COMPATIBLE_20)) {
      testOrderbyDefault("JSON", true, true);
    }
  }

  ///////////////////////
  // OrderByDefaultNeg //
  ///////////////////////

  public void testOrderByDefaultNegBLOB() throws Exception
  {
    testOrderbyDefaultNeg("BLOB", false, false);
  }

  public void testOrderByDefaultNegBLOBWithIndex() throws Exception
  {
    testOrderbyDefaultNeg("BLOB", true, false);
  }

  public void testOrderByDefaultNegBLOBWithFilter() throws Exception
  {
    testOrderbyDefaultNeg("BLOB", false, true);
  }

  public void testOrderByDefaultNegBLOBWithIndexAndFilter() throws Exception
  {
    testOrderbyDefaultNeg("BLOB", true, true);
  }
  public void testOrderByDefaultNegCLOB() throws Exception
  {
    testOrderbyDefaultNeg("CLOB", false, false);
  }

  public void testOrderByDefaultNegCLOBWithIndex() throws Exception
  {
    testOrderbyDefaultNeg("CLOB", true, false);
  }

  public void testOrderByDefaultNeg2WithFilter() throws Exception
  {
    testOrderbyDefaultNeg("CLOB", false, true);
  }

  public void testOrderByDefaultNegCLOBWithIndexAndFilter() throws Exception
  {
    testOrderbyDefaultNeg("CLOB", true, true);
  }

  public void testOrderByDefaultNegVARCHAR2() throws Exception
  {
    testOrderbyDefaultNeg("VARCHAR2", false, false);
  }

  public void testOrderByDefaultNegVARCHAR2WithIndex() throws Exception
  {
    testOrderbyDefaultNeg("VARCHAR2", true, false);
  }

  public void testOrderByDefaultNegVARCHAR2WithFilter() throws Exception
  {
    testOrderbyDefaultNeg("VARCHAR2", false, true);
  }

  public void testOrderByDefaultNegVARCHAR2WithIndexAndFilter() throws Exception
  {
    testOrderbyDefaultNeg("VARCHAR2", true, true);
  }

  public void testOrderByDefaultNegJSON() throws Exception
  {
    if (isCompatibleOrGreater(COMPATIBLE_20)) {
      testOrderbyDefaultNeg("JSON", false, false);
    }
  }

  public void testOrderByDefaultNegJSONWithIndex() throws Exception
  {
    if (isCompatibleOrGreater(COMPATIBLE_20)) {
      testOrderbyDefaultNeg("JSON", true, false);
    }
  }

  public void testOrderByDefaultNegJSONWithFilter() throws Exception
  {
    if (isCompatibleOrGreater(COMPATIBLE_20)) {
      testOrderbyDefaultNeg("JSON", false, true);
    }
  }

  public void testOrderByDefaultNegJSONWithIndexAndFilter() throws Exception
  {
    if (isCompatibleOrGreater(COMPATIBLE_20)) {
      testOrderbyDefaultNeg("JSON", true, true);
    }
  }

  ////////////////
  // OrderByLax //
  ////////////////

  public void testOrderByLaxLOB() throws Exception
  {
    testOrderbyLax("BLOB", false, false);
  }

  public void testOrderByLaxtBLOBWithIndex() throws Exception
  {
    testOrderbyLax("BLOB", true, false);
  }

  public void testOrderByLaxBLOBWithFilter() throws Exception
  {
    testOrderbyLax("BLOB", false, true);
  }

  public void testOrderByLaxBLOBWithIndexAndFilter() throws Exception
  {
    testOrderbyLax("BLOB", true, true);
  }
  public void testOrderByLaxCLOB() throws Exception
  {
    testOrderbyLax("CLOB", false, false);
  }

  public void testOrderByLaxCLOBWithIndex() throws Exception
  {
    testOrderbyLax("CLOB", true, false);
  }

  public void testOrderByLaxCLOB2WithFilter() throws Exception
  {
    testOrderbyLax("CLOB", false, true);
  }

  public void testOrderByLaxWithIndexAndFilter() throws Exception
  {
    testOrderbyLax("CLOB", true, true);
  }

  public void testOrderByLaxVARCHAR2() throws Exception
  {
    testOrderbyLax("VARCHAR2", false, false);
  }

  public void testOrderByLaxVARCHAR2WithIndex() throws Exception
  {
    testOrderbyLax("VARCHAR2", true, false);
  }

  public void testOrderByLaxVARCHAR2WithFilter() throws Exception
  {
    testOrderbyLax("VARCHAR2", false, true);
  }

  public void testOrderByLaxVARCHAR2WithIndexAndFilter() throws Exception
  {
    testOrderbyLax("VARCHAR2", true, true);
  }

  public void testOrderByLaxJSON() throws Exception
  {
    if (isCompatibleOrGreater(COMPATIBLE_20)) {
      testOrderbyLax("JSON", false, false);
    }
  }

  public void testOrderByLaxtJSONWithIndex() throws Exception
  {
    if (isCompatibleOrGreater(COMPATIBLE_20)) {
      testOrderbyLax("JSON", true, false);
    }
  }

  public void testOrderByLaxJSONWithFilter() throws Exception
  {
    if (isCompatibleOrGreater(COMPATIBLE_20)) {
      testOrderbyLax("JSON", false, true);
    }
  }

  public void testOrderByLaxJSONWithIndexAndFilter() throws Exception
  {
    if (isCompatibleOrGreater(COMPATIBLE_20)) {
      testOrderbyLax("JSON", true, true);
    }
  }

  public void testOrderBySimplify() throws Exception {
    OracleCollection col;
    col = db.admin().createCollection("test", null);
    OracleCursor cursor = null;
    col.insert((db.createDocumentFromString(new String("{\"name\" : \"Jason\", \"age\" : 45}"))));
    col.insert((db.createDocumentFromString(new String("{\"name\" : \"Eric\", \"age\" : 35}"))));
    col.insert((db.createDocumentFromString(new String("{\"name\" : \"debby\", \"age\" : 47}"))));

    try{
      cursor = col.find().filter("{\"$orderby\" : {\"_id\" : 1 , \"name\" : 1}}").getCursor();

      while(cursor.hasNext())
        cursor.next();

      assertEquals(3,col.find().count());
    }
    catch (Exception e){
      fail("No exception should have occured");
    }
    finally {
      cursor.close();
    }

  }

  public void testBindTypedParameterStrict() throws Exception {
    OracleCollection col;
    col = db.admin().createCollection("test", null);
    OracleCursor cursor = null;
    int iteratorCount = 0;
    
    try {

      OracleJsonFactory factory = new OracleJsonFactory();
      OracleJsonObject oracleJsonObject = factory.createObject();

      //CASE : INTERVALDS
      Instant start = Instant.parse("2017-10-03T10:15:30.00Z");
      Instant end = Instant.parse("2017-10-03T10:16:30.00Z");
      Duration duration = Duration.between(start, end);
      OracleJsonIntervalDS val1 = factory.createIntervalDS(duration);
      oracleJsonObject.put("created",val1);
      OracleDocument doc1 = db.createDocumentFrom(oracleJsonObject);
      col.insert(doc1);
      cursor = col.find().filter(doc1).getCursor();
      while(cursor.hasNext())
      {
        iteratorCount++;
        cursor.next();
      }
      assertEquals(1,iteratorCount);
      iteratorCount = 0;

      //CASE : ITERVALYM
      Period period = Period.ofMonths(6);
      OracleJsonIntervalYM val2 = factory.createIntervalYM(period);
      oracleJsonObject.put("created",val2);
      OracleDocument doc2 = db.createDocumentFrom(oracleJsonObject);
      col.insert(doc2);
      cursor = col.find().filter(doc2).getCursor();
      while(cursor.hasNext())
      {
        iteratorCount++;
        cursor.next();
      }
      assertEquals(1,iteratorCount);
      iteratorCount = 0;

      //CASE : DATE
      LocalDateTime now = LocalDateTime.now();
      OracleJsonDate val3 = factory.createDate(now);
      oracleJsonObject.put("created",val3);
      OracleDocument doc3 = db.createDocumentFrom(oracleJsonObject);
      col.insert(doc3);
      cursor = col.find().filter(doc3).getCursor();
      while(cursor.hasNext())
      {
        iteratorCount++;
        cursor.next();
      }
      assertEquals(1,iteratorCount);
      iteratorCount = 0;

      //CASE : TIMESTAMP
      OracleJsonTimestamp val4 = factory.createTimestamp(now);
      oracleJsonObject.put("created",val4);
      OracleDocument doc4 = db.createDocumentFrom(oracleJsonObject);
      col.insert(doc4);
      cursor = col.find().filter(doc4).getCursor();
      while(cursor.hasNext())
      {
        iteratorCount++;
        cursor.next();
      }
      assertEquals(1,iteratorCount);
      iteratorCount = 0;

      //CASE : DECIMAL
      long i = 12345678910L;
      OracleJsonDecimal val5 = factory.createDecimal(i);
      oracleJsonObject.put("created",val5);
      OracleDocument doc5 = db.createDocumentFrom(oracleJsonObject);
      col.insert(doc5);
      cursor = col.find().filter(doc5).getCursor();
      while(cursor.hasNext())
      {
        iteratorCount++;
        cursor.next();
      }
      assertEquals(1,iteratorCount);
      iteratorCount = 0;

      //CASE : STRING
      OracleJsonString val6 = factory.createString("abc");
      oracleJsonObject.put("created",val6);
      OracleDocument doc6 = db.createDocumentFrom(oracleJsonObject);
      col.insert(doc6);
      cursor = col.find().filter(doc6).getCursor();
      while(cursor.hasNext())
      {
        iteratorCount++;
        cursor.next();
      }
      assertEquals(1,iteratorCount);
      iteratorCount = 0;

      //CASE : BINARY
      byte[] bArr = {45};
      OracleJsonBinary val7 = factory.createBinary(bArr);
      oracleJsonObject.put("created",val7);
      OracleDocument doc7 = db.createDocumentFrom(oracleJsonObject);
      col.insert(doc7);
      cursor = col.find().filter(doc7).getCursor();
      while(cursor.hasNext())
      {
        iteratorCount++;
        cursor.next();
      }
      assertEquals(1,iteratorCount);
      iteratorCount = 0;

      //CASE : DOUBLE/FLOAT
      OracleJsonDouble val8 = factory.createDouble(2.0);
      oracleJsonObject.put("created",val8);
      OracleDocument doc8 = db.createDocumentFrom(oracleJsonObject);
      col.insert(doc8);
      cursor = col.find().filter(doc8).getCursor();
      while(cursor.hasNext())
      {
        iteratorCount++;
        cursor.next();
      }
      assertEquals(1,iteratorCount);
      iteratorCount = 0;

      /* ### TODO
      CASE : TIMESTAMPTZ (needs to be fixed in db)
      OffsetDateTime time = OffsetDateTime.now(ZoneOffset.UTC);
      OracleJsonTimestampTZ val9 = factory.createTimestampTZ(time);
      oracleJsonObject.put("created",val9);
      OracleDocument doc9 = db.createDocumentFrom(oracleJsonObject);
      col.insert(doc9);
      cursor = col.find().filter(doc9).getCursor();
      while(cursor.hasNext())
      {
        iteratorCount++;
        cursor.next();
      }
      assertEquals(1,iteratorCount);
      */
    }
    catch (Exception e){
      fail("No exception should have occured");
    }
    finally {
      if(cursor != null)
        cursor.close();
    }
  }
}
