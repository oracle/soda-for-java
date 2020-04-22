/* Copyright (c) 2017, 2020, Oracle and/or its affiliates. 
All rights reserved.*/

/*
   DESCRIPTION
    The tests for modifiers operators in filter spec
    The tested operators include: $between, $instr, $like, $hasSubstring, $boolean, $timestamp, 
        $number, $string, $double, $upper, $lower, $ceiling, $floor, $abs, $type, $length, $size
 */

/**
 *  @author  Vincent Liu
 */

package oracle.json.tests.soda;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.io.File;
import java.io.FileInputStream;
import java.sql.SQLException;

import oracle.soda.OracleCollection;
import oracle.soda.OracleCursor;
import oracle.soda.OracleDocument;
import oracle.soda.OracleException;
import oracle.soda.OracleOperationBuilder;
import oracle.soda.rdbms.impl.OracleOperationBuilderImpl;
import oracle.soda.rdbms.impl.WriteResult;
import oracle.json.parser.QueryException;

import oracle.json.testharness.SodaTestCase;
import oracle.json.testharness.SodaTestCase.IndexType;
import oracle.soda.rdbms.impl.SODAUtils;

public class test_OracleOperationBuilder5 extends SodaTestCase {
  
  private void checkKeys(OracleCollection col, OracleDocument filterDoc,
    HashSet<String> expectedKeys) throws Exception {
    OracleCursor c = col.find().filter(filterDoc).getCursor();
    HashSet<String> keys = new HashSet<String>();

    while (c.hasNext())
      keys.add(c.next().getKey());

    c.close();
    assertEquals(keys, expectedKeys);
  }
  
  // Tests with $between
  public void testBetweenClob() throws Exception {
    testBetween("CLOB", false);
    testBetween("CLOB", true);
  }
  
  public void testBetweenBlob() throws Exception {
    testBetween("BLOB", false);
    testBetween("BLOB", true);
  }
  
  public void testBetweenVarchar2() throws Exception {
    testBetween("VARCHAR2", false);
    testBetween("VARCHAR2", true);
  }
  
  private void testBetween(String contentColumnType, boolean withIndex) throws Exception {
    if (isJDCSMode())
      if (!contentColumnType.equalsIgnoreCase("BLOB"))
        return;

    if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel))
      return;
    
    OracleDocument mDoc = client.createMetadataBuilder().keyColumnAssignmentMethod("CLIENT")
      .contentColumnType(contentColumnType).build();
    
    String colName = "testBetween" + contentColumnType + (withIndex?"Idx":"");
    OracleCollection col;
    if (isJDCSMode())
    {
      col = db.admin().createCollection(colName, null);
    } else
    {
      col = db.admin().createCollection(colName, mDoc);
    }
    
    String indexSpec, indexName = "searchIndexOnTestBetween" + contentColumnType;
    IndexType indexType = IndexType.noIndex;
    if (withIndex) {
      // test with "search_on"="text"
      indexSpec = "{\"name\" : \"" + indexName + "\","
          + "\"search_on\" : \"text\","
          + "\"dataguide\" : \"on\"}";
      col.admin().createIndex(db.createDocumentFromString(indexSpec));
      indexType = IndexType.textIndex;
    }
      
    String key1="id00a", key2="id00b", key3="id00c";
    OracleDocument doc = null, filterDoc = null;
    HashSet<String> expectedKeys = new HashSet<String>();
    String docStr1 = "{\"a\":{\"b\":{\"number\":101, \"str\":\"a001a\", \"double\":3.141," +
        "\"time\":\"2017-03-22T09:43:00\", \"number1\":-3.5, \"array\":[1]}}}";
    String docStr2 = "{\"a\":{\"b\":{\"number\":111, \"str\":\"A002\", \"double\":3.1427," +
        "\"time\":\"2017-03-22T09:43:07.089112Z\", \"number1\":4, \"array\":[1,2,3]}}}";
    String docStr3 = "{\"a\":{\"b\":{\"number\":121, \"str\":\"a03\", \"double\":4.1405," +
        "\"time\":\"2017-03-22T12:43:07.076112Z\", \"number1\":-5, \"array\":[1,2,3,4,5]}}}";

    if (isJDCSMode()) 
    {
      doc = col.insertAndGet(db.createDocumentFromString(docStr1));
      key1 = doc.getKey();
      doc = col.insertAndGet(db.createDocumentFromString(docStr2));
      key2 = doc.getKey();
      doc = col.insertAndGet(db.createDocumentFromString(docStr3));
      key3 = doc.getKey();
    } else
    {
      doc = col.insertAndGet(db.createDocumentFromString(key1, docStr1));
      doc = col.insertAndGet(db.createDocumentFromString(key2, docStr2));
      doc = col.insertAndGet(db.createDocumentFromString(key3, docStr3));
    }
    
    // Test $between with number value    
    filterDoc = db.createDocumentFromString("{\"a.b.number\":{\"$between\": [101,110]}}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key1, col.find().filter(filterDoc).getOne().getKey());
    chkExplainPlan(col.find().filter(filterDoc), indexType, indexName);
    
    filterDoc = db.createDocumentFromString("{\"a.b.number\":{\"$between\": [101,121]}}");
    assertEquals(3, col.find().filter(filterDoc).count());
    chkExplainPlan(col.find().filter(filterDoc), indexType, indexName);
    
    // Test $between with string value
    filterDoc = db.createDocumentFromString("{\"a.b.str\":{\"$between\": [\"a001\", \"a002\"]}}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key1, col.find().filter(filterDoc).getOne().getKey());
    chkExplainPlan(col.find().filter(filterDoc), indexType, indexName);
    
    // Test $between with $double
    filterDoc = db.createDocumentFromString("{\"a.b.double\": {\"$double\": " +
        "{\"$between\": [3.1410, 3.1427]} }}");
    assertEquals(2, col.find().filter(filterDoc).count());
    expectedKeys.clear();
    expectedKeys.add(key1);
    expectedKeys.add(key2);
    checkKeys(col, filterDoc, expectedKeys);
    chkExplainPlan(col.find().filter(filterDoc), indexType, indexName);
    
    // Test $between with timestamp value
    filterDoc = db.createDocumentFromString("{\"a.b.time\": {\"$timestamp\": " +
        "{\"$between\": [\"2017-03-22T09:43:07.000000Z\", \"2017-03-22T12:43:07.066666Z\"]}} }");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key2, col.find().filter(filterDoc).getOne().getKey());
    chkExplainPlan(col.find().filter(filterDoc), indexType, indexName);
    
    // when the modifier operator is enabled or not, 
    // the same bounds pair could produce the different result.
    filterDoc = db.createDocumentFromString("{\"a.b.time\":" +
        "{\"$between\": [\"2017-03-22T09:43:00.000000Z\", \"2017-03-22T12:50:00.000000Z\"]} }");
    assertEquals(2, col.find().filter(filterDoc).count());
    chkExplainPlan(col.find().filter(filterDoc), indexType, indexName);
    
    filterDoc = db.createDocumentFromString("{\"a.b.time\": {\"$timestamp\": " +
        "{\"$between\": [\"2017-03-22T09:43:00.000000Z\", \"2017-03-22T12:50:00.000000Z\"]}} }");
    assertEquals(3, col.find().filter(filterDoc).count());   
    chkExplainPlan(col.find().filter(filterDoc), indexType, indexName);
    
    // Test $between with $number
    filterDoc = db.createDocumentFromString("{\"a.b.double\": {\"$number\": " +
        "{\"$between\": [3, 3.2]}} }");
    assertEquals(2, col.find().filter(filterDoc).count());
    expectedKeys.clear();
    expectedKeys.add(key1);
    expectedKeys.add(key2);
    checkKeys(col, filterDoc, expectedKeys);
    chkExplainPlan(col.find().filter(filterDoc), indexType, indexName);
    
    // Test $between with $string
    filterDoc = db.createDocumentFromString("{\"a.b.number\": {\"$string\": " +
        "{\"$between\": [\"000\", \"110\"]}} }");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key1, col.find().filter(filterDoc).getOne().getKey());
    chkExplainPlan(col.find().filter(filterDoc), indexType, indexName);
    
    // Test $between with $ceiling
    // those those trailing functions(e.g. ceiling, floor, abs, length will use index)
    filterDoc = db.createDocumentFromString("{\"a.b.double\": {\"$ceiling\": " +
        "{\"$between\": [4, 4]}} }");
    assertEquals(2, col.find().filter(filterDoc).count());
    expectedKeys.clear();
    expectedKeys.add(key1);
    expectedKeys.add(key2);
    checkKeys(col, filterDoc, expectedKeys);
    
    // Test $between with $floor
    filterDoc = db.createDocumentFromString("{\"a.b.double\": {\"$floor\": " +
        "{\"$between\": [4, 4]}} }");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key3, col.find().filter(filterDoc).getOne().getKey());

    // ### Bug: abs does not work on 12.2.0.1 (wrong result)
    if (!SODAUtils.sqlSyntaxBelow_18(sqlSyntaxLevel)) {
      // Test $between with $abs
      filterDoc = db.createDocumentFromString("{\"a.b.number1\": {\"$abs\": " +
              "{\"$between\": [3, 5]}} }");
      assertEquals(3, col.find().filter(filterDoc).count());
    }
    
    // Test $between with $length
    filterDoc = db.createDocumentFromString("{\"a.b.str\": {\"$length\": " +
        "{\"$between\": [3, 4]}} }");
    assertEquals(2, col.find().filter(filterDoc).count());
    expectedKeys.clear();
    expectedKeys.add(key2);
    expectedKeys.add(key3);
    checkKeys(col, filterDoc, expectedKeys);
    
    // Test $between with $size
    filterDoc = db.createDocumentFromString("{\"a.b.array\": {\"$size\": " +
        "{\"$between\": [1,1]}} }");
    // ### Bug on 12.2.0.1: else branch shows the incorrect
    // behavior
    if (!SODAUtils.sqlSyntaxBelow_18(sqlSyntaxLevel)) {
      assertEquals(1, col.find().filter(filterDoc).count());
      expectedKeys.clear();
      expectedKeys.add(key1);
      checkKeys(col, filterDoc, expectedKeys);
    }
    else {
      assertEquals(3, col.find().filter(filterDoc).count());
    }
    
    // Test $between with $and
    filterDoc = db.createDocumentFromString("{ \"$and\" : [ " +
            "{\"a.b.array[1]\":{\"$between\": [1,2]}}," +
            "{\"a.b.number\":{\"$between\": [101,120]}} ]}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key2, col.find().filter(filterDoc).getOne().getKey());
    chkExplainPlan(col.find().filter(filterDoc), indexType, indexName);
    
    // Test $between with $or
    filterDoc = db.createDocumentFromString("{ \"$or\" : [ " +
        "{\"a.b.str\":{\"$between\": [\"a002\",\"b00\"]}}," +
        "{\"a.b.number1\":{\"$between\": [-4, 0]}} ]}");
    assertEquals(2, col.find().filter(filterDoc).count());
    expectedKeys.clear();
    expectedKeys.add(key1);
    expectedKeys.add(key3);
    checkKeys(col, filterDoc, expectedKeys);
    chkExplainPlan(col.find().filter(filterDoc), indexType, indexName);
    
    // Test $between with $not and $and
    filterDoc = db.createDocumentFromString("{ \"$and\" : [ " +
        // select doc1 and doc2
        "{\"a.b.str\": {\"$not\":{\"$between\": [\"a002\",\"b00\"]}}}," +
        // select doc2 and doc3
        "{\"a.b.double\":{\"$between\": [3.1415, 5]}} ]}");
    // blocked by bug25770318: NullPointerException when "$not" is joined with $between
    // assertEquals(1, col.find().filter(filterDoc).count());
    // assertEquals(key2, col.find().filter(filterDoc).getOne().getKey());
    
    // Test $between with $nor
    filterDoc = db.createDocumentFromString("{ \"$nor\" : [ " +
        // select doc3
        "{\"a.b.str\": {\"$between\": [\"a002\",\"b00\"]}}," +
        // select doc2
        "{\"a.b.double\":{\"$between\": [3.1415, 4]}} ] }");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key1, col.find().filter(filterDoc).getOne().getKey());
    
    // Test $between with object value
    filterDoc = db.createDocumentFromString("{\"a.b.number\":{\"$between\": {\"value\":[100,101]}}}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $between value is object");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      assertEquals("Expected array for logical operator $between.", queryException.getMessage());
    }
    
    // Test $between with number value
    filterDoc = db.createDocumentFromString("{\"a.b.number\":{\"$between\": 100}}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $between value is number");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      assertEquals("Expected array for logical operator $between.", queryException.getMessage());
    }
    
    // Test $between with string value
    filterDoc = db.createDocumentFromString("{\"a.b.number\":{\"$between\": \"100\"}}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $between value is string");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      assertEquals("Expected array for logical operator $between.", queryException.getMessage());
    }
    
    // Test $between with boolean value
    filterDoc = db.createDocumentFromString("{\"a.b.number\":{\"$between\": true}}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $between value is boolean");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      assertEquals("Expected array for logical operator $between.", queryException.getMessage());
    }
    
    // Test $between with null 
    filterDoc = db.createDocumentFromString("{\"a.b.number\":{\"$between\": null}}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $between value is null");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      assertEquals("Expected array for logical operator $between.", queryException.getMessage());
    }
    
    // Test $between with array having only one item
    filterDoc = db.createDocumentFromString("{\"a.b.number\":{\"$between\": [101]}}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when the array in $between value has only one element");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      String expMsg = "The argument to $between must be an array of two scalars, one of which must not be null.";
      assertEquals(expMsg, queryException.getMessage());
    }
    
    // Test $between with array having 2+ items
    filterDoc = db.createDocumentFromString("{\"a.b.number\":{\"$between\": [101, 102, 103]}}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when the array in $between value has 3 elements");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      String expMsg = "The argument to $between must be an array of two scalars, one of which must not be null.";
      assertEquals(expMsg, queryException.getMessage());
    }

    if (withIndex) {
      col.admin().dropIndex(indexName);
    }
  }
  
  // Tests with $instr and $hasSubstring.(both 2 does not use index)
  public void testInstrClob() throws Exception {
    testInstr("CLOB");
  }
  
  public void testInstrBlob() throws Exception {
    testInstr("BLOB");
  }
  
  public void testInstrVarchar2() throws Exception {
    testInstr("VARCHAR2");
  }
  
  // Tests for $instr and $hasSubstring
  private void testInstr(String contentColumnType) throws Exception {
    if (isJDCSMode())
      if (!contentColumnType.equalsIgnoreCase("BLOB"))
        return;

    if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel))
      return;
    
    OracleDocument mDoc = client.createMetadataBuilder().keyColumnAssignmentMethod("CLIENT")
      .contentColumnType(contentColumnType).build();
    
    String colName = "testInstr" + contentColumnType;
    OracleCollection col;
    if (isJDCSMode())
    {
      col = db.admin().createCollection(colName, null);
    } else
    {
      col = db.admin().createCollection(colName, mDoc);
    }
      
    String key1="id00a", key2="id00b", key3="id00c";
    OracleDocument doc = null, filterDoc = null;
    HashSet<String> expectedKeys = new HashSet<String>();
    String docStr1 = "{\"a\":{\"b\":{\"number\":100, \"str1\":\"You Are Welcome\", \"str2\":\"A001a\"}}}";
    String docStr2 = "{\"a\":{\"b\":{\"number\":101, \"str1\":\"you are welcome \", \"str2\":\"a001A\"}}}";
    String docStr3 = "{\"a\":{\"b\":{\"boolean\":true, \"str1\":\"WELCOME!!!\", \"str2\":\"a001a\"}}}";

    if (isJDCSMode()) 
    {
      doc = col.insertAndGet(db.createDocumentFromString(docStr1));
      key1 = doc.getKey();
      doc = col.insertAndGet(db.createDocumentFromString(docStr2));
      key2 = doc.getKey();
      doc = col.insertAndGet(db.createDocumentFromString(docStr3));
      key3 = doc.getKey();
    } else
    {
      doc = col.insertAndGet(db.createDocumentFromString(key1, docStr1));
      doc = col.insertAndGet(db.createDocumentFromString(key2, docStr2));
      doc = col.insertAndGet(db.createDocumentFromString(key3, docStr3));
    }
    
    // Test $instr with string value    
    filterDoc = db.createDocumentFromString("{\"a.b.str1\":{\"$instr\":\"welcome\"}}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key2, col.find().filter(filterDoc).getOne().getKey());
    
    filterDoc = db.createDocumentFromString("{\"a.b.str1\":{\"$instr\":\"Are \"}}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key1, col.find().filter(filterDoc).getOne().getKey());
    
    // Test $instr with $string
    filterDoc = db.createDocumentFromString("{\"a.b.number\": {\"$string\": " + "{\"$instr\": \"100\"}} }");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key1, col.find().filter(filterDoc).getOne().getKey());
    
    // Test $instr with $upper
    filterDoc = db.createDocumentFromString("{\"a.b.str1\": {\"$upper\": " + "{\"$instr\": \"YOU\"}} }");
    assertEquals(2, col.find().filter(filterDoc).count());
    expectedKeys.clear();
    expectedKeys.add(key1);
    expectedKeys.add(key2);
    checkKeys(col, filterDoc, expectedKeys);
    
    // Test $instr with $lower
    filterDoc = db.createDocumentFromString("{\"a.b.str1\": {\"$lower\": " + "{\"$instr\": \"welcome!!!\"}} }");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key3, col.find().filter(filterDoc).getOne().getKey());
    
    // Test $hasSubstring with string value
    filterDoc = db.createDocumentFromString("{\"a.b.str2\":{\"$hasSubstring\":\"A001a\"}}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key1, col.find().filter(filterDoc).getOne().getKey());
    
    filterDoc = db.createDocumentFromString("{\"a.b.str2\":{\"$hasSubstring\":\"A00\"}}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key1, col.find().filter(filterDoc).getOne().getKey());
    
    // Test $hasSubstring with $string
    filterDoc = db.createDocumentFromString("{\"a.b.number\": {\"$string\": {\"$hasSubstring\": \"101\"}} }");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key2, col.find().filter(filterDoc).getOne().getKey());
    
    // Test $hasSubstring with $upper
    filterDoc = db.createDocumentFromString("{\"a.b.str2\": {\"$upper\": {\"$hasSubstring\": \"A001A\"}} }");
    assertEquals(3, col.find().filter(filterDoc).count());
    
    // Test $hasSubstring with $lower.
    filterDoc = db.createDocumentFromString("{\"a.b.str2\": {\"$lower\": {\"$hasSubstring\": \"A001A\"}} }");
    assertEquals(0, col.find().filter(filterDoc).count());
    
    // Test $instr and $hasSubstring with $and
    filterDoc = db.createDocumentFromString("{ \"$and\" : [ " +
        "{\"a.b.str1\": {\"$lower\":{\"$instr\":\"you are \"}} }," +
        "{\"a.b.str2\":{\"$hasSubstring\": \"a001\"}} ]}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key2, col.find().filter(filterDoc).getOne().getKey());
    
    // Test $instr and $hasSubstring with $or
    filterDoc = db.createDocumentFromString("{ \"$or\" : [ " +
        "{\"a.b.str1\":{\"$instr\":\"you are Welcome\"} }," +
        "{\"a.b.str2\":{\"$hasSubstring\": \"a001a\"}} ]}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key3, col.find().filter(filterDoc).getOne().getKey());
    
    // Test $instr with $not
    filterDoc = db.createDocumentFromString("{\"a.b.str2\": {\"$not\": {\"$instr\": \"001\"}} }");
    assertEquals(0, col.find().filter(filterDoc).count());
    
    // Test $hasSubstring with $not
    filterDoc = db.createDocumentFromString("{\"a.b.str1\": {\"$not\": {\"$hasSubstring\": \"are\"}} }");
    assertEquals(2, col.find().filter(filterDoc).count());
    expectedKeys.clear();
    expectedKeys.add(key1);
    expectedKeys.add(key3);
    checkKeys(col, filterDoc, expectedKeys);
    
    // Test $instr and $hasSubstring with $nor
    filterDoc = db.createDocumentFromString("{ \"$nor\" : [ " +
        "{\"a.b.str1\":{\"$instr\":\"you are\"} }," +
        "{\"a.b.str2\":{\"$hasSubstring\": \"A001\"}} ]}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key3, col.find().filter(filterDoc).getOne().getKey());
    

    // Test $instr with number value
    filterDoc = db.createDocumentFromString("{\"a.b.str1\":{\"$instr\": 101}}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $instr value is a number");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      assertEquals("The operand for $instr must be a string.", queryException.getMessage());
    }
    
    // Test $instr with null
    filterDoc = db.createDocumentFromString("{\"a.b.str1\":{\"$instr\": null}}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $instr value is null");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      assertEquals("The operand for $instr must be a string.", queryException.getMessage());
    }
    
    // Test $hasSubstring with boolean value
    filterDoc = db.createDocumentFromString("{\"a.b.str1\":{\"$hasSubstring\": true}}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $hasSubstring value is non-string");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      assertEquals("The operand for $hasSubstring must be a string.", queryException.getMessage());
    }

    // Test $hasSubstring with object value
    filterDoc = db.createDocumentFromString("{\"a.b.str1\":{\"$hasSubstring\": {\"key\":100}}}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $hasSubstring value is an object");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      String expMsg = "A container object cannot be the operand for the $hasSubstring operator.";
      assertEquals(expMsg, queryException.getMessage());
    }
    
    // Test $hasSubstring with array value
    filterDoc = db.createDocumentFromString("{\"a.b.str1\":{\"$hasSubstring\": [1,2,3]}}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $hasSubstring value is an array");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      String expMsg = "A container array cannot be the operand for the $hasSubstring operator.";
      assertEquals(expMsg, queryException.getMessage());
    }
    
  }
  
  // Tests with $like. ($like operator does not use index)
  public void testLikeClob() throws Exception {
    testLike("CLOB");
  }
  
  public void testLikeBlob() throws Exception {
    testLike("BLOB");
  }
  
  public void testLikeVarchar2() throws Exception {
    testLike("VARCHAR2");
  }
  
  // Tests for $like
  private void testLike(String contentColumnType) throws Exception {
    if (isJDCSMode())
      if (!contentColumnType.equalsIgnoreCase("BLOB"))
        return;

    if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel))
      return;
    
    OracleDocument mDoc = client.createMetadataBuilder().keyColumnAssignmentMethod("CLIENT")
      .contentColumnType(contentColumnType).build();
    
    String colName = "testLike" + contentColumnType;
    OracleCollection col;
    if (isJDCSMode())
    {
      col = db.admin().createCollection(colName, null);
    } else
    {
      col = db.admin().createCollection(colName, mDoc);
    }
      
    String key1="id001", key2="id002", key3="id003";
    OracleDocument doc = null, filterDoc = null;
    HashSet<String> expectedKeys = new HashSet<String>();
    String docStr1 = "{\"a\":{\"b\":{\"number\":100, \"str\":\"you are welcome\"}}}";
    String docStr2 = "{\"a\":{\"b\":{\"number\":101, \"str\":\"You Are Welcome\"}}}";
    String docStr3 = "{\"a\":{\"b\":{\"boolean\":true, \"str\":\"are \"}}}";

    if (isJDCSMode()) 
    {
      doc = col.insertAndGet(db.createDocumentFromString(docStr1));
      key1 = doc.getKey();
      doc = col.insertAndGet(db.createDocumentFromString(docStr2));
      key2 = doc.getKey();
      doc = col.insertAndGet(db.createDocumentFromString(docStr3));
      key3 = doc.getKey();
    } else
    {
      doc = col.insertAndGet(db.createDocumentFromString(key1, docStr1));
      doc = col.insertAndGet(db.createDocumentFromString(key2, docStr2));
      doc = col.insertAndGet(db.createDocumentFromString(key3, docStr3));
    }
    
    // Test $like with string value    
    filterDoc = db.createDocumentFromString("{\"a.b.str\":{\"$like\":\"are%\"}}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key3, col.find().filter(filterDoc).getOne().getKey());
    
    filterDoc = db.createDocumentFromString("{\"a.b.str\":{\"$like\":\"%welcome\"}}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key1, col.find().filter(filterDoc).getOne().getKey());
    
    filterDoc = db.createDocumentFromString("{\"a.b.str\":{\"$like\":\"%are%\"}}");
    assertEquals(2, col.find().filter(filterDoc).count());
    expectedKeys.clear();
    expectedKeys.add(key1);
    expectedKeys.add(key3);
    checkKeys(col, filterDoc, expectedKeys);
    
    // Test $like with $string
    filterDoc = db.createDocumentFromString("{\"a.b.number\": {\"$string\": {\"$like\": \"%01\"}} }");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key2, col.find().filter(filterDoc).getOne().getKey());
    
    filterDoc = db.createDocumentFromString("{\"a.b.boolean\": {\"$string\": {\"$like\": \"true\"}} }");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key3, col.find().filter(filterDoc).getOne().getKey());
    
    // Test $like with $upper
    filterDoc = db.createDocumentFromString("{\"a.b.str\": {\"$upper\": {\"$like\": \"%ARE%\"}} }");
    assertEquals(3, col.find().filter(filterDoc).count());
    
    // Test $like with $lower.
    filterDoc = db.createDocumentFromString("{\"a.b.str\": {\"$lower\": {\"$like\": \"%welcome\"}} }");
    assertEquals(2, col.find().filter(filterDoc).count());
    expectedKeys.clear();
    expectedKeys.add(key1);
    expectedKeys.add(key2);
    checkKeys(col, filterDoc, expectedKeys);
    
    // Test $like with $and
    filterDoc = db.createDocumentFromString("{ \"$and\" : [ " +
        "{\"a.b.str\": {\"$lower\":{\"$like\":\"you are %\"}} }," +
        "{\"a.b.str\":{\"$like\": \"%Wel%\"}} ]}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key2, col.find().filter(filterDoc).getOne().getKey());
    
    // Test $like with $or
    filterDoc = db.createDocumentFromString("{ \"$or\" : [ " +
        "{\"a.b.str\":{\"$like\":\"You%\"} }," +
        "{\"a.b.str\":{\"$like\": \"%Are%\"}} ]}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key2, col.find().filter(filterDoc).getOne().getKey());
    
    // Test $like with $not
    filterDoc = db.createDocumentFromString("{\"a.b.str\": {\"$not\": {\"$like\": \"%come\"}} }");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key3, col.find().filter(filterDoc).getOne().getKey());
    
    // Test $like with $nor
    filterDoc = db.createDocumentFromString("{ \"$nor\" : [ " +
        "{\"a.b.str\":{\"$like\":\"%are%\"} }," +
        "{\"a.b.str\":{\"$like\": \"%welcome\"}} ]}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key2, col.find().filter(filterDoc).getOne().getKey());
    
    // Test $like with object value
    filterDoc = db.createDocumentFromString("{\"a.b.str\":{\"$like\": {\"key\":\"value\"}}}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $like value is an object");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      String expMsg = "A container object cannot be the operand for the $like operator.";
      assertEquals(expMsg, queryException.getMessage());
    }
    
    // Test $like with array value
    filterDoc = db.createDocumentFromString("{\"a.b.str\":{\"$like\": [\"abc\", \"def\"]}}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $like value is an array");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      String expMsg = "A container array cannot be the operand for the $like operator.";
      assertEquals(expMsg, queryException.getMessage());
    }

    // Test $like with null
    filterDoc = db.createDocumentFromString("{\"a.b.str\":{\"$like\": null}}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $like value is null");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      assertEquals("The operand for $like must be a string.", queryException.getMessage());
    }
    
    // Test $like with number value
    filterDoc = db.createDocumentFromString("{\"a.b.str\":{\"$like\": 100}}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $like value is a number");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      assertEquals("The operand for $like must be a string.", queryException.getMessage());
    }
    
    // Test $like with boolean value
    filterDoc = db.createDocumentFromString("{\"a.b.str\":{\"$like\": true}}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $like value is a bool");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      assertEquals("The operand for $like must be a string.", queryException.getMessage());
    }
    
  }
  
  // Tests with $boolean. (boolean() is not finalized yet!!!)
  public void testBooleanClob() throws Exception {
    testBoolean("CLOB",false);
    testBoolean("CLOB",true);
  }
  
  public void testBooleanBlob() throws Exception {
    testBoolean("BLOB",false);
    testBoolean("BLOB",true);
  }
  
  public void testBooleanVarchar2() throws Exception {
    testBoolean("VARCHAR2",false);
    testBoolean("VARCHAR2",true);
  }
  
  // Tests for $boolean
  private void testBoolean(String contentColumnType, boolean withIndex) throws Exception {

    if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel))
      return;
        
    OracleDocument mDoc = null;

    if (isJDCSMode()) {
      if (!contentColumnType.equalsIgnoreCase("BLOB"))  
        return;

      // ### replace with new builder once it becomes available
      mDoc = db.createDocumentFromString("{\"keyColumn\":{\"name\":\"ID\",\"sqlType\":\"VARCHAR2\",\"maxLength\":255,\"assignmentMethod\":\"UUID\"},\"contentColumn\":{\"name\":\"JSON_DOCUMENT\",\"sqlType\":\"BLOB\"},\"lastModifiedColumn\":{\"name\":\"LAST_MODIFIED\"},\"versionColumn\":{\"name\":\"VERSION\",\"method\":\"UUID\"},\"creationTimeColumn\":{\"name\":\"CREATED_ON\"},\"readOnly\":false}");
    }
    else {
      mDoc = client.createMetadataBuilder().contentColumnType(contentColumnType).build();
    }

    String colName = "testBoolean" + contentColumnType;
    OracleCollection col = db.admin().createCollection(colName, mDoc);
    
    String indexSpec = null, indexName = colName + "Idx";
    IndexType indexType = IndexType.noIndex;
    if (withIndex) {
      indexSpec = "{\"name\" : \"" + indexName + "\","
          + "\"search_on\" : \"text_value\","
          + "\"dataguide\" : \"on\"}";
      col.admin().createIndex(db.createDocumentFromString(indexSpec));
      indexType = IndexType.textIndex;
    }
    
    String key1, key2, key3;
    OracleDocument doc = null, filterDoc = null;
    HashSet<String> expectedKeys = new HashSet<String>();
    String docStr1 = "{\"a\":{\"b\":{\"number\":0, \"str\":\"true\", \"bool\":true}}}";
    String docStr2 = "{\"a\":{\"b\":{\"number\":1, \"str\":\"false\", \"bool\":false}}}";
    String docStr3 = "{\"a\":{\"b\":{\"number\":100, \"str\":\"abc\", \"bool\":null}}}";

    doc = col.insertAndGet(db.createDocumentFromString(docStr1));
    key1 = doc.getKey();
    doc = col.insertAndGet(db.createDocumentFromString(docStr2));
    key2 = doc.getKey();
    doc = col.insertAndGet(db.createDocumentFromString(docStr3));
    key3 = doc.getKey();
    
    // Test $boolean with number input    
    filterDoc = db.createDocumentFromString("{\"a.b.number\":{\"$boolean\": {\"$eq\":true}}}");
    assertEquals(0, col.find().filter(filterDoc).count());
    chkExplainPlan(col.find().filter(filterDoc), indexType, indexName);
    
    filterDoc = db.createDocumentFromString("{\"a.b.number\":{\"$boolean\": {\"$eq\":false}}}");
    assertEquals(0, col.find().filter(filterDoc).count());

    chkExplainPlan(col.find().filter(filterDoc), indexType, indexName);
    
    // Test $boolean with string input    
    filterDoc = db.createDocumentFromString("{\"a.b.str\":{\"$boolean\": true}}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key1, col.find().filter(filterDoc).getOne().getKey());
    chkExplainPlan(col.find().filter(filterDoc), indexType, indexName);
    
    filterDoc = db.createDocumentFromString("{\"a.b.str\":{\"$boolean\": false}}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key2, col.find().filter(filterDoc).getOne().getKey());
    chkExplainPlan(col.find().filter(filterDoc), indexType, indexName);
    
    // Test $boolean with boolean input 
    filterDoc = db.createDocumentFromString("{\"a.b.bool\":{\"$boolean\": true}}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key1, col.find().filter(filterDoc).getOne().getKey());
    chkExplainPlan(col.find().filter(filterDoc), indexType, indexName);
    
    filterDoc = db.createDocumentFromString("{\"a.b.bool\":{\"$boolean\": false}}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key2, col.find().filter(filterDoc).getOne().getKey());
    chkExplainPlan(col.find().filter(filterDoc), indexType, indexName);
    
    // Test $boolean with non-boolean operand
    // ### Bug: static typing was added in 18, so this only gives expected
    // type error in 18 or above.
    if (!SODAUtils.sqlSyntaxBelow_18(sqlSyntaxLevel)) {
      filterDoc = db.createDocumentFromString("{\"a.b.bool\":{\"$boolean\": 1}}");
      try {
        col.find().filter(filterDoc).getOne();
        fail("No exception when $boolean's operand is non-bool");
      } catch (OracleException e) {
        // blocked by bug27021099: SODA Java layer failed to error out for such filter spec
        if (e.getCause() instanceof SQLException) {
          SQLException sqlException = (SQLException) e.getCause();
          assertTrue(sqlException.getMessage().contains("ORA-40442"));
        }
        //QueryException queryException = (QueryException) e.getCause();
        //assertEquals("", queryException.getMessage());
      }
    }
    
    // Test $boolean wrapped with $string
    filterDoc = db.createDocumentFromString("{\"a.b.str\": {\"$string\": {\"$boolean\":true} } }");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $string operator wraps $boolean operator.");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      String expMsg = "$string operator cannot wrap $boolean operator. ";
      assertEquals(expMsg, queryException.getMessage());
    }
    
    if (withIndex) {
      col.admin().dropIndex(indexName);
    }
    col.admin().drop();

  }
  
  // Test with $timestamp
  public void testTimestampClob() throws Exception {
    testTimestamp("CLOB", false);
    testTimestamp("CLOB", true);
  }
  
  public void testTimestampBlob() throws Exception {
    testTimestamp("BLOB", false);
    testTimestamp("BLOB", true);
  }
  
  public void testTimestampVarchar2() throws Exception {
    testTimestamp("VARCHAR2", false);
    testTimestamp("VARCHAR2", true);
  }
  
  private void testTimestamp(String contentColumnType, boolean withIndex) throws Exception {
    if (isJDCSMode())
      if (!contentColumnType.equalsIgnoreCase("BLOB"))
        return;

    if (SODAUtils.sqlSyntaxBelow_18(sqlSyntaxLevel))
      return;
    
    OracleDocument mDoc = client.createMetadataBuilder().keyColumnAssignmentMethod("CLIENT")
      .contentColumnType(contentColumnType).build();
    
    String colName = "testTimestamp" + contentColumnType + (withIndex?"Idx":"");
    OracleCollection col;
    if (isJDCSMode())
    {
      col = db.admin().createCollection(colName, null);
    } else
    {
      col = db.admin().createCollection(colName, mDoc);
    }
    
    OracleDocument filterDoc = null, doc = null;
    String name = null, docStr = null;
    String dateTime0 = "2016-01-01T00:00:00", dateTime1 = "2016-07-25T17:30:08";

    String[] key = new String[1000];
    for (int num = 0; num < 1000; num++) {
      if (num == 1) {
        docStr = "{\"order\" : { \"orderDateTime\": \"" + dateTime1 + "\" } }";
      } else {
        String timestamp = dateTime0 + "." + (100000 + num);
        // the generated timestamp value is like: "2016-01-01T00:00:00.100002"
        docStr = "{\"order\" : { \"orderDateTime\": \"" + timestamp + "\" } }";  
      }
      if (isJDCSMode()) 
      {
        doc = col.insertAndGet(db.createDocumentFromString(docStr));
        key[num] = doc.getKey();
      } else
      {
        doc = col.insertAndGet(db.createDocumentFromString("id" + num, docStr));
        key[num] = doc.getKey();
      }
    }
    
    IndexType indexType = IndexType.noIndex;
    String timeIndexName = "timestampIndexOnTestTimestamp" + contentColumnType;
    String dateIndexName = "dateIndexOnTestTimestamp" + contentColumnType;
    if (withIndex) {
      String timeIndexSpec =
        "{ \"name\":\"" + timeIndexName + "\", \n" +
        "  \"fields\": [\n" +
        "    { \"path\":\"order.orderDateTime\", \"datatype\":\"timestamp\", \"order\":\"asc\"} \n" +
        "] }";
      col.admin().createIndex(db.createDocumentFromString(timeIndexSpec));
      
      String dateIndexSpec =
        "{ \"name\":\"" + dateIndexName + "\", \n" +
        "  \"fields\": [\n" +
        "    { \"path\":\"order.orderDateTime\", \"datatype\":\"date\", \"order\":\"asc\"} \n" +
        "] }";
      col.admin().createIndex(db.createDocumentFromString(dateIndexSpec));
      
      indexType = IndexType.funcIndex;
    }

    // test with "$timestamp" + "$eq"
    filterDoc = db.createDocumentFromString("{ \"order.orderDateTime\": {\"$timestamp\" : {\"$eq\" : \"" + dateTime1 + "\"} } }");
    assertEquals(1, col.find().filter(filterDoc).count());
    doc = col.find().filter(filterDoc).getOne();
    assertEquals(key[1], doc.getKey());
    chkExplainPlan(col.find().filter(filterDoc), indexType, timeIndexName);

    // test with "$timestamp" + "$ne"
    filterDoc = db.createDocumentFromString("{ \"order.orderDateTime\": {\"$timestamp\" : {\"$ne\" : \"" + dateTime1 + "\"} } }");
    assertEquals(999, col.find().filter(filterDoc).count());

    // test with "$timestamp" + "$gt"
    filterDoc = db.createDocumentFromString("{ \"order.orderDateTime\": {\"$timestamp\" : {\"$gt\" : \"2016-07-01T00:00:00\"} } }");
    assertEquals(1, col.find().filter(filterDoc).count());
    doc = col.find().filter(filterDoc).getOne();
    assertEquals(key[1], doc.getKey());
    chkExplainPlan(col.find().filter(filterDoc), indexType, timeIndexName);

    // test with "$timestamp" + "$lt"
    filterDoc = db.createDocumentFromString("{ \"order.orderDateTime\": {\"$timestamp\" : {\"$lt\" : \"" + dateTime0 + ".100002\"} } }");
    assertEquals(1, col.find().filter(filterDoc).count());
    doc = col.find().filter(filterDoc).getOne();
    assertEquals(key[0], doc.getKey());
    chkExplainPlan(col.find().filter(filterDoc), indexType, timeIndexName);

    // test with "$timestamp" + "$gte"
    filterDoc = db.createDocumentFromString("{ \"order.orderDateTime\": {\"$timestamp\" : {\"$gte\" : \"" + dateTime1 + "\"} } }");
    assertEquals(1, col.find().filter(filterDoc).count());
    doc = col.find().filter(filterDoc).getOne();
    assertEquals(key[1], doc.getKey());
    chkExplainPlan(col.find().filter(filterDoc), indexType, timeIndexName);

    // test with "$timestamp" + "$lte"
    filterDoc = db.createDocumentFromString("{ \"order.orderDateTime\": {\"$timestamp\" : {\"$lte\" : \"" + dateTime0 + ".100000\"} } }");
    assertEquals(1, col.find().filter(filterDoc).count());
    doc = col.find().filter(filterDoc).getOne();
    assertEquals(key[0], doc.getKey());
    chkExplainPlan(col.find().filter(filterDoc), indexType, timeIndexName);

    // $between
    filterDoc = db.createDocumentFromString("{ \"order.orderDateTime\": {\"$timestamp\" : {\"$between\" : " +
        "[\"" + dateTime0 + ".101000\", \"" + dateTime1 + "\" ]} } }");
    assertEquals(1, col.find().filter(filterDoc).count());
    doc = col.find().filter(filterDoc).getOne();
    assertEquals(key[1], doc.getKey());
    chkExplainPlan(col.find().filter(filterDoc), indexType, timeIndexName);

    docStr = "{\"order\" : { \"orderDateTime\": \"" + dateTime1 + "\", \"string\":\"abc\"," +
            "\"booleean\":true,\"object\":{\"key\":\"value\"},\"array\":[1,2,3],\"null\":null}}";
    if (isJDCSMode()) 
      {
        col.insertAndGet(db.createDocumentFromString(docStr));
      } else
      {
        col.insertAndGet(db.createDocumentFromString("id20001", docStr));
      }
    // when invalid input is fed to $timestamp, the doc is just ignored.
    // Test $timestamp with invalid string input
    filterDoc = db.createDocumentFromString("{ \"order.string\": {\"$timestamp\" : {\"$eq\" : \"" + dateTime1 + "\"} } }");
    assertEquals(0, col.find().filter(filterDoc).count());
    
    // Test $timestamp with boolean input
    filterDoc = db.createDocumentFromString("{ \"order.boolean\": {\"$timestamp\" : {\"$eq\" : \"" + dateTime1 + "\"} } }");
    assertEquals(0, col.find().filter(filterDoc).count());
    
    // Test $timestamp with object input
    filterDoc = db.createDocumentFromString("{ \"order.object\": {\"$timestamp\" : {\"$eq\" : \"" + dateTime1 + "\"} } }");
    assertEquals(0, col.find().filter(filterDoc).count());
    
    // Test $timestamp with array input
    filterDoc = db.createDocumentFromString("{ \"order.array\": {\"$timestamp\" : {\"$eq\" : \"" + dateTime1 + "\"} } }");
    assertEquals(0, col.find().filter(filterDoc).count());
    
    // Test $timestamp with null input
    filterDoc = db.createDocumentFromString("{ \"order.null\": {\"$timestamp\" : {\"$eq\" : \"" + dateTime1 + "\"} } }");
    assertEquals(0, col.find().filter(filterDoc).count());
    
    // Test $timestamp with inexistent location path
    filterDoc = db.createDocumentFromString("{ \"order.abc\": {\"$timestamp\" : {\"$eq\" : \"" + dateTime1 + "\"} } }");
    assertEquals(0, col.find().filter(filterDoc).count());
    
    // Test $date with null operand
    filterDoc = db.createDocumentFromString("{\"order.orderDateTime\":{\"$timestamp\": null}}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $date's operand is null");
    } catch (OracleException e) {
      // blocked by bug27021099: Java layer should catch such invalid filter spec
      //assertEquals("", e.getMessage());
    }
    
    // Test $date with invalid timestamp string operand
    filterDoc = db.createDocumentFromString("{\"order.orderDateTime\":{\"$timestamp\": \"abc\"}}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $timestamp operand is invalid string");
    } catch (OracleException e) {
      if (e.getCause() instanceof SQLException) {
        SQLException sqlException = (SQLException) e.getCause();
        // ORA-01858: a non-numeric character was found where a numeric was expected
        assertTrue(sqlException.getMessage().contains("ORA-01858"));
      }
    }
    
    filterDoc = db.createDocumentFromString("{\"order.orderDateTime\":{\"$timestamp\": true}}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $date's operand is null");
    } catch (OracleException e) {
      // blocked by bug27021099: Java layer should catch such invalid filter spec
      //assertEquals("", e.getMessage());
    }
    
    filterDoc = db.createDocumentFromString("{\"order.orderDateTime\":{\"$timestamp\": \"\"}}");
    assertEquals(0, col.find().filter(filterDoc).count());

    filterDoc = db.createDocumentFromString("{\"order.orderDateTime\":{\"$timestamp\": 100}}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $date's operand is null");
    } catch (OracleException e) {
      // blocked by bug27021099: Java layer should catch such invalid filter spec
      // assertEquals("", e.getMessage());
    }
    
    filterDoc = db.createDocumentFromString("{\"order.orderDateTime\":{\"$timestamp\": [1,2,3] }}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $timestamp operand is not object or string");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      assertEquals("The components of the $timestamp operator must be objects.", queryException.getMessage());
    }
    
    filterDoc = db.createDocumentFromString("{\"order.orderDateTime\":{\"$timestamp\": {\"v1\":100} }}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $timestamp operand is invalid object");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      assertEquals("The field name v1 is not a recognized operator.", queryException.getMessage());
    }
    
    // Test $string wrapped by $timestamp
    filterDoc = db.createDocumentFromString("{\"order.orderDateTime\": {\"$timestamp\": {\"$string\": {\"$gte\":\"2016-01-01T00:00:00\"}}}}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $timestamp operator wraps $string operator.");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      String expMsg = "$timestamp operator cannot wrap $string operator. ";
      assertEquals(expMsg, queryException.getMessage());
    }
    
    if (withIndex) {
      col.admin().dropIndex(timeIndexName);
      col.admin().dropIndex(dateIndexName);
    }
    
    col.admin().drop();
  }
  
  // Tests with $date
  public void testDateClob() throws Exception {
    testDate("CLOB", false);
    testDate("CLOB", true);
  }
  
  public void testDateBlob() throws Exception {
    testDate("BLOB", false);
    testDate("BLOB", true);
  }
  
  public void testDateVarchar2() throws Exception {
    testDate("VARCHAR2", false);
    testDate("VARCHAR2", true); 
  }
  
  private void testDate(String contentColumnType, boolean withIndex) throws Exception {
    if (isJDCSMode())
      if (!contentColumnType.equalsIgnoreCase("BLOB"))
        return;

    if (SODAUtils.sqlSyntaxBelow_18(sqlSyntaxLevel))
      return;
    
    OracleDocument mDoc = client.createMetadataBuilder()
        .contentColumnType(contentColumnType)
        .keyColumnAssignmentMethod("CLIENT").build();
    
    String colName = "testDate" + contentColumnType + (withIndex?"Idx":"");
    OracleCollection col;
    if (isJDCSMode())
    {
      col = db.admin().createCollection(colName, null);
    } else
    {
      col = db.admin().createCollection(colName, mDoc);
    }
    OracleDocument filterDoc = null, doc = null;
    String docStr = null;
    HashSet<String> expectedKeys = new HashSet<String>();
    String dateTime0 = "2014-01-01T00:00:00", dateTime1 = "2017-01-01";
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    Date date = format.parse(dateTime0);
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    
    String[] key = new String[1000];
    // the date range is: 2014-01-01T00:00:00 - 2016-09-25T00:00:00
    for (int num = 1; num <= 1000; num++) {
      if (num == 1) {
        docStr = "{\"order\" : { \"orderDate\": \"" + dateTime0 + "\" } }";
      } else if (num == 2) {
        docStr = "{\"order\" : { \"orderDate\": \"" + dateTime1 + "\" } }";
      } else {
        cal.add(Calendar.DAY_OF_YEAR, 1);
        String dateTime = format.format(cal.getTime());
        docStr = "{\"order\" : { \"orderDate\": \"" + dateTime + "\" } }";
      }
      if (isJDCSMode()) 
      {
        doc = col.insertAndGet(db.createDocumentFromString(docStr));
        key[num-1] = doc.getKey();
      } else
      {
        doc = col.insertAndGet(db.createDocumentFromString("id" + num, docStr));
        key[num-1] = doc.getKey();
      }
    }
    
    IndexType indexType = IndexType.noIndex;
    String timeIndexName = "timestampIndexOnTestDate" + contentColumnType;
    String dateIndexName = "dateIndexOnTestDate" + contentColumnType;
    if (withIndex) {
      String timeIndexSpec =
        "{ \"name\":\"" + timeIndexName + "\", \n" +
        "  \"fields\": [\n" +
        "    { \"path\":\"order.orderDate\", \"datatype\":\"timestamp\", \"order\":\"asc\"} \n" +
        "] }";
      col.admin().createIndex(db.createDocumentFromString(timeIndexSpec));
      
      String dateIndexSpec =
        "{ \"name\":\"" + dateIndexName + "\", \n" +
        "  \"fields\": [\n" +
        "    { \"path\":\"order.orderDate\", \"datatype\":\"date\", \"order\":\"asc\"} \n" +
        "] }";
      col.admin().createIndex(db.createDocumentFromString(dateIndexSpec));
      
      indexType = IndexType.funcIndex;
    }

    // test with "$date" + "$lte"
    filterDoc = db.createDocumentFromString("{ \"order.orderDate\": {\"$date\" : {\"$lte\" : \"2016-11-25T17:30:08\"} } }");
    assertEquals(999, col.find().filter(filterDoc).count());
    col.find().filter(filterDoc).count();
    chkExplainPlan(col.find().filter(filterDoc), indexType, dateIndexName);
    
    // test with "$date" + implied "$eq"
    filterDoc = db.createDocumentFromString("{ \"order.orderDate\": {\"$date\" : \"2016-01-01\"} }");
    assertEquals(1, col.find().filter(filterDoc).count());
    doc = col.find().filter(filterDoc).getOne();
    assertEquals(key[731], doc.getKey());
    chkExplainPlan(col.find().filter(filterDoc), indexType, dateIndexName);
    
    // test with "$date" + "$lt"
    filterDoc = db.createDocumentFromString("{ \"order.orderDate\": {\"$date\" : {\"$lt\" : \"2014-01-02\"} } }");
    assertEquals(1, col.find().filter(filterDoc).count());
    doc = col.find().filter(filterDoc).getOne();
    assertEquals(key[0], doc.getKey());
    chkExplainPlan(col.find().filter(filterDoc), indexType, dateIndexName);
    
    // test with "$date" + "$gt"
    filterDoc = db.createDocumentFromString("{ \"order.orderDate\": {\"$date\" : {\"$gt\" : \"2016-12-01\"} } }");
    assertEquals(1, col.find().filter(filterDoc).count());
    doc = col.find().filter(filterDoc).getOne();
    assertEquals(key[1], doc.getKey());
    chkExplainPlan(col.find().filter(filterDoc), indexType, dateIndexName);
    
    // test with "$date" + "$gte"
    filterDoc = db.createDocumentFromString("{ \"order.orderDate\": {\"$date\" : {\"$gte\" : \"2016-09-24\"} } }");
    assertEquals(3, col.find().filter(filterDoc).count());
    expectedKeys.clear();
    expectedKeys.add(key[1]);
    expectedKeys.add(key[998]);
    expectedKeys.add(key[999]);
    checkKeys(col, filterDoc, expectedKeys);
    chkExplainPlan(col.find().filter(filterDoc), indexType, dateIndexName);
    
    // test with "$date" + "$between"
    filterDoc = db.createDocumentFromString("{ \"order.orderDate\": {\"$date\" : {\"$between\" : [\"2016-01-01T00:00:00\", \"2016-10-01\"] }}}");
    assertEquals(269, col.find().filter(filterDoc).count());
    chkExplainPlan(col.find().filter(filterDoc), indexType, dateIndexName);
    
    docStr = "{\"order\" : { \"orderDate\": \"2017-01-02\", \"str\":\"\", \"num\":20170102 }}";
    if (isJDCSMode()) 
    {
      col.insertAndGet(db.createDocumentFromString(docStr));
    } else
    {
      col.insertAndGet(db.createDocumentFromString("id20001", docStr));
    }
    // Test $date with empty string input
    filterDoc = db.createDocumentFromString("{ \"order.str\": {\"$date\" : {\"$eq\" : \"2017-01-02\" }}}");
    assertEquals(0, col.find().filter(filterDoc).count());

    // Test $date with number input
    filterDoc = db.createDocumentFromString("{ \"order.num\": {\"$date\" : {\"$eq\" : \"2017-01-02\" }}}");
    assertEquals(0, col.find().filter(filterDoc).count());
    
    // Test $date with null operand
    filterDoc = db.createDocumentFromString("{\"order.orderDate\":{\"$date\": null}}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $date operand is null");
    } catch (OracleException e) {
      if (e.getCause() instanceof SQLException) {
        SQLException sqlException = (SQLException) e.getCause();
          // ORA-40597: JSON path expression syntax error ('$?(@.order.orderDate.date() == null)
          assertTrue(sqlException.getMessage().contains("ORA-40597"));
      }
    }
      
    // Test $date with invalid date string operand
    filterDoc = db.createDocumentFromString("{\"order.orderDate\":{\"$date\": \"abc\"}}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $date operand is invalid string");
    } catch (OracleException e) {
      if (e.getCause() instanceof SQLException) {
        SQLException sqlException = (SQLException) e.getCause();
        // ORA-01858: a non-numeric character was found where a numeric was expected
        assertTrue(sqlException.getMessage().contains("ORA-01858"));
      }
    }
    
    filterDoc = db.createDocumentFromString("{\"order.orderDate\":{\"$date\": true}}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $date operand is boolean value");
    } catch (OracleException e) {
      if (e.getCause() instanceof SQLException) {
        SQLException sqlException = (SQLException) e.getCause();
        // ORA-40597: JSON path expression syntax error ('$?(@.order.orderDate.date() == null)
        assertTrue(sqlException.getMessage().contains("ORA-40597"));
      }
    }
    
    filterDoc = db.createDocumentFromString("{\"order.orderDate\":{\"$date\": \"\"}}");
    assertEquals(0, col.find().filter(filterDoc).count());

    filterDoc = db.createDocumentFromString("{\"order.orderDate\":{\"$date\": 100}}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $date operand is number value");
    } catch (OracleException e) {
      if (e.getCause() instanceof SQLException) {
        SQLException sqlException = (SQLException) e.getCause();
        // ORA-40442: JSON path expression syntax error ('$?(@.order.orderDate.date() == $B0)')
        assertTrue(sqlException.getMessage().contains("ORA-40442"));
      }
    }
    
    filterDoc = db.createDocumentFromString("{\"order.orderDate\":{\"$date\": [1,2,3] }}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $date operand is not object or string");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      assertEquals("The components of the $date operator must be objects.", queryException.getMessage());
    }
    
    filterDoc = db.createDocumentFromString("{\"order.orderDate\":{\"$date\": {\"v1\":100} }}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $date operand is invalid object");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      assertEquals("The field name v1 is not a recognized operator.", queryException.getMessage());
    }
    
    // Test $date wrapped by $timestamp
    filterDoc = db.createDocumentFromString("{\"order.orderDateTime\": {\"$timestamp\": {\"$date\": {\"$gte\":\"2016-01-01\"}}}}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $timestamp operator wraps $date operator.");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      String expMsg = "$timestamp operator cannot wrap $date operator. ";
      assertEquals(expMsg, queryException.getMessage());
    }
    
    if (withIndex) {
      col.admin().dropIndex(timeIndexName);
      col.admin().dropIndex(dateIndexName);
    }
    
    col.admin().drop();
  }
  
  // Tests with $number
  public void testNumberClob() throws Exception {
    testNumber("CLOB", false);
    testNumber("CLOB", true);
  }
  
  public void testNumberBlob() throws Exception {
    testNumber("BLOB", false);
    testNumber("BLOB", true);
  }
  
  public void testNumberVarchar2() throws Exception {
    testNumber("VARCHAR2", false);
    // blocked by bug25902132: the plan fetched by SODA java is different from sqlplus
    // testNumber("VARCHAR2", true);
  }
  
  private void testNumber(String contentColumnType, boolean withIndex) throws Exception {
    if (isJDCSMode())
      if (!contentColumnType.equalsIgnoreCase("BLOB"))
        return;

    if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel))
      return;
    
    OracleDocument mDoc = client.createMetadataBuilder().keyColumnAssignmentMethod("CLIENT")
      .contentColumnType(contentColumnType).build();
    
    String colName = "testNumber" + contentColumnType + (withIndex?"Idx":"");
    OracleCollection col;
    if (isJDCSMode())
    {
      col = db.admin().createCollection(colName, null);
    } else
    {
      col = db.admin().createCollection(colName, mDoc);
    }
    
    IndexType indexType = IndexType.noIndex;
    String numberIndexName = "numberIndexOnTestNumber" + contentColumnType;
    String stringIndexName = "stringIndexOnTestNumber" + contentColumnType;
    if (withIndex) {
      String numberIndexSpec =
        "{ \"name\":\"" + numberIndexName + "\", \n" +
        "  \"fields\": [\n" +
        "    { \"path\":\"a.b.string\", \"datatype\":\"number\"} \n" +
        "] }";
      col.admin().createIndex(db.createDocumentFromString(numberIndexSpec));
      
      String stringIndexSpec =
        "{ \"name\":\"" + stringIndexName + "\", \n" +
        "  \"fields\": [\n" +
        "    { \"path\":\"a.b.string\", \"datatype\":\"string\"} \n" +
        "] }";
      col.admin().createIndex(db.createDocumentFromString(stringIndexSpec));
      
      indexType = IndexType.funcIndex;
    }
      
    OracleDocument doc = null, filterDoc = null;
    HashSet<String> expectedKeys = new HashSet<String>();
    
    String[] key = new String[1002];
    for (int number = 0; number < 1000; number++) {
      String docStr = "{\"a\":{\"b\":{\"number\":11, \"string\": \"11." + number + "\", \"float\":3.0001," +
        "\"bool\":true, \"array\":[101, 100] }}}";
      if (isJDCSMode()) 
      {
        doc = col.insertAndGet(db.createDocumentFromString(docStr));
      } else
      {
        doc = col.insertAndGet(db.createDocumentFromString("id-" + number, docStr));;
      }
      key[number] = doc.getKey();
    }
    
    String docStr1 = "{\"a\":{\"b\":{\"number\":12, \"string\":12.1, \"float\":3.0002," +
        "\"bool\":true, \"array\":[102, 100] }}}";
    String docStr2 = "{\"a\":{\"b\":{\"number\":13, \"string\":\"12.2\", \"float\":3.0003," +
        "\"bool\":false, \"array\":[103, 100, 110], \"invalid_str\":\"abc\", \"empty_str\":\"\", \"null\":null }}}";

    if (isJDCSMode()) 
    {
      doc = col.insertAndGet(db.createDocumentFromString(docStr1));
      key[1000] = doc.getKey();
      doc = col.insertAndGet(db.createDocumentFromString(docStr2));
      key[1001] = doc.getKey();
    } else
    {
      doc = col.insertAndGet(db.createDocumentFromString("id-1001", docStr1));
      key[1000] = doc.getKey();
      doc = col.insertAndGet(db.createDocumentFromString("id-1002", docStr2));
      key[1001] = doc.getKey();
    }    
    
    // Test $number with number input("id-1001")
    filterDoc = db.createDocumentFromString("{\"a.b.number\": { \"$number\": {\"$between\": [12, 12.5] }}}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[1000], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $number with string input
    filterDoc = db.createDocumentFromString("{\"a.b.string\": { \"$number\": {\"$eq\": 12.1 }}}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[1000], col.find().filter(filterDoc).getOne().getKey());
    chkExplainPlan(col.find().filter(filterDoc), indexType, numberIndexName);
    
    // Test $number with float input
    filterDoc = db.createDocumentFromString("{\"a.b.float\": { \"$number\": {\"$eq\": 3.0001 }}}");
    assertEquals(1000, col.find().filter(filterDoc).count());
    
    // Test $number with array item input
    filterDoc = db.createDocumentFromString("{\"a.b.array[0]\": { \"$number\": {\"$eq\": 102 }}}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[1000], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $number with $between
    filterDoc = db.createDocumentFromString("{\"a.b.string\": { \"$number\": {\"$between\": [11.115, 11.118] }}}");
    // the range should cover 11.115, 11.116, 11.117, 11.118
    assertEquals(4, col.find().filter(filterDoc).count());
    chkExplainPlan(col.find().filter(filterDoc), indexType, numberIndexName);
    
    // Test $number with $ne
    filterDoc = db.createDocumentFromString("{\"a.b.string\": { \"$number\": {\"$ne\": 11 }}}");
    assertEquals(1001, col.find().filter(filterDoc).count());
    
    // Test $number with $gt
    filterDoc = db.createDocumentFromString("{\"a.b.string\": { \"$number\": {\"$gt\": 12 }}}");
    assertEquals(2, col.find().filter(filterDoc).count());
    expectedKeys.clear();
    expectedKeys.add(key[1000]);
    expectedKeys.add(key[1001]);
    checkKeys(col, filterDoc, expectedKeys);
    chkExplainPlan(col.find().filter(filterDoc), indexType, numberIndexName);
    
    // Test $number with $gte
    filterDoc = db.createDocumentFromString("{\"a.b.string\": { \"$number\": {\"$gte\": 11.999 }}}");
    assertEquals(3, col.find().filter(filterDoc).count());
    expectedKeys.clear();
    expectedKeys.add(key[999]);
    expectedKeys.add(key[1000]);
    expectedKeys.add(key[1001]);
    checkKeys(col, filterDoc, expectedKeys);
    chkExplainPlan(col.find().filter(filterDoc), indexType, numberIndexName);
    
    // Test $number with $lt
    filterDoc = db.createDocumentFromString("{\"a.b.string\": { \"$number\": {\"$lt\": 11.001 }}}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[0], col.find().filter(filterDoc).getOne().getKey());
    chkExplainPlan(col.find().filter(filterDoc), indexType, numberIndexName);
    
    // Test $number with $lte
    filterDoc = db.createDocumentFromString("{\"a.b.string\": { \"$number\": {\"$lte\": 11 }}}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[0], col.find().filter(filterDoc).getOne().getKey());
    chkExplainPlan(col.find().filter(filterDoc), indexType, numberIndexName);
    
    // Test $number with array input(each the array elements will be applied to number())
    filterDoc = db.createDocumentFromString("{\"a.b.array\": { \"$number\": {\"$eq\": 103 }}}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[1001], col.find().filter(filterDoc).getOne().getKey());
    
    filterDoc = db.createDocumentFromString("{\"a.b.array\": { \"$number\": {\"$eq\": 100 }}}");
    assertEquals(1002, col.find().filter(filterDoc).count());
    
    filterDoc = db.createDocumentFromString("{\"a.b.array\": { \"$number\": {\"$eq\": 110 }}}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[1001], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $number with boolean input (the result is still TBD)
    filterDoc = db.createDocumentFromString("{\"a.b.bool\": { \"$number\": {\"$eq\": 0 }}}");
    // assertEquals(1, col.find().filter(filterDoc).count());
 
    // Test $number with invalid string input
    filterDoc = db.createDocumentFromString("{\"a.b.invalid_str\": { \"$number\": {\"$eq\": 0 }}}");
    assertEquals(0, col.find().filter(filterDoc).count());
    
    // Test $number with empty string input
    filterDoc = db.createDocumentFromString("{\"a.b.empty_str\": { \"$number\": {\"$eq\": 0 }}}");
    assertEquals(0, col.find().filter(filterDoc).count());
    
    // Test $number with null input
    filterDoc = db.createDocumentFromString("{\"a.b.null\": { \"$number\": {\"$eq\": 0 }}}");
    assertEquals(0, col.find().filter(filterDoc).count());
    
    // Test $number with object input
    filterDoc = db.createDocumentFromString("{\"a.b\": { \"$number\": {\"$lte\": 1 }}}");
    assertEquals(0, col.find().filter(filterDoc).count());
    
    // Test $number with non-number operand
    // ### Bug: static typing was added in 18, so this only
    // gives the expected wrong types error in 18 or above
    if (!SODAUtils.sqlSyntaxBelow_18(sqlSyntaxLevel)) {
      filterDoc = db.createDocumentFromString("{\"a.b.c\": {\"$number\": true } }");
      try {
        col.find().filter(filterDoc).getOne();
        fail("No exception when $number oprand is non number");
      } catch (OracleException e) {
        // blocked by bug27021099: Java layer should catch such invalid spec
        if (e.getCause() instanceof SQLException) {
          SQLException sqlException = (SQLException) e.getCause();
          assertTrue(sqlException.getMessage().contains("ORA-40597"));
        }
        //QueryException queryException = (QueryException) e.getCause();
        //assertEquals("", queryException.getMessage());
      }
    }
    
    // Test $number wrapped by $string
    // ### Bug: static typing was added in 18, so this only
    // gives the expected wrong types error in 18 or above
    if (!SODAUtils.sqlSyntaxBelow_18(sqlSyntaxLevel)) {
      filterDoc = db.createDocumentFromString("{\"a.b.number\": {\"$string\": {\"$number\": 1}}}");
      try {
        col.find().filter(filterDoc).getOne();
        fail("No exception when $string operator wraps $number operator.");
      } catch (OracleException e) {
        QueryException queryException = (QueryException) e.getCause();
        String expMsg = "$string operator cannot wrap $number operator. ";
        assertEquals(expMsg, queryException.getMessage());
      }

      if (withIndex) {
        col.admin().dropIndex(numberIndexName);
        col.admin().dropIndex(stringIndexName);
      }
    }
    
    col.admin().drop();
  }
  
  // Test with $string
  public void testStringClob() throws Exception {
    testString("CLOB", false);
    testString("CLOB", true);
  }
  
  public void testStringBlob() throws Exception {
    testString("BLOB", false);
    testString("BLOB", true);
  }
  
  public void testStringVarchar2() throws Exception {
    testString("VARCHAR2", false);
    // blocked by bug25902132: the plan fetched by SODA java is different from sqlplus
    // testString("VARCHAR2", true);
  }
  
  private void testString(String contentColumnType, boolean withIndex) throws Exception {
    if (isJDCSMode())
      if (!contentColumnType.equalsIgnoreCase("BLOB"))
        return;


    if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel))
      return;
    
    OracleDocument mDoc = client.createMetadataBuilder().keyColumnAssignmentMethod("CLIENT")
      .contentColumnType(contentColumnType).build();
    
    String colName = "testString" + contentColumnType + (withIndex?"Idx":"");
    OracleCollection col;
    if (isJDCSMode())
    {
      col = db.admin().createCollection(colName, null);
    } else
    {
      col = db.admin().createCollection(colName, mDoc);
    }
    
    IndexType indexType = IndexType.noIndex;
    String numberIndexName = "numberIndexOnTestString" + contentColumnType;
    String stringIndexName = "stringIndexOnTestString" + contentColumnType;
    if (withIndex) {
      String numberIndexSpec =
        "{ \"name\":\"" + numberIndexName + "\", \n" +
        "  \"fields\": [\n" +
        "    { \"path\":\"a.b.string\", \"datatype\":\"number\"} \n" +
        "] }";
      col.admin().createIndex(db.createDocumentFromString(numberIndexSpec));
      
      String stringIndexSpec =
        "{ \"name\":\"" + stringIndexName + "\", \n" +
        "  \"fields\": [\n" +
        "    { \"path\":\"a.b.string\", \"datatype\":\"string\"} \n" +
        "] }";
      col.admin().createIndex(db.createDocumentFromString(stringIndexSpec));
      
      indexType = IndexType.funcIndex;
    }
      
    OracleDocument doc = null, filterDoc = null;
    HashSet<String> expectedKeys = new HashSet<String>();
    
    String[] key = new String[1000];
    for (int number = 0; number < 1000; number++) {
      String docStr = "{\"a\":{\"b\":{\"number\":11, \"string\":" + (11.0 + (number/1000.0)) + "}}}";
      if (isJDCSMode()) 
      {
        col.insertAndGet(db.createDocumentFromString(docStr));
      } else
      {
        col.insertAndGet(db.createDocumentFromString("id-" + number, docStr));
      }
    }
    
    String docStr1 = "{\"a\":{\"b\":{\"number\":12, \"string\":12.1, \"bool\":true, \"array\":[102, 0] }}}";
    String docStr2 = "{\"a\":{\"b\":{\"number\":12.0, \"string\":\"12.2\", \"bool\":false, \"array\":[103, 104, 105], " +
            "\"empty_str\":\"\", \"null\":null }}}";

    if (isJDCSMode()) 
    {
      col.insertAndGet(db.createDocumentFromString(docStr1));
      col.insertAndGet(db.createDocumentFromString(docStr2));
    } else
    {
      col.insertAndGet(db.createDocumentFromString("id-1001", docStr1));
      col.insertAndGet(db.createDocumentFromString("id-1002", docStr2));
    }
    
    
    // Test $string with number and string input
    filterDoc = db.createDocumentFromString("{\"a.b.number\": { \"$string\": \"12\"}}");
    // ### Bug: number canonicalization doesn't work properly on 12.2.0.1
    if (!SODAUtils.sqlSyntaxBelow_18(sqlSyntaxLevel)) {
      assertEquals(2, col.find().filter(filterDoc).count());
      expectedKeys.clear();
      expectedKeys.add("id-1001");
      expectedKeys.add("id-1002");
      if (!isJDCSMode())
        checkKeys(col, filterDoc, expectedKeys);
    }
    else {
      assertEquals(1, col.find().filter(filterDoc).count());
      expectedKeys.clear();
      expectedKeys.add("id-1001");
      if (!isJDCSMode())
        checkKeys(col, filterDoc, expectedKeys);
    }
    
    filterDoc = db.createDocumentFromString("{\"a.b.string\": { \"$string\": \"11.999\"}}");
    assertEquals(1, col.find().filter(filterDoc).count());
    if (!isJDCSMode())
      assertEquals("id-999", col.find().filter(filterDoc).getOne().getKey());
    chkExplainPlan(col.find().filter(filterDoc), indexType, stringIndexName);
    
    filterDoc = db.createDocumentFromString("{\"a.b.string\": { \"$string\": \"12.2\"}}");
    assertEquals(1, col.find().filter(filterDoc).count());
    if (!isJDCSMode())
      assertEquals("id-1002", col.find().filter(filterDoc).getOne().getKey());
    chkExplainPlan(col.find().filter(filterDoc), indexType, stringIndexName);
    
    // Test $string with boolean input
    filterDoc = db.createDocumentFromString("{\"a.b.bool\": { \"$string\": {\"$eq\": \"true\"}}}");
    assertEquals(1, col.find().filter(filterDoc).count());
    if (!isJDCSMode())
      assertEquals("id-1001", col.find().filter(filterDoc).getOne().getKey());
    
    filterDoc = db.createDocumentFromString("{\"a.b.bool\": { \"$string\": {\"$eq\": \"false\"}}}");
    assertEquals(1, col.find().filter(filterDoc).count());
    if (!isJDCSMode())
      assertEquals("id-1002", col.find().filter(filterDoc).getOne().getKey());
    
    // Test $string with array input
    filterDoc = db.createDocumentFromString("{\"a.b.array\": { \"$string\": \"103\" }}");
    assertEquals(1, col.find().filter(filterDoc).count());
    if (!isJDCSMode())
      assertEquals("id-1002", col.find().filter(filterDoc).getOne().getKey());
    
    filterDoc = db.createDocumentFromString("{\"a.b.array\": { \"$string\": \"104\" }}");
    assertEquals(1, col.find().filter(filterDoc).count());
    if (!isJDCSMode())
      assertEquals("id-1002", col.find().filter(filterDoc).getOne().getKey());
    
    filterDoc = db.createDocumentFromString("{\"a.b.array\": { \"$string\": \"105\" }}");
    assertEquals(1, col.find().filter(filterDoc).count());
    if (!isJDCSMode())
      assertEquals("id-1002", col.find().filter(filterDoc).getOne().getKey());
    
    // Test $string with $ne
    filterDoc = db.createDocumentFromString("{\"a.b.string\": { \"$string\": {\"$ne\": \"11.0\"}}}");
    // ### Bug with canonicalization on 12.2.0.1 (the behavior
    // in the else branch is a bug).
    if (!SODAUtils.sqlSyntaxBelow_18(sqlSyntaxLevel)) {
      assertEquals(1002, col.find().filter(filterDoc).count());
    }
    else {
      assertEquals(1001, col.find().filter(filterDoc).count());
    }
    // Test $string with $gt
    filterDoc = db.createDocumentFromString("{\"a.b.string\": { \"$string\": {\"$gt\": \"11.998\"}}}");
    assertEquals(3, col.find().filter(filterDoc).count());
    expectedKeys.clear();
    expectedKeys.add("id-999");
    expectedKeys.add("id-1001");
    expectedKeys.add("id-1002");
    if (!isJDCSMode())
      checkKeys(col, filterDoc, expectedKeys);
    chkExplainPlan(col.find().filter(filterDoc), indexType, stringIndexName);
    
    // Test $string with $gte
    filterDoc = db.createDocumentFromString("{\"a.b.string\": { \"$string\": {\"$gte\": \"11.999\"}}}");
    assertEquals(3, col.find().filter(filterDoc).count());
    expectedKeys.clear();
    expectedKeys.add("id-999");
    expectedKeys.add("id-1001");
    expectedKeys.add("id-1002");
    if (!isJDCSMode())
      checkKeys(col, filterDoc, expectedKeys);
    chkExplainPlan(col.find().filter(filterDoc), indexType, stringIndexName);
    
    // Test $string with $lt
    filterDoc = db.createDocumentFromString("{\"a.b.string\": { \"$string\": {\"$lt\": \"11.001\"}}}");
    assertEquals(1, col.find().filter(filterDoc).count());
    if (!isJDCSMode())
      assertEquals("id-0", col.find().filter(filterDoc).getOne().getKey());
    chkExplainPlan(col.find().filter(filterDoc), indexType, stringIndexName);
    
    // Test $string with $lte
    filterDoc = db.createDocumentFromString("{\"a.b.string\": { \"$string\": {\"$lte\": \"11.0\"}}}");
    assertEquals(1, col.find().filter(filterDoc).count());
    if (!isJDCSMode())
      assertEquals("id-0", col.find().filter(filterDoc).getOne().getKey());
    chkExplainPlan(col.find().filter(filterDoc), indexType, stringIndexName);
    
    // Test $string with $startsWith
    filterDoc = db.createDocumentFromString("{\"a.b.string\": { \"$string\": {\"$startsWith\": \"12\"}}}");
    assertEquals(2, col.find().filter(filterDoc).count());
    expectedKeys.clear();
    expectedKeys.add("id-1001");
    expectedKeys.add("id-1002");
    if (!isJDCSMode())
      checkKeys(col, filterDoc, expectedKeys);
    // $startsWith does not support the functional index
    //chkExplainPlan(col.find().filter(filterDoc), indexType, stringIndexName);
    
    // Test $string with $regex
    filterDoc = db.createDocumentFromString("{\"a.b.string\": { \"$string\": {\"$regex\": \"12.*\"}}}");
    assertEquals(2, col.find().filter(filterDoc).count());
    expectedKeys.clear();
    expectedKeys.add("id-1001");
    expectedKeys.add("id-1002");
    if (!isJDCSMode())
      checkKeys(col, filterDoc, expectedKeys);
    
    // Test $string with empty string input
    filterDoc = db.createDocumentFromString("{\"a.b.empty_str\": { \"$string\": \"\"}}");
    // ### Bug with empty string matching on 12.2.0.1
    // (the else branch below is buggy behavior).
    if (!SODAUtils.sqlSyntaxBelow_18(sqlSyntaxLevel))
      assertEquals(1, col.find().filter(filterDoc).count());
    else
      assertEquals(0, col.find().filter(filterDoc).count());

    // Test $string with null input
    filterDoc = db.createDocumentFromString("{\"a.b.null\": { \"$string\": \"null\"}}");
    // ### Bug with null matching on 12.2.0.1
    // (the else branch below is buggy behavior).
    if (!SODAUtils.sqlSyntaxBelow_18(sqlSyntaxLevel))
      assertEquals(0, col.find().filter(filterDoc).count());
    else
      assertEquals(1, col.find().filter(filterDoc).count());
    
    // Test $string with non-string operand
    // ### Bug: static typing was added in 18, so this only
    // gives the expected wrong types error in 18 or above
    if (!SODAUtils.sqlSyntaxBelow_18(sqlSyntaxLevel)) {
      filterDoc = db.createDocumentFromString("{\"a.b.c\": {\"$string\": 123 } }");
      try {
        col.find().filter(filterDoc).getOne();
        fail("No exception when $string's operand is non string");
      } catch (OracleException e) {
        // blocked by bug27021099: Java layer should catch such invalid spec
        if (e.getCause() instanceof SQLException) {
          SQLException sqlException = (SQLException) e.getCause();
          assertTrue(sqlException.getMessage().contains("ORA-40442"));
        }
        //QueryException queryException = (QueryException) e.getCause();
        //assertEquals("", queryException.getMessage());
      }
    }
    
    // Test $string wrapped by $string
    filterDoc = db.createDocumentFromString("{\"a.b.string\": {\"$string\": {\"$string\": \"12.1\"}}}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $string operator wraps $string operator.");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      String expMsg = "$string operator cannot wrap $string operator. ";
      assertEquals(expMsg, queryException.getMessage());
    }
    
    if (withIndex) {
      col.admin().dropIndex(numberIndexName);
      col.admin().dropIndex(stringIndexName);
    }
    
    col.admin().drop();
  }
  
  // Tests with $double
  public void testDoubleClob() throws Exception {
    testDouble("CLOB", false);
    testDouble("CLOB", true);
  }
  
  public void testDoubleBlob() throws Exception {
    testDouble("BLOB", false);
    testDouble("BLOB", true);
  }
  
  public void testDoubleVarchar2() throws Exception {
    testDouble("VARCHAR2", false);
    testDouble("VARCHAR2", true);
  }
  
  private void testDouble(String contentColumnType, boolean withIndex) throws Exception {
    if (isJDCSMode())
      if (!contentColumnType.equalsIgnoreCase("BLOB"))
        return;

    if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel))
      return;
    
    OracleDocument mDoc = client.createMetadataBuilder().keyColumnAssignmentMethod("CLIENT")
      .contentColumnType(contentColumnType).build();
    
    String colName = "testDouble" + contentColumnType + (withIndex?"Idx":"");
    OracleCollection col;
    if (isJDCSMode())
    {
      col = db.admin().createCollection(colName, null);
    } else
    {
      col = db.admin().createCollection(colName, mDoc);
    }
    
    String indexSpec, indexName = "searchIndexOnTestDouble" + contentColumnType;
    IndexType indexType = IndexType.noIndex;
    if (withIndex) {
      indexSpec = "{\"name\" : \"" + indexName + "\","
          + "\"search_on\" : \"text_value\","
          + "\"dataguide\" : \"on\"}";
      col.admin().createIndex(db.createDocumentFromString(indexSpec));
      indexType = IndexType.textIndex;
    }

    OracleDocument doc = null, filterDoc = null;
    HashSet<String> expectedKeys = new HashSet<String>();
    
    String docStr1 = "{\"a\":{\"b\":{\"number\":0, \"string\":\"12.0000001\" }}}";
    String docStr2 = "{\"a\":{\"b\":{\"number\":1.00001, \"string\":\"12.0000002\" }}}";
    String docStr3 = "{\"a\":{\"b\":{\"number\":1.01, \"string\":\"12.0000003\", \"bool\":true, \"array\":[103, 104, 105], " +
        "\"empty_str\":\"\", \"null\":null, \"invalid_str\":\"abc\" }}}";

    String[] key = new String[3];
    if (isJDCSMode()) 
    {
      doc = col.insertAndGet(db.createDocumentFromString(docStr1));
      key[0] = doc.getKey();
      doc = col.insertAndGet(db.createDocumentFromString(docStr2));
      key[1] = doc.getKey();
      doc = col.insertAndGet(db.createDocumentFromString(docStr3));
      key[2] = doc.getKey();
    } else
    {
      doc = col.insertAndGet(db.createDocumentFromString("id-1001", docStr1));
      key[0] = doc.getKey();
      doc = col.insertAndGet(db.createDocumentFromString("id-1002", docStr2));
      key[1] = doc.getKey();
      doc = col.insertAndGet(db.createDocumentFromString("id-1003", docStr3));
      key[2] = doc.getKey();
    }    
    
    // Test $double with number input
    filterDoc = db.createDocumentFromString("{\"a.b.number\": { \"$double\": 0}}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[0], col.find().filter(filterDoc).getOne().getKey());
    chkExplainPlan(col.find().filter(filterDoc), indexType, indexName);
    
    // conversion will bring a little precision loss
    filterDoc = db.createDocumentFromString("{\"a.b.number\": { \"$double\": {\"$between\":[1.0000099999, 1.0000100001]}}}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[1], col.find().filter(filterDoc).getOne().getKey());
    chkExplainPlan(col.find().filter(filterDoc), indexType, indexName);
    
    // Test $double with string input
    filterDoc = db.createDocumentFromString("{\"a.b.string\": { \"$double\": {\"$between\":[12.00000009999, 12.00000010001]}}}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[0], col.find().filter(filterDoc).getOne().getKey());
    chkExplainPlan(col.find().filter(filterDoc), indexType, indexName);
    
    // Test $double with $eq
    filterDoc = db.createDocumentFromString("{\"a.b.string\": { \"$double\": {\"$eq\": 12}}}");
    assertEquals(0, col.find().filter(filterDoc).count());
    chkExplainPlan(col.find().filter(filterDoc), indexType, indexName);
    
    // Test $double with $ne
    filterDoc = db.createDocumentFromString("{\"a.b.string\": { \"$double\": {\"$ne\": 12}}}");
    assertEquals(3, col.find().filter(filterDoc).count());
    
    // Test $double with $gt
    filterDoc = db.createDocumentFromString("{\"a.b.array\": { \"$double\": {\"$gt\": 104}}}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[2], col.find().filter(filterDoc).getOne().getKey());
    chkExplainPlan(col.find().filter(filterDoc), indexType, indexName);
    
    // Test $double with $gte
    filterDoc = db.createDocumentFromString("{\"a.b.array\": { \"$double\": {\"$gte\": 104}}}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[2], col.find().filter(filterDoc).getOne().getKey());
    chkExplainPlan(col.find().filter(filterDoc), indexType, indexName);
    
    //Test $double with $lte.
    filterDoc = db.createDocumentFromString("{\"a.b.array\": { \"$double\": {\"$lte\": 104}}}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[2], col.find().filter(filterDoc).getOne().getKey());
    chkExplainPlan(col.find().filter(filterDoc), indexType, indexName);
    
    // when invalid input is fed to double(), the result is UNKNOWN.
    // Test $double with boolean input
    filterDoc = db.createDocumentFromString("{\"a.b.bool\": { \"$double\": {\"$between\":[0, 1]}}}");
    assertEquals(0, col.find().filter(filterDoc).count());
    chkExplainPlan(col.find().filter(filterDoc), indexType, indexName);
    
    // Test $double with object input
    filterDoc = db.createDocumentFromString("{\"a\": { \"$double\": {\"$gte\":0 }}}");
    assertEquals(0, col.find().filter(filterDoc).count());
    chkExplainPlan(col.find().filter(filterDoc), indexType, indexName);
    
    // Test $double with empty string input
    filterDoc = db.createDocumentFromString("{\"a.b.empty_str\": { \"$double\": {\"$gte\":0 }}}");
    assertEquals(0, col.find().filter(filterDoc).count());
    chkExplainPlan(col.find().filter(filterDoc), indexType, indexName);
    
    // Test $double with non-number string input
    filterDoc = db.createDocumentFromString("{\"a.b.invalid_str\": { \"$double\": {\"$gte\":0 }}}");
    assertEquals(0, col.find().filter(filterDoc).count());
    chkExplainPlan(col.find().filter(filterDoc), indexType, indexName);
    
    // Test $double with null input
    filterDoc = db.createDocumentFromString("{\"a.b.null\": { \"$double\": {\"$gte\":0 }}}");
    assertEquals(0, col.find().filter(filterDoc).count());
    chkExplainPlan(col.find().filter(filterDoc), indexType, indexName);

    // Test $double with non-double operand
    // ### Bug: static typing was added in 18, so this only
    // gives the expected wrong types error in 18 or above
    if (!SODAUtils.sqlSyntaxBelow_18(sqlSyntaxLevel)) {
      filterDoc = db.createDocumentFromString("{\"a.b.string\": {\"$double\": true } }");
      try {
        col.find().filter(filterDoc).getOne();
        fail("No exception when $double's operand is non number");
      } catch (OracleException e) {
        // blocked by bug27021099: Java layer should catch such invalid spec
        if (e.getCause() instanceof SQLException) {
          SQLException sqlException = (SQLException) e.getCause();
          assertTrue(sqlException.getMessage().contains("ORA-40597"));
        }
        //QueryException queryException = (QueryException) e.getCause();
        //assertEquals("", queryException.getMessage());
      }
    }
    
    // Test $double wrapped by $string
    filterDoc = db.createDocumentFromString("{\"a.b.number\": {\"$string\": {\"$double\": 12.1}}}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $string operator wraps $double operator.");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      String expMsg = "$string operator cannot wrap $double operator. ";
      assertEquals(expMsg, queryException.getMessage());
    }
    
    if (withIndex) {
      col.admin().dropIndex(indexName);
    }
  }
  
  // Tests with $upper and $lower
  public void testUpperAndLowerClob() throws Exception {
    testUpperAndLower("CLOB");
  }
  
  public void testUpperAndLowerBlob() throws Exception {
    testUpperAndLower("BLOB");
  }
  
  public void testUpperAndLowerVarchar2() throws Exception {
    testUpperAndLower("VARCHAR2");
  }
  
  private void testUpperAndLower(String contentColumnType) throws Exception {
    if (isJDCSMode())
      if (!contentColumnType.equalsIgnoreCase("BLOB"))
        return;

    if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel))
      return;
    
    OracleDocument mDoc = client.createMetadataBuilder().keyColumnAssignmentMethod("CLIENT")
      .contentColumnType(contentColumnType).build();
    
    String colName = "testUpperAndLower" + contentColumnType;
    OracleCollection col;
    if (isJDCSMode())
    {
      col = db.admin().createCollection(colName, null);
    } else
    {
      col = db.admin().createCollection(colName, mDoc);
    }
    
    OracleDocument doc = null, filterDoc = null;
    HashSet<String> expectedKeys = new HashSet<String>();
    
    String docStr1 = "{\"a\":{\"b\":{\"upper_str\":\"AAA\", \"lower_str\":\"aaa\", \"mixed_str\":\"FooBar001\" }}}";
    String docStr2 = "{\"a\":{\"b\":{\"upper_str\":\"ABC\", \"lower_str\":\"abc\", \"mixed_str\":\"fooBAR002\" }}}";
    String docStr3 = "{\"a\":{\"b\":{\"upper_str\":\"CCC\", \"lower_str\":\"ccc\", \"mixed_str\":\"FOOBar003\", \"bool\":true, " +
        "\"array\":[\"Hello\", \" World!\"], \"empty_str\":\"\", \"null\":null, \"number\":100 }}}";

    String[] key = new String[3];
    if (isJDCSMode()) 
    {
      doc = col.insertAndGet(db.createDocumentFromString(docStr1));
      key[0] = doc.getKey();
      doc = col.insertAndGet(db.createDocumentFromString(docStr2));
      key[1] = doc.getKey();
      doc = col.insertAndGet(db.createDocumentFromString(docStr3));
      key[2] = doc.getKey();
    } else
    {
      doc = col.insertAndGet(db.createDocumentFromString("id-1001", docStr1));
      key[0] = doc.getKey();
      doc = col.insertAndGet(db.createDocumentFromString("id-1002", docStr2));
      key[1] = doc.getKey();
      doc = col.insertAndGet(db.createDocumentFromString("id-1003", docStr3));
      key[2] = doc.getKey();
    }    
    
    // Test $upper with upper string input
    filterDoc = db.createDocumentFromString("{\"a.b.upper_str\": { \"$upper\": \"AAA\"}}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[0], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $lower with upper string input
    filterDoc = db.createDocumentFromString("{\"a.b.upper_str\": { \"$lower\": \"aaa\"}}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[0], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $upper with lower string input
    filterDoc = db.createDocumentFromString("{\"a.b.lower_str\": { \"$upper\": {\"$eq\":\"ABC\"} }}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[1], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $lower with lower string input
    filterDoc = db.createDocumentFromString("{\"a.b.lower_str\": { \"$lower\": {\"$eq\":\"abc\"} }}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[1], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $upper with mixed case string input
    filterDoc = db.createDocumentFromString("{\"a.b.mixed_str\": { \"$upper\": {\"$gt\":\"FOOBAR002\"} }}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[2], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $lower with mixed case string input
    filterDoc = db.createDocumentFromString("{\"a.b.mixed_str\": { \"$lower\": {\"$gt\":\"foobar002\"} }}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[2], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $upper with $ne
    filterDoc = db.createDocumentFromString("{\"a.b.mixed_str\": { \"$upper\": {\"$ne\":\"FOOBar003\"} }}");
    assertEquals(3, col.find().filter(filterDoc).count());
    
    // Test $lower with $ne
    filterDoc = db.createDocumentFromString("{\"a.b.mixed_str\": { \"$lower\": {\"$ne\":\"FooBar001\"} }}");
    assertEquals(3, col.find().filter(filterDoc).count());
    
    // Test $upper with $lt
    filterDoc = db.createDocumentFromString("{\"a.b.mixed_str\": { \"$upper\": {\"$lt\":\"FOOBAR002\"} }}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[0], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $lower with $lt
    filterDoc = db.createDocumentFromString("{\"a.b.mixed_str\": { \"$lower\": {\"$lt\":\"foobar002\"} }}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[0], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $upper with $gte
    filterDoc = db.createDocumentFromString("{\"a.b.mixed_str\": { \"$upper\": {\"$gte\":\"FOOBAR003\"} }}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[2], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $lower with $gte
    filterDoc = db.createDocumentFromString("{\"a.b.mixed_str\": { \"$lower\": {\"$gte\":\"foobar003\"} }}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[2], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $upper with $lte
    filterDoc = db.createDocumentFromString("{\"a.b.mixed_str\": { \"$upper\": {\"$lte\":\"FOOBAR001\"} }}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[0], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $lower with $lte
    filterDoc = db.createDocumentFromString("{\"a.b.mixed_str\": { \"$lower\": {\"$lte\":\"foobar001\"} }}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[0], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $upper with $startsWith
    filterDoc = db.createDocumentFromString("{\"a.b.mixed_str\": { \"$upper\": {\"$startsWith\":\"FOOBAR00\"} }}");
    assertEquals(3, col.find().filter(filterDoc).count());
    
    // Test $lower with $startsWith
    filterDoc = db.createDocumentFromString("{\"a.b.mixed_str\": { \"$lower\": {\"$startsWith\":\"foobar00\"} }}");
    assertEquals(3, col.find().filter(filterDoc).count());
    
    // Test $upper with $regex
    filterDoc = db.createDocumentFromString("{\"a.b.mixed_str\": { \"$upper\": {\"$regex\":\"FOOB.*\"} }}");
    assertEquals(3, col.find().filter(filterDoc).count());
    
    // Test $lower with $regex
    filterDoc = db.createDocumentFromString("{\"a.b.mixed_str\": { \"$lower\": {\"$regex\":\"foob.*\"} }}");
    assertEquals(3, col.find().filter(filterDoc).count());
    
    // Test $upper with array input
    filterDoc = db.createDocumentFromString("{\"a.b.array\": { \"$upper\": \"HELLO\" }}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[2], col.find().filter(filterDoc).getOne().getKey());
    
     // Test $lower with array input
    filterDoc = db.createDocumentFromString("{\"a.b.array\": { \"$lower\": \" world!\" }}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[2], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $upper with boolean input
    filterDoc = db.createDocumentFromString("{\"a.b.bool\": { \"$upper\": \"TRUE\" }}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[2], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $lower with boolean input
    filterDoc = db.createDocumentFromString("{\"a.b.bool\": { \"$lower\": \"true\" }}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[2], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $upper with null input
    filterDoc = db.createDocumentFromString("{\"a.b.null\": { \"$upper\": \"NULL\" }}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[2], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $lower with null input
    filterDoc = db.createDocumentFromString("{\"a.b.null\": { \"$lower\": \"null\" }}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[2], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $upper with empty string input
    filterDoc = db.createDocumentFromString("{\"a.b.empty_str\": { \"$upper\": \"\" }}");
    assertEquals(0, col.find().filter(filterDoc).count());
    
    // Test $lower with empty string input
    filterDoc = db.createDocumentFromString("{\"a.b.empty_str\": { \"$lower\": \"\" }}");
    assertEquals(0, col.find().filter(filterDoc).count());
    
    // Test $upper with number input
    filterDoc = db.createDocumentFromString("{\"a.b.number\": { \"$upper\": \"100\" }}");
    assertEquals(0, col.find().filter(filterDoc).count());
    
    // Test $lower with number input
    filterDoc = db.createDocumentFromString("{\"a.b.number\": { \"$lower\": \"100\" }}");
    assertEquals(0, col.find().filter(filterDoc).count());
    
    // Test $lower with non-string operand
    // ### Bug: static typing was added in 18, so this only
    // gives the expected wrong types error in 18 or above
    if (!SODAUtils.sqlSyntaxBelow_18(sqlSyntaxLevel)) {
      filterDoc = db.createDocumentFromString("{\"a.b.mixed_str\": {\"$lower\": 101 } }");
      try {
        col.find().filter(filterDoc).getOne();
        fail("No exception when $lower's operand is non string");
      } catch (OracleException e) {
        // blocked by bug27021099: Java layer should catch such invalid spec
        if (e.getCause() instanceof SQLException) {
          SQLException sqlException = (SQLException) e.getCause();
          assertTrue(sqlException.getMessage().contains("ORA-40442"));
        }
        //QueryException queryException = (QueryException) e.getCause();
        //assertEquals("", queryException.getMessage());
      }
    }

    // Test $upper with non-string operand
    // ### Bug: static typing was added in 18, so this only
    // gives the expected wrong types error in 18 or above
    if (!SODAUtils.sqlSyntaxBelow_18(sqlSyntaxLevel)) {
      filterDoc = db.createDocumentFromString("{\"a.b.mixed_str\": {\"$upper\": true } }");
      try {
        col.find().filter(filterDoc).getOne();
        fail("No exception when $upper's operand is non string");
      } catch (OracleException e) {
        // blocked by bug27021099: Java layer should catch such invalid spec
        if (e.getCause() instanceof SQLException) {
          SQLException sqlException = (SQLException) e.getCause();
          assertTrue(sqlException.getMessage().contains("ORA-40597"));
        }
        //QueryException queryException = (QueryException) e.getCause();
        //assertEquals("", queryException.getMessage());
      }
    }
    
    // Test $lower wrapped by $string
    filterDoc = db.createDocumentFromString("{\"a.b.number\": {\"$string\": {\"$lower\": \"100\"}}}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $string operator wraps $lower operator.");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      String expMsg = "$string operator cannot wrap $lower operator. ";
      assertEquals(expMsg, queryException.getMessage());
    }
    
    // Test $upper wrapped by $string
    filterDoc = db.createDocumentFromString("{\"a.b.number\": {\"$string\": {\"$upper\": \"100\"}}}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $string operator wraps $upper operator.");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      String expMsg = "$string operator cannot wrap $upper operator. ";
      assertEquals(expMsg, queryException.getMessage());
    }
    
  }
  
  // Tests with $ceiling and $floor
  public void testCeilingAndFloorClob() throws Exception {
    testCeilingAndFloor("CLOB");
  }
  
  public void testCeilingAndFloorBlob() throws Exception {
    testCeilingAndFloor("BLOB");
  }
  
  public void testCeilingAndFloorVarchar2() throws Exception {
    testCeilingAndFloor("VARCHAR2");
  }
  
  private void testCeilingAndFloor(String contentColumnType) throws Exception {
    if (isJDCSMode())
      if (!contentColumnType.equalsIgnoreCase("BLOB"))
        return;

    if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel))
      return;
    
    OracleDocument mDoc = client.createMetadataBuilder().keyColumnAssignmentMethod("CLIENT")
      .contentColumnType(contentColumnType).build();
    
    String colName = "testCeilingAndFloor" + contentColumnType;
    OracleCollection col;
    if (isJDCSMode())
    {
      col = db.admin().createCollection(colName, null);
    } else
    {
      col = db.admin().createCollection(colName, mDoc);
    }
    
    OracleDocument doc = null, filterDoc = null;
    
    String docStr1 = "{\"a\":{\"b\":{\"int\":7, \"float\":7.14 }}}";
    String docStr2 = "{\"a\":{\"b\":{\"int\":8, \"float\":8.33 }}}";
    String docStr3 = "{\"a\":{\"b\":{\"int\":9.0, \"float\":9.55, \"bool\":false, \"array\":[1.2, 1.8, 2.4], " +
        "\"empty_str\":\"\", \"null\":null, \"string\":\"abc\" }}}";

    String[] key = new String[3];
    if (isJDCSMode()) 
    {
      doc = col.insertAndGet(db.createDocumentFromString(docStr1));
      key[0] = doc.getKey();
      doc = col.insertAndGet(db.createDocumentFromString(docStr2));
      key[1] = doc.getKey();
      doc = col.insertAndGet(db.createDocumentFromString(docStr3));
      key[2] = doc.getKey();
    } else
    {
      doc = col.insertAndGet(db.createDocumentFromString("id-1001", docStr1));
      key[0] = doc.getKey();
      doc = col.insertAndGet(db.createDocumentFromString("id-1002", docStr2));
      key[1] = doc.getKey();
      doc = col.insertAndGet(db.createDocumentFromString("id-1003", docStr3));
      key[2] = doc.getKey();
    }    
    
    // Test $ceiling with integer input
    filterDoc = db.createDocumentFromString("{\"a.b.int\": { \"$ceiling\": 7}}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[0], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $floor with integer input
    filterDoc = db.createDocumentFromString("{\"a.b.int\": { \"$floor\": 9}}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[2], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $ceiling with float input
    filterDoc = db.createDocumentFromString("{\"a.b.float\": { \"$ceiling\": {\"$eq\":8} }}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[0], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $floor with float input
    filterDoc = db.createDocumentFromString("{\"a.b.float\": { \"$floor\": {\"$eq\":9} }}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[2], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $ceiling with $ne
    filterDoc = db.createDocumentFromString("{\"a.b.float\": { \"$ceiling\": {\"$ne\":7} }}");
    assertEquals(3, col.find().filter(filterDoc).count());
    
    // Test $floor with $ne
    filterDoc = db.createDocumentFromString("{\"a.b.float\": { \"$floor\": {\"$ne\":10} }}");
    assertEquals(3, col.find().filter(filterDoc).count());
    
    // Test $ceiling with $gt
    filterDoc = db.createDocumentFromString("{\"a.b.float\": { \"$ceiling\": {\"$gt\":9} }}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[2], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $floor with $gt
    filterDoc = db.createDocumentFromString("{\"a.b.float\": { \"$floor\": {\"$gt\":8} }}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[2], col.find().filter(filterDoc).getOne().getKey());

    // Test $ceiling with $gte
    filterDoc = db.createDocumentFromString("{\"a.b.float\": { \"$ceiling\": {\"$gte\":10} }}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[2], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $floor with $gte
    filterDoc = db.createDocumentFromString("{\"a.b.float\": { \"$floor\": {\"$gte\":9} }}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[2], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $ceiling with $lt
    filterDoc = db.createDocumentFromString("{\"a.b.float\": { \"$ceiling\": {\"$lt\":9} }}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[0], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $floor with $lt
    filterDoc = db.createDocumentFromString("{\"a.b.float\": { \"$floor\": {\"$lt\":8} }}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[0], col.find().filter(filterDoc).getOne().getKey());
    
   // Test $ceiling with $lte
    filterDoc = db.createDocumentFromString("{\"a.b.float\": { \"$ceiling\": {\"$lte\":8.0 } }}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[0], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $floor with $lte
    filterDoc = db.createDocumentFromString("{\"a.b.float\": { \"$floor\": {\"$lte\":7.0 } }}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[0], col.find().filter(filterDoc).getOne().getKey());

    // Test $ceiling with array input
    filterDoc = db.createDocumentFromString("{\"a.b.array\": { \"$ceiling\": 3 }}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[2], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $floor with array input
    filterDoc = db.createDocumentFromString("{\"a.b.array\": { \"$floor\": 1 }}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[2], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $ceiling with boolean input (the result is still TBD)
    filterDoc = db.createDocumentFromString("{\"a.b.bool\": { \"$ceiling\": {\"$eq\": 0 }}}");
    assertEquals(0, col.find().filter(filterDoc).count());
    
    // Test $floor with boolean input (the result is still TBD)
    filterDoc = db.createDocumentFromString("{\"a.b.bool\": { \"$floor\": {\"$eq\": 0 }}}");
    assertEquals(0, col.find().filter(filterDoc).count());
 
    // Test $ceiling with invalid string input
    filterDoc = db.createDocumentFromString("{\"a.b.string\": { \"$ceiling\": {\"$eq\": 0 }}}");
    assertEquals(0, col.find().filter(filterDoc).count());
    
    // Test $floor with invalid string input
    filterDoc = db.createDocumentFromString("{\"a.b.string\": { \"$floor\": {\"$eq\": 0 }}}");
    assertEquals(0, col.find().filter(filterDoc).count());
    
    // Test $ceiling with empty string input
    filterDoc = db.createDocumentFromString("{\"a.b.empty_str\": { \"$ceiling\": {\"$eq\": 0 }}}");
    assertEquals(0, col.find().filter(filterDoc).count());
    
    // Test $floor with empty string input
    filterDoc = db.createDocumentFromString("{\"a.b.empty_str\": { \"$floor\": {\"$eq\": 0 }}}");
    assertEquals(0, col.find().filter(filterDoc).count());
    
    // Test $ceiling with null input
    filterDoc = db.createDocumentFromString("{\"a.b.null\": { \"$ceiling\": {\"$eq\": 0 }}}");
    assertEquals(0, col.find().filter(filterDoc).count());
    
    // Test $floor with null input
    filterDoc = db.createDocumentFromString("{\"a.b.null\": { \"$floor\": {\"$eq\": 0 }}}");
    assertEquals(0, col.find().filter(filterDoc).count());
    
    // Test $ceiling with object input
    filterDoc = db.createDocumentFromString("{\"a.b\": { \"$ceiling\": {\"$lte\": 1 }}}");
    assertEquals(0, col.find().filter(filterDoc).count());
    
    // Test $floor with object input
    filterDoc = db.createDocumentFromString("{\"a.b\": { \"$floor\": {\"$lte\": 1 }}}");
    assertEquals(0, col.find().filter(filterDoc).count());
    
    // Test $ceiling wrapped by $number
    filterDoc = db.createDocumentFromString("{\"a.b.string\": {\"$number\": {\"$ceiling\": 0}}}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $number operator wraps $ceiling operator.");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      String expMsg = "$number operator cannot wrap $ceiling operator. ";
      assertEquals(expMsg, queryException.getMessage());
    }
    
    // Test $floor wrapped by $number
    filterDoc = db.createDocumentFromString("{\"a.b.string\": {\"$number\": {\"$floor\": 0}}}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $number operator wraps $floor operator.");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      String expMsg = "$number operator cannot wrap $floor operator. ";
      assertEquals(expMsg, queryException.getMessage());
    }
    
  }
  
  // Tests with $abs
  public void testAbsClob() throws Exception {
    testAbs("CLOB");
  }
  
  public void testAbsBlob() throws Exception {
    testAbs("BLOB");
  }
  
  public void testAbsVarchar2() throws Exception {
    testAbs("VARCHAR2");
  }
  
  private void testAbs(String contentColumnType) throws Exception {
    if (isJDCSMode())
      if (!contentColumnType.equalsIgnoreCase("BLOB"))
        return;

    if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel))
      return;
    
    OracleDocument mDoc = client.createMetadataBuilder().keyColumnAssignmentMethod("CLIENT")
      .contentColumnType(contentColumnType).build();
    
    String colName = "testAbs" + contentColumnType;
    OracleCollection col;
    if (isJDCSMode())
    {
      col = db.admin().createCollection(colName, null);
    } else
    {
      col = db.admin().createCollection(colName, mDoc);
    }
    
    OracleDocument doc = null, filterDoc = null;
    
    String docStr1 = "{\"a\":{\"b\":{\"pos\":3.2, \"neg\":-3.8 }}}";
    String docStr2 = "{\"a\":{\"b\":{\"pos\":4.5, \"neg\":-4.7 }}}";
    String docStr3 = "{\"a\":{\"b\":{\"pos\":7.77, \"neg\":-5.23, \"zero\":0, \"bool\":true, " +
        "\"array\":[-1.2, 1.8, -2.4], \"number_str\":\"-2.5\", \"non_number_str\":\"abc\" }}}";

    String[] key = new String[3];
    if (isJDCSMode()) 
    {
      doc = col.insertAndGet(db.createDocumentFromString(docStr1));
      key[0] = doc.getKey();
      doc = col.insertAndGet(db.createDocumentFromString(docStr2));
      key[1] = doc.getKey();
      doc = col.insertAndGet(db.createDocumentFromString(docStr3));
      key[2] = doc.getKey();
    } else
    {
      doc = col.insertAndGet(db.createDocumentFromString("id-1001", docStr1));
      key[0] = doc.getKey();
      doc = col.insertAndGet(db.createDocumentFromString("id-1002", docStr2));
      key[1] = doc.getKey();
      doc = col.insertAndGet(db.createDocumentFromString("id-1003", docStr3));
      key[2] = doc.getKey();
    }    
    
    // Test $abs with positive input and $gt
    filterDoc = db.createDocumentFromString("{\"a.b.pos\": { \"$abs\": {\"$gt\": 5.2}}}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[2] , col.find().filter(filterDoc).getOne().getKey());
    
    // Test $abs with negative input and $eq
    filterDoc = db.createDocumentFromString("{\"a.b.neg\": { \"$abs\": {\"$eq\": 3.80}}}");
    // ### Wrong result on 12.2.0.1
    if (!SODAUtils.sqlSyntaxBelow_18(sqlSyntaxLevel)) {
      assertEquals(1, col.find().filter(filterDoc).count());
      assertEquals(key[0] , col.find().filter(filterDoc).getOne().getKey());
    
      // Test $abs with zero input
      filterDoc = db.createDocumentFromString("{\"a.b.zero\": { \"$abs\": 0 }}");
      assertEquals(1, col.find().filter(filterDoc).count());
      assertEquals(key[2] , col.find().filter(filterDoc).getOne().getKey());
    }
    
    // Test $abs with $ne
    filterDoc = db.createDocumentFromString("{\"a.b.neg\": { \"$abs\": {\"$ne\":-3.8} }}");
    assertEquals(3, col.find().filter(filterDoc).count());
    
    // Test $abs with $gte
    filterDoc = db.createDocumentFromString("{\"a.b.neg\": { \"$abs\": {\"$gte\": 5.23}}}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[2], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $abs with $lt
    filterDoc = db.createDocumentFromString("{\"a.b.neg\": { \"$abs\": {\"$lt\": 3.9}}}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[0], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $abs with $lte
    // ### Wrong result on 12.2.0.1
    if (!SODAUtils.sqlSyntaxBelow_18(sqlSyntaxLevel)) {
      filterDoc = db.createDocumentFromString("{\"a.b.neg\": { \"$abs\": {\"$lte\": 3.8}}}");
      assertEquals(1, col.find().filter(filterDoc).count());
      assertEquals(key[0], col.find().filter(filterDoc).getOne().getKey());

      // Test $abs with array input 
      filterDoc = db.createDocumentFromString("{\"a.b.array\": { \"$abs\":1.2 }}");
      assertEquals(1, col.find().filter(filterDoc).count());
      assertEquals(key[2] , col.find().filter(filterDoc).getOne().getKey());
    
      filterDoc = db.createDocumentFromString("{\"a.b.array\": { \"$abs\":1.8 }}");
      assertEquals(1, col.find().filter(filterDoc).count());
      assertEquals(key[2], col.find().filter(filterDoc).getOne().getKey());
    
      filterDoc = db.createDocumentFromString("{\"a.b.array\": { \"$abs\":2.4 }}");
      assertEquals(1, col.find().filter(filterDoc).count());
      assertEquals(key[2], col.find().filter(filterDoc).getOne().getKey());
    }
    
    // Test $abs with boolean input
    filterDoc = db.createDocumentFromString("{\"a.b.bool\": { \"$abs\": {\"$gt\":0} }}");
    assertEquals(0, col.find().filter(filterDoc).count());
    
    // Test $abs with string input
    filterDoc = db.createDocumentFromString("{\"a.b.number_str\": { \"$abs\": 2.5 }}");
    assertEquals(0, col.find().filter(filterDoc).count());
    
    filterDoc = db.createDocumentFromString("{\"a.b.non_number_str\": {\"$abs\":{\"$gte\":0}} }");
    assertEquals(0, col.find().filter(filterDoc).count());
    
    // Test $abs with object string
    filterDoc = db.createDocumentFromString("{\"a\": { \"$abs\": {\"$gt\":0} }}");
    assertEquals(0, col.find().filter(filterDoc).count());
    
    // Test $abs wrapped by $number
    filterDoc = db.createDocumentFromString("{\"a.b.pos\": {\"$number\": {\"$abs\": 0}}}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $number operator wraps $abs operator.");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      String expMsg = "$number operator cannot wrap $abs operator. ";
      assertEquals(expMsg, queryException.getMessage());
    }

  }
  
  // Tests with $type
  public void testTypeClob() throws Exception {
    testType("CLOB");
  }
  
  public void testTypeBlob() throws Exception {
    testType("BLOB");
  }
  
  public void testTypeVarchar2() throws Exception {
    testType("VARCHAR2");
  }
  
  private void testType(String contentColumnType) throws Exception {
    if (isJDCSMode())
      if (!contentColumnType.equalsIgnoreCase("BLOB"))
        return;

    if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel))
      return;
    
    OracleDocument mDoc = client.createMetadataBuilder().keyColumnAssignmentMethod("CLIENT")
      .contentColumnType(contentColumnType).build();
    
    String colName = "testType" + contentColumnType;
    OracleCollection col;
    if (isJDCSMode())
    {
      col = db.admin().createCollection(colName, null);
    } else
    {
      col = db.admin().createCollection(colName, mDoc);
    }
    
    OracleDocument doc = null, filterDoc = null;
    HashSet<String> expectedKeys = new HashSet<String>();
    
    String docStr1 = "{\"a\":{\"b\":{\"string\":\"abcd\", \"number\":-3.8, \"bool\":true }}}";
    String docStr2 = "{\"a\":{\"b\":{\"array\":[1, true, \"abc\"], \"null\":null }}}";

    String[] key = new String[2];
    if (isJDCSMode()) 
    {
      doc = col.insertAndGet(db.createDocumentFromString(docStr1));
      key[0] = doc.getKey();
      doc = col.insertAndGet(db.createDocumentFromString(docStr2));
      key[1] = doc.getKey();
    } else
    {
      doc = col.insertAndGet(db.createDocumentFromString("id-1001", docStr1));
      key[0] = doc.getKey();
      doc = col.insertAndGet(db.createDocumentFromString("id-1002", docStr2));   
      key[1] = doc.getKey();   
    }      
    
    // Test $type with string input
    filterDoc = db.createDocumentFromString("{\"a.b.string\": {\"$type\":\"string\"} }");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[0], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $type with number input
    filterDoc = db.createDocumentFromString("{\"a.b.number\": {\"$type\":\"number\"} }");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[0], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $type with boolean input
    filterDoc = db.createDocumentFromString("{\"a.b.bool\": {\"$type\": \"boolean\" } }");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[0], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $type with null input
    filterDoc = db.createDocumentFromString("{\"a.b.null\": {\"$type\": \"null\" } }");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[1], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $type with array input
    // Note: the following will test if the field is an array. To
    // test the type of some element of the array, append [*] to the array
    // field step, e.g. a.b.array[*]. See subsequent tests below.
    // ### Bug: type does not work properly on 12.2.0.1
    if (!SODAUtils.sqlSyntaxBelow_18(sqlSyntaxLevel)) {
      filterDoc = db.createDocumentFromString("{\"a.b.array\": {\"$type\": \"array\" } }");
      assertEquals(1, col.find().filter(filterDoc).count());
      assertEquals(key[1], col.find().filter(filterDoc).getOne().getKey());

      filterDoc = db.createDocumentFromString("{\"a.b.array[*]\": {\"$type\": \"number\" } }");
      assertEquals(1, col.find().filter(filterDoc).count());
      assertEquals(key[1], col.find().filter(filterDoc).getOne().getKey());

      filterDoc = db.createDocumentFromString("{\"a.b.array[*]\": {\"$type\": {\"$eq\":\"boolean\"} } }");
      assertEquals(1, col.find().filter(filterDoc).count());
      assertEquals(key[1], col.find().filter(filterDoc).getOne().getKey());

      filterDoc = db.createDocumentFromString("{\"a.b.array[*]\": {\"$type\": {\"$eq\":\"string\"} } }");
      assertEquals(1, col.find().filter(filterDoc).count());
      assertEquals(key[1], col.find().filter(filterDoc).getOne().getKey());
    }
    
    // Test $type with object input 
    filterDoc = db.createDocumentFromString("{\"a\": {\"$type\": \"object\" } }");
    assertEquals(2, col.find().filter(filterDoc).count());
    
    // Test $type with $ne 
    filterDoc = db.createDocumentFromString("{\"a\": {\"$type\": {\"$ne\":\"object\"} } }");
    assertEquals(0, col.find().filter(filterDoc).count());
    
    // Test $type with $upper
    filterDoc = db.createDocumentFromString("{\"a.b.bool\": {\"$type\": {\"$upper\":\"BOOLEAN\"} } }");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $type operator wraps $upper operator.");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      String expMsg = "$type operator cannot wrap $upper operator. ";
      assertEquals(expMsg, queryException.getMessage());
    }
    
    // Test $type with $lower
    filterDoc = db.createDocumentFromString("{\"a.b.null\": {\"$type\": {\"$lower\":\"null\"} } }");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $type operator wraps $lower operator.");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      String expMsg = "$type operator cannot wrap $lower operator. ";
      assertEquals(expMsg, queryException.getMessage());
    }
    
  }
  
  // Tests with $length
  public void testLengthClob() throws Exception {
    testLength("CLOB");
  }
  
  public void testLengthBlob() throws Exception {
    testLength("BLOB");
  }
  
  public void testLengthVarchar2() throws Exception {
    testLength("VARCHAR2");
  }
  
  private void testLength(String contentColumnType) throws Exception {
    if (isJDCSMode())
      if (!contentColumnType.equalsIgnoreCase("BLOB"))
        return;

    if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel))
      return;
    
    OracleDocument mDoc = client.createMetadataBuilder().keyColumnAssignmentMethod("CLIENT")
      .contentColumnType(contentColumnType).build();
    
    String colName = "testLength" + contentColumnType;
    OracleCollection col;
    if (isJDCSMode())
    {
      col = db.admin().createCollection(colName, null);
    } else
    {
      col = db.admin().createCollection(colName, mDoc);
    }
    
    OracleDocument doc = null, filterDoc = null;
    HashSet<String> expectedKeys = new HashSet<String>();
    
    String docStr1 = "{\"a\":{\"b\":{\"c\":\"abcd\" }}}";
    String docStr2 = "{\"a\":{\"b\":{\"c\":\"123456\", \"empty_str\":\"\", \"array\":[\"a\", \"ab\"], " +
        "\"number\":100, \"bool\":true, \"null\":null }}}";

    String[] key = new String[2];
    if (isJDCSMode()) 
    {
      doc = col.insertAndGet(db.createDocumentFromString(docStr1));
      key[0] = doc.getKey();
      doc = col.insertAndGet(db.createDocumentFromString(docStr2));
      key[1] = doc.getKey();
    } else
    {
      doc = col.insertAndGet(db.createDocumentFromString("id-1001", docStr1));
      key[0] = doc.getKey();
      doc = col.insertAndGet(db.createDocumentFromString("id-1002", docStr2));   
      key[1] = doc.getKey();   
    }   
    
    // Test $length with string input
    filterDoc = db.createDocumentFromString("{\"a.b.c\": {\"$length\":4} }");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[0], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $length with $eq
    filterDoc = db.createDocumentFromString("{\"a.b.c\": {\"$length\": {\"$eq\":4} } }");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[0], col.find().filter(filterDoc).getOne().getKey());
    
    filterDoc = db.createDocumentFromString("{\"a.b.c\": {\"$length\": 6 } }");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[1], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $length with $ne
    filterDoc = db.createDocumentFromString("{\"a.b.c\": {\"$length\": {\"$ne\":4} } }");;
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[1], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $length with $gt
    filterDoc = db.createDocumentFromString("{\"a.b.c\": {\"$length\": {\"$gt\":4} } }");;
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[1], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $length with $gte
    filterDoc = db.createDocumentFromString("{\"a.b.c\": {\"$length\": {\"$gte\":6} } }");;
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[1], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $length with $lt
    filterDoc = db.createDocumentFromString("{\"a.b.c\": {\"$length\": {\"$lt\":5} } }");;
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[0], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $length with $lte
    filterDoc = db.createDocumentFromString("{\"a.b.c\": {\"$length\": {\"$lte\":4} } }");;
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[0], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $length with array input
    filterDoc = db.createDocumentFromString("{\"a.b.array\": {\"$length\": 2 } }");;
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[1], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $length with empty string
    filterDoc = db.createDocumentFromString("{\"a.b.empty_str\": {\"$length\": 0 } }");;
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[1], col.find().filter(filterDoc).getOne().getKey());
   
    // Test $length with number input
    filterDoc = db.createDocumentFromString("{\"a.b.number\": {\"$length\": {\"$gte\":0} } }");;
    assertEquals(0, col.find().filter(filterDoc).count());
    
    // Test $length with boolean input
    filterDoc = db.createDocumentFromString("{\"a.b.bool\": {\"$length\": {\"$gte\":0} } }");;
    assertEquals(0, col.find().filter(filterDoc).count());
    
    // Test $length with null input
    filterDoc = db.createDocumentFromString("{\"a.b.null\": {\"$length\": {\"$gte\":0} } }");;
    assertEquals(0, col.find().filter(filterDoc).count());
    
    // Test $length with object input
    filterDoc = db.createDocumentFromString("{\"a.b\": {\"$length\": {\"$gte\":0} } }");;
    assertEquals(0, col.find().filter(filterDoc).count());
    
    // Test $length with $string
    filterDoc = db.createDocumentFromString("{\"a.b.number\": {\"$string\": {\"$length\":3} } }");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $string operator wraps $length operator.");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      String expMsg = "$string operator cannot wrap $length operator. ";
      assertEquals(expMsg, queryException.getMessage());
    }
    
  }
  
//Tests with $size
  public void testSizeClob() throws Exception {
    testSize("CLOB");
  }
  
  public void testSizeBlob() throws Exception {
    testSize("BLOB");
  }
  
  public void testSizeVarchar2() throws Exception {
    testSize("VARCHAR2");
  }
  
  private void testSize(String contentColumnType) throws Exception {
    if (isJDCSMode())
      if (!contentColumnType.equalsIgnoreCase("BLOB"))
        return;

    if (SODAUtils.sqlSyntaxBelow_18(sqlSyntaxLevel)) 
      return;
    
    OracleDocument mDoc = client.createMetadataBuilder().keyColumnAssignmentMethod("CLIENT")
      .contentColumnType(contentColumnType).build();
    
    String colName = "testSize" + contentColumnType;
    OracleCollection col;
    if (isJDCSMode())
    {
      col = db.admin().createCollection(colName, null);
    } else
    {
      col = db.admin().createCollection(colName, mDoc);
    }
    
    OracleDocument doc = null, filterDoc = null;
    HashSet<String> expectedKeys = new HashSet<String>();
    
    String docStr1 = "{\"a\":{\"b\":{\"c\":[1,2,3,4] }}}";
    String docStr2 = "{\"a\":{\"b\":{\"c\":[\"a1\", 102, true], \"empty_str\":\"\", \"string\":\"Hello\", " +
            "\"number\":100, \"bool\":true, \"null\":null, \"empty_array\":[]}}}";

    String[] key = new String[2];
    if (isJDCSMode()) 
    {
      doc = col.insertAndGet(db.createDocumentFromString(docStr1));
      key[0] = doc.getKey();
      doc = col.insertAndGet(db.createDocumentFromString(docStr2));
      key[1] = doc.getKey();
    } else
    {
      doc = col.insertAndGet(db.createDocumentFromString("id-1001", docStr1));
      key[0] = doc.getKey();
      doc = col.insertAndGet(db.createDocumentFromString("id-1002", docStr2));   
      key[1] = doc.getKey();   
    }   
    
    // Test $size with array input
    filterDoc = db.createDocumentFromString("{\"a.b.c\": {\"$size\":4} }");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[0], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $size with $eq
    filterDoc = db.createDocumentFromString("{\"a.b.c\": {\"$size\":{\"$eq\":4}} }");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[0], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $size with $ne
    filterDoc = db.createDocumentFromString("{\"a.b.c\": {\"$size\": {\"$ne\":4} } }");;
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[1], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $size with $gt
    filterDoc = db.createDocumentFromString("{\"a.b.c\": {\"$size\": {\"$gt\":3} } }");;
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[0], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $size with $lt
    filterDoc = db.createDocumentFromString("{\"a.b.c\": {\"$size\": {\"$lt\":4} } }");;
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[1], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $size with $gte
    filterDoc = db.createDocumentFromString("{\"a.b.c\": {\"$size\": {\"$gte\":4} } }");;
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[0], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $size with $lte
    filterDoc = db.createDocumentFromString("{\"a.b.c\": {\"$size\": {\"$lte\":4} } }");;
    assertEquals(2, col.find().filter(filterDoc).count());
    
    // Test $size with empty array
    filterDoc = db.createDocumentFromString("{\"a.b.empty_array\": {\"$size\": 0} }");;
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[1], col.find().filter(filterDoc).getOne().getKey());
    
    // Test $size with non-array input
    filterDoc = db.createDocumentFromString("{\"a.b.empty_str\": {\"$size\": 1 } }");
    assertEquals(1, col.find().filter(filterDoc).count());
    
    filterDoc = db.createDocumentFromString("{\"a.b.number\": {\"$size\": 1 } }");
    assertEquals(1, col.find().filter(filterDoc).count());
    
    filterDoc = db.createDocumentFromString("{\"a.b.bool\": {\"$size\": 1 } }");
    assertEquals(1, col.find().filter(filterDoc).count());
    
    filterDoc = db.createDocumentFromString("{\"a.b.null\": {\"$size\": 1 } }");
    assertEquals(1, col.find().filter(filterDoc).count());
    
    // Test $size with unknown field
    filterDoc = db.createDocumentFromString("{\"a.b.unknown\": {\"$size\": 0 } }");
    assertEquals(0, col.find().filter(filterDoc).count());
    
    filterDoc = db.createDocumentFromString("{\"a.b.unknown\": {\"$size\": 1 } }");
    assertEquals(0, col.find().filter(filterDoc).count());
    
    // Test $size with non-number operand
    filterDoc = db.createDocumentFromString("{\"a.b.c\": {\"$size\": true } }");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $size's operand is non number");
    } catch (OracleException e) {
      // blocked by bug27021099: Java layer should catch such invalid spec
      if (e.getCause() instanceof SQLException) {
        SQLException sqlException = (SQLException) e.getCause();
        assertTrue(sqlException.getMessage().contains("ORA-40597"));
      }
    }
    
    filterDoc = db.createDocumentFromString("{\"a.b.c\": {\"$size\": null } }");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $size's operand is non number");
    } catch (OracleException e) {
      // blocked by bug27021099: Java layer should catch such invalid spec
      if (e.getCause() instanceof SQLException) {
        SQLException sqlException = (SQLException) e.getCause();
        assertTrue(sqlException.getMessage().contains("ORA-40597"));
      }
    }
    
    // Test $size wrapped by $string
    filterDoc = db.createDocumentFromString("{\"a.b.c\": {\"$string\": {\"$size\":3} } }");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $string operator wraps $size operator.");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      String expMsg = "$string operator cannot wrap $size operator. ";
      assertEquals(expMsg, queryException.getMessage());
    }
    
  }
  
}
