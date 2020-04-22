/* Copyright (c) 2016, 2020, Oracle and/or its affiliates. 
All rights reserved.*/

/*
   DESCRIPTION
    OracleOperationBuilder3 includes spatial query and index tests
 */

/**
 *  @author  Vincent Liu  
 */

package oracle.json.tests.soda;

import java.util.HashSet;
import java.sql.SQLException;

import oracle.soda.OracleCollection;
import oracle.soda.OracleCursor;
import oracle.soda.OracleDocument;
import oracle.soda.OracleException;

import oracle.soda.rdbms.impl.OracleOperationBuilderImpl;
import oracle.soda.rdbms.impl.SODAUtils;

import oracle.json.parser.QueryException;

import oracle.json.testharness.SodaTestCase;

public class test_OracleOperationBuilder3 extends SodaTestCase {
  
  String[] columnSqlTypes = {
      "CLOB", "BLOB", "VARCHAR2"
  };
  
  private void checkKeys(OracleCollection col, OracleDocument filterDoc,
    HashSet<String> expectedKeys) throws Exception {
    OracleCursor c = col.find().filter(filterDoc).getCursor();
    HashSet<String> keys = new HashSet<String>();

    while (c.hasNext())
      keys.add(c.next().getKey());

    c.close();

    assertEquals(keys, expectedKeys);
  }
  
  private void chkExplainPlan(OracleOperationBuilderImpl builderImpl, 
      boolean indexUsed, String indexName) throws OracleException
  {
    String plan = null;
    
    if (indexUsed) {
      plan = builderImpl.explainPlan("basic");
      // Note: (?s) allows matching across return lines
      if (!plan.matches("(?s).*DOMAIN INDEX.*"))
      {
        fail("DOMAIN INDEX is not found in explain plan:\n" + plan);
      }
      
      if (!plan.matches("(?s).*" + indexName + ".*"))
      {
        fail(indexName + " is not found in explain plan:\n" + plan);
      }
    }
    
    return;
  }
  
  // tests for $near operator
  public void testSpatialOp1() throws Exception {
    if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel))
      return;

    for (String columnSqlType : columnSqlTypes) {
      testSpatialOp1(columnSqlType, false);
      testSpatialOp1(columnSqlType, true);
    }
  }
  
  private void testSpatialOp1(String contentColumnType, boolean withIndex) throws Exception {
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
    
    String colName = "testSpatialOp1" + contentColumnType + (withIndex?"Idx":"");
    if (withIndex) {
      // to bypass the spatial index restriction: (refer to bug23542273)
      // the table name cannot contain spaces or mixed-case letters in a quoted string.
      colName = colName.toUpperCase();
    }
    
    OracleCollection col = db.admin().createCollection(colName, mDoc);
    
    // Point at Long Beach
    String docStr1 = "{\"location\" : {\"type\": \"Point\", \"coordinates\": [33.7243,-118.1579]} }";

    // LineString near Las Vegas
    String docStr2 = "{\"location\" : {\"type\" : \"LineString\", \"coordinates\" : " +
        "[[36.1290,-115.1037], [35.0869,-114.9499], [36.0846,-115.3234]]} }";
   
    // Polygon near Phoenix
    String docStr3 = "{\"location\" : {\"type\" : \"Polygon\", \"coordinates\" : " +
        "[[[33.4222,-112.0605], [33.3855,-112.2253], [33.3121,-112.1044], [33.3305,-111.8737], " +
        "[33.4222,-112.0605]]]} }";

    String key1, key2, key3;
    OracleDocument filterDoc = null, doc = null;
    OracleCursor cursor = null;

    doc = col.insertAndGet(db.createDocumentFromString(docStr1));
    key1 = doc.getKey();
    doc = col.insertAndGet(db.createDocumentFromString(docStr2));
    key2 = doc.getKey();
    doc = col.insertAndGet(db.createDocumentFromString(docStr3));
    key3 = doc.getKey();
    
    // create spatial index with mixed case index name
    String indexSpec = null, indexName = "locationIndex";
    if (withIndex) {
      indexSpec = "{\"name\" : \"" + indexName + "\", \"spatial\" : \"location\"}";
      col.admin().createIndex(db.createDocumentFromString(indexSpec));
    }
    
    filterDoc = db.createDocumentFromString(
        "{ \"location\" : { \"$near\" : {\n" +
        "    \"$geometry\" : { \"type\" : \"Point\", \"coordinates\" : [34.0162,-118.2019] },\n" +
        "    \"$distance\" : 10, \n" +
        "    \"$unit\"     : \"KM\"} \n" +
        "}}");
    assertEquals(0, col.find().filter(filterDoc).count());
    chkExplainPlan((OracleOperationBuilderImpl)col.find().filter(filterDoc), withIndex, indexName);
    
    // only doc1(Long Beach) is within 50 kilometers of Los Angeles
    filterDoc = db.createDocumentFromString(
        "{ \"location\" : { \"$near\" : {\n" +
        "    \"$geometry\" : { \"type\" : \"Point\", \"coordinates\" : [34.0162,-118.2019] },\n" +
        "    \"$distance\" : 50, \n" +
        "    \"$unit\"     : \"KM\"} \n" +
        "}}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key1, col.find().filter(filterDoc).getOne().getKey());
    chkExplainPlan((OracleOperationBuilderImpl)col.find().filter(filterDoc), withIndex, indexName);

    // doc1(Long Beach) and doc2(Las Vegas) are both within 500 kilometers of Los Angeles
    filterDoc = db.createDocumentFromString(
        "{ \"location\" : { \"$near\" : {\n" +
        "    \"$geometry\" : { \"type\" : \"Point\", \"coordinates\" : [34.0162,-118.2019] },\n" +
        "    \"$distance\" : 500, \n" +
        "    \"$unit\"     : \"KM\"} \n" +
        "}}");
    assertEquals(2, col.find().filter(filterDoc).count());
    HashSet<String> expectedKeys = new HashSet<String>();
    expectedKeys.add(key1);
    expectedKeys.add(key2);
    checkKeys(col, filterDoc, expectedKeys);
    chkExplainPlan((OracleOperationBuilderImpl)col.find().filter(filterDoc), withIndex, indexName);
    
    // all 3 are within 800 kilometers of Los Angeles
    filterDoc = db.createDocumentFromString(
        "{ \"location\" : { \"$near\" : {\n" +
        "    \"$geometry\" : { \"type\" : \"Point\", \"coordinates\" : [34.0162,-118.2019] },\n" +
        "    \"$distance\" : 800, \n" +
        "    \"$unit\"     : \"KM\"} \n" +
        "}}");
    assertEquals(3, col.find().filter(filterDoc).count());
    chkExplainPlan((OracleOperationBuilderImpl)col.find().filter(filterDoc), withIndex, indexName);
    
    // Test with the path containing array step
    filterDoc = db.createDocumentFromString(
        "{ \"location[0]\" : { \"$near\" : {\n" +
        "    \"$geometry\" : { \"type\" : \"Point\", \"coordinates\" : [34.0162,-118.2019] },\n" +
        "    \"$distance\" : 800, \n" +
        "    \"$unit\"     : \"KM\"} \n" +
        "}}");
    assertEquals(3, col.find().filter(filterDoc).count());
    chkExplainPlan((OracleOperationBuilderImpl)col.find().filter(filterDoc), withIndex, indexName);
    
    // all 3 are within 500 miles of Los Angeles
    // when no unit item given, the default, "mile", should be used.
    filterDoc = db.createDocumentFromString(
        "{ \"location\" : { \"$near\" : {\n" +
        "    \"$geometry\" : { \"type\" : \"Point\", \"coordinates\" : [34.0162,-118.2019] },\n" +
        "    \"$distance\" : 500 }\n" +
        "}}");
    assertEquals(3, col.find().filter(filterDoc).count());
    chkExplainPlan((OracleOperationBuilderImpl)col.find().filter(filterDoc), withIndex, indexName);
    
    // test with LineString geometry object
    filterDoc = db.createDocumentFromString(
        "{ \"location\" : { \"$near\" : {\n" +
        "    \"$geometry\" : { \"type\" : \"LineString\", \"coordinates\" : " +
        "    [[34.0162,-118.2019], [37.6142,-117.2680], [36.7565,-119.0917]] },\n" +
        "    \"$distance\" : 250, \n" +
        "    \"$unit\"     : \"KM\"} \n" +
        "}}");
    assertEquals(2, col.find().filter(filterDoc).count());
    expectedKeys.clear();
    expectedKeys.add(key1);
    expectedKeys.add(key2);
    checkKeys(col, filterDoc, expectedKeys);
    chkExplainPlan((OracleOperationBuilderImpl)col.find().filter(filterDoc), withIndex, indexName);
    
    // test with Polygon geometry object
    filterDoc = db.createDocumentFromString(
        "{ \"location\" : { \"$near\" : {\n" +
        "    \"$geometry\" : { \"type\" : \"Polygon\", \"coordinates\" : [[[34.0162,-118.2019], " +
        "    [34.0185,-118.3172], [33.9707,-118.3112], [33.9661,-118.1826], [34.0162,-118.2019]]] },\n" +
        "    \"$distance\" : 20, \n" +
        "    \"$unit\"     : \"KM\"} \n" +
        "}}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key1, col.find().filter(filterDoc).getOne().getKey());
    chkExplainPlan((OracleOperationBuilderImpl)col.find().filter(filterDoc), withIndex, indexName);
  
    // test $near with $not
    filterDoc = db.createDocumentFromString("{ \"location\" : { \"$not\" : {\"$near\" : {\n"
        + "    \"$geometry\" : { \"type\" : \"Point\", \"coordinates\" : [34.0162,-118.2019] },\n"
        + "    \"$distance\" : 500, \n" + "    \"$unit\"     : \"KM\"} \n"
        + "}}}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key3, col.find().filter(filterDoc).getOne().getKey());
 
    // test $near with $and
    filterDoc = db.createDocumentFromString("{ \"$and\" : [ {\"location.type\":\"Point\"}, \n"
        + "{\"location\" : {\"$near\" : {\n"
        + "    \"$geometry\" : { \"type\" : \"Point\", \"coordinates\" : [34.0162,-118.2019] },\n"
        + "    \"$distance\" : 500, \n" + "    \"$unit\"     : \"KM\"} \n"
        + "}}] }");

    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key1, col.find().filter(filterDoc).getOne().getKey());
    chkExplainPlan((OracleOperationBuilderImpl) col.find().filter(filterDoc),withIndex, indexName);
 
    if (withIndex) {
      col.admin().dropIndex(indexName);
    }
  }

  //tests for $intersects operator
  public void testSpatialOp2() throws Exception {
    if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel))
      return;

    for (String columnSqlType : columnSqlTypes) {
      testSpatialOp2(columnSqlType, false);
      testSpatialOp2(columnSqlType, true);
    }
  }
  
  private void testSpatialOp2(String contentColumnType, boolean withIndex) throws Exception {
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
    
    String colName = "testSpatialOp2" + contentColumnType + (withIndex?"Idx":"");
    if (withIndex) {
      // to bypass spatial index restriction(bug23542273):
      // the table name cannot contain spaces or mixed-case letters in a quoted string
      colName = colName.toUpperCase();
    }
    
    OracleCollection col = db.admin().createCollection(colName, mDoc);
    
    String docStr1 = "{\"features\" : {\"type\": \"Feature\", \n" +
        "\"geometry\": {\"type\" : \"LineString\", \"coordinates\": \n" +
        "    [[33.7757,-118.2280],[33.9433,-118.1291], [33.7477,-118.1071]] }, \n" +
        "\"properties\": {\"prop0\" : \"value0\"} \n" +
        "} }";

    String docStr2 = "{\"features\" : {\"type\": \"Feature\", \n" +
        "\"geometry\": {\"type\" : \"Polygon\", \"coordinates\": \n" +
        "    [[[33.8761,-117.5551], [34.0868,-117.4260], [34.0583,-117.1843], " +
        "    [33.9388,-117.2268], [33.8761,-117.5551]]] }, \n" +
        "\"properties\": {\"prop1\" : \"value1\"} \n" +
        "} }";

    String key1, key2;
    OracleDocument filterDoc = null, doc = null;

    doc = col.insertAndGet(db.createDocumentFromString(docStr1));
    key1 = doc.getKey();
    doc = col.insertAndGet(db.createDocumentFromString(docStr2));
    key2 = doc.getKey();
    
    // create spatial index with upper index name
    String indexSpec = null, indexName = "SPATIALINDEX2";
    if (withIndex) {
      indexSpec = "{\"name\" : \"" + indexName + "\", \"spatial\" : \"features.geometry\"}";
      col.admin().createIndex(db.createDocumentFromString(indexSpec));
    }
  
    // test with "Point" type
    filterDoc = db.createDocumentFromString(
        "{ \"features.geometry\" : { \"$intersects\" : {\n" +
        "    \"$geometry\" : { \"type\" : \"Point\", \"coordinates\" : [34.4613,-118.2019] } \n" +
        "}} }");
    
    assertEquals(0, col.find().filter(filterDoc).count());
    chkExplainPlan((OracleOperationBuilderImpl)col.find().filter(filterDoc), withIndex, indexName);
 
    // test with "Polygon" type
    filterDoc = db.createDocumentFromString(
        "{ \"features.geometry\" : { \"$intersects\" : {\n" +
        "    \"$geometry\" : { \"type\" : \"Polygon\", \"coordinates\" : [[[34.0379,-118.2534]," +
        "     [34.0584,-117.9423], [33.8373,-118.3474], [33.8362,-117.9245], [34.0379,-118.2534]]] } \n" +
        "}} }");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key1, col.find().filter(filterDoc).getOne().getKey());
    chkExplainPlan((OracleOperationBuilderImpl)col.find().filter(filterDoc), withIndex, indexName);
    
    // test with "LineString" type
    filterDoc = db.createDocumentFromString(
        "{ \"features.geometry\" : { \"$intersects\" : {\n" +
        "    \"$geometry\" : { \"type\" : \"LineString\", \"coordinates\" : [[34.0379,-118.2534]," +
        "     [33.9091,-118.3983], [33.9468,-117.4054], [34.0572,-117.7514]] } \n" +
        "}} }");
 
    assertEquals(2, col.find().filter(filterDoc).count());
    HashSet<String> expectedKeys = new HashSet<String>();
    expectedKeys.add(key1);
    expectedKeys.add(key2);
    checkKeys(col, filterDoc, expectedKeys);
    chkExplainPlan((OracleOperationBuilderImpl)col.find().filter(filterDoc), withIndex, indexName);

    // test $intersects with implied $and
    filterDoc = db.createDocumentFromString("{ \"features.properties.prop0\" : \"value0\", \n" +
        "\"features.geometry\" : { \"$intersects\" : {\n" +
        "    \"$geometry\" : { \"type\" : \"LineString\", \"coordinates\" : [[34.0379,-118.2534]," +
        "     [33.9091,-118.3983], [33.9468,-117.4054], [34.0572,-117.7514]] } \n" +
        "}} }");

    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key1, col.find().filter(filterDoc).getOne().getKey());
    chkExplainPlan((OracleOperationBuilderImpl) col.find().filter(filterDoc),withIndex, indexName);

    if (withIndex) {
      col.admin().dropIndex(indexName);
    }
  }  
 
  //tests for $within operator
  public void testSpatialOp3() throws Exception {
    if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel))
      return;

    for (String columnSqlType : columnSqlTypes) {
      testSpatialOp3(columnSqlType, false);
      testSpatialOp3(columnSqlType, true);
    }
  }
  
  private void testSpatialOp3(String contentColumnType, boolean withIndex) throws Exception {
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
    
    String colName = "testSpatialOp3" + contentColumnType + (withIndex?"Idx":"");
    if (withIndex) {
      // to bypass spatial index restriction: the table name cannot contain spaces or mixed-case letters in a quoted string
      colName = colName.toUpperCase();
    }
    
    OracleCollection col = db.admin().createCollection(colName, mDoc);
    
    String docStr1 = "{\"object\" : { \n" +
        "\"geometry\": {\"type\" : \"LineString\", \"coordinates\": \n" +
        "    [[33.9331,-118.1277], [33.9001,-118.1277], [33.9331,-118.0277]]}, \n" +
        "\"properties\": {\"prop\" : \"value1\"} \n" +
        "} }";
 
    String docStr2 = "{\"object\" : { \n" +
        "\"geometry\": {\"type\" : \"LineString\", \"coordinates\": \n" +
        "    [[33.7757,-118.2280],[33.9433,-118.1291], [33.7477,-118.1071]] }, \n" +
        "\"properties\": {\"prop\" : \"value2\"} \n" +
        "} }";

    String docStr3 = "{\"object\" : { \n" +
        "\"geometry\": {\"type\" : \"Polygon\", \"coordinates\": \n" +
        "    [[[33.8761,-117.5551], [34.0868,-117.4260], [34.0583,-117.1843], " +
        "    [33.9388,-117.2268], [33.8761,-117.5551]]] }, \n" +
        "\"properties\": {\"prop\" : \"value3\"} \n" +
        "} }";

    String key1, key2, key3;
    OracleDocument filterDoc = null, doc = null;

    doc = col.insertAndGet(db.createDocumentFromString(docStr1));
    key1 = doc.getKey();
    doc = col.insertAndGet(db.createDocumentFromString(docStr2));
    key2 = doc.getKey();
    doc = col.insertAndGet(db.createDocumentFromString(docStr3));
    key3 = doc.getKey();
    
    // create spatial index with lower case index name
    String indexSpec = null, indexName = "spatial_index3";
    if (withIndex) {
      indexSpec = "{\"name\" : \"" + indexName + "\", \"spatial\" : \"object.geometry\"}";
      col.admin().createIndex(db.createDocumentFromString(indexSpec));
    }
   
    // the referred geometry is LineString, so $within return false 
    filterDoc = db.createDocumentFromString(
        "{ \"object.geometry\" : { \"$within\" : {\n" +
        "    \"$geometry\" : { \"type\" : \"LineString\", \"coordinates\" : [[34.0379,-118.2534]," +
        "     [34.0584,-117.9423], [33.8362,-117.9245], [33.8373,-118.3474]] } \n" +
        "}} }");
    
    assertEquals(0, col.find().filter(filterDoc).count());
    chkExplainPlan((OracleOperationBuilderImpl)col.find().filter(filterDoc), withIndex, indexName);

    // the referred geometry contains doc1, and intersects with doc2
    filterDoc = db.createDocumentFromString(
        "{ \"object.geometry\" : { \"$within\" : {\n" +
        "    \"$geometry\" : { \"type\" : \"Polygon\", \"coordinates\" : [[[34.0379,-118.2534]," +
        "     [34.0584,-117.9423], [33.8362,-117.9245], [33.8373,-118.3474], [34.0379,-118.2534]]] } \n" +
        "}} }");
    
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key1, col.find().filter(filterDoc).getOne().getKey());
    chkExplainPlan((OracleOperationBuilderImpl)col.find().filter(filterDoc), withIndex, indexName);
    
    filterDoc = db.createDocumentFromString(
        "{ \"object.geometry\" : { \"$within\" : {\n" +
        "    \"$geometry\" : { \"type\" : \"Polygon\", \"coordinates\" : [[[34.2810,-118.4490]," +
        "     [34.0584,-117.9423], [33.7449,-118.1030], [33.7334,-118.3941], [34.2810,-118.4490]]] } \n" +
        "}} }");
    
    assertEquals(2, col.find().filter(filterDoc).count());
    HashSet<String> expectedKeys = new HashSet<String>();
    expectedKeys.add(key1);
    expectedKeys.add(key2);
    checkKeys(col, filterDoc, expectedKeys);
    chkExplainPlan((OracleOperationBuilderImpl)col.find().filter(filterDoc), withIndex, indexName);

    filterDoc = db.createDocumentFromString(
        "{ \"object.geometry\" : { \"$within\" : {\n" +
        "    \"$geometry\" : { \"type\" : \"Polygon\", \"coordinates\" : [[[34.2810,-118.4490]," +
        "     [34.2061,-117.1060], [33.4887,-117.1554], [33.7334,-118.3941], [34.2810,-118.4490]]] } \n" +
        "}} }");
    
    assertEquals(3, col.find().filter(filterDoc).count());
    expectedKeys = new HashSet<String>();
    expectedKeys.add(key1);
    expectedKeys.add(key2);
    expectedKeys.add(key3);
    checkKeys(col, filterDoc, expectedKeys);
    chkExplainPlan((OracleOperationBuilderImpl)col.find().filter(filterDoc), withIndex, indexName);

    // test $within with implied $and + $not
    filterDoc = db.createDocumentFromString("{ \"object.properties.prop\" : \"value3\", \n" +
        "\"object.geometry\" : { \"$not\" : {\"$within\" : {\n" +
        "    \"$geometry\" : { \"type\" : \"Polygon\", \"coordinates\" : [[[34.2810,-118.4490]," +
        "     [34.0584,-117.9423], [33.7449,-118.1030], [33.7334,-118.3941], [34.2810,-118.4490]]] } \n" +
        "}}} }");

     assertEquals(1, col.find().filter(filterDoc).count());
     assertEquals(key3, col.find().filter(filterDoc).getOne().getKey());
 
    // test $within with $orderby + skip() + limit()
    filterDoc = db.createDocumentFromString("{ \"$query\":{\"object.geometry\" : { \"$within\" : {\n" + 
        "    \"$geometry\" : { \"type\" : \"Polygon\", \"coordinates\" : [[[34.2810,-118.4490]," + 
        "     [34.2061,-117.1060], [33.4887,-117.1554], [33.7334,-118.3941], [34.2810,-118.4490]]] } \n" +
        "}}},  \"$orderby\" : {\"object.properties.prop\":1} }");

    OracleCursor cursor = col.find().filter(filterDoc).skip(1).limit(1).getCursor();
    assertEquals(true, cursor.hasNext());
    assertEquals(key2, cursor.next().getKey());
    assertEquals(false, cursor.hasNext());
    cursor.close();
    chkExplainPlan((OracleOperationBuilderImpl) col.find().filter(filterDoc).skip(1).limit(1),withIndex, indexName);
 
    if (withIndex) {
      col.admin().dropIndex(indexName);
    }
  }

  public void testSpatialNeg() throws Exception {

    if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel))
      return;

    OracleDocument mDoc = null;

    if (isJDCSMode()) {
      // ### replace with new builder once it becomes available
      mDoc = db.createDocumentFromString("{\"keyColumn\":{\"name\":\"ID\",\"sqlType\":\"VARCHAR2\",\"maxLength\":255,\"assignmentMethod\":\"UUID\"},\"contentColumn\":{\"name\":\"JSON_DOCUMENT\",\"sqlType\":\"BLOB\"},\"lastModifiedColumn\":{\"name\":\"LAST_MODIFIED\"},\"versionColumn\":{\"name\":\"VERSION\",\"method\":\"UUID\"},\"creationTimeColumn\":{\"name\":\"CREATED_ON\"},\"readOnly\":false}");
    }
    else {
      mDoc = client.createMetadataBuilder().build();
    }

    String colName = "testSpatialNeg";
    // to bypass spatial index restriction(bug23542273): 
    // the table name cannot contain spaces or mixed-case letters in a quoted string
    colName = colName.toUpperCase();
    OracleCollection col = db.admin().createCollection(colName, mDoc);
    
    OracleDocument filterDoc = null;
   
    // $near's value is null
    filterDoc = db.createDocumentFromString("{ \"object.geometry\" : { \"$near\" : null} }");

    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $near's value is null");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      assertEquals("The components of the $near operator must be objects.", queryException.getMessage());
    }
  
    // missing $geometry object
    filterDoc = db.createDocumentFromString(
      "{ \"object.geometry\" : { \"$near\" : { \"$distance\" : 100}} }");
 
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $geometry is missing");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      assertEquals("Operator $near must have a $geometry field.", queryException.getMessage());
    }
  
    // $geometry value is invalid
    filterDoc = db.createDocumentFromString("{ \"object.geometry\" : { \"$within\" : {\"$geometry\":\"value\"} } }");

    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $geometry value is invalid");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      assertEquals("The components of the $geometry operator must be objects.", queryException.getMessage());
    }
  
    // $intersects's value is not an object
    filterDoc = db.createDocumentFromString("{ \"object.geometry\" : { \"$intersects\" : 100} }");

    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $intersects's value is not an object");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      assertEquals("The components of the $intersects operator must be objects.", queryException.getMessage());
    }

    // $within's value is invalid
    filterDoc = db.createDocumentFromString("{ \"object.geometry\" : { \"$within\" : {\"unkownProp\":\"value\"} } }");

    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $within value is invalid");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      assertEquals("The unkownProp field for operator $within is not recognized.", queryException.getMessage());
    }

    // missing $distance for $near
    filterDoc = db.createDocumentFromString("{ \"object.geometry\" : { \"$near\" : {\n" +
        "    \"$geometry\" : { \"type\" : \"Point\", \"coordinates\" : [34.0162,-118.2019] },\n" +
        "    \"$unit\" : \"KM\"} \n" +
        "}}");

    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $distance is missing for $near");
    } catch (OracleException e) {
      // 24947007:error message needs to be updated.
      QueryException queryException = (QueryException) e.getCause();
      assertEquals("Query syntax error.", queryException.getMessage());
    }
  
    // specified $distance for $intersects
    filterDoc = db.createDocumentFromString("{ \"object.geometry\" : { \"$intersects\" : {\n" + 
        "    \"$geometry\" : {\"type\" : \"Polygon\", \"coordinates\": \n" +
        "    [[[33.9331,-118.1277], [33.9001,-118.1277], [33.9331,-118.0277], [33.9331,-118.1277]]]},\n" +
        "    \"$distance\" : \"1000\"} \n" +
        "}}");

    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $distance is specified for $intersects");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      assertEquals("The $distance field is invalid for the $intersects operator.", queryException.getMessage());
    }

    // specified $distance for $within
    filterDoc = db.createDocumentFromString("{ \"object.geometry\" : { \"$within\" : {\n" +
        "    \"$geometry\" : {\"type\" : \"Polygon\", \"coordinates\": \n" +
        "    [[[33.9331,-118.1277], [33.9001,-118.1277], [33.9331,-118.0277], [33.9331,-118.1277]]]},\n" +
        "    \"$distance\" : \"1000\"} \n" +
        "}}");

    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $distance is specified for $within");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      assertEquals("The $distance field is invalid for the $within operator.", queryException.getMessage());
    }

    // specified $unit for $intersects
    filterDoc = db.createDocumentFromString("{ \"object.geometry\" : { \"$intersects\" : {\n" +
        "    \"$geometry\" : {\"type\" : \"Polygon\", \"coordinates\": \n" +
        "    [[[33.9331,-118.1277], [33.9001,-118.1277], [33.9331,-118.0277], [33.9331,-118.1277]]]},\n" +
        "    \"$unit\" : \"KM\"} \n" +
        "}}");

    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $unit is specified for $intersects");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      assertEquals("The $unit field is invalid for the $intersects operator.", queryException.getMessage());
    }

    // specified $unit for $within
    filterDoc = db.createDocumentFromString("{ \"object.geometry\" : { \"$within\" : {\n" +
        "    \"$geometry\" : {\"type\" : \"Polygon\", \"coordinates\": \n" +
        "    [[[33.9331,-118.1277], [33.9001,-118.1277], [33.9331,-118.0277], [33.9331,-118.1277]]]},\n" +
        "    \"$unit\" : \"KM\"} \n" +
        "}}");

    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $unit is specified for $within");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      assertEquals("The $unit field is invalid for the $within operator.", queryException.getMessage());
    }

    // the value of $distance is invalid
    filterDoc = db.createDocumentFromString("{ \"object.geometry\" : { \"$near\" : {\n" +
        "    \"$geometry\" : { \"type\" : \"Point\", \"coordinates\" : [34.0162,-118.2019] },\n" +
        "    \"$distance\" : \"KM\"} \n" +
        "}}");

    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $distance value is invalid");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      assertEquals("The operand for $distance must be a number.", queryException.getMessage());
    }

    // the value of $unit is invalid
    filterDoc = db.createDocumentFromString("{ \"object.geometry\" : { \"$near\" : {\n" +
        "    \"$geometry\" : { \"type\" : \"Point\", \"coordinates\" : [34.0162,-118.2019] },\n" +
        "    \"$distance\" : 100, \"$unit\": 0 } \n" +
        "}}");

    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $unit value is invalid");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      assertEquals("The operand for $unit must be a string.", queryException.getMessage());
    }
 
    // invalid field
    filterDoc = db.createDocumentFromString("{ \"object.geometry\" : { \"$near\" : {\n" +
        "    \"$geometry\" : { \"type\" : \"Point\", \"coordinates\" : [34.0162,-118.2019] },\n" +
        "    \"$unknownProp\" : \"value1\", \"$distance\" : 1000} \n" +
        "}}");

    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when invalid field is presented");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      assertEquals("The $unknownProp field for operator $near is not recognized.", queryException.getMessage());
    }

    // filter spec is not valid json doc
    filterDoc = db.createDocumentFromString("{ \"object.geometry\" : { \"$near\" : {\n" +
        "    \"$geometry\" : { \"type\" : \"Point\", \"coordinates\" : [34.0162,-118.2019] },\n" +
        "    \"$distance\" : 1000} \n" +
        "} \n" +
        "}}");

    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when filter spec is non json");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Invalid filter condition.", e.getMessage());
    }
  
    // only support "and" relationships at the root level 
    // test $near as part of $or
    filterDoc = db.createDocumentFromString(" { \"$or\" : \n" +
        "[ { \"object.geometry.type\" : \"LineString\" }, \n" +
        "  {\"object.geometry\" : { \"$near\" : {\n" +
        "    \"$geometry\" : { \"type\" : \"Point\", \"coordinates\" : [34.0162,-118.2019] },\n" +
        "    \"$distance\" : 100} }\n" +
        "  } ]}");

    try {
      col.find().filter(filterDoc).count();
      fail("No exception when used spatial operator in $or");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      assertEquals("Spatial operators must only be used at the top level.", queryException.getMessage());
    }
   
    // test $intersects as part of $nor
    filterDoc = db.createDocumentFromString("{\"$nor\": \n" +
        "[ {\"features.properties.prop0\" : \"value0\"}, \n" +
        " {\"features.geometry\" : { \"$intersects\" : {\n" +
        "    \"$geometry\" : { \"type\" : \"Point\", \"coordinates\" : [34.4613,-118.2019] } \n" +
        "}} }] }");
    
    try {
      col.find().filter(filterDoc).count();
      fail("No exception when used spatial operator in $nor");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      assertEquals("Spatial operators must only be used at the top level.", queryException.getMessage());
    }
    
    String docStr1 = "{\"object\" : { \n" +
        "\"geometry\": {\"type\" : \"LineString\", \"coordinates\": \n" +
        "    [[33.7757,-118.2280],[33.9433,-118.1291], [33.7477,-118.1071]] }\n" +
        "} }";

    col.insertAndGet(db.createDocumentFromString(docStr1));

    // Polygon is not closed, so it's invalid
    filterDoc = db.createDocumentFromString("{ \"object.geometry\" : { \"$intersects\" : {\n" +
        "    \"$geometry\" : {\"type\" : \"Polygon\", \"coordinates\": \n" +
        "    [[[34.0162,-118.2019],[34.0584,-117.9423],[33.8373,-118.3474],[33.8362,-117.9245], [34.0379,-118.2534]]] }} \n" +
        "}}");

    try {
      col.find().filter(filterDoc).count();
      // blocked by bug24845118: 
      // only when sva=true or index is created, the validation can be processed
      // fail("No exception when polygon is not closed");
    } catch (OracleException e) {
      // Expect an OracleException
      if (e.getCause() instanceof SQLException) {
        SQLException sqlException = (SQLException) e.getCause();
        // ORA-13348: polygon boundary is not closed
        assertTrue(sqlException.getMessage().contains("ORA-13348"));
      }
    }

    // negative tests for spatial index
    // spatial value in index spec is not a string
    String indexSpec = "{\"name\" : \"INDEX_N1\", \"spatial\" : {\"location\" : \"pathValue\"}}";
 
    try {
      col.admin().createIndex(db.createDocumentFromString(indexSpec));
      fail("No exception when spatial value is not a string");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      assertEquals("Invalid value for property spatial: expected STRING, found OBJECT.", queryException.getMessage());
    }

    String indexName2 = "INDEX_N2";
    indexSpec = "{\"name\" : \"" + indexName2 + "\", \"spatial\" : \"location\" }";
    col.admin().createIndex(db.createDocumentFromString(indexSpec));

    // create duplicated spatial index
    try {
      indexSpec = "{\"name\" : \"INDEX_N2_2\", \"spatial\" : \"location\" }";
      col.admin().createIndex(db.createDocumentFromString(indexSpec));
      fail("No exception when create duplicated spatial index");
    } catch (OracleException e) {
      SQLException sqlException = (SQLException) e.getCause();
      // ORA-29879: cannot create multiple domain indexes on a column list using same indextype
      assertTrue(sqlException.getMessage().contains("ORA-29879"));
    }
    
    // Test duplicate name index on different collection.
    OracleCollection col2 = db.admin().createCollection("testSpatialNeg2", mDoc);
    try {
      String indexSpec2 = "{\"name\" : \"" + indexName2 + "\", \"spatial\" : \"location2\" }";
      col2.admin().createIndex(db.createDocumentFromString(indexSpec2));
      // the error is suppressed by SODA Java layer
      fail("No exception when using the same index name on different collections");
    } catch (OracleException e) {
      assertTrue(e.getMessage().contains("An index with the specified name already exists in the schema."));
    }
    
    col2.admin().drop();
    col.admin().dropIndex(indexName2);

    // Test name field with non-string value.
    indexSpec = "{\"name\" : [\"INDEX_N2_1\", \"INDEX_N2_2\"], \"spatial\" : \"location\" }";
    try {
      col.admin().createIndex(db.createDocumentFromString(indexSpec));
      fail("No exception when the value of name field is non-String.");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      String errMsg = "Invalid value for property index name: expected STRING, found ARRAY.";
      assertEquals(errMsg, queryException.getMessage());
    }
    
  }

  public void testContains() throws Exception {

    if (SODAUtils.sqlSyntaxBelow_12_2(sqlSyntaxLevel))
      return;

    for (String columnSqlType : columnSqlTypes) {
      // $contains works only when index has been enabled
      testContains(columnSqlType, true);
    }
  }
  
  private void testContains(String contentColumnType, boolean withIndex) throws Exception {
    if (isJDCSMode())
      if (!contentColumnType.equalsIgnoreCase("BLOB"))
        return;

    OracleDocument mDoc = client.createMetadataBuilder().contentColumnType(contentColumnType)
        .keyColumnAssignmentMethod("CLIENT").build();
    
    String colName = "testContains" + contentColumnType;
    OracleCollection col;
    if (isJDCSMode())
    {
      col = db.admin().createCollection(colName, null);
    } else
    {
      col = db.admin().createCollection(colName, mDoc);
    }
    
    String indexName = "testContainsIndex";
    if (withIndex) {
      // create full-text DataGuide index
      String dataGuideIndexSpec = "{\"name\" : \"" + indexName + "\", \"dataguide\" : \"on\"}"; 
      col.admin().createIndex(db.createDocumentFromString(dataGuideIndexSpec));
    }
    
    String docStr1 = 
      "{\"family\" : {\"id\":10, \"ages\":[40,38,12], \"address\" : {\"street\" : \"10 Main Street\"}}}";

    String docStr2 = 
      "{\"family\" : {\"id\":11, \"ages\":[42,40,10,5], \"address\" : {\"street\" : \"250 East Street\", \"apt\" : 20}}}";

    String docStr3 = 
      "{\"family\" : {\"id\":12, \"ages\":[25,23], \"address\" : {\"street\" : \"300 Oak Street\", \"apt\" : 10}}}";

    String key1 = "id001", key2 = "id002", key3 = "id003";
    OracleDocument doc, filterDoc;
    HashSet<String> expectedKeys = new HashSet<String>();
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
      doc = col.insertAndGet(db.createDocumentFromString(key1, docStr1));
      key[0] = doc.getKey();
      doc = col.insertAndGet(db.createDocumentFromString(key2, docStr2));
      key[1] = doc.getKey();
      doc = col.insertAndGet(db.createDocumentFromString(key3, docStr3));   
      key[2] = doc.getKey();   
    }   
    
    assertEquals(3, col.find().count());
    
    filterDoc = db.createDocumentFromString("{\"family\" : { \"$contains\" : \"10\" }}");
    assertEquals(3, col.find().filter(filterDoc).count());
    expectedKeys.clear();
    expectedKeys.add(key[0]);
    expectedKeys.add(key[1]);
    expectedKeys.add(key[2]);
    checkKeys(col, filterDoc, expectedKeys);
    chkExplainPlan((OracleOperationBuilderImpl) col.find().filter(filterDoc), withIndex, indexName);
    
    // doc1 has a 10 as number value
    filterDoc = db.createDocumentFromString("{\"family.id\" : { \"$contains\" : \"10\" }}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[0], col.find().filter(filterDoc).getOne().getKey());
    chkExplainPlan((OracleOperationBuilderImpl) col.find().filter(filterDoc), withIndex, indexName);
    
    // doc2 has a 10 in the array of values
    filterDoc = db.createDocumentFromString("{\"family.ages\" : { \"$contains\" : \"10\" }}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[1], col.find().filter(filterDoc).getOne().getKey());
    chkExplainPlan((OracleOperationBuilderImpl) col.find().filter(filterDoc), withIndex, indexName);
    
    // doc1 and doc3 both have a 10 in family.address value
    filterDoc = db.createDocumentFromString("{\"family.address\" : { \"$contains\" : \"10\" }}");
    assertEquals(2, col.find().filter(filterDoc).count());
    expectedKeys.clear();
    expectedKeys.add(key[0]);
    expectedKeys.add(key[2]);
    checkKeys(col, filterDoc, expectedKeys);
    chkExplainPlan((OracleOperationBuilderImpl) col.find().filter(filterDoc), withIndex, indexName);
    
    // only doc3 have a 10 in family.address.apt value
    filterDoc = db.createDocumentFromString("{\"family.address.apt\" : { \"$contains\" : \"10\" }}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[2], col.find().filter(filterDoc).getOne().getKey());
    chkExplainPlan((OracleOperationBuilderImpl) col.find().filter(filterDoc), withIndex, indexName);
    
    // all 3 docs contain "Street"
    filterDoc = db.createDocumentFromString("{\"family\" : { \"$contains\" : \"Street\" }}");
    assertEquals(3, col.find().filter(filterDoc).count());
    chkExplainPlan((OracleOperationBuilderImpl) col.find().filter(filterDoc), withIndex, indexName);
    
    // "reet" does not match "... Street"
    filterDoc = db.createDocumentFromString("{\"family\" : { \"$contains\" : \"reet\" }}");
    assertEquals(0, col.find().filter(filterDoc).count());
    chkExplainPlan((OracleOperationBuilderImpl) col.find().filter(filterDoc), withIndex, indexName);
    
    // "25" matches doc3("ages":[25,23]), but not match doc2("250 East Street")
    filterDoc = db.createDocumentFromString("{\"family\" : { \"$contains\" : \"25\" }}");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[2], col.find().filter(filterDoc).getOne().getKey());
    chkExplainPlan((OracleOperationBuilderImpl) col.find().filter(filterDoc), withIndex, indexName);
    
    // docs have "address" key, but do not contain "address" value
    filterDoc = db.createDocumentFromString("{\"family\" : { \"$contains\" : \"address\" }}");
    assertEquals(0, col.find().filter(filterDoc).count());
    chkExplainPlan((OracleOperationBuilderImpl) col.find().filter(filterDoc), withIndex, indexName);
    
    // no doc has family.postno path
    filterDoc = db.createDocumentFromString("{\"family.postno\" : { \"$contains\" : \"10\" }}");
    assertEquals(0, col.find().filter(filterDoc).count());
    chkExplainPlan((OracleOperationBuilderImpl) col.find().filter(filterDoc), withIndex, indexName);
    
    // test $contains with skip(), limit(), + $orderby

    // blocked by bug 28996376 since 20181130, will uncomment when the bug is fixed.
    if (isJDCSMode())
      return;
          
    filterDoc = db.createDocumentFromString("{\"$query\":{ \"family\":{\"$contains\":\"Street\"}},"+
                                            " \"$orderby\":{\"family.id\":1} }");
    assertEquals(3, col.find().filter(filterDoc).count());
    assertEquals(key[2], col.find().filter(filterDoc).skip(2).limit(1).getOne().getKey());
    chkExplainPlan((OracleOperationBuilderImpl) col.find().filter(filterDoc).skip(2).limit(1), withIndex, indexName);
    
    // test $contains with implied $and
    filterDoc = db.createDocumentFromString("{ \"family.address\" : { \"$contains\" : \"10\" }, \n" +
                                              "\"family.ages\" : { \"$contains\" : \"40\" } }");
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals(key[0], col.find().filter(filterDoc).getOne().getKey());
    chkExplainPlan((OracleOperationBuilderImpl) col.find().filter(filterDoc), withIndex, indexName);
    
    // test when path is empty
    filterDoc = db.createDocumentFromString("{ \"``\" : { \"$contains\" : \"40\" }}");
    assertEquals(2, col.find().filter(filterDoc).count());
    expectedKeys.clear();
    expectedKeys.add(key[0]);
    expectedKeys.add(key[1]);
    checkKeys(col, filterDoc, expectedKeys);
    chkExplainPlan((OracleOperationBuilderImpl) col.find().filter(filterDoc), withIndex, indexName);

    // ### not(contains(...)) doesn't work on 12.2.0.1
    // first query returns 'index not defined' (though index is defined),
    // second query returns wrong results (the condition with $not gets ignored)
    if (!SODAUtils.sqlSyntaxBelow_18(sqlSyntaxLevel)) {
      // test $contains with $not
      filterDoc = db.createDocumentFromString("{ \"family.address\" : { \"$not\": { \"$contains\" : \"10\" }}}");
      assertEquals(1, col.find().filter(filterDoc).count());
      assertEquals(key[1], col.find().filter(filterDoc).getOne().getKey());
      chkExplainPlan((OracleOperationBuilderImpl) col.find().filter(filterDoc), withIndex, indexName);

      // test $contains + $and + ($not + $contains)
      filterDoc = db.createDocumentFromString("{ \"family.address\" : { \"$contains\" : \"10\"}, \n" +
              "\"family.ages\" : { \"$not\" : { \"$contains\" : \"23\" }} }");
      assertEquals(1, col.find().filter(filterDoc).count());
      assertEquals(key[0], col.find().filter(filterDoc).getOne().getKey());
      chkExplainPlan((OracleOperationBuilderImpl) col.find().filter(filterDoc), withIndex, indexName);
    }
    
    // negative tests
    
    // $contains's value is a number
    filterDoc = db.createDocumentFromString("{ \"family.address\" : { \"$contains\" : 10 }}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $contains value is a number");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      // blocked by bug25068141: error message needs to be updated
      // assertEquals("The operand for $contains must be a string.", queryException.getMessage());
    }
    
    // $contains's value is an object
    filterDoc = db.createDocumentFromString("{ \"family.address\" : { \"$contains\" : {\"apt\": 10 }}}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $contains value is an obejct");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      // blocked by bug25068141: error message needs to be updated
      // assertEquals("The operand for $contains must be a string.", queryException.getMessage());
    }
    
    // $contains's value is a bool
    filterDoc = db.createDocumentFromString("{ \"family.address\" : { \"$contains\" : true}}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $contains value is a bool");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      // blocked by bug25068141: error message needs to be updated
      // assertEquals("The operand for $contains must be a string.", queryException.getMessage());
    }
    
    // $contains's value is an array
    filterDoc = db.createDocumentFromString("{ \"family.address\" : { \"$contains\" : [\"val0\", \"val1\", \"val2\"]}}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $contains value is an array");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      // blocked by bug25068141: error message needs to be updated
      // assertEquals("The operand for $contains must be a string.", queryException.getMessage());
    }
    
    // test when $contains's value is null
    filterDoc = db.createDocumentFromString("{ \"family.address\" : { \"$contains\" : null }}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $contains value is null");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      // blocked by bug25068141
      //assertEquals("The operand for $contains must be a string.", queryException.getMessage());
    }
    
    // test when no path is specified
    filterDoc = db.createDocumentFromString("{ \"$contains\" : \"val0\" }");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when no path is specified");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      assertEquals("Operator $contains not expected.", queryException.getMessage());
    }
    
    // test when invalid field exists 
    filterDoc = db.createDocumentFromString("{ \"family.address\" : { \"$contains\" : \"id\", \"$unknownField\": \"val0\"}}");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $unknownField is presented");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      assertEquals("The field name $unknownField is not a recognized operator.", queryException.getMessage());
    }
    
    // test when $contains is part of $or
    filterDoc = db.createDocumentFromString("{ \"$or\": [ {\"family.address\" : { \"$contains\" : \"East\" }}, \n" +
        "{ \"family.ages\" : { \"$contains\" : \"11\" }} ] }");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $contains is not used at the top level.");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      assertEquals("$contains must only be used at the top level.", queryException.getMessage());
    }
    
    // test when $contains is part of $nor
    filterDoc = db.createDocumentFromString("{ \"$nor\": [ {\"family.address\" : { \"$contains\" : \"East\" }}, \n" +
        "{ \"family.ages\" : { \"$contains\" : \"11\" }} ] }");
    try {
      col.find().filter(filterDoc).getOne();
      fail("No exception when $contains is not used at the top level.");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      assertEquals("$contains must only be used at the top level.", queryException.getMessage());
    }
    
    // test $contains without JSON index enabled
    OracleDocument mDoc2 = client.createMetadataBuilder().contentColumnType(contentColumnType).build();
    String colName2 = "testContains2" + contentColumnType;
    OracleCollection col2 = db.admin().createCollection(colName2, mDoc2);
    filterDoc = db.createDocumentFromString("{ \"family.address\" : { \"$contains\" : \"10\" }}");
    try {
      col2.find().filter(filterDoc).getOne();
      fail("No exception when no index is enabled for the collection");
    } catch (OracleException e) {
      SQLException sqlException = (SQLException) e.getCause();
      // ORA-40467: JSON_TEXTCONTAINS() cannot be evaluated without a JSON-enabled context index
      assertTrue(sqlException.getMessage().contains("ORA-40467"));
    }
    col2.admin().drop();
    
    // test $contains with the path containing array step
    filterDoc = db.createDocumentFromString("{ \"family[0].address\" : { \"$contains\" : \"10\" }}");
    try {
      col.find().filter(filterDoc).getOne();
      // blocked by bug25059913: no error when array step path is provided for $contains
      // fail("No exception when the path contains array step");
    } catch (OracleException e) {
      QueryException queryException = (QueryException) e.getCause();
      assertEquals("", queryException.getMessage());
    }
    
    if (withIndex) {
      col.admin().dropIndex(indexName);
    }
  }
    
}
