/* $Header: xdk/test/txjjson/src/oracle/json/tests/soda/test_StartKey.java /st_xdk_soda1/3 2024/10/16 04:37:58 vemahaja Exp $ */

/* Copyright (c) 2020, 2024, Oracle and/or its affiliates. */

/*
   MODIFIED    (MM/DD/YY)
    jspiegel    01/03/20 - Creation
 */
package oracle.json.tests.soda;

import java.util.ArrayList;
import java.util.List;

import jakarta.json.Json;
import jakarta.json.JsonObject;

import oracle.json.testharness.SodaTestCase;
import oracle.soda.OracleCollection;
import oracle.soda.OracleCursor;
import oracle.soda.OracleDocument;
import oracle.soda.OracleException;
import oracle.soda.OracleOperationBuilder;
import oracle.soda.rdbms.OracleRDBMSMetadataBuilder;
import oracle.soda.rdbms.impl.OracleOperationBuilderImpl;
import oracle.soda.rdbms.impl.OracleDatabaseImpl;

/**
 * @version $Header: xdk/test/txjjson/src/oracle/json/tests/soda/test_StartKey.java /st_xdk_soda1/3 2024/10/16 04:37:58 vemahaja Exp $
 * @author Josh Spiegel [josh.spiegel@oracle.com]
 */
public class test_StartKey extends SodaTestCase {
  
  static final String[] TEXT = new String[] { 
      "Alfa", "Bravo", "Charlie", "Delta", "Echo", "Foxtrot", "Golf Hotel", 
      "India", "Juliett", "Kilo", "Lima", "Mike", "November", "Oscar", "Papa",
      "Quebec", "Romeo", "Sierra", "Tango", "Uniform", "Victor", "Whiskey",
      "X-ray", "Yankee", "Zulu"
  };
  
  public void testAscendingTextServer() throws Exception {
    OracleCollection col = null;
    try
    {
      col = createTextualCollection(false);
    }
    catch (OracleException e)
    {
      if (e.getMessage().contains("oracle.sql.json.OracleJsonFactory class is not available. Ensure the JDBC jar includes oracle.sql.json support.") && !OracleDatabaseImpl.isOracleJsonAvailable())
        return;
    }
    col.admin().createIndex(db.createDocumentFrom("{\"name\" : \"sindex\", \"fields\" : [{\"path\":\"status\", \"order\":\"asc\", \"datatype\":\"string\"}]}"));
    

    OracleOperationBuilderImpl builder = (OracleOperationBuilderImpl)col.find()
                             .filter("{\"status\": {\"$gt\" :\"Foxtrot\"}}")
                             .limit(2);

    String plan = builder.explainPlan("ALL");

    if (is23ButNotDot2() && isCompatibleOrGreater(COMPATIBLE_20)){
      assertTrue(plan.contains("INDEX RANGE SCAN"));}
    else
      assertTrue(plan.contains("INDEX FULL SCAN"));
    
    OracleCursor cursor = builder.getCursor();
    
    List<String> found = new ArrayList<String>();
    // iterate the first two document
    String key = null;
    while (cursor.hasNext()) {
      OracleDocument doc = cursor.next();
      key = doc.getKey();
      found.add(doc.getContentAs(JsonObject.class).getString("status"));
    }
    
    while (true) {
      builder = (OracleOperationBuilderImpl)col.find()
          .filter("{\"status\": {\"$gt\" :\"Foxtrot\"}}")
          .startKey(key, true, false)
          .limit(2);
      
      plan = builder.explainPlan("ALL");
      assertTrue(plan.contains("INDEX RANGE SCAN"));
      
      cursor = builder.getCursor();
      
      if (!cursor.hasNext()) {
        break;
      }
      
      while (cursor.hasNext()) {
        OracleDocument doc = cursor.next();
        key = doc.getKey();
        found.add(doc.getContentAs(JsonObject.class).getString("status"));
      }
    }
    assertEquals(TEXT.length-6, found.size());
    for (int i = 6; i < TEXT.length; i++) {
      assertTrue(found.contains(TEXT[i]));
    }

  }
  
  public void testAscendingTextClient() throws Exception {
    if (isJDCSOrATPMode()) {
      // client assigned keys
      return; 
    }
    OracleCollection col = null;

    try 
    {
      col = createTextualCollection(true);
    }
    catch (OracleException e)
    {
      if (e.getMessage().contains("oracle.sql.json.OracleJsonFactory class is not available. Ensure the JDBC jar includes oracle.sql.json support.") && !OracleDatabaseImpl.isOracleJsonAvailable())
        return;
    }

    OracleCursor cursor = col.find()
                             .startKey(TEXT[0], true, true)
                             .getCursor();
    int i = 0; 
    while (cursor.hasNext()) {
      assertEquals(TEXT[i], cursor.next().getKey());
      i++;
    }
    
    assertEquals(i, TEXT.length);
  }
  
  public void testDescendingTextClient() throws Exception {
    if (isJDCSOrATPMode()) {
      // client assigned keys
      return; 
    }
    OracleCollection col = null;

    try
    {
      col = createTextualCollection(true);
    }
    catch (OracleException e)
    {
      if (e.getMessage().contains("oracle.sql.json.OracleJsonFactory class is not available. Ensure the JDBC jar includes oracle.sql.json support.") && !OracleDatabaseImpl.isOracleJsonAvailable())
        return;
    }

    OracleCursor cursor = col.find()
                             .startKey(TEXT[TEXT.length-1], false, true)
                             .getCursor();
    int i = TEXT.length-1; 
    while (cursor.hasNext()) {
      assertEquals(TEXT[i], cursor.next().getKey());
      i--;
    }
    
    assertEquals(i, -1);
  }
  
  public void testAscendingTextClientByLimitInclusive() throws Exception {
    if (isJDCSOrATPMode()) {
      // client assigned keys
      return; 
    }
    OracleCollection col = null;

    try
    {
      col = createTextualCollection(true);
    }
    catch (OracleException e)
    {
      if (e.getMessage().contains("oracle.sql.json.OracleJsonFactory class is not available. Ensure the JDBC jar includes oracle.sql.json support.") && !OracleDatabaseImpl.isOracleJsonAvailable())
        return;
    }

    iterateText(col, 1, true, true);
    iterateText(col, 2, true, true);
    iterateText(col, 4, true, true);
    iterateText(col, TEXT.length, true, true);
  }
  
  public void testAscendingTextClientByLimitExclusive() throws Exception {
    if (isJDCSOrATPMode()) {
      // client assigned keys
      return; 
    }
    OracleCollection col = null;

    try
    {
      col = createTextualCollection(true);
    }
    catch (OracleException e)
    {
      if (e.getMessage().contains("oracle.sql.json.OracleJsonFactory class is not available. Ensure the JDBC jar includes oracle.sql.json support.") && !OracleDatabaseImpl.isOracleJsonAvailable())
        return;
    }

    iterateText(col, 1, false, true);
    iterateText(col, 2, false, true);
    iterateText(col, 4, false, true);
    iterateText(col, TEXT.length, false, true);
  }

  public void testDescendingTextClientByLimitInclusive() throws Exception {
    if (isJDCSOrATPMode()) {
      // client assigned keys
      return; 
    }
    OracleCollection col = null;

    try
    {
      col = createTextualCollection(true);
    }
    catch (OracleException e)
    {
      if (e.getMessage().contains("oracle.sql.json.OracleJsonFactory class is not available. Ensure the JDBC jar includes oracle.sql.json support.") && !OracleDatabaseImpl.isOracleJsonAvailable())
        return;
    }

    iterateText(col, 1, true, false);
    iterateText(col, 2, true, false);
    iterateText(col, 4, true, false);
    iterateText(col, TEXT.length, true, false);
  }
  
  private void iterateText(OracleCollection col, int batch, boolean inclusive, boolean ascending) throws OracleException {
    int start = ascending ? 0 : TEXT.length-1;
    while (start < TEXT.length && start >= 0) {
      OracleCursor cursor = col.find()
                               .startKey(TEXT[start], ascending, inclusive)
                               .limit(batch)
                               .getCursor();
      if (!inclusive) {
        start = next(start, ascending);
      }
      while (cursor.hasNext()) {
        String key = cursor.next().getKey();
        assertEquals(TEXT[start], key);
        start = next(start, ascending);
      }
    }
    if (ascending)
      assertEquals(start, TEXT.length);
    else
      assertEquals(start, -1);
  }
  
  private int next(int i, boolean ascending) {
    return ascending ? i+1 : i-1;
  }

  private OracleCollection createTextualCollection(boolean clientAssigned) throws Exception {
    OracleCollection col;
    if (clientAssigned) {
      OracleRDBMSMetadataBuilder md = client.createMetadataBuilder();
      md.keyColumnAssignmentMethod("CLIENT");
      col = db.admin().createCollection("col", md.build());
    } else {
      col = db.admin().createCollection("col");
    }
    
    for (String v : TEXT) {
      JsonObject obj = Json.createObjectBuilder().add("status", v).build();
      OracleDocument doc = clientAssigned ? 
          db.createDocumentFrom(v, obj) :
          db.createDocumentFrom(obj);
          
      col.insert(doc);
    }
    return col;
  }
  
}
