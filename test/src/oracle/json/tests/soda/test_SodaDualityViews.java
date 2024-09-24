/* $Header: xdk/test/txjjson/src/oracle/json/tests/soda/test_SodaDualityViews.java /st_xdk_soda1/7 2023/11/21 16:33:13 xhutchin Exp $ */

/* Copyright (c) 2022, 2023, Oracle and/or its affiliates. */

/*
   DESCRIPTION
    SodaDualityViews

   PRIVATE CLASSES
    <list of private classes defined - with one-line descriptions>

   NOTES
    <other useful comments, qualifications, etc.>

   MODIFIED    (MM/DD/YY)
    daareval    03/09/23 - Tests varchar pk and raw PK 
    daareval    09/30/22 - Add test for dualityviews
    daareval    09/30/22 - Creation
 */

/**
 *  @version $Header: xdk/test/txjjson/src/oracle/json/tests/soda/test_SodaDualityViews.java /st_xdk_soda1/7 2023/11/21 16:33:13 xhutchin Exp $
 *  @author  daareval
 *  @since   release specific (what release of product did this appear in)
 */

package oracle.json.tests.soda;

import java.io.File;
import java.io.FileInputStream;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.math.BigInteger;

import java.sql.Statement;
import java.sql.SQLException;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import oracle.soda.OracleException;
import oracle.soda.OracleCollection;
import oracle.soda.OracleCursor;
import oracle.soda.OracleCollectionAdmin;
import oracle.soda.OracleDocument;

import oracle.soda.rdbms.impl.OracleOperationBuilderImpl;
import oracle.soda.rdbms.impl.TableCollectionImpl;
import oracle.soda.rdbms.impl.OracleDocumentFragmentImpl;
import oracle.soda.rdbms.impl.OracleDocumentImpl;
import oracle.soda.rdbms.impl.OracleDatabaseImpl;

import oracle.json.testharness.SodaTestCase;

import oracle.sql.json.OracleJsonFactory;
import oracle.sql.json.OracleJsonObject;
import oracle.sql.json.OracleJsonArray;

import java.lang.StringBuilder;

public class test_SodaDualityViews extends SodaTestCase{

   //Insert/Replace/Remove Items for dualv (NUMBER PRIMARY KEY)
   public void testDualvCollectionWithNumberKey() throws Exception {
      if (!isCompatibleOrGreater(COMPATIBLE_23))
      return;

      //Setup dualv
      setupDualityView("NUMBER");

      //Create collection
      OracleCollection col = db.admin().createCollection("mydualvColl", 
         db.createDocumentFromString("{\"dualityViewName\" : \"PROJ_DUALV\"}"));
      assertNotNull(col);
      OracleDocument filterDoc = db.createDocumentFromString("{\"proj_id\" : 6}");

      //Insert document from dualv
      OracleDocument doc = db.createDocumentFromString(
         "{\"proj_id\" : 6, " + 
         "\"proj_name\" : \"DualV_Project\", " + 
         "\"proj_status\" : \"Active\", " + 
         "\"specs\": [{" + 
         "\"spec_id\" : 6, " + 
         "\"spec_name\" : \"DualV_Spec\", " + 
         "\"last_mod\" : \"2022-09-08T14:00:01\"}]}"
      );
      
      OracleDocument insertedDoc = col.insertAndGet(doc);
      assertNotNull(insertedDoc);
      
      long numDocs = col.find().count();
      assertEquals(numDocs, 6);

      //Replace item from dualv
      OracleDocument replaceDoc = db.createDocumentFromString(
         "{\"proj_id\" : 6, " + 
         "\"proj_name\" : \"DualV_Project_Replace\", " + 
         "\"proj_status\" : \"Active\", " + 
         "\"specs\": [{" + 
         "\"spec_id\" : 6, " + 
         "\"spec_name\" : \"DualV_Spec_Replace\", " + 
         "\"last_mod\" : \"2022-09-08T14:00:01\"}]}"
      );

      String sKey = insertedDoc.getKey();
      col.find().key(sKey).replaceOne(replaceDoc);
      numDocs = col.find().filter("{\"proj_name\" : \"DualV_Project_Replace\"}").count();
      assertEquals(numDocs, 1);

      //Remove item from dualv
      numDocs = col.find().count();
      assertEquals(numDocs, 6);

      assertEquals(col.find().filter(filterDoc).remove(), 1);

      numDocs = col.find().count();
      assertEquals(numDocs, 5);

      //Drop dualv
      dropDualityView();
   }  

   //Insert/Replace/Remove Items for dualv (NUMBER PRIMARY KEY)-(OSON)
   public void testDualvCollectionWithNumberKeyOSON() throws Exception {
      if (!isCompatibleOrGreater(COMPATIBLE_23))
      return;

      //Setup dualv
      setupDualityView("NUMBER");

      //Create collection
      OracleJsonFactory factory = new OracleJsonFactory();
      OracleJsonObject objColl = factory.createObject();
      objColl.put("dualityViewName", "PROJ_DUALV");
      OracleCollection col = db.admin().createCollection("mydualvColl", 
         db.createDocumentFrom(objColl));
      assertNotNull(col);

      //Filter document
      OracleJsonObject objFilter = factory.createObject();
      objFilter.put("proj_id", 6); 
      OracleDocument filterDoc = db.createDocumentFrom(objFilter);

      //Insert document from dualv
      OracleJsonObject obj = factory.createObject();
      OracleJsonObject objSpecs = factory.createObject();
      OracleJsonArray  arrSpecs = factory.createArray();
      obj.put("proj_id", 6);
      obj.put("proj_name", "DualV_Project_OSON");
      obj.put("proj_status", "Active");
      objSpecs.put("spec_id", 7);
      objSpecs.put("spec_name", "DualV_Spec_OSON");
      objSpecs.put("last_mod", OffsetDateTime.now(ZoneOffset.UTC));
      arrSpecs.add(objSpecs);
      obj.put("specs", arrSpecs);

      OracleDocument insertedDoc = col.insertAndGet(db.createDocumentFrom(obj));
      assertNotNull(insertedDoc);
      
      long numDocs = col.find().count();
      assertEquals(numDocs, 6);

      //Replace item from dualv
      obj.put("proj_name", "DualV_Project_OSON_REPLACE");
      objSpecs.put("spec_name", "DualV_Spec_OSON_REPLACE");
      objSpecs.put("last_mod", OffsetDateTime.now(ZoneOffset.UTC));
      obj.put("specs", arrSpecs);

      String sKey = insertedDoc.getKey();
      col.find().key(sKey).replaceOne(db.createDocumentFrom(obj));
      numDocs = col.find().filter(filterDoc).count();
      assertEquals(numDocs, 1);

      //Remove item from dualv
      numDocs = col.find().count();
      assertEquals(numDocs, 6);

      assertEquals(col.find().filter(filterDoc).remove(), 1);

      numDocs = col.find().count();
      assertEquals(numDocs, 5);

      //Drop dualv
      dropDualityView();
   }

   //Insert/Replace/Remove Items for dualv (VARCHAR2 PRIMARY KEY)
   public void testDualvCollectionWithVarcharKey() throws Exception {
      if (!isCompatibleOrGreater(COMPATIBLE_23))
      return;

      //Setup dualv
      setupDualityView("VARCHAR2(100)");

      //Create collection
      OracleCollection col = db.admin().createCollection("mydualvColl", 
         db.createDocumentFromString("{\"dualityViewName\" : \"PROJ_DUALV\"}"));
      assertNotNull(col);

      OracleDocument filterDoc = db.createDocumentFromString("{\"proj_id\" : \"006\"}");
      
      //Insert document from dualv
      OracleDocument doc = db.createDocumentFromString(
         "{\"proj_id\" : \"006\", " + 
         "\"proj_name\" : \"DualV_Project\", " + 
         "\"proj_status\" : \"Active\", " + 
         "\"specs\": [{" + 
         "\"spec_id\" : 6, " + 
         "\"spec_name\" : \"DualV_Spec\", " + 
         "\"last_mod\" : \"2021-09-08T14:00:01\"}]}"
      );

      OracleDocument insertedDoc = col.insertAndGet(doc);
      assertNotNull(insertedDoc);
      
      long numDocs = col.find().count();
      assertEquals(numDocs, 6);

      //Replace item from dualv
      OracleDocument replaceDoc = db.createDocumentFromString(
         "{\"proj_id\" : \"006\", " + 
         "\"proj_name\" : \"DualV_Project_Replace\", " + 
         "\"proj_status\" : \"false\", " + 
         "\"specs\": [{" + 
         "\"spec_id\" : 7, " + 
         "\"spec_name\" : \"DualV_Spec_Replace\", " + 
         "\"last_mod\" : \"2021-09-08T14:00:01\"}]}"
      );
      
      String sKey = insertedDoc.getKey();
      col.find().key(sKey).replaceOne(replaceDoc);
      numDocs = col.find().filter("{\"proj_name\" : \"DualV_Project_Replace\"}").count();
      assertEquals(numDocs, 1);

      //Remove item from dualv
      numDocs = col.find().count();
      assertEquals(numDocs, 6);

      assertEquals(col.find().filter(filterDoc).remove(), 1);

      numDocs = col.find().count();
      assertEquals(numDocs, 5);

      //Drop dualv
      dropDualityView();
   }

   //Insert/Replace/Remove Items for dualv (RAW PRIMARY KEY)
   public void testDualvCollectionWithRawKey() throws Exception {
      if (!isCompatibleOrGreater(COMPATIBLE_23))
      return;

      //Setup dualv
      setupDualityView("RAW(2000)");

      OracleJsonFactory factory = new OracleJsonFactory();
      String hexString = "06";
      BigInteger bigInt = new BigInteger(hexString, 16);
      byte[] bytes = bigInt.toByteArray();

      //Create collection
      OracleCollection col = db.admin().createCollection("mydualvColl", 
         db.createDocumentFromString("{\"dualityViewName\" : \"PROJ_DUALV\"}"));
      assertNotNull(col);

      OracleJsonObject objFilter = factory.createObject();
      objFilter.put("proj_id", bytes);
      OracleDocument filterDoc = db.createDocumentFrom(objFilter);

      //Insert document from dualv
      OracleJsonObject obj = factory.createObject();
      OracleJsonObject objSpecs = factory.createObject();
      OracleJsonArray  arrSpecs = factory.createArray();
      obj.put("proj_id", bytes);
      obj.put("proj_name", "DualV_Project");
      obj.put("proj_status", "Active");
      objSpecs.put("spec_id", 6);
      objSpecs.put("spec_name", "DualV_Spec");
      objSpecs.put("last_mod", OffsetDateTime.now(ZoneOffset.UTC));
      arrSpecs.add(objSpecs);
      obj.put("specs", arrSpecs);

      OracleDocument insertedDoc = col.insertAndGet(db.createDocumentFrom(obj));
      assertNotNull(insertedDoc);
      
      long numDocs = col.find().count();
      assertEquals(numDocs, 6);

      //Replace item from dualv
      obj.put("proj_name", "DualV_Project_RAW_REPLACE");
      objSpecs.put("spec_name", "DualV_Spec_RAW_REPLACE");
      objSpecs.put("last_mod", OffsetDateTime.now(ZoneOffset.UTC));
      obj.put("specs", arrSpecs);

      String sKey = insertedDoc.getKey();
      col.find().key(sKey).replaceOne(db.createDocumentFrom(obj));
      numDocs = col.find().filter(filterDoc).count();
      assertEquals(numDocs, 1);

      //Remove item from dualv
      numDocs = col.find().count();
      assertEquals(numDocs, 6);

      assertEquals(col.find().filter(filterDoc).remove(), 1);

      numDocs = col.find().count();
      assertEquals(numDocs, 5);

      //Drop dualv
      dropDualityView();
   }

   //Insert/Replace/Remove Items for dualv (Composite KEY) (NUMBER AND VARCHAR2)
   public void testDualvCollectionWithCompositeKeyNV() throws Exception {
      if (!isCompatibleOrGreater(COMPATIBLE_23))
      return;

      //Setup dualv
      setupDualityViewComposite("NUMBER", "VARCHAR2(100)");

      //Create collection
      OracleCollection col = db.admin().createCollection("mydualvColl", 
         db.createDocumentFromString("{\"dualityViewName\" : \"PROJ_DUALV\"}"));
      assertNotNull(col);
      OracleDocument filterDoc = db.createDocumentFromString("{\"_id\":{ \"proj_id\": 6, \"proj_name\": \"Test with Composite key\" }}");

      //Insert document from dualv
      OracleDocument doc = db.createDocumentFromString(
         "{\"_id\" : { \"proj_id\": 6, \"proj_name\": \"Test with Composite key\" }, " + 
         "\"proj_status\" : \"Active\", " + 
         "\"specs\": [{" + 
         "\"spec_id\" : 6, " + 
         "\"spec_name\" : \"DualV_Spec\", " + 
         "\"last_mod\" : \"2022-09-08T14:00:01\"}]}"
      );
      
      OracleDocument insertedDoc = col.insertAndGet(doc);
      assertNotNull(insertedDoc);
      
      long numDocs = col.find().count();
      assertEquals(numDocs, 6);

      //Replace item from dualv
      OracleDocument replaceDoc = db.createDocumentFromString(
         "{\"_id\" : { \"proj_id\": 6, \"proj_name\": \"Test with Composite key\" }, " + 
         "\"proj_status\" : \"Inactive\", " + 
         "\"specs\": [{" + 
         "\"spec_id\" : 6, " + 
         "\"spec_name\" : \"DualV_Spec_Replace\", " + 
         "\"last_mod\" : \"2022-09-08T14:00:01\"}]}"
      );

      String sKey = insertedDoc.getKey();
      col.find().key(sKey).replaceOne(replaceDoc);
      numDocs = col.find().filter(filterDoc).count();
      assertEquals(numDocs, 1);

      //Remove item from dualv
      numDocs = col.find().count();
      assertEquals(numDocs, 6);

      assertEquals(col.find().filter(filterDoc).remove(), 1);

      numDocs = col.find().count();
      assertEquals(numDocs, 5);

      //Drop dualv
      dropDualityView();
   }

   //Insert/Replace/Remove Items for dualv (Composite KEY) (VARCHAR2 AND VARCHAR2)
   public void testDualvCollectionWithCompositeKeyVV() throws Exception{
      if (!isCompatibleOrGreater(COMPATIBLE_23))
      return;

      //Setup dualv
      setupDualityViewComposite("VARCHAR2(100)", "VARCHAR2(100)");

      //Create collection
      OracleCollection col = db.admin().createCollection("mydualvColl", 
         db.createDocumentFromString("{\"dualityViewName\" : \"PROJ_DUALV\"}"));
      assertNotNull(col);
      OracleDocument filterDoc = db.createDocumentFromString("{\"_id\":{ \"proj_id\": \"006\", \"proj_name\": \"Test with Composite key\" }}");

      //Insert document from dualv
      OracleDocument doc = db.createDocumentFromString(
         "{\"_id\" : { \"proj_id\": \"006\", \"proj_name\": \"Test with Composite key\" }, " + 
         "\"proj_status\" : \"Active\", " + 
         "\"specs\": [{" + 
         "\"spec_id\" : 6, " + 
         "\"spec_name\" : \"DualV_Spec\", " + 
         "\"last_mod\" : \"2022-09-08T14:00:01\"}]}"
      );
      
      OracleDocument insertedDoc = col.insertAndGet(doc);
      assertNotNull(insertedDoc);
      
      long numDocs = col.find().count();
      assertEquals(numDocs, 6);

      //Replace item from dualv
      OracleDocument replaceDoc = db.createDocumentFromString(
         "{\"_id\" : { \"proj_id\": \"006\", \"proj_name\": \"Test with Composite key\" }, " + 
         "\"proj_status\" : \"Inactive\", " + 
         "\"specs\": [{" + 
         "\"spec_id\" : 6, " + 
         "\"spec_name\" : \"DualV_Spec_Replace\", " + 
         "\"last_mod\" : \"2022-09-08T14:00:01\"}]}"
      );

      String sKey = insertedDoc.getKey();
      col.find().key(sKey).replaceOne(replaceDoc);
      numDocs = col.find().filter(filterDoc).count();
      assertEquals(numDocs, 1);

      //Remove item from dualv
      numDocs = col.find().count();
      assertEquals(numDocs, 6);

      assertEquals(col.find().filter(filterDoc).remove(), 1);

      numDocs = col.find().count();
      assertEquals(numDocs, 5);

      //Drop dualv
      dropDualityView();
   }

   //Insert/Replace/Remove Items for dualv (Composite KEY) (NUMBER AND NUMBER)
   public void testDualvCollectionWithCompositeKeyNN() throws Exception{
      if (!isCompatibleOrGreater(COMPATIBLE_23))
      return;

      //Setup dualv
      setupDualityViewComposite("NUMBER", "NUMBER");

      //Create collection
      OracleCollection col = db.admin().createCollection("mydualvColl", 
         db.createDocumentFromString("{\"dualityViewName\" : \"PROJ_DUALV\"}"));
      assertNotNull(col);
      OracleDocument filterDoc = db.createDocumentFromString("{\"_id\":{ \"proj_id\": 6, \"proj_name\": 6 }}");

      //Insert document from dualv
      OracleDocument doc = db.createDocumentFromString(
         "{\"_id\" : { \"proj_id\": 6, \"proj_name\": 6 }, " + 
         "\"proj_status\" : \"Active\", " + 
         "\"specs\": [{" + 
         "\"spec_id\" : 6, " + 
         "\"spec_name\" : \"DualV_Spec\", " + 
         "\"last_mod\" : \"2022-09-08T14:00:01\"}]}"
      );
      
      OracleDocument insertedDoc = col.insertAndGet(doc);
      assertNotNull(insertedDoc);
      
      long numDocs = col.find().count();
      assertEquals(numDocs, 6);

      //Replace item from dualv
      OracleDocument replaceDoc = db.createDocumentFromString(
         "{\"_id\" : { \"proj_id\": 6, \"proj_name\": 6 }, " + 
         "\"proj_status\" : \"Inactive\", " + 
         "\"specs\": [{" + 
         "\"spec_id\" : 6, " + 
         "\"spec_name\" : \"DualV_Spec_Replace\", " + 
         "\"last_mod\" : \"2022-09-08T14:00:01\"}]}"
      );

      String sKey = insertedDoc.getKey();
      col.find().key(sKey).replaceOne(replaceDoc);
      numDocs = col.find().filter(filterDoc).count();
      assertEquals(numDocs, 1);

      //Remove item from dualv
      numDocs = col.find().count();
      assertEquals(numDocs, 6);

      assertEquals(col.find().filter(filterDoc).remove(), 1);

      numDocs = col.find().count();
      assertEquals(numDocs, 5);

      //Drop dualv
      dropDualityView();
   }

   //Test remove when version and key are correct NUMBER PRIMARY KEY
   public void testRemoveWithKeyAndVersion() throws Exception {
      if (!isCompatibleOrGreater(COMPATIBLE_23))
      return;

      //Setup dualv
      setupDualityView("NUMBER");

      OracleCollection col = db.admin().createCollection("mydualvColl", 
         db.createDocumentFromString("{\"dualityViewName\" : \"PROJ_DUALV\"}"));
      
      //Create collection
      OracleDocument doc = db.createDocumentFromString(
         "{\"proj_id\" : 6, " + 
            "\"proj_name\" : \"DualV_Secret_Project\", " + 
            "\"proj_status\" : \"Confidential\", " + 
            "\"specs\": [{" + 
            "\"spec_id\" : 6, " + 
            "\"spec_name\" : \"DualV_Spec\", " + 
            "\"last_mod\" : \"2021-09-08T14:00:01\"}]}"
            );
      col.insert(doc);

      OracleDocument filterSpec = db.createDocumentFromString("{\"proj_id\" : 6}");
      OracleDocument myDoc = col.find().filter(filterSpec).getCursor().next();

      String key = myDoc.getKey(), version = myDoc.getVersion();
      
      assertNotNull(key);
      assertNotNull(version);

      assertEquals(1, col.find().key(key).version(version).remove());

      //Drop dualv
      dropDualityView();
   }

   //Test remove when version is incorrect and key are correct NUMBER PRIMARY KEY
   public void testNegRemoveWithKeyAndVersion() throws Exception {
      if (!isCompatibleOrGreater(COMPATIBLE_23))
      return;

      //Setup dualv
      setupDualityView("NUMBER");

      //Create collection
      OracleCollection col = db.admin().createCollection("mydualvColl", 
         db.createDocumentFromString("{\"dualityViewName\" : \"PROJ_DUALV\"}"));

      //Insert document from dualv
      OracleDocument doc = db.createDocumentFromString(
         "{\"proj_id\" : 6, " + 
            "\"proj_name\" : \"DualV_Secret_Project\", " + 
            "\"proj_status\" : \"Confidential\", " + 
            "\"specs\": [{" + 
            "\"spec_id\" : 6, " + 
            "\"spec_name\" : \"DualV_Spec\", " + 
            "\"last_mod\" : \"2021-09-08T14:00:01\"}]}"
            );
      col.insert(doc);

      OracleDocument filterSpec = db.createDocumentFromString("{\"proj_id\" : 6}");
      OracleDocument myDoc = col.find().filter(filterSpec).getCursor().next();

      String key = myDoc.getKey(), version = myDoc.getVersion();
      
      assertNotNull(key);
      assertNotNull(version);

      try {
	 StringBuilder newVersion = new StringBuilder(version);
         newVersion.setCharAt(version.length()-1, '6');
         col.find().key(key).version(newVersion.toString()).remove();
         fail("No expected exception thrown"); 
      } catch (Exception e) {
         assertTrue(e.getCause().getMessage().contains("ORA-61715: ETAG mismatch"));
      } finally {
         col.find().key(key).version(version).remove();
      }

      //Drop dualv
      dropDualityView();
   }

   //Test index with number
   public void testIndexNumber() throws Exception {
      if (!isCompatibleOrGreater(COMPATIBLE_23) || isDBVersion23dot2())
      return;

      //Setup dualv
      setupDualityView("NUMBER");

      //Create collection
      OracleCollection col = db.admin().createCollection("mydualvColl", 
         db.createDocumentFromString("{\"dualityViewName\" : \"PROJ_DUALV\"}"));
      assertNotNull(col);

      
      OracleDocument d;
      String plan;
      OracleOperationBuilderImpl obuilder;
      
      //Insert documents from dualv
      for (int i = 100; i < 5000; i++) {
         d = db.createDocumentFromString(
            "{\"proj_id\" :" + i + ", " + 
            "\"proj_name\" : \"Project_Example " + i + "\", " + 
            "\"proj_status\" : \"Active\", " + 
            "\"specs\": [{" + 
            "\"spec_id\" : " + i + ", " + 
            "\"spec_name\" : \"Spec_Example " + i + "\", " + 
            "\"last_mod\" : \"2021-09-08T14:00:01\"}]}"
         );
         col.insert(d);
      }

      //Check if the following "find" use index unique scan
      obuilder = (OracleOperationBuilderImpl) col.find().filter("{\"proj_id\" : 100}");
      plan = obuilder.explainPlan("all");
      checkIndexUnique(plan);

      //Check if the following "finds" use index range scan
      obuilder = (OracleOperationBuilderImpl) col.find().filter("{\"proj_id\" : { \"$between\": [100, 500] }}");
      plan = obuilder.explainPlan("all");
      checkIndexRange(plan);

      obuilder = (OracleOperationBuilderImpl) col.find().filter("{ \"$and\": [ {\"proj_id\": {\"$gte\": 100 }}, {\"proj_id\": {\"$lte\": 500}} ]}");
      plan = obuilder.explainPlan("all");
      checkIndexRange(plan);

      obuilder = (OracleOperationBuilderImpl) col.find().filter("{\"proj_id\" : { \"$gt\": 100 }}");
      plan = obuilder.explainPlan("all");
      checkIndexRange(plan);

      obuilder = (OracleOperationBuilderImpl) col.find().filter("{\"proj_id\" : { \"$gte\": 100 }}");
      plan = obuilder.explainPlan("all");
      checkIndexRange(plan);

      obuilder = (OracleOperationBuilderImpl) col.find().filter("{\"proj_id\" : { \"$lt\": 100 }}");
      plan = obuilder.explainPlan("all");
      checkIndexRange(plan);

      obuilder = (OracleOperationBuilderImpl) col.find().filter("{\"proj_id\" : { \"$lte\": 100 }}");
      plan = obuilder.explainPlan("all");
      checkIndexRange(plan);
      
      //Drop dualv
      dropDualityView();
   }

   //Test index with varchar2
   public void testIndexVarchar2() throws Exception {
      if (!isCompatibleOrGreater(COMPATIBLE_23) || isDBVersion23dot2())
      return;

      //Setup dualv
      setupDualityView("VARCHAR2(100)");

      //Create collection
      OracleCollection col = db.admin().createCollection("mydualvColl", 
         db.createDocumentFromString("{\"dualityViewName\" : \"PROJ_DUALV\"}"));
      assertNotNull(col);
      

      OracleDocument d;
      String plan;
      OracleOperationBuilderImpl obuilder;

      //Insert documents from dualv
      for (int i = 100; i < 5000; i++) {
         d = db.createDocumentFromString(
            "{\"proj_id\" : \"" + i + "\", " + 
            "\"proj_name\" : \"Project_Example " + i + "\", " + 
            "\"proj_status\" : \"Active\", " + 
            "\"specs\": [{" + 
            "\"spec_id\" : " + i + ", " + 
            "\"spec_name\" : \"Spec_Example " + i + "\", " + 
            "\"last_mod\" : \"2021-09-08T14:00:01\"}]}"
         );
         col.insert(d);
      }

      //Check if the following "find" use index unique scan
      obuilder = (OracleOperationBuilderImpl) col.find().filter("{\"proj_id\" : \"100\"}");
      plan = obuilder.explainPlan("all");
      checkIndexUnique(plan);

      //Check if the following "finds" use index range scan
      obuilder = (OracleOperationBuilderImpl) col.find().filter("{\"proj_id\" : { \"$between\": [\"100\", \"500\"] }}");
      plan = obuilder.explainPlan("all");
      checkIndexRange(plan);


      obuilder = (OracleOperationBuilderImpl) col.find().filter("{ \"$and\": [ {\"proj_id\": {\"$gte\": \"110\"}}, {\"proj_id\": {\"$lte\": \"120\"}} ]}");
      plan = obuilder.explainPlan("all");
      checkIndexRange(plan);


      obuilder = (OracleOperationBuilderImpl) col.find().filter("{\"proj_id\" : { \"$gt\": \"110\" }}");
      plan = obuilder.explainPlan("all");
      checkIndexRange(plan);

      obuilder = (OracleOperationBuilderImpl) col.find().filter("{\"proj_id\" : { \"$gte\": \"110\" }}");
      plan = obuilder.explainPlan("all");
      checkIndexRange(plan);

      obuilder = (OracleOperationBuilderImpl) col.find().filter("{\"proj_id\" : { \"$lt\": \"110\" }}");
      plan = obuilder.explainPlan("all");
      checkIndexRange(plan);

      obuilder = (OracleOperationBuilderImpl) col.find().filter("{\"proj_id\" : { \"$lte\": \"110\" }}");
      plan = obuilder.explainPlan("all");
      checkIndexRange(plan);

      //Drop dualv
      dropDualityView();
   }

   //Test Index with Composite Key (NUMBER AND VARCHAR2)
   public void testIndexCompositeNV() throws Exception {
      if (!isCompatibleOrGreater(COMPATIBLE_23) || isDBVersion23dot2())
      return;

      //Setup dualv
      setupDualityViewComposite("NUMBER", "VARCHAR2(100)");

      //Create collection
      OracleCollection col = db.admin().createCollection("mydualvColl", 
         db.createDocumentFromString("{\"dualityViewName\" : \"PROJ_DUALV\"}"));
      assertNotNull(col);
      

      OracleDocument d;
      String plan;
      OracleOperationBuilderImpl obuilder;

      //Insert documents from dualv
      for (int i = 100; i < 5000; i++) {
         d = db.createDocumentFromString(
            "{\"_id\" : { \"proj_id\":" + i + ", \"proj_name\": \"Project_Example " + i + "\" }, " + 
            "\"proj_status\" : \"Active\", " + 
            "\"specs\": [{" + 
            "\"spec_id\" : " + i + ", " + 
            "\"spec_name\" : \"Spec_Example " + i + "\", " + 
            "\"last_mod\" : \"2021-09-08T14:00:01\"}]}"
         );
         col.insert(d);
      }

      //Check if the following "finds" use index range scan
      obuilder = (OracleOperationBuilderImpl) col.find().filter("{\"_id\" : { \"proj_id\":100 } }");
      plan = obuilder.explainPlan("all");
      checkIndexRange(plan);

      obuilder = (OracleOperationBuilderImpl) col.find().filter("{\"_id\" : { \"proj_id\": { \"$between\": [100, 500]}}}");
      plan = obuilder.explainPlan("all");
      checkIndexRange(plan);

      obuilder = (OracleOperationBuilderImpl) col.find().filter("{\"$and\":[ { \"_id\":{ \"proj_id\":{ \"$gte\":110 } } }, {\"_id\":{ \"proj_id\":{ \"$lte\":120 } } } ] }");
      plan = obuilder.explainPlan("all");
      checkIndexRange(plan);

      obuilder = (OracleOperationBuilderImpl) col.find().filter("{ \"_id\" : { \"proj_id\" : { \"$gt\": 110 } } }");
      plan = obuilder.explainPlan("all");
      checkIndexRange(plan);

      obuilder = (OracleOperationBuilderImpl) col.find().filter("{ \"_id\" : { \"proj_id\" : { \"$gte\": 110 } } }");
      plan = obuilder.explainPlan("all");
      checkIndexRange(plan);

      obuilder = (OracleOperationBuilderImpl) col.find().filter("{ \"_id\" : { \"proj_id\" : { \"$lt\": 110 } } }");
      plan = obuilder.explainPlan("all");
      checkIndexRange(plan);

      obuilder = (OracleOperationBuilderImpl) col.find().filter("{ \"_id\" : { \"proj_id\" : { \"$lte\": 110 } } }");
      plan = obuilder.explainPlan("all");
      checkIndexRange(plan);

      //Drop dualv
      dropDualityView();
   }
   
   private void checkIndexRange(String plan) {
      // Note: (?s) allows matching across return lines
      if (!plan.matches("(?s).*INDEX RANGE SCAN.*"))
      fail("Index range scan is not found.");
   }

   private void checkIndexUnique(String plan) {
      // Note: (?s) allows matching across return lines
      if (!plan.matches("(?s).*INDEX UNIQUE SCAN.*"))
      fail("Index unique scan is not found.");
   }

   private void dropDualityView() {
      try {
         Statement stmt = this.conn.createStatement();
         stmt.executeUpdate("DROP view proj_dualv");
         stmt.executeUpdate("DROP table specifications");
         stmt.executeUpdate("DROP table projects");
      } catch (SQLException e) {
         if (e.getMessage().contains("ORA-00942"))
         return;
      }
   }

   private void setupDualityView(String typePK) throws Exception {
      
      //Drop dualv
      dropDualityView();

      //Create tables
      String projects = "CREATE TABLE projects(" + 
         "proj_id " + typePK + " PRIMARY KEY, " + 
         "proj_name VARCHAR2(100), " + 
         "proj_status VARCHAR2(100))";

      String specifications = "CREATE TABLE specifications (" + 
         "proj_id  " + typePK + ", " + 
         "spec_id  NUMBER PRIMARY KEY, " +  
         "spec_name VARCHAR2(100), " + 
         "spec_url  VARCHAR2(100), " + 
         "last_mod  TIMESTAMP, " + 
         "FOREIGN KEY(proj_id)" + 
         "REFERENCES projects(proj_id))";

      //Insert data into tables
      String insertProjects = "INSERT ALL \n";
      String insertSpecifications = "INSERT ALL \n";

      for (int i = 1; i <= 5;i++) {
         if (typePK == "NUMBER") {
            insertProjects += "INTO PROJECTS VALUES (" + i + ", 'Project_" + i + "', 'Active') \n";
            insertSpecifications += "INTO SPECIFICATIONS VALUES (" + i + ", " + i + ", 'Spec_" + i + "', " +
               "'www.url" + i + ".com', TO_TIMESTAMP('2022-01-01 12:01:01', 'YYYY-MM-DD HH24:MI:SS')) \n";
         } else if (typePK == "VARCHAR2(100)") {
            insertProjects += "INTO PROJECTS VALUES ('00" + i + "', 'Project_" + i + "', 'Active') \n";
            insertSpecifications += "INTO SPECIFICATIONS VALUES ('00" + i + "', " + i + ", 'Spec_" + i + "', " +
               "'www.url" + i + ".com', TO_TIMESTAMP('2022-01-01 12:01:01', 'YYYY-MM-DD HH24:MI:SS')) \n";
         } else if (typePK == "RAW(2000)") {
            insertProjects += "INTO PROJECTS VALUES (hextoraw('" + i + "'), 'Project_" + i + "', 'Active') \n";
            insertSpecifications += "INTO SPECIFICATIONS VALUES ('" + i + "', " + i + ", 'Spec_" + i + "', " +
               "'www.url" + i + ".com', TO_TIMESTAMP('2022-01-01 12:01:01', 'YYYY-MM-DD HH24:MI:SS')) \n";
         }
      }
      insertProjects += "SELECT * FROM DUAL";
      insertSpecifications += "SELECT * FROM DUAL";


      //Create dualv
      String dualityView = "create or replace json relational duality view proj_dualv as " + 
         "select json {" + 
         "proj_id, " + 
         "proj_name, " + 
         "proj_status, " + 
         "'specs' : (select JSON_ArrayAgg(JSON{spec_id, spec_name, last_mod})" + 
         "from specifications s with (insert, update, delete)" + 
         "where p.proj_id = s.proj_id) NULL ON NULL}" + 
         "from projects p with (insert, update, delete)";

      Statement stmt = this.conn.createStatement();
      stmt.executeUpdate(projects);
      stmt.executeUpdate(specifications);
      stmt.executeUpdate(insertProjects);
      stmt.executeUpdate(insertSpecifications);
      stmt.executeUpdate(dualityView);
   }

   private void setupDualityViewComposite(String typePK, String typePK2) throws Exception {
      
      //Drop dualv
      dropDualityView();

      //Create tables
      String projects = "CREATE TABLE projects(" + 
         "proj_id " + typePK + ", " + 
         "proj_name " + typePK2 + ", " + 
         "proj_status VARCHAR2(100), " +
         "CONSTRAINT pk_projects PRIMARY KEY (proj_id, proj_name))";
            

      String specifications = "CREATE TABLE specifications (" + 
         "proj_id  " + typePK + ", " + 
         "proj_name " + typePK2 + ", " + 
         "spec_id  NUMBER PRIMARY KEY, " + 
         "spec_name VARCHAR2(100), " + 
         "spec_url  VARCHAR2(100), " + 
         "last_mod  TIMESTAMP, " + 
         "FOREIGN KEY(proj_id, proj_name)" + 
         "REFERENCES projects(proj_id, proj_name))";
      
      String insertProjects = "INSERT ALL \n";
      String insertSpecifications = "INSERT ALL \n";

      //Insert data into tables
      for (int i = 1; i <= 5;i++) {
         if (typePK == "NUMBER" && typePK2 == "VARCHAR2(100)") {
            insertProjects += "INTO PROJECTS VALUES (" + i + ", 'Project_" + i + "', 'Active') \n";
            insertSpecifications += "INTO SPECIFICATIONS VALUES (" + i + ", 'Project_" + i + "', " + i + ", 'Spec_" + i + "', " + 
               "'www.url" + i + ".com', TO_TIMESTAMP('2022-01-01 12:01:01', 'YYYY-MM-DD HH24:MI:SS')) \n";
         } else if (typePK == "VARCHAR2(100)" && typePK2 == "VARCHAR2(100)") {
            insertProjects += "INTO PROJECTS VALUES ('00" + i + "', 'Project_" + i + "', 'Active') \n";
            insertSpecifications += "INTO SPECIFICATIONS VALUES ('00" + i + "', 'Project_" + i + "', " + i + ", 'Spec_" + i + "', " + 
               "'www.url" + i + ".com', TO_TIMESTAMP('2022-01-01 12:01:01', 'YYYY-MM-DD HH24:MI:SS')) \n";
         } else if (typePK == "NUMBER" && typePK2 == "NUMBER") {
            insertProjects += "INTO PROJECTS VALUES (" + i + ", " + i + ", 'Active') \n";
            insertSpecifications += "INTO SPECIFICATIONS VALUES (" + i + ", " + i + ", " + i + ", 'Spec_" + i + "', " +
               "'www.url" + i + ".com', TO_TIMESTAMP('2022-01-01 12:01:01', 'YYYY-MM-DD HH24:MI:SS')) \n";
         }
      }
      insertProjects += "SELECT * FROM DUAL";
      insertSpecifications += "SELECT * FROM DUAL";

      //Create dualv
      String dualityView = "create or replace json relational duality view proj_dualv as " + 
         "select json { '_id': {'proj_id': proj_id, 'proj_name': proj_name }, " + 
         "proj_status, " + 
         "'specs' : (select JSON_ArrayAgg(JSON{spec_id, spec_name, last_mod})" + 
         "from specifications s with (insert, update, delete)" + 
         "where (p.proj_id = s.proj_id) and (p.proj_name = s.proj_name)) NULL ON NULL}" + 
         "from projects p with (insert, update, delete)";
      
      Statement stmt = this.conn.createStatement();
      stmt.executeUpdate(projects);
      stmt.executeUpdate(specifications);
      stmt.executeUpdate(insertProjects);
      stmt.executeUpdate(insertSpecifications);
      stmt.executeUpdate(dualityView);
   }
}
