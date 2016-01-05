/* Copyright (c) 2014, 2015, Oracle and/or its affiliates. 
All rights reserved.*/

/*
   DESCRIPTION
    OracleCollection tests
 */

/**
 *  @author  Vincent Liu
 */

package oracle.json.tests.soda;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import java.nio.charset.Charset;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashSet;

import oracle.jdbc.OraclePreparedStatement;

import oracle.soda.OracleCursor;
import oracle.soda.OracleCollection;
import oracle.soda.OracleCollectionAdmin;
import oracle.soda.OracleDocument;
import oracle.soda.OracleException;

import oracle.soda.rdbms.impl.OracleDocumentImpl;
import oracle.soda.rdbms.impl.OracleDatabaseImpl;

import oracle.soda.rdbms.OracleRDBMSMetadataBuilder;

import oracle.json.testharness.SodaTestCase;

import javax.json.JsonString;

public class test_OracleDocument2 extends SodaTestCase {
  
  public void testGetContent() throws Exception {
    
    OracleDocument metaDoc = client.createMetadataBuilder().build();
    OracleCollection col = dbAdmin.createCollection("testGetContent", metaDoc);
    
    OracleDocument doc = db.createDocumentFromString(null, "{ \"value\" : \"1\" }", null);
    
    // Test with getContentAsByteArray()
    assertEquals("{ \"value\" : \"1\" }", new String(doc.getContentAsByteArray(), "UTF-8"));
    
    // Test with getContentAsStream()
    OracleDocumentImpl docImpl = (OracleDocumentImpl) doc;
    assertEquals("{ \"value\" : \"1\" }", inputStream2String(docImpl.getContentAsStream()));
    
    // Test with getContentAsString()
    assertEquals("{ \"value\" : \"1\" }", doc.getContentAsString());
    
    doc = col.insertAndGet(doc);
    // Test with getContentAsByteArray() when content is null
    assertNull(doc.getContentAsByteArray());
    
    // Test with getContentAsStream() when content is null
    docImpl = (OracleDocumentImpl) doc;
    assertNull(docImpl.getContentAsStream());
    
    // Test with getContentAsString() when content is null
    assertNull(doc.getContentAsString());

    // Test getContentAsString() for non-JSON content
    doc = db.createDocumentFromString(null, "abcd efgh ", "text/plain");
    try {
      doc.getContentAsString();
      fail("No exception when call getContentAsString() for non-JSON content");
    } catch (OracleException e) {
      // Expect an OracleException
      assertEquals("Media type of the document is not \"application/json\". " +
          "getContentAsString() is only supported for JSON documents.", e.getMessage());
    }

  }
  
  public void testGetContentType() throws Exception {

    OracleDocument metaDoc = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("CLIENT")
        .contentColumnType("BLOB").mediaTypeColumnName("MediaType").build();
    
    OracleCollection col = dbAdmin.createCollection("testGetContentType", metaDoc);
    
    col.insert(db.createDocumentFromString("id-1", "{ \"value\" : \"1\" }", "application/json"));
    col.insert(db.createDocumentFromString("id-2", "12345abcd", "text/plain"));
    col.insert(db.createDocumentFromString("id-3", "111111111", "text/plain"));
    
    // Test with contentType is JSON
    OracleDocument doc = col.find().key("id-1").getOne();
    assertEquals("application/json", doc.getMediaType());
    
    // Test with contentType is non-JSON
    doc = col.find().key("id-2").getOne();
    assertEquals("text/plain", doc.getMediaType());
    
    // Test with contentType is null
    doc = col.find().key("id-3").getOne();
    assertEquals("text/plain", doc.getMediaType());
    
    // Negative case
    // Test when there is no content column for collection
    metaDoc = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("CLIENT").build();
    
    OracleCollection col2 = dbAdmin.createCollection("testGetContentType2", metaDoc);
    //if removing media column, the collection is JSON only storage 
    assertFalse(col2.admin().isHeterogeneous());
    OracleDocument doc2 = col2.insertAndGet(db.createDocumentFromString("id-1", "{ \"value\" : \"1\" }", null));
    assertEquals("application/json", doc2.getMediaType());
    
  }
  
  public void testGetKey() throws Exception {
    
    // Test with KEY_ASSIGNMENT_METHOD = "SEQUENCE"
    OracleDocument metaDoc = client.createMetadataBuilder()
        .contentColumnType("BLOB")
        .keyColumnAssignmentMethod("SEQUENCE").keyColumnSequenceName("keyseql")
        .mediaTypeColumnName("MediaType").build();
    OracleCollection col = dbAdmin.createCollection("testGetKey", metaDoc);
    
    OracleDocument doc = db.createDocumentFromString(null, "{ \"value\" : \"1\" }", "application/json");
    assertNull(doc.getKey());
    
    doc = col.insertAndGet(doc);
    assertNotNull(doc.getKey());
    
    doc = col.insertAndGet(db.createDocumentFromString(null, "{ \"value\" : \"2\" }", "application/json"));
    assertNotNull(doc.getKey());
    
    // Test with KEY_ASSIGNMENT_METHOD = "GUID"
    metaDoc = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("GUID").build();
    
    col = dbAdmin.createCollection("testGetKey2", metaDoc);
    
    doc = db.createDocumentFromString(null, "{ \"value\" : \"2\" }", "application/json");
    assertNull(doc.getKey());
    doc = col.insertAndGet(doc);
    String key1 = doc.getKey();
    assertNotNull(key1);
    assertEquals("{ \"value\" : \"2\" }",
        new String(col.find().key(key1).getOne().getContentAsByteArray(), "UTF-8"));
    
    // Test with KEY_ASSIGNMENT_METHOD = "UUID"
    metaDoc = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("UUID").build();
    col = dbAdmin.createCollection("testGetKey3", metaDoc);
    
    doc = db.createDocumentFromString(null, "{ \"value\" : \"3\" }", "application/json");
    assertNull(doc.getKey());
    doc = col.insertAndGet(doc);
    key1 = doc.getKey();
    assertNotNull(key1);
    assertEquals("{ \"value\" : \"3\" }",
        new String(col.find().key(key1).getOne().getContentAsByteArray(), "UTF-8"));
    
    // Test with KEY_ASSIGNMENT_METHOD = "CLIENT"
    metaDoc = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("CLIENT").build();
    col = dbAdmin.createCollection("testGetKey4", metaDoc);
    
    doc = db.createDocumentFromString("id-4", "{ \"value\" : \"4\" }", "application/json");
    doc = col.insertAndGet(doc);
    key1 = doc.getKey();
    assertEquals("id-4", key1);
    assertEquals("{ \"value\" : \"4\" }",
        new String(col.find().key(key1).getOne().getContentAsByteArray(), "UTF-8"));
    
  }

  private void testGetVersion(String versionMethod, String colName) throws Exception {
    
    OracleDocument metaDoc = client.createMetadataBuilder()
        .versionColumnMethod(versionMethod).build();
    
    OracleCollection col = dbAdmin.createCollection(colName, metaDoc);
    
    OracleDocument doc = db.createDocumentFromString(null, "{ \"value\" : \"1\" }", "application/json");
    assertNull(doc.getVersion());
    doc = col.insertAndGet(doc);
    String version1 = doc.getVersion();

    doc = col.insertAndGet(db.createDocumentFromString(null, "{ \"value\" : \"2\" }", "application/json"));
    String version2 = doc.getVersion();
    if (!versionMethod.equalsIgnoreCase("SEQUENTIAL"))

    {
      assertEquals("{ \"value\" : \"2\" }", new String(col.find().version(version2).getOne().getContentAsByteArray(), "UTF-8"));
      assertFalse(version1.equals(version2));
    }
  }
  
  public void testGetVersion() throws Exception {
    // Test with VERSION_METHOD = "SEQUENTIAL"
    testGetVersion("SEQUENTIAL", "testGetVersion");
    
    // Test with VERSION_METHOD = "TIMESTAMP"
    testGetVersion("TIMESTAMP", "testGetVersion2");
    
    // Test with VERSION_METHOD = "UUID"
    testGetVersion("UUID", "testGetVersion3");
    
    // Test with VERSION_METHOD = "SHA256"
    testGetVersion("SHA256", "testGetVersion4");

    // Test with VERSION_METHOD = "MD5"
    testGetVersion("MD5", "testGetVersion5");

    String sName = schemaName.toUpperCase();
    // Test with VERSION_METHOD = "NONE"
    // VERSION_METHOD="NONE" means the user's code/table is expected to set the version, 
    // i.e. via a SQL default or trigger
    // so we need access existing table to test it;
    OracleRDBMSMetadataBuilder mBuilder = client.createMetadataBuilder().schemaName(sName)
        .keyColumnAssignmentMethod("CLIENT")
        .versionColumnMethod("NONE");
    OracleDocument metaDoc = mBuilder.tableName("SODATBL").build();
    
    OracleCollection col = dbAdmin.createCollection("testGetVersion6", metaDoc);
    
    String insertSql = "insert into sodatbl values('id-1', 'application/json', SYSTIMESTAMP, SYSTIMESTAMP, null, c2b('{ \"value\" : \"a\" }'))";
    PreparedStatement stmt = conn.prepareStatement(insertSql);
    stmt.execute();
    // auto-commit set on
    //conn.commit();
    
    OracleDocument doc = db.createDocumentFromString("id-x", "{ \"value\" : \"b\" }", "application/json");
    doc = col.insertAndGet(doc);
    assertNotNull(doc.getVersion());
    doc = col.find().key("id-x").getOne();
    assertEquals("{ \"value\" : \"b\" }", new String(doc.getContentAsByteArray(), "UTF-8"));
    
    // clean up the created row data
    assertEquals(2, col.find().remove());
    assertEquals(0, col.find().count());

    // Negative tests
    // Test when there is no version column for the collection
    // remove both VERSION_COLUMN_NAME and VERSION_METHOD
    OracleDocument metaDoc2 = client.createMetadataBuilder().removeOptionalColumns().build(); 
    OracleCollection col2 = dbAdmin.createCollection("testGetVersion7", metaDoc2);
    
    OracleDocument doc2 = col2.insertAndGet(db.createDocumentFromString(null, "{ \"value\" : \"a\" }", "application/json"));
    assertNull(doc2.getVersion());
  }
  
  public void testGetCreatedOnAndLastModified() throws Exception {
    
    OracleDocument metaDoc = client.createMetadataBuilder().creationTimeColumnName("createdOnCol")
        .lastModifiedColumnName("lastModifiedCol").build();
    
    OracleCollection col = dbAdmin.createCollection("testGetCreatedOn", metaDoc);
    OracleDocument doc = db.createDocumentFromString(null, "{ \"value\" : \"a\" }", "application/json");
    assertNull(doc.getCreatedOn());
    assertNull(doc.getLastModified());
    
    // Test when createOn and lastModified are generated
    doc = col.insertAndGet(doc);
    String lastModified = doc.getLastModified();
    assertNotNull(lastModified);
    doc = col.find().getOne();
    String createdOn = doc.getCreatedOn();
    assertEquals(lastModified, createdOn);
    
    // Test when there is no createOn and lastModified column for the collection
    OracleDocument metaDoc2 = client.createMetadataBuilder().removeOptionalColumns().build();
    
    OracleCollection col2 = dbAdmin.createCollection("testGetCreatedOn2", metaDoc2);
    OracleDocument doc2 = db.createDocumentFromString(null, "{ \"value\" : \"a\" }", "application/json");
    assertNull(doc2.getCreatedOn());
    assertNull(doc2.getLastModified());
  }
  
  public void testGetContentLength() throws Exception {
    String jasnStr =
      "{\n" +
      "  \"name\":\"Fred Flintstone\",\n" +
      "  \"address\": {\n" +
      "          \"street\":\"123 Stone Street\",\n" +
      "          \"city\":\"Bedrock\",\n" +
      "          \"state\":\"Pangea\",\n" +
      "          \"zipcode\":12345\n" +
      "            },\n" +
      "  \"married\": true,\n" +
      "  \"email\": null,\n" + 
      "  \"friends\":[\"Barney Rubble\", \"Betty Rubble\"]\n" +
      "}";

    OracleDocument metaDoc = client.createMetadataBuilder().build();

    OracleCollection col = dbAdmin.createCollection("testGetContentLength", metaDoc);
    OracleDocument doc = db.createDocumentFromString(null, jasnStr, null);
    assertEquals(jasnStr.length(), doc.getContentLength());
    
    col.insert(doc);
    doc = col.find().getOne();
    assertEquals(jasnStr.length(), doc.getContentLength());
    
    // Test it when the content is not available
    doc = col.insertAndGet(db.createDocumentFromString(null, "{ \"value\" : \"a\" }", null));
    // when content is not available, getContentLength() just return -1
    assertEquals(-1, doc.getContentLength());
    
    doc = db.createDocumentFromString(null, null, null);
    assertEquals(-1, doc.getContentLength());
  }

  private void basicTestForEncoding(Charset charsetName, OracleCollection col)  throws Exception {
    
    String docStr1 =
    "{\n" +
    "  \"employee\": {\n" +
    "    \"employeeId\": \"EMP0002\", \"name\": \"Martha Raynolds\", \"salary\": 8000, \"gender\": \"Female\", \n"+ 
    "    \"address\": {\"domestic\": true},\n" +
    "    \"tag\": [\"ppt\", \"excel\", \"word\"]\n" +
    "  }\n" +
    "}";

    String docStr2 = 
    "{\n"+
    "  \"employee\": {\n" +
    "    \"employeeId\": \"EMP0003\", \"name\": \"Roger Jones\", \"salary\": 9200, \"gender\": \"Male\",\n" + 
    "    \"address\": {\"domestic\": true, \"street\": \"PO Box 27 Irving, texas 98553\"},\n" +
    "    \"tag\": [\"finance\", \"excel\", \"word\"]\n" +
    "  }\n" +
    "}";

    String docStr3 =
    "{\n" +
    "  \"employee\": {\n" +
    "    \"employeeId\": \"EMP0004\", \"name\": \"Robert Myers\", \"salary\": 8500, \"gender\": \"Male\",\n" + 
    "    \"address\": {\"domestic\": false, \"street\": \"821 Nordic. Road, Irving Texas 98558\"},\n" +
    "    \"tag\": [\"software programming\"]\n" +
    "  }\n" +
    "}";

    OracleDocument doc = db.createDocumentFromByteArray("id-1", docStr1.getBytes(charsetName), null); 
    col.insert(doc);
    doc = db.createDocumentFromByteArray("id-3", docStr1.getBytes(charsetName), null); 
    col.insert(doc);
    
    doc = col.findOne("id-1");
    assertEquals(docStr1, doc.getContentAsString());
    
    // Test with save(doc2)  
    doc = db.createDocumentFromByteArray("id-2", docStr2.getBytes(charsetName), null);
    col.save(doc);
    doc = col.findOne("id-2");
    assertEquals(docStr2, doc.getContentAsString());
    
    // Test with replace
    doc = db.createDocumentFromByteArray(null, docStr3.getBytes(charsetName), null);
    col.find().key("id-3").replaceOne(doc);
    doc = col.find().key("id-3").getOne();
    doc = col.findOne("id-3");
    assertEquals(docStr3, doc.getContentAsString());
    
    OracleDocument filterDoc = null;
    // should match doc1
    String filter1 = "{ \"employee.address.domestic\" : true,  \"employee.salary\" : {\"$lt\" : 9000, \"$gte\" : 8000}} ";
    filterDoc = db.createDocumentFromString(filter1);
    doc = col.find().filter(filterDoc).getOne();
    assertEquals(1, col.find().filter(filterDoc).count());
    assertEquals("id-1", doc.getKey());
    
    // should match doc1 and doc3
    filterDoc = db.createDocumentFromString(
        "{\"employee.salary\" : {\"$lt\" : 9000, \"$gte\" : 8000}, \"$orderby\" : {\"employee.employeeId\":1} } ");
    OracleCursor cursor = col.find().filter(filterDoc).getCursor();
    assertEquals(true, cursor.hasNext());
    assertEquals("id-1", cursor.next().getKey());
    assertEquals("id-3", cursor.next().getKey());
    cursor.close();

    // should match doc2 and doc1
    filterDoc = db.createDocumentFromString(
        "{\"employee.address.domestic\" : true, \"$orderby\" : {\"employee.employeeId\": -1} } ");
    cursor = col.find().filter(filterDoc).getCursor();
    assertEquals(true, cursor.hasNext());
    assertEquals("id-2", cursor.next().getKey());
    assertEquals("id-1", cursor.next().getKey());
    cursor.close();

    // should match doc2, but "$contains" is not supported yet
    /*filterDoc = db.createDocumentFromString("{ \"employee.address.street\" : {\"$contains\" : \"98553\"} }");
    assertEquals(1, col.find().filter(filterDoc).count());
    doc = col.find().filter(filterDoc).getOne();
    assertEquals("id-2", doc.getKey());*/

    // should match doc3
    filterDoc = db.createDocumentFromString("{ \"employee.name\" : {\"$startsWith\" : \"Robert\"} }");
    assertEquals(1, col.find().filter(filterDoc).count());
    doc = col.find().filter(filterDoc).getOne();
    assertEquals("id-3", doc.getKey());

    // should match doc3
    filterDoc = db.createDocumentFromString(" {\"employee.address.street\" : {\"$regex\" : \".*Nordic.*\"} }");
    assertEquals(1, col.find().filter(filterDoc).count());
    doc = col.find().filter(filterDoc).getOne();
    assertEquals("id-3", doc.getKey());

    // should match doc3
    filterDoc = db.createDocumentFromString("{\"employee.tag[*]\" : {\"$regex\" : \"soft.*\"} }");
    assertEquals(1, col.find().filter(filterDoc).count());
    doc = col.find().filter(filterDoc).getOne();
    assertEquals("id-3", doc.getKey());

    // should match doc2
    filterDoc = db.createDocumentFromString("{ \"employee.name\" : {\"$startsWith\" : \"Roger\"}, " +
            "\"employee.address.street\" : {\"$regex\" : \".*98553.*\"}  }");
    assertEquals(1, col.find().filter(filterDoc).count());
    cursor = col.find().filter(filterDoc).getCursor();
    assertEquals(true, cursor.hasNext());
    assertEquals("id-2", cursor.next().getKey());
    assertEquals(false, cursor.hasNext());
    cursor.close();

    // should match doc1
    filterDoc = db.createDocumentFromString("{\"employee.employeeId\" : {\"$regex\" : \"EMP0{3}[0-2]\"} }");
    assertEquals(1, col.find().filter(filterDoc).count());
    doc = col.find().filter(filterDoc).getOne();
    assertEquals("id-1", doc.getKey());

    // should match doc3
    //must pass "{"$regex" : "Ro\\w{4}\\s?\\w+"} }" into json parser, or "JsonParsingException: Unexpected char" will be reported
    filterDoc = db.createDocumentFromString("{ \"employee.name\" : {\"$regex\" : \"Ro\\\\w{4}\\\\s?\\\\w+\"} }");            
    assertEquals(1, col.find().filter(filterDoc).count());
    doc = col.find().filter(filterDoc).getOne();
    assertEquals("id-3", doc.getKey());
    
    filterDoc = db.createDocumentFromString("{ \"employee.gender\" : {\"$regex\" : \"M\\\\w+\"}, " +
              "\"employee.address.street\" : {\"$regex\" : \"\\\\d+.+\" } }");           
    assertEquals(1, col.find().filter(filterDoc).count());
    doc = col.find().filter(filterDoc).getOne();
    assertEquals("id-3", doc.getKey());

    // should match doc2 and doc3
    filterDoc = db.createDocumentFromString("{\"employee.address.street\" : {\"$exists\" : true }, \"$orderby\" : {\"employee.employeeId\":1} }");
    cursor = col.find().filter(filterDoc).getCursor();
    assertEquals(true, cursor.hasNext());
    assertEquals("id-2", cursor.next().getKey());
    assertEquals("id-3", cursor.next().getKey());
    cursor.close();

    // should match doc1 and doc3
    filterDoc = db.createDocumentFromString(
        "{ \"employee.employeeId\" : {\"$in\" : [\"EMP0002\", \"EMP0004\"]}, \"$orderby\" : {\"employee.employeeId\":1} }");
    assertEquals(2, col.find().filter(filterDoc).count());
    cursor = col.find().filter(filterDoc).getCursor();
    assertEquals(true, cursor.hasNext());
    assertEquals("id-1", cursor.next().getKey());
    assertEquals("id-3", cursor.next().getKey());
    cursor.close();

    // should match doc1 and doc3
    filterDoc = db.createDocumentFromString(
        "{ \"employee.tag[0]\" : {\"$in\" : [\"ppt\", \"software programming\"]} } ");
    assertEquals(2, col.find().filter(filterDoc).count());
    cursor = col.find().filter(filterDoc).getCursor();
    assertEquals(true, cursor.hasNext());
    HashSet<String> keys = new HashSet<String>();
    keys.add("id-1");
    keys.add("id-3");
    assertTrue(keys.contains(cursor.next().getKey()));
    assertTrue(keys.contains(cursor.next().getKey()));
    cursor.close();

    // should match doc1, 2, 3
    filterDoc = db.createDocumentFromString("{ \"employee.employeeId\" : {\"$startsWith\" : \"EMP\"} }");
    assertEquals(3, col.find().filter(filterDoc).count());

    // should match doc3
    filterDoc = db.createDocumentFromString(
        "{ \"employee.gender\": \"Male\", \"employee.address.domestic\": false }");
    assertEquals(1, col.find().filter(filterDoc).count());
    doc = col.find().filter(filterDoc).getOne();
    assertEquals("id-3", doc.getKey());

    col.find().remove();
    
    for (int i = 1; i <= 10; i++) {
      col.insertAndGet(db.createDocumentFromString("id-" + i, "{ \"dataValue\" : " + i + " }", null));
    }
    
    String fStr = "{ \"$query\" : {\"dataValue\" : {\"$gt\" : 9 }}, \"$orderby\" : {\"d\" : -1} }";
    filterDoc = db.createDocumentFromString(fStr);
    assertEquals(1, col.find().filter(filterDoc).count());
    doc =  col.find().filter(filterDoc).getOne();
   
    OracleCollectionAdmin colAdmin = col.admin();
    String contentSqlType = ((JsonString) getValue(colAdmin.getMetadata(), path("contentColumn", "sqlType"))).getString();
    // Test to create index
    if( !(contentSqlType.equalsIgnoreCase("NVARCHAR2") || contentSqlType.equalsIgnoreCase("NCLOB"))) {
      colAdmin.indexAll("indexForEncodingTests");
      colAdmin.dropIndex("indexForEncodingTests");
    }
 
    col.admin().truncate();
    assertEquals(0, col.find().count());
  }
 
  public void testDocumentEncoding() throws Exception {
    final Charset ASCII = Charset.forName("US-ASCII");
    final Charset ISO_8859_1 = Charset.forName("ISO-8859-1");
    final Charset UTF8 = Charset.forName("UTF-8");
    final Charset UTF16 = Charset.forName("UTF-16");
    final Charset UTF16LE = Charset.forName("UTF-16LE");
    final Charset UTF16BE = Charset.forName("UTF-16BE");
    final Charset UTF32 = Charset.forName("UTF-32");
    final Charset UTF32LE = Charset.forName("UTF-32LE");
    final Charset UTF32BE = Charset.forName("UTF-32BE");
    final Charset[] CHARSETS = { ASCII, ISO_8859_1, UTF8, UTF16, UTF16LE, UTF16BE, UTF32, UTF32LE, UTF32BE };
    
    // Test with BLOB
    OracleDocument meta = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("CLIENT")
        .contentColumnType("BLOB")
        .build();
    OracleDocument mDoc = db.createDocumentFromByteArray(null, meta.getContentAsString().getBytes(UTF8), null);
    OracleCollection col = dbAdmin.createCollection("testDocumentEncoding", mDoc);
    for (Charset c : CHARSETS) {
      // "UTF-32LE" and "UTF-32BE" are not supported in BLOB and RAW, and "UTF-32" is not decided
      if(c != UTF32 && c != UTF32LE && c != UTF32BE)
        basicTestForEncoding(c, col);
    }
    
    // Test with RAW
    /* ### Oracle Database does not support RAW storage for JSON
    OracleDocument meta2 = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("CLIENT").contentColumnType("RAW").build();
    OracleDocument mDoc2 = db.createDocumentFromByteArray(null, 
        meta2.getContentAsString().getBytes(UTF16), null);
    OracleCollection col2 = dbAdmin.createCollection("testDocumentEncoding2",
        mDoc2);
    for (Charset c : CHARSETS) {
      // "UTF-32LE" and "UTF-32BE" are not supported in BLOB and RAW, and
      // "UTF-32" is not decided
      if (c != UTF32 && c != UTF32LE && c != UTF32BE)
        basicTestForEncoding(c, col2);
    }
    */
    
    // Test with VARCHAR2
    OracleDocument meta3 = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("CLIENT").contentColumnType("VARCHAR2").build();
    OracleDocument mDoc3 = db.createDocumentFromByteArray(null, 
        meta3.getContentAsString().getBytes(UTF32), null);
    OracleCollection col3 = dbAdmin.createCollection("testDocumentEncoding3",mDoc3);
    for (Charset c : CHARSETS) {
      basicTestForEncoding(c, col3);
    }
    
    // Test with NVARCHAR2
    /* ### Oracle Database does not support NVARCHAR2 storage for JSON
    OracleDocument meta4 = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("CLIENT").contentColumnType("NVARCHAR2").build();
    OracleDocument mDoc4 = db.createDocumentFromByteArray(null, 
        meta4.getContentAsString().getBytes(UTF16LE), null);
    OracleCollection col4 = dbAdmin.createCollection("testDocumentEncoding4",mDoc4);
    for (Charset c : CHARSETS) {
      basicTestForEncoding(c, col4);
    }
    */

    // Test with CLOB
    OracleDocument meta5 = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("CLIENT").contentColumnType("CLOB").build();
    OracleDocument mDoc5 = db.createDocumentFromByteArray(null, 
        meta5.getContentAsString().getBytes(UTF16BE), null);
    OracleCollection col5 = dbAdmin.createCollection("testDocumentEncoding5",mDoc5);
    for (Charset c : CHARSETS) {
      basicTestForEncoding(c, col5);
    }
    
    // Test with NCLOB
    /* ### Oracle Database does not support NCLOB storage for JSON
    OracleDocument meta6 = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("CLIENT").contentColumnType("NCLOB").build();
    OracleDocument mDoc6 = db.createDocumentFromByteArray(null, 
        meta6.getContentAsString().getBytes(UTF8), null);
    OracleCollection col6 = dbAdmin.createCollection("testDocumentEncoding6",mDoc6);
    for (Charset c : CHARSETS) {
      basicTestForEncoding(c, col6);
    }
    */ 
  }

  private void testlargeContentWithCol(OracleCollection col) throws Exception {
    OracleDocument doc = null;
    OracleCollectionAdmin colAdmin = col.admin();
    OracleDatabaseImpl dbImpl = (OracleDatabaseImpl) db;
    
    // Test insert() with big JSON document
    InputStream in = new FileInputStream(new File("data/PurchaseOrders.json"));
    String key1 = "id-1";
    
    doc = dbImpl.createDocumentFromStream(key1, in, null);
    col.insert(doc);
    in.close();
    
    doc = col.findOne(key1);
    in = new FileInputStream(new File("data/PurchaseOrders.json"));
    assertEquals(inputStream2String(in), new String(doc.getContentAsByteArray(), "UTF-8"));
    in.close();
    
    // Test save() with big non-JSON document
    byte[] imageBytes = File2Bytes("data/oracle.jpg");
    if(colAdmin.isHeterogeneous()) {
      // Test with non-JSON content
      doc = db.createDocumentFromByteArray(key1, imageBytes, "image/jpeg");
      col.save(doc);
      
      doc = col.findOne(key1);
      assertEquals(imageBytes.length, doc.getContentAsByteArray().length);
      assertEquals(new String(imageBytes, "UTF-8"), new String(doc.getContentAsByteArray(), "UTF-8"));
      assertEquals("image/jpeg", doc.getMediaType());
    }
 
    // Test replace with big JSON document
    in = new FileInputStream(new File("data/PurchaseOrders.json"));
    String jsonFileString = inputStream2String(in);
    in.close();
    doc = db.createDocumentFromString(key1, jsonFileString, "application/json");
    col.find().key(key1).replaceOne(doc);
    
    doc = col.findOne(key1);
    assertEquals(jsonFileString, doc.getContentAsString());
    assertEquals("application/json", doc.getMediaType());
    
    if(colAdmin.isHeterogeneous()) {
      OracleDocument descDoc = colAdmin.getMetadata();
      JsonString jStr = (JsonString) getValue(descDoc, path("versionColumn", "method"));
      String versionMethod = jStr.toString();

      if ( versionMethod.contains("SEQUENTIAL") || versionMethod.contains("UUID") ) {
        // process 25m video doc
        File srcFile = new File("data/java.flv");
        String mediaType = "video/x-flv";
        InputStream srcStream = new FileInputStream(srcFile);
        doc = dbImpl.createDocumentFromStream(key1, srcStream, mediaType);
        col.save(doc);
        srcStream.close();

        doc = col.find().key(key1).getOne();
        assertEquals(mediaType, doc.getMediaType());
        assertEquals(srcFile.length(), doc.getContentLength());

        InputStream resultStream = ((OracleDocumentImpl) doc).getContentAsStream();
        srcStream = new FileInputStream(srcFile);

        compareInputStream(srcStream, resultStream);
        srcStream.close();
        resultStream.close();

        // process 100m xml doc
        /* ### Commented out for now. Need to find an XML file
               we can use, or generate one.
        srcFile = new File("data/some_file.xml");
        mediaType = "application/xml";
        srcStream = new FileInputStream(srcFile);
        doc = dbImpl.createDocumentFromStream("id-2", srcStream, mediaType);
        doc = col.insertAndGet(doc);
        String key2 = doc.getKey();
        srcStream.close();

        doc = col.find().key(key2).getOne();
        assertEquals(mediaType, doc.getMediaType());
        assertEquals(srcFile.length(), doc.getContentLength());

        resultStream = ((OracleDocumentImpl) doc).getContentAsStream();
        srcStream = new FileInputStream(srcFile);

        compareInputStream(srcStream, resultStream);
        srcStream.close();
        resultStream.close();
        */
      }
    }
    
    colAdmin.truncate();
    
  }
  
  public void testlargeContent() throws Exception {
    
    // Test with heterogeneous and versionColumnMethod=SEQUENTIAL
    OracleDocument mDoc = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("CLIENT").mediaTypeColumnName("Media_Type")
        .contentColumnType("BLOB")
        .versionColumnMethod("SEQUENTIAL")
        .build();
    OracleCollection col = dbAdmin.createCollection("testlargeContent", mDoc);
    testlargeContentWithCol(col);
    
    // Test non-heterogeneous and versionColumnMethod=TIMESTAMP
    OracleDocument mDoc2 = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("CLIENT")
        .contentColumnType("CLOB")
        .versionColumnMethod("TIMESTAMP")
        .build();
    OracleCollection col2 = dbAdmin.createCollection("testlargeContent2", mDoc2);
    testlargeContentWithCol(col2);
    
   // Test non-heterogeneous and versionColumnMethod=SHA256
    OracleDocument mDoc3 = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("CLIENT")
        .contentColumnType("CLOB")
        .versionColumnMethod("SHA256")
        .build();
    OracleCollection col3 = dbAdmin.createCollection("testlargeContent3", mDoc3);
    testlargeContentWithCol(col3);
    
    // Test with heterogeneous and versionColumnMethod=UUID
    OracleDocument mDoc4 = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("CLIENT").mediaTypeColumnName("Media_Type")
        .contentColumnType("BLOB")
        .versionColumnMethod("UUID")
        .build();
    OracleCollection col4 = dbAdmin.createCollection("testlargeContent4", mDoc4);
    testlargeContentWithCol(col4);
    
    // Test with heterogeneous and versionColumnMethod=MD5
    OracleDocument mDoc5 = client.createMetadataBuilder()
        .keyColumnAssignmentMethod("CLIENT").mediaTypeColumnName("Media_Type")
        .contentColumnType("BLOB")
        .versionColumnMethod("MD5")
        .build();
    OracleCollection col5 = dbAdmin.createCollection("testlargeContent5", mDoc5);
    testlargeContentWithCol(col5);
    
  }
  
}
