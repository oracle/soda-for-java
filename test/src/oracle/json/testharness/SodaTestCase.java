/* Copyright (c) 2014, 2015, Oracle and/or its affiliates. 
All rights reserved.*/

/**
 * DESCRIPTION
 *  Rest Utils
 */

/**
 * @author  Jianye Wang
 */

package oracle.json.testharness;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import java.util.Iterator;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;

import junit.framework.Assert;

import oracle.jdbc.OracleConnection;

import oracle.soda.OracleCollectionAdmin;
import oracle.soda.OracleCursor;
import oracle.soda.OracleDatabase;
import oracle.soda.OracleDatabaseAdmin;
import oracle.soda.OracleDocument;
import oracle.soda.OracleException;
import oracle.soda.OracleOperationBuilder;

import oracle.soda.rdbms.OracleRDBMSClient;

import oracle.soda.rdbms.impl.OracleDocumentImpl;
import oracle.soda.rdbms.impl.OracleOperationBuilderImpl;

public class SodaTestCase extends DatabaseTestCase {

  protected OracleDatabase db = null;
  protected OracleDatabaseAdmin dbAdmin = null;
  protected String schemaName = null;
  protected OracleRDBMSClient client = null;

  protected void setUp() throws Exception{
    super.setUp();
    client = new OracleRDBMSClient();
    db = client.getDatabase(conn);
    dbAdmin = db.admin();
    dropAllCollections();
    schemaName = System.getProperty("UserName"); 
  }
  
  protected void tearDown() throws Exception {
    dropAllCollections();
    conn.close();
  }
  
  private void dropAllCollections() throws Exception {
    String colName = null;
    Iterator<String> colIterator = dbAdmin.getCollectionNames().iterator();
    while (colIterator.hasNext()) {
      try {
        colName = colIterator.next();
        OracleCollectionAdmin colAdmin = db.openCollection(colName).admin();
        colAdmin.drop();
      }
      catch(Exception ex)
      {
        ex.printStackTrace();
      }
    }
  }
  
  public static void printHeader(OracleCursor jcur) throws Exception {
    while (jcur.hasNext()) {
      OracleDocument d = jcur.next();

      System.out.println("Key " + d.getKey() + " version " + d.getVersion()
            + " last mod " + d.getLastModified() + " created on "
            + d.getCreatedOn());
    }
    jcur.close();
  }
  
  public static String inputStream2String(InputStream is) throws Exception {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    int i = -1;
    while ((i = is.read()) != -1) {
      stream.write(i);
    }
    return stream.toString();
  }
 
  public static byte[] File2Bytes(String filePath){  
    byte[] buffer = null;  
    try {  
        File file = new File(filePath);  
        FileInputStream fis = new FileInputStream(file);  
        ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);  
        byte[] b = new byte[1024];  
        int n;  
        while ((n = fis.read(b)) != -1) {  
            bos.write(b, 0, n);  
        }  
        fis.close();  
        bos.close();  
        buffer = bos.toByteArray();  
    } catch (FileNotFoundException e) {  
        e.printStackTrace();  
    } catch (IOException e) {  
        e.printStackTrace();  
    }  
    return buffer;  
  }
 
  public static JsonValue getValue(OracleDocument doc, String... path) throws Exception {
    InputStream in = new ByteArrayInputStream(doc.getContentAsByteArray());

    JsonReader reader = Json.createReader(in);
    JsonValue v = reader.read();
    in.close();
    for (int i = 0; i < path.length; i++) {
        v = ((JsonObject)v).get(path[i]);
    }
    return v;
  }
  
  public static String[] path(String... path) {
    return path;
  }
  
  public static void verifyNullContentDocument (OracleDocument doc) throws Exception {
    
    if (doc.getMediaType() == null || doc.getMediaType().equalsIgnoreCase("application/json")) {
      // getContentAsString() is supported only for JSON content
      assertNull(doc.getContentAsString()); 
    }

    assertNull(doc.getContentAsByteArray());
    assertNull(((OracleDocumentImpl) doc).getContentAsStream());
    assertEquals(-1, doc.getContentLength());
    
    assertNotNull(doc.getMediaType());
    assertNotNull(doc.getCreatedOn());
    assertNotNull(doc.getKey());
    assertNotNull(doc.getLastModified());
    assertNotNull(doc.getVersion());
  }

  public static void compareInputStream(InputStream srcSrm, InputStream rltSrm) throws IOException {
    
    byte[] srcbuffer = new byte[1024*1024];
    byte[] resultbuffer = new byte[1024*1024];
    int rlen = 0, slen = 0, counter = 0;
    while ((rlen = rltSrm.read(resultbuffer)) != -1) {
      counter++; 
      //System.out.println("Debug: " + counter + "th compare");
      slen = srcSrm.read(srcbuffer);
      assertEquals(slen, rlen);
      for(int i=0; i<rlen; i++)
        assertEquals(srcbuffer[i], resultbuffer[i]);
    }
    
  } 
  
  public enum IndexType {
    noIndex, textIndex, funcIndex
  }
  
  //to check whether index is picked up by execution plan
  public static void chkExplainPlan(OracleOperationBuilder builder, 
      IndexType indextype, String indexName) throws OracleException
  {
    chkExplainPlan((OracleOperationBuilderImpl)builder, indextype, indexName);
  }
  
  public static void chkExplainPlan(OracleOperationBuilderImpl builderImpl, 
      IndexType indextype, String indexName) throws OracleException
  {
    String plan = null;
    final String textIndexKey = "DOMAIN INDEX";
    final String funcIndexKey = "INDEX RANGE SCAN";
    
    switch (indextype) {
      case textIndex:
        plan = builderImpl.explainPlan("basic");
        // Note: (?s) allows matching across return lines
        if (!plan.matches("(?s).*" + textIndexKey + ".*"))
        {
          Assert.fail(textIndexKey + " is not found in explain plan:\n" + plan);
        }
        if (!plan.matches("(?s).*" + indexName + ".*"))
        {
          Assert.fail(indexName + " is not found in explain plan:\n" + plan);
        }
        break;
      case funcIndex:
        plan = builderImpl.explainPlan("basic");
        // Note: (?s) allows matching across return lines
        if (!plan.matches("(?s).*" + funcIndexKey + ".*"))
        {
          Assert.fail(funcIndexKey + " is not found in explain plan:\n" + plan);
        }
        if (!plan.matches("(?s).*" + indexName + ".*"))
        {
          Assert.fail(indexName + " is not found in explain plan:\n" + plan);
        }
        break;
      default:
        return;
    }
    
    return;
    
  }

  public enum SrearchIndexType {
    // no index 
    noIndex,
    // used for "text" index, "text_value" index && QBE tests text value
    textIndex,
    // used for "text_value" index && QBE tests numeric value
    numberIndex 
  }
  
  // check whether the plan pick up search index correctly
  public static void chkSearchIndexExplainPlan(OracleOperationBuilder builder, 
      SrearchIndexType searchIndextype, String indexName) throws OracleException
  {
    chkSearchIndexExplainPlan((OracleOperationBuilderImpl)builder, searchIndextype, indexName);
  }
  
  public static void chkSearchIndexExplainPlan(OracleOperationBuilderImpl builderImpl, 
      SrearchIndexType searchIndextype, String indexName) throws OracleException
  {
    String plan = builderImpl.explainPlan("all");
    final String domainIndexKey = "DOMAIN INDEX";
    final String textIndexKey1 = "HASPATH";
    final String textIndexKey2 = "INPATH";
    final String numberIndexKey = "sdatap";
    
    switch (searchIndextype) {
      case textIndex:
        // Note: (?s) allows matching across return lines
        if (!plan.matches("(?s).*" + textIndexKey1 + ".*") 
            && !plan.matches("(?s).*" + textIndexKey2 + ".*"))
        {
          Assert.fail(textIndexKey2 + " and " + textIndexKey2 
              + " are both not found in explain plan:\n" + plan);
        }
        
        break;
      case numberIndex:
        if (!plan.matches("(?s).*" + numberIndexKey + ".*"))
        {
          Assert.fail(numberIndexKey + " is not found in explain plan:\n" + plan);
        }
        break;
      default:
        return;
    }
    
    if (!plan.matches("(?s).*" + domainIndexKey + ".*"))
    {
      Assert.fail(domainIndexKey + " is not found in explain plan:\n" + plan);
    }
    if (!plan.matches("(?s).*" + indexName + ".*"))
    {
      Assert.fail(indexName + " is not found in explain plan:\n" + plan);
    }
    
    return;
  }
  
}

