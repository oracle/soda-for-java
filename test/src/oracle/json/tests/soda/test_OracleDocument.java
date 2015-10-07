/* Copyright (c) 2014, 2015, Oracle and/or its affiliates. 
All rights reserved.*/

/*
   DESCRIPTION
    OracleDocument charset related tests
 */

/**
 *  @author  Josh Spiegel
 */
package oracle.json.tests.soda;

import java.io.IOException;
import java.nio.charset.Charset;

import junit.framework.TestCase;
import oracle.jdbc.OracleConnection;
import oracle.json.testharness.ConnectionFactory;
import oracle.json.testharness.JsonTestCase;
import oracle.json.testharness.SodaUtils;
import oracle.soda.OracleDatabase;
import oracle.soda.OracleDocument;
import oracle.soda.OracleException;
import oracle.soda.rdbms.OracleRDBMSClient;

public final class test_OracleDocument extends JsonTestCase {
    
    static final Charset UTF8    = Charset.forName("UTF-8");
    static final Charset UTF16   = Charset.forName("UTF-16");
    static final Charset UTF32   = Charset.forName("UTF-32");
    static final Charset UTF16LE = Charset.forName("UTF-16LE");
    static final Charset UTF16BE = Charset.forName("UTF-16BE");
    static final Charset UTF32LE = Charset.forName("UTF-32LE");
    static final Charset UTF32BE = Charset.forName("UTF-32BE");
    
    // https://bugs.openjdk.java.net/browse/JDK-8013053?page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel
    static final Charset UTF32LEBOM = Charset.forName("X-UTF-32BE-BOM");
    static final Charset UTF32BEBOM = Charset.forName("X-UTF-32LE-BOM");
    static final Charset UTF16LEBOM = Charset.forName("X-UTF-16LE-BOM");
    
    static final Charset[] CHARSETS = { UTF8, UTF16, UTF32, UTF32LEBOM, UTF32BEBOM, UTF16LE, UTF16BE, UTF32LE, UTF32BE, UTF16LEBOM }; 
    
    OracleConnection con;
    OracleDatabase db;
    
    @Override 
    public final void setUp() throws Exception {
        con = ConnectionFactory.createConnection();
        db = new OracleRDBMSClient().getDatabase(con);
    }
    
    @Override
    public final void tearDown() throws Exception {
        con.close();
    }

    public void testCharsetDetection() throws Exception {
        // Some characters that can start a JSON document
        String[] leadingChars = {
                "\"", "[", "{", " ", "-", "0"
        };
        
        for (String leading : leadingChars) { 
            for(int start = 1; start < Character.MAX_CODE_POINT; start++) {
                StringBuilder builder = new StringBuilder();
                builder.append(leading);
                
                // Add next 3 characters
                int ct = 0;
                for (int cp = start; cp < Character.MAX_CODE_POINT; cp++) {
                    if ((cp >= 0xD800 && cp <= 0xDFFF) || !Character.isDefined(cp)) {
                        continue;
                    }
                    builder.appendCodePoint(cp);
                    ct++;
                    if (ct >= 3) {
                        break;
                    }
                }
                
                String content = builder.toString();
                for (Charset c : CHARSETS) {
                    byte[] encoded = content.getBytes(c);
                    OracleDocument doc = db.createDocumentFromByteArray(null, encoded, null);
                    if (!content.equals(doc.getContentAsString())) {
                        fail("Charset failed " + start + " " + c.toString());
                    }
                }
                if (start > 0xFFFF) {
                    start += 0xFFF; // speed it up after we are past the common characters
                }
            }
        }
    }
    
    public void testCharsetDetectionUnknown() throws OracleException {

        String expected = "Media type of the document is not \"application/json\". getContentAsString() " +
                          "is only supported for JSON documents.";

        OracleDocument doc = db.createDocumentFromByteArray(null, new byte[] { 0 }, "text/plain");
        try {
            doc.getContentAsString();
            fail("Expected exception");
        } catch (OracleException e) {
            assertEquals(expected, e.getMessage());
        }
        
        doc = db.createDocumentFromByteArray(null, new byte[] { 0 }, "AN INVALID CONTENT TYPE");
        try {
            doc.getContentAsString();
            fail("Expected exception");
        } catch (OracleException e) {
            assertEquals(expected, e.getMessage());
        }        
    }
    
    public void testCharsetDetectionTinyJSON() throws OracleException {
        for (Charset c : CHARSETS) {
            byte[] json = "1".getBytes(c);
            OracleDocument d = db.createDocumentFromByteArray(null, json, null);
            assertEquals("1", d.getContentAsString());
        }
        for (Charset c : CHARSETS) {
            byte[] json = "{}".getBytes(c);
            OracleDocument d = db.createDocumentFromByteArray(null, json, null);
            assertEquals("{}", d.getContentAsString());
        }        
    }
    
    public void testCharsetDetectionCaseSensitivity() throws OracleException {
        for (Charset c : CHARSETS) {
            byte[] json = "[1, 2, 3]".getBytes(c);
            OracleDocument d = db.createDocumentFromByteArray(null, json, "APPLICATION/JSON");
            assertEquals("[1, 2, 3]", d.getContentAsString());
        }
    }
    
    public void testCharsetDetectionJsonFiles() throws OracleException, IOException {
        String [] files  = {
        "data/putreplace.json",
        "data/updatepost.json",
        "data/createcollection.json",
        "data/putcreate.json",
        "data/arraydelete.json",
        "data/createfuncindex.json",
        "data/arrayinsert.json",
        "data/arrayget.json",
        "data/colspec.json",
        "data/dropindexspec.json"
        };
        for (String file : files) {
            String fileContent = SodaUtils.slurpFile(file);
            for (Charset c : CHARSETS) {
                byte[] bytes = fileContent.getBytes(c);
                OracleDocument d = db.createDocumentFromByteArray(null, bytes, null);
                assertEquals(fileContent, d.getContentAsString());
            }
        }
    }
    
    public void testNullString() throws OracleException, IOException {
        OracleDocument d = db.createDocumentFromByteArray(null, null, null);
        assertEquals(null, d.getContentAsString());
    }

}    
    

