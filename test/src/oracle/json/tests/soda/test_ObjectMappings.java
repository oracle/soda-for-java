package oracle.json.tests.soda;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import jakarta.json.JsonStructure;
import jakarta.json.JsonValue;
import jakarta.json.stream.JsonParser;

import oracle.jdbc.OracleDatabaseMetaData;
import oracle.json.testharness.SodaTestCase;
import oracle.soda.OracleCollection;
import oracle.soda.OracleDocument;
import oracle.soda.OracleException;
import oracle.soda.rdbms.impl.OracleDocumentImpl;
import oracle.soda.rdbms.impl.OracleDatabaseImpl;

public class test_ObjectMappings extends SodaTestCase {
  
  private static final String HELLO = "{\"hello\":\"world\"}";
  private OracleCollection col;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    col = db.admin().createCollection("col");
  }
  
  public void testNegativeCreate() {
    try {
      db.createDocumentFrom(123);
      fail();
    } catch (OracleException e) {
      if (e.getMessage().contains("oracle.sql.json.OracleJsonFactory class is not available. Ensure the JDBC jar includes oracle.sql.json support.") && !OracleDatabaseImpl.isOracleJsonAvailable())
        return;

      assertEquals("Unsupported type: class java.lang.Integer", e.getMessage());
    }
  }
  
  public void testNegativeGet() {
    try {
      OracleDocument d = db.createDocumentFrom("{}");
      d.getContentAs(Integer.class);
      fail();
    } catch (OracleException e) {
      if (e.getMessage().contains("oracle.sql.json.OracleJsonFactory class is not available. Ensure the JDBC jar includes oracle.sql.json support.") && !OracleDatabaseImpl.isOracleJsonAvailable())
        return;

      assertEquals("Unsupported type: class java.lang.Integer", e.getMessage());
    }
  }

}
