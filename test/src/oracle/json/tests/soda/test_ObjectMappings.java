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
import oracle.sql.json.OracleJsonFactory;
import oracle.sql.json.OracleJsonGenerator;
import oracle.sql.json.OracleJsonObject;
import oracle.sql.json.OracleJsonParser;

public class test_ObjectMappings extends SodaTestCase {

  private static final String HELLO = "{\"hello\":\"world\"}";
  private OracleJsonFactory factory = new OracleJsonFactory();
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

  public void testSimple() throws Exception {

    JsonObject obj = Json.createObjectBuilder()
            .add("name", "pear")
            .add("count", 47)
            .build();

    col.save(db.createDocumentFrom(obj));

    OracleDocument doc = col.find().getOne();

    JsonObject obj2 = doc.getContentAs(JsonObject.class);

    assertEquals("pear", obj2.getString("name"));
    assertEquals(47, obj2.getInt("count"));
  }

  public boolean isJdbc19() {
    try {
      OracleDatabaseMetaData omd = (OracleDatabaseMetaData) conn.getMetaData();
      String ver = omd.getDriverVersion();
      return ver.startsWith("19");
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  public boolean isBinary(OracleDocument doc) {
    OracleDocumentImpl impl = (OracleDocumentImpl)doc;
    return impl.isBinary();
  }


  public void testOracleSimple() throws Exception {

    OracleJsonObject obj = factory.createObject();
    obj.put("name", "pear");
    obj.put("count", 47);

    col.save(db.createDocumentFrom(obj));

    OracleDocument doc = col.find().getOne();


    if (!isBinary(doc) && isJdbc19()) {
      return; // 19 driver doesn't have a text parser
    }

    OracleJsonObject obj2 = doc.getContentAs(OracleJsonObject.class);

    assertEquals("pear", obj2.getString("name"));
    assertEquals(47, obj2.getInt("count"));
  }

  public void testJsonObject() throws OracleException, IOException {
    JsonObject obj = Json.createObjectBuilder()
            .add("hello", "world")
            .build();
    roundTripHello(obj);
  }

  public void testParserJsonObject() throws OracleException, IOException {
    roundTripHello(Json.createParser(new StringReader(HELLO)));
  }

  /*
  public void testOracleParserJsonObject() throws OracleException, IOException {
    OracleJsonParser parser = factory.createJsonTextParser(new StringReader(HELLO));
    roundTripHello(parser);
  }
  */

  public void testStringJsonObject() throws OracleException, IOException {
    roundTripHello(HELLO);
  }

  public void testReaderJsonObject() throws OracleException, IOException {
    roundTripHello(new StringReader(HELLO));
  }

  public void testBytesJsonObject() throws OracleException, IOException {
    byte[] utf8 = HELLO.getBytes(StandardCharsets.UTF_8);
    roundTripHello(utf8);
  }

  public void testInputStreamJsonObject() throws OracleException, IOException {
    byte[] utf8 = HELLO.getBytes(StandardCharsets.UTF_8);
    roundTripHello(new ByteArrayInputStream(utf8));
  }

  public void testOracleJsonParser() throws OracleException, IOException {
    JsonParser parser = Json.createParser(new StringReader(HELLO));
    roundTripHello(parser);
  }

  public void testOsonJsonObject() throws OracleException, IOException {
    byte[] oson = osonHello();
    roundTripHello(oson);
  }

  public void testOsonStreamJsonObject() throws OracleException, IOException {
    byte[] oson = osonHello();
    roundTripHello(new ByteArrayInputStream(oson));
  }

  private void assertHello(OracleDocument doc) throws OracleException, IOException {

    JsonObject hello = null;

    try
    {
      if (isCompatibleOrGreater(COMPATIBLE_23) && !isDBVersion23dot2())
      {
        JsonObject obj = doc.getContentAs(JsonObject.class);
        hello = hello(obj.getString("_id"));
      }
      else
      {
        hello = hello();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }


    // JsonObject
    JsonObject obj2 = doc.getContentAs(JsonObject.class);
    assertEquals(hello, obj2);

    // JsonValue
    JsonValue jv = doc.getContentAs(JsonValue.class);
    assertEquals(hello, jv);

    // JsonStructure
    JsonStructure struct = doc.getContentAs(JsonStructure.class);
    assertEquals(hello, struct);

    // JsonParser
    try
    {
      if (isCompatibleOrGreater(COMPATIBLE_23) && !isDBVersion23dot2())
      {
        JsonParser parser = doc.getContentAs(JsonParser.class);
        assertEquals(JsonParser.Event.START_OBJECT, parser.next());
        assertEquals(JsonParser.Event.KEY_NAME, parser.next());
        assertEquals(JsonParser.Event.VALUE_STRING, parser.next());
        assertEquals(JsonParser.Event.KEY_NAME, parser.next());
        assertEquals(JsonParser.Event.VALUE_STRING, parser.next());
        assertEquals(JsonParser.Event.END_OBJECT, parser.next());
        parser.close();
      }
      else
      {
        JsonParser parser = doc.getContentAs(JsonParser.class);
        assertEquals(JsonParser.Event.START_OBJECT, parser.next());
        assertEquals(JsonParser.Event.KEY_NAME, parser.next());
        assertEquals(JsonParser.Event.VALUE_STRING, parser.next());
        assertEquals(JsonParser.Event.END_OBJECT, parser.next());
        parser.close();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    // Reader
    Reader reader = doc.getContentAs(Reader.class);
    assertEquals('{',  reader.read());
    assertEquals('\"', reader.read());
    assertEquals('h',  reader.read());
    assertEquals('e',  reader.read());

    // String
    String str = doc.getContentAs(String.class);
    assertEquals(hello.toString(), str);

    // CharSequence
    CharSequence cs = doc.getContentAs(CharSequence.class);
    assertEquals(hello.toString(), cs.toString());

    // byte[]
    byte[] bytes = doc.getContentAs(byte[].class);
    assertEquals(doc.getContentAsByteArray(), bytes);

    // InputStream
    InputStream is = doc.getContentAs(InputStream.class);
    for (int i = 0; i < bytes.length; i++) {
      assertEquals(bytes[i], is.read());
    }

    if (!isBinary(doc) && isJdbc19()) {
      return; // 19 driver doesn't have a text parser
    }

    // OracleJsonObject
    OracleJsonObject oobj2 = doc.getContentAs(OracleJsonObject.class);
    assertEquals("world", oobj2.getString("hello"));


    // OracleJsonParser
    // JsonParser treats value as string but oraclejsonparser treats it as binary
    try
    {
      if (isCompatibleOrGreater(COMPATIBLE_23) && !isDBVersion23dot2())
      {
        OracleJsonParser oparser = doc.getContentAs(OracleJsonParser.class);
        assertEquals(OracleJsonParser.Event.START_OBJECT, oparser.next());
        assertEquals(OracleJsonParser.Event.KEY_NAME, oparser.next());
        assertEquals(OracleJsonParser.Event.VALUE_STRING, oparser.next());
        assertEquals(OracleJsonParser.Event.KEY_NAME, oparser.next());
        assertEquals(OracleJsonParser.Event.VALUE_BINARY, oparser.next());
        assertEquals(OracleJsonParser.Event.END_OBJECT, oparser.next());
        oparser.close();
      }
      else
      {
        OracleJsonParser oparser = doc.getContentAs(OracleJsonParser.class);
        assertEquals(OracleJsonParser.Event.START_OBJECT, oparser.next());
        assertEquals(OracleJsonParser.Event.KEY_NAME, oparser.next());
        assertEquals(OracleJsonParser.Event.VALUE_STRING, oparser.next());
        assertEquals(OracleJsonParser.Event.END_OBJECT, oparser.next());
        oparser.close();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }

  private void roundTripHello(Object hello) throws OracleException, IOException {
    OracleDocument doc = db.createDocumentFrom(hello);

    col.save(doc);

    OracleDocument doc2 = col.find().getOne();

    assertHello(doc2);
  }

  private byte[] osonHello() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    OracleJsonGenerator gen = factory.createJsonBinaryGenerator(baos);
    gen.writeParser(Json.createParser(new StringReader(HELLO)));
    gen.close();
    byte[] oson = baos.toByteArray();
    return oson;
  }

  private JsonObject hello(String str) {
    return  Json.createObjectBuilder()
            .add("hello", "world")
            .add("_id", str)
            .build();
  }

  private JsonObject hello() {
    return  Json.createObjectBuilder()
            .add("hello", "world")
            .build();
  }

  public void testJsonArray() throws OracleException, IOException {
    JsonArray arr = Json.createArrayBuilder()
            .add("hello")
            .build();
    roundTripArray(arr);
  }

  private void roundTripArray(Object arr) throws OracleException, IOException {
    OracleDocument doc = db.createDocumentFrom(arr);
    OracleDocument docm;

    if (isCompatibleOrGreater(COMPATIBLE_20))
    {
      docm = client.createMetadataBuilder().keyColumnAssignmentMethod("UUID").contentColumnType("JSON").build();
    }
    else
    {
      docm = client.createMetadataBuilder().keyColumnAssignmentMethod("UUID").contentColumnType("BLOB").build();
    }

    OracleCollection colm = db.admin().createCollection("colm", docm);

    colm.save(doc);

    OracleDocument doc2 = colm.find().getOne();

    JsonArray jarr = doc2.getContentAs(JsonArray.class);
    assertEquals("hello", jarr.getString(0));
  }

}
