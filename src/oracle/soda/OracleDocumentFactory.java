/* Copyright (c) 2014, 2020, Oracle and/or its affiliates. 
All rights reserved.*/

package oracle.soda;

/**
 *  A factory for creating {@link OracleDocument} objects.
 */
public interface OracleDocumentFactory
{

  /**
   * Creates a new document with the provided <code>String</code> JSON content.
   * <p>
   * This method is equivalent to invoking
   * {@link #createDocumentFromString(String, String, String)
   * createDocumentFromString(null, content, null)}
   * </p>
   *
   * @see #createDocumentFromString(String, String, String)
   * @see #createDocumentFromString(String, String)
   *
   * @param content           document content. Can be <code>null</code>
   *
   * @return                  created <code>OracleDocument</code>, with
   *                          media type <code>"application/json"</code>
   *
   * @throws OracleException  if the document cannot be created
   */
  OracleDocument createDocumentFromString(String content)
    throws OracleException;

  /**
   * Creates a new document with the provided key and
   * <code>String</code> JSON content.
   * <p>
   * This method is equivalent to invoking
   * {@link #createDocumentFromString(String, String, String)
   * createDocumentFromString(key, content, null)}
   * </p>
   *
   * @see #createDocumentFromString(String, String, String)
   * @see #createDocumentFromString(String)
   *
   *
   * @param key               document key. Can be <code>null</code>
   * @param content           document content. Can be <code>null</code>
   *
   * @return                  created <code>OracleDocument</code>, with
   *                          media type <code>"application/json"</code>
   *
   * @throws OracleException  if the document cannot be created
   */
  OracleDocument createDocumentFromString(String key,
                                          String content)
          throws OracleException;

  /**
   * Creates a new document with the provided key, <code>String</code>
   * content, and media type.
   * <p>
   * If the media type is <code>"application/json"</code> or <code>null</code>, 
   * the provided <code>content</code> must be JSON, as defined in RFC 4627. 
   * </p>
   *
   * @param key               document key. Can be <code>null</code>
   * @param content           document content. Can be <code>null</code>
   * @param mediaType         document media type. Can be <code>null</code>,
   *                          in which case the media type defaults to
   *                          <code>"application/json"</code>. An implementation
   *                          might not support any particular media type
   *
   * @return                  created <code>OracleDocument</code>
   * @throws OracleException  if the document cannot be created
   */
  OracleDocument createDocumentFromString(String key,
                                          String content,
                                          String mediaType)
    throws OracleException;

  
  /**
   * Creates a new document with the provided key and provided
   * JSON <code>content</code> object.
   * <p>
   * This method is equivalent to invoking
   * {@link #createDocumentFrom(String, Object)
   * createDocumentFrom(null, content)}
   * </p>
   *
   * @see #createDocumentFrom(String, Object)
   *
   *
   * @param content           document content. Must not be <code>null</code>
   *
   * @return                  created <code>OracleDocument</code>, with
   *                          media type <code>"application/json"</code>
   *
   * @throws OracleException  if the document cannot be created from the 
   *                          specified value
   */
  OracleDocument createDocumentFrom(Object content)
    throws OracleException;
  
  /**
   * Creates a new document with the provided key and JSON <code>content</code>
   * object.  The content object must be an instance
   * of one of the following types:
   * <br><br>
   * <table border="1" cellpadding="5" summary="Supported types">
   * <tr>
   * <th>Class</th>
   * <th>Description</th>
   * </tr>
   * <tr>
   * <td>
   * {@code javax.json.JsonValue}<br>
   * {@code oracle.sql.json.OracleJsonValue}
   * </td>
   * <td> A instance of {@code JsonValue} or {@code OracleJsonValue}.  This includes derivations
   * such as {@code JsonObject} and {@code JsonArray}. 
   * 
   * For example: <pre><code> JsonBuilderFactory factory = Json.createBuilderFactory(null);
   * JsonObject obj = factory.createObjectBuilder()
   *                         .add("name", "pear")
   *                         .add("count", 47)
   *                         .build();
   * OracleDocument doc = db.createDocumentFrom(obj);
   * </code></pre>
   * </td>
   * </tr>
   * <tr>
   * <td>
   * {@code javax.json.stream.JsonParser}<br>
   * {@code oracle.sql.json.OracleJsonParser}
   * </td>
   * <td>
   * A JSON event stream. 
   * </td>
   * </tr>
   * <tr>
   * <td>{@code java.lang.String}<br>
   *     {@code java.lang.CharSequence}<br>
   *     {@code java.io.Reader}<br>
   * </td>
   * <td>A JSON text value. </td>
   * </tr>
   * <tr>
   * <td>{@code java.io.InputStream}<br>
   *     {@code byte[]}<br>
   * </td>
   * <td>Either a JSON text value (UTF8, UTF16, etc) or Oracle binary JSON.
   * </td>
   * </tr>
   * </table>
   *
   * @see <a href="https://javaee.github.io/jsonp/">Java API for JSON Processing</a>
   *
   * @param key               document key. Can be <code>null</code>
   * @param content           document content. Must not be <code>null</code>
   *
   * @return                  created <code>OracleDocument</code>, with
   *                          media type <code>"application/json"</code>
   *
   * @throws OracleException  if the document cannot be created from the 
   *                          specified value.
   */
   OracleDocument createDocumentFrom(String key, Object content)
    throws OracleException;
  
  /**
   * Creates a new document with the provided <code>byte[]</code> JSON content.
   * <p>
   * This method is equivalent to invoking
   * {@link #createDocumentFromByteArray(String, byte[], String)
   * createDocumentFromByteArray(null, content, null)}
   * </p>
   *
   * @see #createDocumentFromByteArray(String, byte[], String)
   * @see #createDocumentFromByteArray(String, byte[])
   *
   * @param content           document content. Can be <code>null</code>
   *
   * @return                  created <code>OracleDocument</code>, with
   *                          media type <code>"application/json"</code>
   *
   * @throws OracleException  if the document cannot be created
   */
  OracleDocument createDocumentFromByteArray(byte[] content)
    throws OracleException;

  /**
   * Creates a new document with the provided key and
   * <code>byte[]</code> JSON content.
   * <p>
   * This method is equivalent to invoking
   * {@link #createDocumentFromByteArray(String, byte[], String)
   * createDocumentFromByteArray(key, content, null)}
   * </p>
   *
   * @see #createDocumentFromByteArray(String, byte[], String)
   * @see #createDocumentFromByteArray(byte[])
   *
   * @param key               document key. Can be <code>null</code>
   * @param content           document content. Can be <code>null</code>
   *
   * @return                  created <code>OracleDocument</code>, with
   *                          media type <code>"application/json"</code>
   *
   * @throws OracleException  if the document cannot be created
   */
  OracleDocument createDocumentFromByteArray(String key,
                                             byte[] content)
    throws OracleException;

  /**
   * Creates a new document with the provided key, <code>byte[]</code>
   * content, and content type.
   * <p>
   * If the media type is <code>"application/json"</code> or <code>null</code>, 
   * the provided <code>content</code> must be JSON, as defined in RFC 4627. 
   * The supported encodings are UTF-8, and UTF16 (BE and LE).
   * </p>
   *
   * @param key               document key. Can be <code>null</code>
   * @param content           document content. Can be <code>null</code>
   * @param mediaType         document media type. Can be <code>null</code>,
   *                          in which case the media type defaults to
   *                          <code>"application/json"</code>. An implementation
   *                          might not support any particular media type
   *
   * @return                  created <code>OracleDocument</code>
   * @throws OracleException  if the document cannot be created
   */
  OracleDocument createDocumentFromByteArray(String key,
                                             byte[] content,
                                             String mediaType)
    throws OracleException;
}
