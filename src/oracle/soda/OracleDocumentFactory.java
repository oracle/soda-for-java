/* Copyright (c) 2014, 2015, Oracle and/or its affiliates. 
All rights reserved.*/

package oracle.soda;

import java.io.InputStream;

/**
 *  A factory for creating {@link OracleDocument} objects.
 */
public interface OracleDocumentFactory
{

  /**
   * Creates a new document, with the provided <code>String</code> JSON content.
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
  public OracleDocument createDocumentFromString(String content)
    throws OracleException;

  /**
   * Creates a new document, with the provided key and
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
  public OracleDocument createDocumentFromString(String key,
                                                 String content)
          throws OracleException;

  /**
   * Creates a new document, with the provided key, <code>String</code>
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
  public OracleDocument createDocumentFromString(String key,
                                                 String content,
                                                 String mediaType)
    throws OracleException;

  /**
   * Creates a new document, with the provided <code>byte[]</code> JSON content.
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
  public OracleDocument createDocumentFromByteArray(byte[] content)
    throws OracleException;

  /**
   * Creates a new document, with the provided key and
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
  public OracleDocument createDocumentFromByteArray(String key,
                                                    byte[] content)
    throws OracleException;

  /**
   * Creates a new document, with the provided key, <code>byte[]</code>
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
  public OracleDocument createDocumentFromByteArray(String key,
                                                    byte[] content,
                                                    String mediaType)
    throws OracleException;
}
