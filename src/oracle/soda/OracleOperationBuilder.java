/* Copyright (c) 2014, 2020, Oracle and/or its affiliates. 
All rights reserved.*/

package oracle.soda;

import java.util.Set;

/**
 * An <code>OracleOperationBuilder</code> builds and executes various read 
 * and write operations on the {@link OracleCollection}, in a chainable
 * manner.
 * <p>
 *
 * An <code>OracleOperationBuilder</code> is first obtained by calling a
 * {@link OracleCollection#find()}.
 * <p>
 *
 * <code>OracleOperationBuilder</code> provides two types of methods: terminal 
 * and non-terminal.
 *
 * <p>
 * Non-terminal methods are used to build up the operation in a chainable
 * manner. They return the same <code>OracleOperationBuilder</code> object, on
 * which other non-terminal or terminal methods can be chained. Non-terminal
 * methods do not cause operation creation or execution. Under the hood, they
 * simply store additional state information in the
 * <code>OracleOperationBuilder</code> object, capturing the specified
 * parts of the operation.
 * <p>
 * Unlike non-terminal methods, terminal methods actually cause
 * operation creation and execution.
 * <p>
 * For example:
 * <pre>
 *     OracleCollection ocollection = ...;
 *     OracleCursor ocursor = ocollection.find().keys(keys).skip(25).limit(25).getCursor();
 * </pre>
 *  
 * In this example, {@link #keys(Set)}, {@link #skip(long)},
 * and {@link #limit(int)} are non-terminal methods that specify parts
 * of the operation. More concretely, they specify that documents matching
 * provided keys should be returned, the first 25 of these results should be
 * skipped,and the number of results should be limited to 25. The
 * {@link #getCursor()} method is a terminal, so it builds the operation
 * from the specified parts and executes it.
 */
public interface OracleOperationBuilder
{
  /**
   * Finds documents with specific keys.
   * <p>
   * This is a non-terminal method, and, as such, it does
   * not cause operation execution.
   * <p>
   * If this method is invoked in conjunction with {@link #key(String)}
   * or {@link #keyLike(String, String)} on the same {@code OracleOperationBuilder}
   * object, then only the method specified last will be honored,
   * and the preceding ones will be ignored.
   * <p>
   *
   * Example:
   * <pre>
   * {@code
   * // key(...) is ignored, because keys(...) is set last.
   * col.find().key(...).keys(...).getCursor();
   * }
   * </pre>
   *
   * @param keys                   a set of keys. Cannot be <code>null</code>
   * @return                       <code>OracleOperationBuilder</code>
   * @throws OracleException       if <code>keys</code> is <code>null</code>,
   *                               or doesn't return any elements.
   */
  OracleOperationBuilder keys(Set<String> keys)
    throws OracleException;

  /**
   * Finds a document with a specific key.
   * <p>
   * This is a non-terminal method, and, as such, it does
   * not cause operation execution.
   * <p>
   * If this method is invoked in conjunction with {@link #keys(Set)}
   * or {@link #keyLike(String, String)} on the same {@code OracleOperationBuilder}
   * object, then only the method specified last will be honored,
   * and the preceding ones will be ignored.
   * <p>
   * Example:
   * <pre>
   * {@code
   * // keys(...) is ignored, because key(...) is set last.
   * col.find().keys(...).key(...).getOne();
   * }
   * </pre>
   *
   * @param key                    the key. Cannot be <code>null</code>
   * @return                       <code>OracleOperationBuilder</code>
   * @throws OracleException       if the key is <code>null</code>
   */
  OracleOperationBuilder key(String key)
    throws OracleException;

  /**
   * Finds documents with keys matching a supplied {@code pattern}.
   * <p>
   * This method is only supported on collections with client-assigned
   * keys, and a key column of type varchar2.
   * <p>
   * The {@code pattern} can contain special pattern matching characters _ (which
   * matches any single character), or % (which matches zero or more characters).
   * The {@code escape} parameter allows specifying an optional escape character, which
   * is used to test for literal occurences of the special pattern matching characters
   * _ and %. 
   * <p>
   * Example 1: passing "%mykey%" for {@code pattern}, and {@code null} for {@code escape}
   * will match documents with keys containing the string "mykey", e.g "mykey20" or "20mykey".
   * <p>
   * Example 2: passing "key_1" for {@code pattern}, and {@code null} for {@code escape}
   * will match documents with keys that start with the string "key" followed by any single
   * character, followed by "1", e.g. "keyA1" or "keyB1".
   * <p>
   * Example 3: passing "mykey!_1" for {@code pattern}, and ! for {@code escape}
   * will match a document with key "mykey_1". Since the _ is escaped in the supplied
   * pattern, it is matched literally.
   * <p>
   * The {@code pattern} and {@code escape} character parameters correspond
   * to the pattern and escape of the Oracle SQL 
   * <a href="https://docs.oracle.com/database/121/SQLRF/conditions007.htm#SQLRF52141">LIKE condition.</a>
   * <p>
   * This is a non-terminal method, and, as such, it does
   * not cause operation execution.
   * <p>
   * If this method is invoked in conjunction with {@link #key(String)}
   * or {@link #keys(Set)} on the same {@code OracleOperationBuilder}
   * object, then only the method specified last will be honored,
   * and the preceding ones will be ignored.
   * <p>
   * Example:
   * <pre>
   * {@code
   * // keys(...) is ignored, because keyLike(...) is set last.
   * col.find().keys(...).keyLike(...).getCursor();
   * }
   * </pre>
   *
   * @param pattern                pattern. Can contain special pattern matching characters
   *                               _ and %. Cannot be <code>null</code>
   * @param escape                 escape character. Can be <code>null</code>,
   *                               which means no escape character will be used
   * @return                       <code>OracleOperationBuilder</code>
   * @throws OracleException       if pattern is <code>null</code>, or if
   *                               this method cannot be invoked on a particular
   *                               collection, because the latter doesn't have client-assigned
   *                               keys of type varchar2.
   *
   * @see <a href="https://docs.oracle.com/database/121/SQLRF/conditions007.htm#SQLRF52141">LIKE Condition</a>
   */
  OracleOperationBuilder keyLike(String pattern, String escape)
    throws OracleException;

  /**
   * Finds documents matching a filter specification
   * (a query-by-example expressed in JSON).
   * <p>
   * This is a non-terminal method, and, as such, it does
   * not cause operation execution.
   * <p>
   *
   * @param filterSpecification    the filter specification. Cannot
   *                               be <code>null</code>
   * @return                       <code>OracleOperationBuilder</code>
   * @throws OracleException       if the <code>filterSpecification</code>
   *                               is <code>null</code> or invalid.
   */
  OracleOperationBuilder filter(OracleDocument filterSpecification)
    throws OracleException;
  
  
  /**
   * Finds documents matching a filter specification
   * (a query-by-example expressed in JSON).  Calling this method is equivalent
   * to the following:
   * <pre>
   * <code>
   *   filter(db.createDocumentFromString(filterSpecification));
   * </code>
   * </pre>
   * @param filterSpecification    the filter specification. Cannot
   *                               be <code>null</code>
   * @return                       <code>OracleOperationBuilder</code>
   * @throws OracleException       if the <code>filterSpecification</code>
   *                               is <code>null</code> or invalid.
   */
  OracleOperationBuilder filter(String filterSpecification)
    throws OracleException;

  /**
   * Finds a document with a specific version.
   * <p>
   * This method can be used in conjunction with the <code>key(...)</code>
   * method to match a document with a specific key and version:
   * <code>col.find().key("k1").version("v1")</code>. This combination
   * is useful for optimistic locking.
   * <p>
   * For example,
   * replace-if-version operation can be specified as follows:
   * <code>col.find("k1").version("v1").replaceOne(d1)</code>
   * <p>
   * Similarly, remove-if-version operation can be specified as follows:
   * <code>col.find().key("k1").version("v1").remove()</code>
   * <p>
   * This is a non-terminal method, and, as such, it does
   * not cause operation execution.
   * <p>
   *
   * @param version                 version. Cannot be <code>null</code>
   * @return                        <code>OracleOperationBuilder</code>
   * @throws OracleException        if the provided version is <code>null</code>
   */
  OracleOperationBuilder version(String version)
    throws OracleException;

  /**
   * Returns <code>OracleCursor</code> which is an iterator over
   * result documents.
   * <p>
   * This is a terminal method, and, as such, it causes operation
   * execution.
   *
   * @return                        <code>OracleCursor</code>
   * @throws OracleException        if there's an error creating
   *                                the cursor
   */
  OracleCursor getCursor() throws OracleException;

  /**
   * Replaces a document.
   * <p>
   * This method is used in conjunction with <code>key(...)</code>,
   * and (optionally) <code>version(...)</code> methods.
   * <p>
   * For example:
   * <pre>
   * // Replace a document with the key "k1" and version "v1",
   * // with the input document 'd1'.
   * OracleDocument doc = col.find().key("k1").version("v1").replaceOneAndGet(d1)
   * </pre>
   * <p>
   * Note that the key and version information (if any) in the input
   * document <code>'d1'</code> is ignored.
   * <p>
   * This is a terminal method, and, as such, it causes operation
   * execution. 
   * <p>
   *
   * @param document                 input document. Cannot be <code>null</code>
   * @return                         result document which contains the
   *                                 key and (if present) the created-on
   *                                 timestamp, last-modified timestamp,
   *                                 and version only. The input document's
   *                                 content is not returned as part of the
   *                                 result document.
   * @throws OracleException         if (1) the input document is <code>null</code>,
   *                                 or (2) there's an error replacing the input
   *                                 <code>document</code>. 
   */
  OracleDocument replaceOneAndGet(OracleDocument document)
    throws OracleException;

  /**
   * Replaces a document.
   * <p>
   * This method is used in conjunction with <code>key(...)</code>,
   * and (optionally) <code>version(...)</code> methods.
   * <p>
   * For example:
   * <pre>
   * // Replace a document with the key "k1" and version "v1"
   * // with the input document 'd1'.
   * col.find().key("k1").version("v1").replaceOne(d1)
   * </pre>
   * <p>
   * Note that the key and version information (if any) in the input
   * document 'd1' is ignored.
   * <p>
   * This is a terminal method, and, as such, it causes operation
   * execution.
   *
   * @param document                 input document. Cannot be <code>null</code>
   * @return                         <code>true</code> if the document was
   *                                 replaced successfully, <code>false</code>
   *                                 otherwise
   * @throws OracleException         if (1) the input document is <code>null</code>,
   *                                 or (2) there's an error replacing the input
   *                                 <code>document</code>. 
   */
  boolean replaceOne(OracleDocument document)
    throws OracleException;

  /**
   * Removes documents.
   * <p>
   * For example, the following removes a document
   * with a particular key and version:
   * <code>col.find().key("k1").version("v1").remove()</code>
   * <p>
   * Documents matching specific keys can be
   * removed as follows:
   * <code>col.find().keys(keys).remove()</code>.
   * <p>
   * Documents matching a filter specification
   * can be removed as follows:
   * <code>col.find().filter(filterSpec).remove()</code>
   * <p>
   * This is a terminal method, and, as such, it causes operation
   * execution.
   * @exception OracleException     if an error during removal occurs
   *
   * @return                        count of the number of documents
   *                                removed.
   */
  int remove()
    throws OracleException;

  /**
   * Specifies an upper limit on the number of results.
   * <p>
   * This is a non-terminal method, and, as such, it does
   * not cause operation execution.
   * <p>
   * This method requires that the results are ordered.  If an order is not 
   * explicitly specified using the {@link #filter(OracleDocument) filter()} method,
   * the documents will be ordered by the document key. See also 
   * {@link #startKey(String, Boolean, Boolean) startKey()}
   * <p>
   * This method should only be invoked as part of building a
   * read operation. If it's invoked as part of building a write operation
   * (e.g. with replace, remove, etc.), it will have no effect. 
   * It is an error to specify this method in conjunction with 
   * a {@link #count()} terminal.
   *
   * @param limit                   limit on the number of results. Must be
   *                                positive.
   * @return                        this <code>OracleOperationBuilder</code>
   *
   * @throws OracleException        if the <code>limit</code> is not positive.
   */
  OracleOperationBuilder limit(int limit)
    throws OracleException;

  /**
   * Specifies the number of results to skip.
   * <p>
   * This is a non-terminal method, and, as such, it does
   * not cause operation execution.
   * <p>
   * This interface assumes the results are ordered (e.g. by key).
   * <p>
   * This method should only be invoked as part of building a
   * read operation. If it's invoked as part of building a write operation
   * (e.g. with replace, remove, etc.), it will have no effect. 
   * It is an error to specify this method in conjunction with 
   * a {@link #count()} terminal.
   *
   * <p>
   *
   * @param skip                    number of results to skip. Must be
   *                                non-negative.
   *
   * @return                        <code>OracleOperationBuilder</code>
   *
   * @throws OracleException        if the <code>skip</code> is negative
   */
  OracleOperationBuilder skip(long skip)
    throws OracleException;

  /**
   * Specifies that the returned documents should contain only the
   * the following:
   * <ul>
   *   <li>key,</li>
   *   <li>creation-on timestamp (if present in the collection),</li>
   *   <li>last-modified timestamp (if present in the collection),</li>
   *   <li>version (if present in the collection).</li>
   * </ul>
   * The documents' contents will not be returned.
   *
   * <p>
   * This is a non-terminal method, and, as such, it does
   * not cause operation execution.
   * <p>
   * This method should only be invoked as part of building a
   * read operation. If it's invoked as part of building
   * a write operation (e.g. replace, remove, etc), it will have 
   * no effect. Also, this method will have no effect if invoked
   * as part of building a {@link #count()} operation.
   * <p>
   *
   * @return                       this <code>OracleOperationBuilder</code>
   */
  OracleOperationBuilder headerOnly();

  /**
   * Counts the number of documents.
   * <p>
   * This is a terminal method, and, as such, it causes operation
   * execution.
   *
   * @return                        document count
   * @throws OracleException        if there is an error getting the count
   */
  long count()
    throws OracleException;

  /**
   * Returns a single document. Use this method to get the 
   * document from the <code>OracleOperationBuilder</code> returned by
   * {@link #key(String)}, for example: <code>col.find().key(key).getOne()</code>
   * <p>
   * If this method is used as a terminal for an operation that
   * returns multiple documents, the first document is returned.
   * <p>
   * This is a terminal method, and, as such, it causes operation
   * execution.
   * <p>
   * For the Oracle RDBMS implementation of SODA, the current
   * limit for the maximum size of document that can be read is 2GB.
   * An exception will be thrown by this method if the document's
   * size exceeds this limit.
   * </p>
   *
   *
   * @return                        returns a single document. <code>null</code>
   *                                if no document is available.
   * @throws OracleException        if there is an error retrieving the
   *                                <code>OracleDocument</code>
   */
  OracleDocument getOne()
    throws OracleException;

  /**
   * Indicates that the operation should lock the documents.
   * <p>
   * This is a non-terminal method, and, as such, it does
   * not cause operation execution.
   * <p>
   * This method should only be invoked as part of building a
   * read operation. If it's invoked as part of building a write operation
   * (e.g. with replace, remove, etc.), it will have no effect.
   * It is an error to specify this method in conjunction with
   * a {@link #count()} terminal, {@link #skip(long)} non-terminal, and
   * {@link #limit(int)} non-terminal.
   * <p>
   * In order to use this method, ensure that the JDBC connection associated
   * with this operation builder object is not in auto-commit mode. 
   * Otherwise, the acquired lock will be immediately unlocked by the automatic
   * commit performed after the read operation. In other words, locking will 
   * have no effect if the connection is in auto-commit mode.
   * <p>
   * 
   * @return                        <code>OracleOperationBuilder</code>
   *
   * @throws OracleException        if {@link #skip(long)} or {@link #limit(int)} are
   *                                already specified on this {@link OracleOperationBuilder} object
   */
  OracleOperationBuilder lock() throws OracleException;

  /**
   * Adds execution hints to the operation
   * @param hints                   contains one or more hints, cannot be <code>null</code>
   * @return                        <code>OracleOperationBuilder</code>
   * @throws OracleException        If <code>hint</code> is null or invalid.
   */
  OracleOperationBuilder hint(String hints) throws OracleException;
  
  /**
   * Merges the specified document into an existing one.  The two documents are 
   * merged using the algorithm described in
   * <a href="https://tools.ietf.org/html/rfc7396">RFC7396 - JSON Merge Patch</a>.
   * For example, merging the document <code>{"count":42, "color":"red"}</code> into an 
   * existing document <code>{"name":"apple", "count":12}</code> produces a new
   * merged document <code>{"name":"apple", "count":42, "color":"red"}</code>. 
   * 
   * <p>
   * This method is used in conjunction with <code>key(...)</code>,
   * and (optionally) <code>version(...)</code> methods.  
   * <p>
   * For example:
   * <pre>
   * // Replace a document with the key "k1" and version "v1"
   * // with the input document 'd1'.
   * col.find().key("k1").version("v1").mergeOne(d1)
   * </pre>
   * <p>
   * Note that the key and version information (if any) in the input
   * document 'd1' is ignored.
   * <p>
   * This is a terminal method, and, as such, it causes operation
   * execution.
   *
   * @param document                 input document. Cannot be <code>null</code>
   * @return                         <code>true</code> if the document was
   *                                 replaced successfully, <code>false</code>
   *                                 otherwise
   * @throws OracleException         if (1) the input document is <code>null</code>,
   *                                 or (2) there's an error replacing the existing
   *                                 <code>document</code>. 
   * @see <a href="https://tools.ietf.org/html/rfc7396">RFC7396 - JSON Merge Patch</a>
   */
  boolean mergeOne(OracleDocument document)
    throws OracleException;
  
  /**
   * Merges a document.
   * <p>
   * This method is the same as {@link #mergeOne(OracleDocument)} but additionally
   * returns the modified document.  The {@link #mergeOne(OracleDocument)} method may 
   * avoid the additional cost of transferring the document back to the caller. 
   * <p>
   * This is a terminal method, and, as such, it causes operation
   * execution. 
   * <p>
   *
   * @param document                 input document. Cannot be <code>null</code>
   * @return                         result document which contains the
   *                                 key and (if present) the created-on
   *                                 timestamp, last-modified timestamp,
   *                                 and version only. The input document's
   *                                 content is not returned as part of the
   *                                 result document.
   * @throws OracleException         if (1) the input document is <code>null</code>,
   *                                 or (2) there's an error replacing the input
   *                                 <code>document</code>. 
   */
  OracleDocument mergeOneAndGet(OracleDocument document)
    throws OracleException;

  
  /**
   * Specifies that only keys that come after (or alternatively before) the
   * specified key. The total ordering over key values in a collection is
   * guaranteed stable and consistent. The ordering may depend on the underlying
   * key storage type and default the collation settings of the database.
   * 
   * <p>
   * This method is expected to be used with the {@link #limit(int)} method to do
   * pagination over a, possibly {@link #filter(OracleDocument) filtered}, set of
   * documents.  Calling {@link #limit(int)} by itself will ensure that the 
   * documents are returned in key order.  For example:
   * </p>
   * 
   * <pre>
   * <code>
   * 
   *   // find the first 10 documents where the "count" property is greater than 20
   *   String filter = "{\"count\": {\"$gt\" : 20}}";
   *   OracleCursor cursor = col.find()
   *                            .filter()
   *                            .limit(10)
   *                            .getCursor();
   * 
   *   String key = null;
   *   while (cursor.hasNext()) {
   *      OracleDocument doc = cursor.next();
   *      key = doc.getKey();
   *      ...
   *   }
   *   ...
   *   
   *   // use startKey() to get the next 10 documents with count greater than 20
   *   cursor = col.find()
   *               .filter(filter)
   *               .startKey(key, true, false)
   *               .limit(10)
   *               .getCursor();
   * </code>
   * </pre>
   * 
   * <p>
   * This is a non-terminal method, and, as such, it does not cause operation
   * execution. This method should only be invoked as part of building a read
   * operation. If it's invoked as part of building a write operation (e.g. with
   * replace, remove, etc.), it will have no effect.
   * </p>
   *
   * @param startKey  the starting key.
   * @param ascending when true, returns documents in ascending order following
   *                  the startKey. When false, returns documents preceding the
   *                  startKey in descending order.
   * @param inclusive when true, the document with the startKey value, if it
   *                  exists, is included in the result.
   * 
   * @return this <code>OracleOperationBuilder</code>
   *
   * @throws OracleException if startKey is null
   */
  OracleOperationBuilder startKey(String startKey, Boolean ascending, Boolean inclusive)
    throws OracleException;
}

  
