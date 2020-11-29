/* Copyright (c) 2014, 2020, Oracle and/or its affiliates. 
All rights reserved.*/

package oracle.soda;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A collection of documents.
 */
public interface OracleCollection
{
  /**
   * Returns an <code>OracleOperationBuilder</code> representing
   * an operation that finds all documents in the collection.
   *
   * @return                       <code>OracleOperationBuilder</code>
   */
  public OracleOperationBuilder find();

  /**
   * Finds an <code>OracleDocument</code> matching a key.
   * <p>
   * This is a convenience method. The same operation can be
   * expressed as: <code>col.find().key("k1").getOne()</code>
   * <p>
   *
   * @param key                    the key. Cannot be <code>null</code>
   * @return                       <code>OracleDocument</code> matching the key.
   *                               <code>null</code> if not found.
   * @throws OracleException       if the key is <code>null</code>
   */
  public OracleDocument findOne(String key)
    throws OracleException;

  /**
   * Inserts a document into the collection.
   * <p>
   * The key will be automatically created, unless this collection
   * is configured with client-assigned keys and the key is provided in the
   * input document.
   *
   * @param document               input document. Cannot have a
   *                               key if the collection is
   *                               configured to auto-generate keys. Cannot
   *                               be <code>null</code>
   *
   * @exception OracleException    if (1) the input <code>document</code> 
   *                               is <code>null</code>, or (2) keys are 
   *                               client-assigned for this
   *                               collection, but the key is not provided
   *                               in the input document, or (3) keys are
   *                               client-assigned for this collection, and
   *                               the key provided in the input document
   *                               already exists in the collection, or
   *                               (4) keys are auto-generated for this collection,
   *                               and the key is provided in the input document,
   *                               or (5) an error during insertion occurs
   */
  public void insert(OracleDocument document)
    throws OracleException;

  /**
   * Inserts a document into the collection.
   * <p>
   * The key will be automatically created, unless this collection
   * is configured with client-assigned keys and the key is provided in the
   * input document.
   *
   * @param document               input document. Cannot have a
   *                               key if the collection is
   *                               configured to auto-generate keys. Cannot
   *                               be <code>null</code>
   *
   * @return                       result document which contains the
   *                               key and (if present) the created-on
   *                               timestamp, last-modified timestamp,
   *                               and version only. The input document's
   *                               content is not returned as part of the
   *                               result document.
   *
   * @exception OracleException    if (1) the input <code>document</code> 
   *                               is <code>null</code>, or (2) keys are 
   *                               client-assigned for this
   *                               collection, but the key is not provided
   *                               in the input document, or (3) keys are
   *                               client-assigned for this collection, and
   *                               the key provided in the input document
   *                               already exists in the collection, or
   *                               (4) keys are auto-generated for this collection,
   *                               and the key is provided in the input document,
   *                               or (5) an error during insertion occurs
   */
  public OracleDocument insertAndGet(OracleDocument document)
    throws OracleException;

  /**
   * Inserts a document into the collection.
   * <p>
   * The key will be automatically created, unless this collection
   * is configured with client-assigned keys and the key is provided in the
   * input document.
   *
   * @param document               input document. Cannot have a
   *                               key if the collection is
   *                               configured to auto-generate keys. Cannot
   *                               be <code>null</code>
   *
   * @param options                <code>null</code> or a {@link Map} of options. 
   *                               The key is option name and the value is the option
   *                               value. The semantics of the options are implementation
   *                               defined. The "hint" is a valid key with string value
   *                               containing one or more insert hints.
   *                               
   * @return                       result document which contains the
   *                               key and (if present) the created-on
   *                               timestamp, last-modified timestamp,
   *                               and version only. The input document's
   *                               content is not returned as part of the
   *                               result document.
   *
   * @exception OracleException    if (1) the input <code>document</code> 
   *                               is <code>null</code>, or (2) keys are 
   *                               client-assigned for this
   *                               collection, but the key is not provided
   *                               in the input document, or (3) keys are
   *                               client-assigned for this collection, and
   *                               the key provided in the input document
   *                               already exists in the collection, or
   *                               (4) keys are auto-generated for this collection,
   *                               and the key is provided in the input document,
   *                               or (5) an error during insertion occurs or
   *                               (6) the value of the hint supplied through options
   *                               is not of string type or contains substring "/*" or "\*\/"
   */
  public OracleDocument insertAndGet(OracleDocument document, Map<String, ?> options)
    throws OracleException;
  /**
   * Saves a document into the collection. This method is
   * equivalent to {@link #insert(OracleDocument)} except that
   * if client-assigned keys are used, and the document with
   * the specified key already exists in the collection, it
   * will be replaced with the input document.
   * <p>
   * The key will be automatically created, unless this collection
   * is configured with client-assigned keys and the key is provided in the
   * input document.
   *
   * @param document               input document. Cannot have a
   *                               key if the collection is
   *                               configured to auto-generate keys. Cannot
   *                               be <code>null</code>
   *
   * @exception OracleException    if (1) the input <code>document</code> is
   *                               <code>null</code>, or (2) keys are client-assigned 
   *                               for this collection, but the key is not provided
   *                               in the input document, or
   *                               (3) keys are auto-generated for this collection,
   *                               and the key is provided in the input document,
   *                               or (4) an error during insertion occurs
   */
  public void save(OracleDocument document)
    throws OracleException;

  /**
   * Saves a document into the collection. This method is
   * equivalent to {@link #insertAndGet(OracleDocument)} except that
   * if client-assigned keys are used, and the document with
   * the specified key already exists in the collection, it
   * will be replaced with the input document.
   * <p>
   * The key will be automatically created, unless this collection
   * is configured with client-assigned keys and the key is provided in the
   * input document.
   *
   * @param document               input document. Cannot have a
   *                               key if the collection is
   *                               configured to auto-generate keys.
   *                               Cannot be <code>null</code>
   *
   * @return                       result document which contains the
   *                               key and (if present) the created-on
   *                               timestamp, last-modified timestamp,
   *                               and version only. The input document's
   *                               content is not returned as part of the
   *                               result document.
   *
   * @exception OracleException    if (1) the input <code>document</code> is 
   *                               <code>null</code>, or (2) keys are client-assigned
   *                               for this collection, but the key is not provided
   *                               in the input document, or (3) keys are auto-generated
   *                               for this collection, and the key is provided in the input
   *                               document, or (4) an error during insertion occurs
   */
  public OracleDocument saveAndGet(OracleDocument document)
    throws OracleException;

  /**
   * Saves a document into the collection. This method is
   * equivalent to {@link #insertAndGet(OracleDocument)} except that
   * if client-assigned keys are used, and the document with
   * the specified key already exists in the collection, it
   * will be replaced with the input document.
   * <p>
   * The key will be automatically created, unless this collection
   * is configured with client-assigned keys and the key is provided in the
   * input document.
   *
   * @param document               input document. Cannot have a
   *                               key if the collection is
   *                               configured to auto-generate keys.
   *                               Cannot be <code>null</code>
   *                               
   * @param options                <code>null</code> or a {@link Map} of options. 
   *                               The key is option name and the value is the option
   *                               value. The semantics of the options are implementation
   *                               defined. The "hint" is a valid key with string value
   *                               containing one or more save hints.
   *
   * @return                       result document which contains the
   *                               key and (if present) the created-on
   *                               timestamp, last-modified timestamp,
   *                               and version only. The input document's
   *                               content is not returned as part of the
   *                               result document.
   *
   * @exception OracleException    if (1) the input <code>document</code> is 
   *                               <code>null</code>, or (2) keys are client-assigned
   *                               for this collection, but the key is not provided
   *                               in the input document, or (3) keys are auto-generated
   *                               for this collection, and the key is provided in the input
   *                               document, or (4) an error during insertion occurs 
   *                               or (5) the value of the hint supplied through options
   *                               is not of string type or contains substring "/*" or "\*\/"
   */
  public OracleDocument saveAndGet(OracleDocument document, Map<String, ?> options)
    throws OracleException;

  /**
   * Inserts multiple documents into the collection.
   * <p>
   * The keys will be automatically created, unless this collection
   * has client-assigned keys and the key is provided in the input document.
   * <p>
   * If there is an error inserting one of the documents,
   * <code>OracleBatchException</code> will be thrown. Invoking
   * <code>OracleBatchException.getProcessed()</code> method
   * returns the number of document processed successfully
   * before the error occurred.
   * <p>
   *
   * @param documents                   an <code>Iterator</code> over input documents.
   *                                    Cannot be <code>null</code>.
   *                                    Documents cannot have <code>key</code>s
   *                                    if the collection is configured to
   *                                    auto-generate keys. 
   *
   * @exception OracleBatchException    if (1) the input <code>documents</code> 
   *                                    <code>Iterator</code> is <code>null</code>, 
   *                                    or (2) keys are client-assigned for this 
   *                                    collection, but the key is not provided
   *                                    for one or more of the input documents,
   *                                    or (3) keys are client-assigned for this
   *                                    collection, and the key provided for one
   *                                    or more of the input documents already
   *                                    exists in the collection, or (4) keys are
   *                                    auto-generated for this collection,
   *                                    but the key is provided in one or more
   *                                    input documents, or (5) if an error
   *                                    during insertion occurs
   */
  public void insert(Iterator<OracleDocument> documents)
    throws OracleBatchException;

  /**
   * Inserts multiple documents into the collection.
   * <p>
   * The keys will be automatically created, unless this collection
   * has client-assigned keys and the key is provided in the input document.
   * <p>
   * If there is an error inserting one of the documents,
   * <code>OracleBatchException</code> will be thrown. Invoking
   * {@link OracleBatchException#getProcessedCount()} method
   * returns the number of document processed successfully
   * before the error occurred.
   * <p>
   * @param documents                   an iterator over input documents.
   *                                    Cannot be <code>null</code>.
   *                                    Documents cannot have <code>key</code>s
   *                                    if the collection is configured to
   *                                    auto-generate keys
   *
   * @return                            an list of result documents, each of which
   *                                    contains the key and (if present) the
   *                                    created-on timestamp, last-modified timestamp,
   *                                    and version only. The input documents'
   *                                    contents are not returned as part of the
   *                                    result documents.
   *
   * @exception OracleBatchException    if (1) the input <code>documents</code> 
   *                                    <code>Iterator</code> is <code>null</code>, 
   *                                    or (2) keys are client-assigned for this 
   *                                    collection, but the key is not provided
   *                                    for one or more of the input documents,
   *                                    or (3) keys are client-assigned for this
   *                                    collection, and the key provided for one
   *                                    or more of the input documents already
   *                                    exists in the collection, or (4) keys are
   *                                    auto-generated for this collection,
   *                                    but the key is provided in one or more
   *                                    input documents, or (5) if an error
   *                                    during insertion occurs
   */
  public List<OracleDocument> insertAndGet(Iterator<OracleDocument> documents)
    throws OracleBatchException;

  /**
   * Inserts multiple documents into the collection with options.
   * <p>
   * The keys will be automatically created, unless this collection
   * has client-assigned keys and the key is provided in the input document.
   * <p>
   * If there is an error inserting one of the documents,
   * <code>OracleBatchException</code> will be thrown. Invoking
   * {@link OracleBatchException#getProcessedCount()} method
   * returns the number of document processed successfully
   * before the error occurred.
   * <p>
   * @param documents                   an iterator over input documents.
   *                                    Cannot be <code>null</code>.
   *                                    Documents cannot have <code>key</code>s
   *                                    if the collection is configured to
   *                                    auto-generate keys
   *
   * @param options                     <code>null</code> or a {@link Map} of options. 
   *                                    The key is option name and the value is the option
   *                                    value. The semantics of the options are implementation
   *                                    defined. The "hint" is a valid key with string value
   *                                    containing one or more insert hints.
   *                                    
   * @return                            an list of result documents, each of which
   *                                    contains the key and (if present) the
   *                                    created-on timestamp, last-modified timestamp,
   *                                    and version only. The input documents'
   *                                    contents are not returned as part of the
   *                                    result documents.
   *
   * @exception OracleBatchException    if (1) the input <code>documents</code> 
   *                                    <code>Iterator</code> is <code>null</code>, 
   *                                    or (2) keys are client-assigned for this 
   *                                    collection, but the key is not provided
   *                                    for one or more of the input documents,
   *                                    or (3) keys are client-assigned for this
   *                                    collection, and the key provided for one
   *                                    or more of the input documents already
   *                                    exists in the collection, or (4) keys are
   *                                    auto-generated for this collection,
   *                                    but the key is provided in one or more
   *                                    input documents, or (5) if an error
   *                                    during insertion occurs or (6) the value
   *                                    of the hint supplied through options
   *                                    is not of string type or contains substring
   *                                    "/*" or "\*\/"
   */
  public List<OracleDocument> insertAndGet(Iterator<OracleDocument> documents, Map<String, ?> options)
    throws OracleBatchException;

  /**
   * Gets an <code>OracleCollectionAdmin</code> object
   *
   * @return  a <code>OracleCollectionAdmin</code> object
   */
  public OracleCollectionAdmin admin();

}
