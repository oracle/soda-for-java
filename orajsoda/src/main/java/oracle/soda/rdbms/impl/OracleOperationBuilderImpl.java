/* Copyright (c) 2014, 2024, Oracle and/or its affiliates. */
/* All rights reserved.*/


/*
   DESCRIPTION

    Creates and executes read and write operations on the
    collection.
 */

/**
 *  This class is not part of the public API, and is
 *  subject to change.
 *
 *  Do not rely on it in your application code.
 *
 *  @author  Max Orgiyan
 *  @author  Doug McMahon
 */

package oracle.soda.rdbms.impl;

import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import jakarta.json.JsonValue;

import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.logging.Logger;
import oracle.json.util.ByteArray;

import oracle.json.common.JsonFactoryProvider;
import oracle.json.common.MetricsCollector;
import oracle.json.common.DocumentCodec;
import oracle.json.logging.OracleLog;
import oracle.json.parser.AndORTree;
import oracle.json.parser.ProjectionSpec;
import oracle.json.parser.QueryException;
import oracle.json.parser.ValueTypePair;
import oracle.json.util.ComponentTime;
import oracle.json.util.JsonByteArray;
import oracle.json.util.Pair;
import oracle.soda.OracleCursor;
import oracle.soda.OracleDocument;
import oracle.soda.OracleException;
import oracle.soda.OracleOperationBuilder;

/**
 * OracleOperationBuilderImpl
 */
public class OracleOperationBuilderImpl implements OracleOperationBuilder {
  private static final int MAX_NUM_OF_KEYS = 1000;

  private static final Logger log =
    Logger.getLogger(OracleOperationBuilderImpl.class.getName());

  private String key;

  private String likePattern;
  private String likeEscape;

  // If isStartKey is true, the value of the key
  // will be stored in the 'key' variable above
  private boolean isStartKey;
  private boolean ascending;
  private boolean keyOrdered;
  private boolean startKeyAscending;
  private boolean startKeyInclusive;

  Set<String> keys;

  private boolean skipFilterKeys;

  private String since;
  private String until;
  private boolean timeRangeInclusive;

  private OracleDocument filterSpec;
  private AndORTree tree;

  private String version;

  private String lastModified;

  private boolean lockRows;

  private int limit;

  private int prefetch;

  private long rowDataLimit;

  private long skip;
  private boolean orderByKey = true;
  private Long asOfScn = null;

  private String asOfTimestamp = null;

  boolean headerOnly;

  private int firstRows;

  private ProjectionSpec proj = null;
  private String projString = null;
  private boolean skipProjErrors = true;

  private final OracleCollectionImpl collection;

  private final MetricsCollector metrics;

  private final Connection connection;

  private final CollectionDescriptor options;

  private final JsonFactoryProvider jProvider;

  private enum Terminal
  {
    COUNT,
    GET_ONE,
    GET_CURSOR,
    REMOVE,
    REPLACE_ONE_AND_GET,
    REPLACE_ONE,
    MERGE_ONE,
    MERGE_ONE_AND_GET,
    // The rest of these are not part of a public API
    PATCH_ONE,
    PATCH_ONE_AND_GET,
    PATCH,
    EXPLAIN_PLAN
  };

  // Stores the value of the new version (unless it's sequential,
  // or none) computed in bindUpdate for a replace operation.
  // Sequential or none come from the database, and thus
  // need a returning clause.
  private String computedVersion;

  private static final boolean PAGINATION_WORKAROUND = Boolean.valueOf(
          System.getProperty("oracle.soda.rdbms.paginationWorkaround", "true"));

  // These flags serve to signal whether PLSQL patch or merge
  // functions should be used during select generation.
  private boolean selectPatchedDoc = false;
  private boolean selectMergedDoc  = false;

  private boolean patchSpecExceptionOnly = false;

  private OracleDocument patchSpec = null;
  
  private String hints = null;

  private boolean eJSON = false;
  
  private boolean errorOnVersionMismatch = false;
  
  private String   newKey = null;
  private String[] dockeySteps = null;

  OracleOperationBuilderImpl(OracleCollectionImpl collection,
                             Connection connection)
  {
    this.collection = collection;
    this.jProvider = collection.getDatabase().getJsonFactoryProvider();

    options = collection.getOptions();

    this.connection = connection;

    ascending = true;

    keyOrdered = false;
  
    startKeyAscending = true;

    startKeyInclusive = true;

    timeRangeInclusive = true;

    firstRows = -1;

    metrics = collection.getMetrics();

    prefetch = 0;

    rowDataLimit = 0;
  }

  // If true, this flag requests that the returned cursor
  // provide the SQL statement that produced the result set.
  private boolean return_query = false;
  private JsonByteArray return_sql_json = null;

  /**
   * Not part of a public API.
   * <p/>
   * Switch on/off save mode for internal SQL.
   */
  public void returnQuery(boolean enableReturn)
  {
    return_query = enableReturn;
    if (enableReturn)
      return_sql_json = new JsonByteArray(SODAConstants.SQL_STATEMENT_SIZE);
    else
      return_sql_json = null;
  }

  private void beginQueryRecord(String sqltext)
  {
    return_sql_json.appendOpenBrace();
    return_sql_json.appendValue("sql");
    return_sql_json.appendColon();
    return_sql_json.appendValue(sqltext);
  }

  private void recordNamedBind(String name, String value)
  {
    return_sql_json.appendComma();
    return_sql_json.appendValue(name);
    return_sql_json.appendColon();
    return_sql_json.appendValue(value);
  }
  
  private void recordJsonValueBind(String name, ValueTypePair item)
  {
    return_sql_json.appendComma();
    return_sql_json.appendValue(name);
    return_sql_json.appendColon();

    switch (item.getValue().getValueType())
    {
      case NUMBER:
        return_sql_json.append(item.getNumberValue().toString());
        break;

      case STRING:
        return_sql_json.appendValue(item.getStringValue());
        break;
        
      case TRUE:
        return_sql_json.appendValue("true");
        break;
        
      case FALSE:
        return_sql_json.appendValue("false");
        break;

      default: // ### Should never happen
        return_sql_json.append("null");
    }
  }

  private void recordQueryKey(int pos, String key)
  {
    return_sql_json.appendComma();
    return_sql_json.appendValue("key_" + Integer.toString(pos));
    return_sql_json.appendColon();
    if (key == null)
      return_sql_json.append("null");
    else
      return_sql_json.appendValue(key);
  }

  public OracleOperationBuilder eJSON() throws OracleException
  {
     if (!options.hasBinaryFormat() && !options.hasJsonType())
       throw SODAUtils.makeException(SODAMessage.EX_EJSON_CANNOT_BE_USED);

     eJSON = true;
    
     return this;
  }
  
  /* Not part of a public API. Public for internal use */
  public void errorOnVersionMismatch() throws OracleException
  {
    if (options.isDualityView())
      throw SODAUtils.makeException(SODAMessage.EX_UNSUPPORTED_METHOD_FOR_DUALV, "errorOnVersionMismatch");
    errorOnVersionMismatch = true;
  }

  public OracleOperationBuilder keyLike(String pattern, String escape)
    throws OracleException
  {
    if (pattern == null)
    {
      throw SODAUtils.makeException(SODAMessage.EX_ARG_CANNOT_BE_NULL,
                                    "pattern");
    }

    if ((options.keyAssignmentMethod != CollectionDescriptor.KEY_ASSIGN_CLIENT) ||
        ((options.keyDataType != CollectionDescriptor.STRING_KEY) &&
         (options.keyDataType != CollectionDescriptor.NCHAR_KEY)))
    {
      throw SODAUtils.makeException(SODAMessage.EX_KEY_LIKE_CANNOT_BE_USED);
    }

    likePattern = pattern;
    likeEscape = escape;

    // Override key(), keys(), and startKey() settings
    key = null;

    keys = null;

    isStartKey = false;

    ascending = true;

    keyOrdered = false;
  
    startKeyAscending = true;

    startKeyInclusive = true;

    return this;
  }

  public OracleOperationBuilder key(String key) throws OracleException
  {

    if (key == null)
    {
      throw SODAUtils.makeException(SODAMessage.EX_ARG_CANNOT_BE_NULL,
                                    "key");
    }

    this.key = collection.canonicalKey(key);

    // Override keyLike(), keys() and startKey() settings
    likePattern = null;

    likeEscape = null;

    keys = null;

    isStartKey = false;

    ascending = true;

    keyOrdered = false;
  
    startKeyAscending = true;

    startKeyInclusive = true;

    return this;
  }

  public OracleOperationBuilder keys(Set<String> keys)
          throws OracleException
  {
    if (keys == null)
    {
      throw SODAUtils.makeException(SODAMessage.EX_ARG_CANNOT_BE_NULL,
                                    "keys");
    }

    if (keys.isEmpty())
    {
      throw SODAUtils.makeException(SODAMessage.EX_SET_IS_EMPTY,
                                    "keys");
    }

    if (keys.contains(null))
    {
      throw SODAUtils.makeException(SODAMessage.EX_SET_CONTAINS_NULL,
                                    "keys");
    }
    this.keys = new HashSet<String>();
    for (String k : keys) {
      this.keys.add(collection.canonicalKey(k));
    }

    // Override key(), keyLike(), and startKey() settings
    likePattern = null;

    likeEscape = null;

    key = null;

    isStartKey = false;

    ascending = true;

    keyOrdered = false;

    startKeyAscending = true;

    startKeyInclusive = true;

    return this;
  }

  /* Not part of a public API */
  public OracleOperationBuilder keyOrder(Boolean ascending)
    throws OracleException
  {
    if (ascending != null)
    {
      this.ascending = ascending;
      this.keyOrdered = true;
    }
    // Else leave it as-is, e.g. as set by startKey()
   
    return this;
  }

  @Override
  public OracleOperationBuilder startKey(String startKey,
                                         Boolean ascending,
                                         Boolean inclusive)
          throws OracleException
  {
    if (startKey == null)
    {
      throw SODAUtils.makeException(SODAMessage.EX_ARG_CANNOT_BE_NULL,
                                    "startKey");
    }

    //### Revisit empty start key

    isStartKey = true;

    key = collection.canonicalKey(startKey);

    if (ascending != null)
    {
      this.ascending = ascending;
    }
    else
    {
      this.ascending = true;
    }
    this.startKeyAscending = this.ascending;

    if (inclusive != null)
    {
      this.startKeyInclusive = inclusive;
    }
    else
    {
      this.startKeyInclusive = true;
    }

    // Override keys() setting. key() setting is overridden because
    // isStartKey is set to true above
    keys = null;

    return this;
  }

  /* Not part of a public API */
  public OracleOperationBuilder timeRange(String since,
                                          String until,
                                          Boolean inclusive)
    throws OracleException
  {
    if (options.timestampColumnName == null)
    {
      throw SODAUtils.makeException(SODAMessage.EX_NO_TIMESTAMP,
                                    options.uriName);
    }

    if ((since == null) && (until == null))
    {
      throw SODAUtils.makeException(
        SODAMessage.EX_SINCE_AND_UNTIL_CANNOT_BE_NULL);
    }

    // If the inbound timestamps include a trailing Z for UTC,
    // strip it off because the RDBMS cannot reliably convert
    // such strings via TO_TIMESTAMP(), and it always runs UTC anyway.
    if (since != null)
      if (since.endsWith("Z"))
        since = since.substring(0, since.length() - 1);
    if (until != null)
      if (until.endsWith("Z"))
        until = until.substring(0, until.length() - 1);

    this.since = since;
    this.until = until;

    if (inclusive != null)
    {
      timeRangeInclusive = inclusive;
    }
    else
    {
      timeRangeInclusive = true;
    }

    // Override last-modified setting
    lastModified = null;

    return this;
  }

  /* Not part of the public API */
  public OracleOperationBuilder asOfScn(long scn) {
    asOfScn = Long.valueOf(scn);

    // Override asOfTimestamp
    asOfTimestamp = null;

    return this;
  }

  /* Not part of the public API */
  public OracleOperationBuilder asOfTimestamp(String tstamp)
  {
    // This is done just to make sure it's not a bad string
    // Avoid any chance of SQL injection if it cannot be a bind variable
    Instant lstamp = ComponentTime.stringToInstant(tstamp);
    asOfTimestamp = ComponentTime.instantToString(lstamp, true); // with Z

    // Override asOfScn
    asOfScn = null;

    return this;
  }

  @Override
  public OracleOperationBuilder filter(String filterSpec) throws OracleException 
  {
    return filter(new OracleDocumentImpl(filterSpec));
  }
  
  
  @Override
  public OracleOperationBuilder filter(OracleDocument filterSpec)
    throws OracleException
  {
    JsonObject jsonObj = null;

    specChecks(filterSpec, "filterSpec");

    if (collection.admin().isHeterogeneous())
    {
      throw SODAUtils.makeException(SODAMessage.EX_NO_QBE_ON_HETERO_COLLECTIONS);
    }

    try
    {
      jsonObj = filterSpec.getContentAs(JsonObject.class);
    }
    catch (Exception e)
    {
      if (OracleLog.isLoggingEnabled())
        log.warning(e.toString());
      throw SODAUtils.makeException(SODAMessage.EX_FILTER_IS_NOT_JSON_OBJECT, e);
    }

    try
    {
      tree = AndORTree.createTree(jsonObj, collection.getStrictMode(), options.isDualityView());
      tree.generateJsonExists();
    }
    catch (QueryException e)
    {
      if (OracleLog.isLoggingEnabled())
        log.warning(e.toString());
      throw SODAUtils.makeException(SODAMessage.EX_INVALID_FILTER, e);
    }

    this.filterSpec = filterSpec;

    return this;
  }

  /* Not part of a public API.
   * Experimental feature, not for use in production.
   */
  public OracleOperationBuilder project(OracleDocument projectionSpec,
                                        boolean skipErrors,
                                        boolean allowArraySteps,
                                        boolean allowPathOverlap,
                                        boolean allowMixedRules)
    throws OracleException
  {
    specChecks(projectionSpec, "projectionSpec");

    proj = new ProjectionSpec(jProvider,
                 ((OracleDocumentImpl) projectionSpec).getContentAsStream());

    try
    {
      projString = proj.getAsString();

      // Validate the projection specification
      boolean validated = proj.validate(true);
      if (!validated)
      {
        throw SODAUtils.makeException(SODAMessage.EX_INVALID_PROJ_SPEC, "validation");
      }
      // Disallow mixed include/exclude proj spec
      if (proj.hasIncludeRules() && proj.hasExcludeRules() && !allowMixedRules)
      {
        throw SODAUtils.makeException(SODAMessage.EX_PROJ_SPEC_MIXED);
      }
      // Disallow array steps
      if (proj.hasArraySteps() && !allowArraySteps)
      {
        throw SODAUtils.makeException(SODAMessage.EX_ARRAY_STEPS_NOT_ALLOWED_IN_PROJ);
      }
      // Disallow overlapping paths (i.e. where one path is a prefix of another).
      if (proj.hasOverlappingPaths() && !allowPathOverlap)
      {
        throw SODAUtils.makeException(SODAMessage.EX_OVERLAPPING_PATHS_NOT_ALLOWED_IN_PROJ);
      }
    }
    catch (QueryException e)
    {
      throw SODAUtils.makeException(SODAMessage.EX_INVALID_PROJ_SPEC, e);
    }

    skipProjErrors = skipErrors;

    // Override headerOnly() setting
    headerOnly = false;

    return this;
  }

  /* Not part of a public API.
   * Experimental feature, not for use in production.
   */
  public OracleOperationBuilder project(OracleDocument projectionSpec,
                                        boolean skipErrors)
    throws OracleException
  {
    return project(projectionSpec, skipErrors, false, false, false);
  }

  public OracleOperationBuilder project(OracleDocument projectionSpec)
    throws OracleException
  {
    return project(projectionSpec, true);
  }

  /*
   * Selects the results for update.
   */
  public OracleOperationBuilder lock() throws OracleException
  {
    if (skip > 0L)
      throw SODAUtils.makeException(SODAMessage.EX_INCOMPATIBLE_METHODS, "lock()", "skip()");
    else if (limit > 0)
      throw SODAUtils.makeException(SODAMessage.EX_INCOMPATIBLE_METHODS, "lock()", "limit()");
    lockRows = true;
    return this;
  }

  /*
   * Set the hint string for the SQL statement
   */
  public OracleOperationBuilder hint(String hints) throws OracleException
  {
    if (hints == null)
      throw SODAUtils.makeException(SODAMessage.EX_ARG_CANNOT_BE_NULL, hints);
    if (hints.indexOf("/*") >= 0 || hints.indexOf("*/") >= 0)
      throw SODAUtils.makeException(SODAMessage.EX_INVALID_HINT, hints);
    this.hints = (hints == "")? null: hints;
    return this;
  }
  
  public OracleOperationBuilder version(String version) throws OracleException
  {
    if (options.versionColumnName == null)
    {
      throw SODAUtils.makeException(SODAMessage.EX_NO_VERSION,
                                    options.uriName);
    }

    if (version == null)
    {
      throw SODAUtils.makeException(SODAMessage.EX_ARG_CANNOT_BE_NULL,
                                    "version");
    }

    this.version = version;

    return this;
  }

  /* Not part of a public API */
  public OracleOperationBuilder lastModified(String lastModified)
    throws OracleException
  {
    if (options.timestampColumnName == null)
    {
      throw SODAUtils.makeException(SODAMessage.EX_NO_TIMESTAMP,
                                    options.uriName);
    }

    if (lastModified == null)
    {
      throw SODAUtils.makeException(SODAMessage.EX_ARG_CANNOT_BE_NULL,
                                    "lastModified");
    }

    if (lastModified.endsWith("Z"))
      lastModified = lastModified.substring(0, lastModified.length() - 1);

    this.lastModified = lastModified;

    // Override timeRange() settings
    since = null;
    until = null;

    timeRangeInclusive = true;

    return this;
  }

  private ResultSet getResultSet(Operation operation) throws OracleException
  {
    return getResultSet(operation, false);
  }
 
  private ResultSet getResultSet(Operation operation, boolean close)
    throws OracleException
  {
    PreparedStatement stmt = operation.getPreparedStatement();
    ResultSet resultSet = null;

    try
    {
      resultSet = stmt.executeQuery();

      if (close)
      {
        resultSet.close();
        resultSet = null;

        stmt.close();
      }

      stmt = null;
    }
    catch (SQLException e)
    {
      if (OracleLog.isLoggingEnabled())
      {
        log.severe(e.toString() + "\n" + operation.getSqlText());
      }
      throw SODAUtils.makeExceptionWithSQLText(e, operation.getSqlText());
    }
    finally
    {
      for (String message : SODAUtils.closeCursor(stmt,
                                                  (close ? resultSet : null)))
      {
        if (OracleLog.isLoggingEnabled())
          log.severe(message);
      }
    }

    return resultSet;
  }

  public OracleCursor getCursor() throws OracleException
  {
    collection.checkJDBCVersion();
    Operation operation = generateOperation(Terminal.GET_CURSOR);
    ResultSet resultSet = getResultSet(operation);

    long prepAndExecTime = metrics.endTiming();

    boolean isModify = (selectPatchedDoc || selectMergedDoc);

    OracleCursorImpl cursor =
      new OracleCursorImpl(options,
                           metrics,
                           operation,
                           resultSet,
                           // Projection is ignored if the operation is a modify
                           isModify ? false : ((proj == null) ? false : true),
                           isModify,
                           eJSON);

    cursor.setElapsedTime(prepAndExecTime);

    // This seems to be the only place where a return query makes sense
    if (return_query)
    {
      return_sql_json.appendCloseBrace();
      cursor.setQuery(return_sql_json.toArray());
    }

    return (cursor);
  }

  // Common checks performed by various patch methods
  private void specChecks(OracleDocument spec, String specType)
    throws OracleException
  {
    if (spec == null)
      throw SODAUtils.makeException(SODAMessage.EX_ARG_CANNOT_BE_NULL,
                                    specType);
    
    if (specType.equals("patchSpec")) 
    {
      collection.writeCheck("patch");
    } 
    else if (specType.equals("mergeSpec")) 
    {
      collection.writeCheck("merge");
      // Any document is a valid mergepatch.  don't unnecessarily convert the
      // possibly binary patchspec to a string.
      return;
    }

    String specAsString = spec.getContentAsString();

    if (specAsString == null || specAsString.isEmpty()) {
      if (specType.equals("patchSpec")) {
        throw SODAUtils.makeException(SODAMessage.EX_SPEC_HAS_NO_CONTENT,
                                      "Patch");
      }
      else if (specType.equals("projectionSpec")){
        throw SODAUtils.makeException(SODAMessage.EX_SPEC_HAS_NO_CONTENT,
                                      "Projection");
      }
      else {
        throw SODAUtils.makeException(SODAMessage.EX_SPEC_HAS_NO_CONTENT,
                                      "Filter");
      }
    }
  }
  
  private void extractKeyWithErrorOnMissing(OracleDocument document) throws OracleException
  {
    if (options.hasVarcharEmbeddedID()) {
      DocumentCodec keyProcessor = collection.getDatabase().getCodecFactory().getCodec();
      
      if (dockeySteps == null)
        dockeySteps = collection.initializeDocumentKeySteps();
      keyProcessor.setKeyPath(dockeySteps);
      
      String extractedKey = null;
      
      if (eJSON)
        extractedKey = collection.extractKeyForEmbeddedIdEJSONCollections(document, null, eJSON);
      else 
        extractedKey = collection.extractKeyForEmbeddedIdCollections(keyProcessor, document, eJSON, null, false);
      
      if (extractedKey == null)
        throw SODAUtils.makeException(SODAMessage.EX_ID_MISSING_IN_REPLACE_OP);
      
      newKey = extractedKey;
    }
  }
  
  private void keyCheckInDocumentSpec(OracleDocument documentSpec,
                                      boolean isMergeOrReplace, 
                                      boolean isReplace) throws OracleException {   
    //### For now, avoid this check for dualv.
    if ((!options.hasVarcharEmbeddedID() && !options.hasMaterializedEmbeddedID()) || options.isDualityView() ||
        documentSpec.getContentAsByteArray() == null || collection.getDatabase().omitIdProcessing())
      return;

    JsonValue jsonValDocumentSpec = documentSpec.getContentAs(JsonValue.class);
    if (isMergeOrReplace)
    {
      if (jsonValDocumentSpec.getValueType() != JsonValue.ValueType.OBJECT)
        throw SODAUtils.makeException(SODAMessage.EX_SPEC_IN_OP_HAS_INCORRECT_TYPE, "object");

      //### Revisit. For non-mat column collections, can allow merge patch to not be an object
      JsonObject jsonObjDocumentSpec = jsonValDocumentSpec.asJsonObject();
         
      if (jsonObjDocumentSpec.containsKey("_id") && !isReplace)
      {
        if (options.isNative())
        {
          if (jsonObjDocumentSpec.isNull("_id"))
            throw SODAUtils.makeException(SODAMessage.EX_ID_CANT_BE_REMOVED);
        }
        else
          throw SODAUtils.makeException(SODAMessage.EX_MODIFYING_ID_NOT_SUPPORTED_FOR_OP, "merge");
      }
    }
    else
    {
      //### Revisit for _id removal cases (exceptions should be thrown for those here).
      if (options.isNative())
        return;

      if (jsonValDocumentSpec.getValueType() != JsonValue.ValueType.ARRAY)
        throw SODAUtils.makeException(SODAMessage.EX_SPEC_IN_OP_HAS_INCORRECT_TYPE, "array");

      JsonArray jsonArrPatchSpec = jsonValDocumentSpec.asJsonArray();
      for(JsonValue elem : jsonArrPatchSpec ) 
      {
        if (elem.getValueType() != JsonValue.ValueType.OBJECT)
          throw SODAUtils.makeException(SODAMessage.EX_PATCH_OPERATION_NOT_AN_OBJECT);     
        JsonObject elemObj = elem.asJsonObject();
        JsonValue  op = elemObj.get("op"), path = elemObj.get("path");
        if (op != null && op.getValueType() ==  JsonValue.ValueType.STRING && !"test".equals(elemObj.getString("op")) && 
            path != null && path.getValueType() ==  JsonValue.ValueType.STRING && "/_id".equals(elemObj.getString("path")))
          throw SODAUtils.makeException(SODAMessage.EX_MODIFYING_ID_NOT_SUPPORTED_FOR_OP, "patch");
      }
    }
  }

  private Pair<List<OracleDocument>, Long>
          getModifiedDocs(OracleDocument patchSpec, boolean isMergePatch)
   throws OracleException
  {
    //
    // Get the patched documents and their keys.
    //

    this.patchSpec = patchSpec;

    // Toggle patched/merged doc to true, so that a PLSQL patch/merge
    // function is invoked as part of a select
    selectPatchedDoc = !isMergePatch;
    selectMergedDoc  = isMergePatch;

    OracleCursor modifiedDocsCursor = null;

    List<OracleDocument> modifiedDocs = new ArrayList<OracleDocument>();

    Long countProcessed = 0L;


    try
    {
      modifiedDocsCursor = getCursor();
      if (modifiedDocsCursor != null)
      {
        while (modifiedDocsCursor.hasNext())
        {
          OracleDocument modifiedDoc = modifiedDocsCursor.next();

          if (modifiedDoc != null)
            modifiedDocs.add(modifiedDoc);

          ++countProcessed;
        }
      }
    }
    catch (OracleException e)
    {
      try
      {
        if (modifiedDocsCursor != null)
          modifiedDocsCursor.close();
      }
      catch (IOException ioE)
      {
        // Since we got an IOException following an OracleException,
        // we can wrap the IOException in another OracleException,
        // and attach is as the next exception.
        OracleException ioEWrapper = new OracleException(ioE);

        e.setNextException(ioEWrapper);
      }

      throw e;
    }
    catch (RuntimeException e)
    {
      try
      {
        if (modifiedDocsCursor != null)
          modifiedDocsCursor.close();
      }
      catch (IOException ioE)
      {
        // Nothing to do, this IOException follows
        // another exception.
        if (OracleLog.isLoggingEnabled())
          log.fine(ioE.getMessage());
      }

      throw e;
    }
    catch (Error e)
    {
      try
      {
        if (modifiedDocsCursor != null)
          modifiedDocsCursor.close();
      }
      catch (IOException ioE)
      {
        // Nothing to do, this IOException follows
        // another exception.
        if (OracleLog.isLoggingEnabled())
          log.fine(ioE.getMessage());
      }

      throw e;
    }
    finally
    {
      // Now toggle it back to false, since we got our modified docs.
      selectPatchedDoc = selectMergedDoc = false;
      patchSpec = null;
    }

    try
    {
      if (modifiedDocsCursor != null)
        modifiedDocsCursor.close();
    }
    catch (IOException ioE)
    {
      throw new OracleException(ioE);
    }

    return new Pair<List<OracleDocument>, Long> (modifiedDocs,
                                                 countProcessed);
  }

  private boolean disableAutoCommit() throws OracleException
  {
    try
    {
      if (connection.getAutoCommit() == true)
      {
        connection.setAutoCommit(false);
        return true;
      }
    }
    catch (SQLException e)
    {
      throw SODAUtils.makeException(SODAMessage.EX_CANT_DISABLE_AUTOCOMMIT, e);
    }

    return false;
  }

  /**
   * Perform a JSON Patch or JSON MergePatch on a set of documents.
   */
  private List<OracleDocument> modifyAndGet(OracleDocument patchSpec,
                                            boolean skipErrors,
                                            boolean isMergePatch)
    throws OracleException
  {
    
    keyCheckInDocumentSpec(patchSpec, isMergePatch, false);
    
    //
    // Handle a single key-based patch first
    //

    if (key != null && !isStartKey)
    {
      OracleDocument patchedDoc;
      if (isMergePatch)
        patchedDoc = mergeOneAndGet(patchSpec);
      else
        patchedDoc = patchOneAndGet(patchSpec);

      List<OracleDocument> l = new ArrayList<OracleDocument>();
      if (patchedDoc != null)
      {
          l.add(patchedDoc);
      }
      // ### Return empty list or null if nothing is patched?
      return l;
    }

    specChecks(patchSpec, (isMergePatch) ? "mergeSpec" : "patchSpec");

    //
    // Now handle a patch involving
    // multiple keys
    //

    boolean manageTransaction = disableAutoCommit();

    try
    {
      if (skipErrors)
        patchSpecExceptionOnly = true;

      List<OracleDocument> patchedDocs =
          getModifiedDocs(patchSpec, isMergePatch).getFirst();

      patchSpecExceptionOnly = false;

      // For each patched document, invoke a replace operation by passing
      // its key to the key(...) method. The key(...) method overrides
      // some other OracleOperationBuilder state (currently keys(...) and startKey(...)).
      // Note: all state overriden by key(...) must be saved and restored
      // after all the replaces are complete!
      if (!patchedDocs.isEmpty())
      {
        List<OracleDocument> finalDocs = new ArrayList<OracleDocument>();

        HashSet<String> savedKeys = null;

        boolean isStartKeySaved = false;
        boolean ascendingSaved = false;
        boolean startKeyInclusiveSaved = false;

        if (keys != null && !keys.isEmpty())
        {
          savedKeys = new HashSet<String>();
          savedKeys.addAll(keys);
        }
        else if (isStartKey == true)
        {
          isStartKeySaved = true;
          ascendingSaved = startKeyAscending;
          startKeyInclusiveSaved = startKeyInclusive;
        }
        skipFilterKeys = true;

        try
        {
          for (OracleDocument d : patchedDocs)
          {
            if (d.getContentAsByteArray() == null)
              continue;

            key(d.getKey());

            OracleDocument returnedDoc = replaceOneAndGet(d);

            if (returnedDoc != null)
            {
              // getContentAsByteArray() is OK here, since
              // we don't currently stream JSON documents.
              ((OracleDocumentImpl) returnedDoc).setContent(d.getContentAsByteArray());

              finalDocs.add(returnedDoc);
            }
          }
        }
        finally
        {
          // Restore OracleOperationBuilder state
          if (savedKeys != null)
          {
            keys = new HashSet<String>();
            keys.addAll(savedKeys);
          }
          else if (isStartKeySaved == true)
          {
            isStartKey = isStartKeySaved;
            startKeyAscending = ascendingSaved;
            startKeyInclusive = startKeyInclusiveSaved;
          }
          skipFilterKeys = false;
        }

        OracleException e = TableCollectionImpl.completeTxnAndRestoreAutoCommit(
          connection,
          manageTransaction,
          true);

        if (e != null)
          throw e;

        // ### Return empty list or null if nothing is patched?
        return finalDocs;
      }
    }
    catch (OracleException e)
    {
      OracleException nE = TableCollectionImpl.completeTxnAndRestoreAutoCommit(
                                               connection,
                                               manageTransaction,
                                               false);
      e.setNextException(nE);

      if (OracleLog.isLoggingEnabled())
      {
        log.severe(e.toString());
      }

      throw e;
    }
    catch (RuntimeException e)
    {
      TableCollectionImpl.completeTxnAndRestoreAutoCommit(connection,
                                                          manageTransaction,
                                                          false);
      if (OracleLog.isLoggingEnabled())
        log.severe(e.toString());

      throw e;
    }
    catch (Error e)
    {
      TableCollectionImpl.completeTxnAndRestoreAutoCommit(connection,
                                                          manageTransaction,
                                                          false);
      if (OracleLog.isLoggingEnabled())
        log.severe(e.toString());

      throw e;
    }
    finally
    {
      patchSpecExceptionOnly = false;
    }

    OracleException e = TableCollectionImpl.completeTxnAndRestoreAutoCommit(
                                            connection,
                                            manageTransaction,
                                            true);

    if (e != null)
      throw e;

    // ### Return empty list or null if nothing is patched?
    return new ArrayList<OracleDocument>();
  }

  // ### return long or int?
  private WriteResult modify(OracleDocument patchSpec,
                             boolean skipErrors,
                             boolean isMergePatch)
    throws OracleException
  {
    keyCheckInDocumentSpec(patchSpec, isMergePatch, false);
    
    //
    // Handle a single key-based patch first
    //

    if (key != null && !isStartKey)
    {
      boolean result;

      if (isMergePatch)
        result = mergeOne(patchSpec);
      else
        result = patchOne(patchSpec);

      if (result)
        return new WriteResultImpl(1L, 1L);

      return new WriteResultImpl(1L, 0L);
    }

    specChecks(patchSpec, (isMergePatch) ? "mergeSpec" : "patchSpec");

    //
    // Now handle a patch involving
    // multiple keys
    //

    boolean manageTransaction = disableAutoCommit();

    if (skipErrors)
      patchSpecExceptionOnly = true;

    try
    {
      Pair<List<OracleDocument>, Long> resultPair =
          getModifiedDocs(patchSpec, isMergePatch);

      List<OracleDocument> modifiedDocs = resultPair.getFirst();

      patchSpecExceptionOnly = false;

      // For each patched document, invoke a replace operation by passing
      // its key to the key(...) method. The key(...) method overrides
      // some other OracleOperationBuilder state (currently keys(...) and startKey(...)).
      // Note: all state overriden by key(...) must be saved and restored
      // after all the replaces are complete!
      if (!modifiedDocs.isEmpty())
      {
        long countOfAppliedPatches = 0;

        HashSet<String> savedKeys = null;

        boolean isStartKeySaved = false;
        boolean ascendingSaved = false;
        boolean startKeyInclusiveSaved = false;

        if (keys != null && !keys.isEmpty())
        {
          savedKeys = new HashSet<String>();
          savedKeys.addAll(keys);
        }
        else if (isStartKey == true)
        {
          isStartKeySaved = true;
          ascendingSaved = startKeyAscending;
          startKeyInclusiveSaved = startKeyInclusive;
        }
        skipFilterKeys = true;

        try
        {
          for (OracleDocument d : modifiedDocs)
          {
            if (((OracleDocumentImpl) d).isBinary())
            {
              if (((OracleDocumentImpl) d).getBinaryContentAsByteArray() == null)
              {
                continue;
              }
            }
            else if (d.getContentAsByteArray() == null)
            {
              continue;
            }

            key(d.getKey());

            if (replaceOne(d))
              countOfAppliedPatches++;
          }
        }
        finally
        {
          // Restore OracleOperationBuilder state
          if (savedKeys != null)
          {
            keys = new HashSet<String>();
            keys.addAll(savedKeys);
          }
          else if (isStartKeySaved == true)
          {
            isStartKey = isStartKeySaved;
            startKeyAscending = ascendingSaved;
            startKeyInclusive = startKeyInclusiveSaved;
          }
          skipFilterKeys = true;
        }

        OracleException e = TableCollectionImpl.completeTxnAndRestoreAutoCommit(
          connection,
          manageTransaction,
          true);

        if (e != null)
          throw e;

        return new WriteResultImpl(resultPair.getSecond(), countOfAppliedPatches);
      }
    }
    catch (OracleException e)
    {
     OracleException nE = TableCollectionImpl.completeTxnAndRestoreAutoCommit(
                                              connection,
                                              manageTransaction,
                                              false);

      e.setNextException(nE);

      if (OracleLog.isLoggingEnabled())
      {
        log.severe(e.toString());
      }

      throw e;
    }
    catch (RuntimeException e)
    {
      TableCollectionImpl.completeTxnAndRestoreAutoCommit(connection,
                                                          manageTransaction,
                                                          false);
      if (OracleLog.isLoggingEnabled())
        log.severe(e.toString());

      throw e;
    }
    catch (Error e)
    {
      TableCollectionImpl.completeTxnAndRestoreAutoCommit(connection,
                                                          manageTransaction,
                                                          false);
      if (OracleLog.isLoggingEnabled())
        log.severe(e.toString());

      throw e;
    }
    finally
    {
      patchSpecExceptionOnly = false;
    }

    OracleException e = TableCollectionImpl.completeTxnAndRestoreAutoCommit(
                                            connection,
                                            manageTransaction,
                                            true);
    if (e != null)
      throw e;

    return new WriteResultImpl(0L, 0L);
  }

  /**
   * Perform a JSON Patch or JSON MergePatch on a single document.
   */
  private OracleDocument getModifiedDoc(OracleDocument patchSpec,
                                        boolean isMergePatch)
    throws OracleException
  {
    if (key == null || isStartKey)
      throw SODAUtils.makeException(SODAMessage.EX_KEY_MUST_BE_SPECIFIED);

    this.patchSpec = patchSpec;

    // Toggle patched/merged doc to true, so that a PLSQL patch/merge
    // function is invoked as part of a select
    selectPatchedDoc = !isMergePatch;
    selectMergedDoc  = isMergePatch;

    OracleDocument modifiedDoc = null;

    try
    {
      modifiedDoc = getOne();
    }
    finally
    {
      // At this point, either the patched doc was obtained successfully
      // or an error was encountered. selectPatchedDoc and selectMergedDoc
      // should be toggled back to false
      selectPatchedDoc = selectMergedDoc = false;
      patchSpec = null;
    }

    return modifiedDoc;
  }

  private boolean modifyOne(OracleDocument patchSpec, boolean isMergePatch)
    throws OracleException
  {
    keyCheckInDocumentSpec(patchSpec, isMergePatch, false);
    
    specChecks(patchSpec, (isMergePatch) ? "mergeSpec" : "patchSpec");

    if (options.versionColumnName == null || !collection.payloadBasedVersioning())
    {
      // Take an optimized path that does the merge in one execution. 
      this.patchSpec = patchSpec;
      Terminal terminal = (isMergePatch) ? Terminal.MERGE_ONE : Terminal.PATCH_ONE;
      int count = replaceOneWithOptionalMerge(patchSpec, terminal, false);
      if (count == 0)
        return false;
      else
        return true;
    } else
    {
      OracleDocument modifiedDoc = getModifiedDoc(patchSpec, isMergePatch);

      if (modifiedDoc != null)
        return replaceOne(modifiedDoc);
    }
    return false;
  }

  private OracleDocument modifyOneAndGet(OracleDocument patchSpec,
                                         boolean isMergePatch,
                                         boolean skipKeyCheck)
    throws OracleException
  {
    keyCheckInDocumentSpec(patchSpec, isMergePatch, false);
    
    specChecks(patchSpec, (isMergePatch) ? "mergeSpec" : "patchSpec");

    if (options.versionColumnName == null || !collection.payloadBasedVersioning())
    {
      this.patchSpec = patchSpec;
      Terminal terminal = (isMergePatch) ? Terminal.MERGE_ONE_AND_GET : Terminal.PATCH_ONE_AND_GET;
      return replaceOneAndGetWithOptionalMerge(patchSpec, terminal, false);
    }
    else
    {
      OracleDocument modifiedDoc = getModifiedDoc(patchSpec, isMergePatch);

      if (modifiedDoc != null)
      {
        return replaceOneAndGet(modifiedDoc);
      }
    }
    return null;
  }

  public WriteResult patch(OracleDocument patchSpec)
    throws OracleException
  {
    collection.checkJDBCVersion();
    if (eJSON)
      throw SODAUtils.makeException(SODAMessage.EX_EJSON_CANNOT_BE_USED);

    if (options.isDualityView())
      throw SODAUtils.makeException(SODAMessage.EX_UNSUPPORTED_FOR_JSON_DUALITY_VIEW);
    // For now, only expose the version of patchAndGet()
    // that skips errors (except for invalid patch spec).
    // Therefore, the second parameters to modifyAndGet(...),
    // which controls whether to skip errors, is set to
    // true below.
    //
    // For efficiency, current implementation
    // of bulk patch fetches both patched docs and keys
    // during its first phase (see getModifiedDocs(...) method).
    // With this approach, a bulk patch that throws errors is
    // not very useful, because the particular document on which
    // the error was encountered cannot be identified.
    return modify(patchSpec, true, false);
  }

  public boolean patchOne(OracleDocument patchSpec) throws OracleException
  {
    collection.checkJDBCVersion();

    if (eJSON)
      throw SODAUtils.makeException(SODAMessage.EX_EJSON_CANNOT_BE_USED);

    if (options.isDualityView())
      throw SODAUtils.makeException(SODAMessage.EX_UNSUPPORTED_FOR_JSON_DUALITY_VIEW);

    return modifyOne(patchSpec, false);
  }

  public OracleDocument patchOneAndGet(OracleDocument patchSpec)
    throws OracleException
  {
    collection.checkJDBCVersion();

    if (eJSON)
      throw SODAUtils.makeException(SODAMessage.EX_EJSON_CANNOT_BE_USED);

    if (options.isDualityView())
      throw SODAUtils.makeException(SODAMessage.EX_UNSUPPORTED_FOR_JSON_DUALITY_VIEW);

    return modifyOneAndGet(patchSpec, false, false);
  }


  public List<OracleDocument> patchAndGet(OracleDocument patchSpec)
                                          throws OracleException
  {
    collection.checkJDBCVersion();

    if (eJSON)
      throw SODAUtils.makeException(SODAMessage.EX_EJSON_CANNOT_BE_USED);

    if (options.isDualityView())
      throw SODAUtils.makeException(SODAMessage.EX_UNSUPPORTED_FOR_JSON_DUALITY_VIEW);

    // For now, only expose the version of patchAndGet()
    // that skips errors (except for invalid patch spec).
    // Therefore, the second parameters to patchAndGet(...),
    // which controls whether to skip errors, is set to
    // true below.
    //
    // For efficiency, current implementation
    // of bulk patch fetches both patched docs and keys
    // during its first phase (see getModifiedDocs(...) method).
    // With this approach, a bulk patch that throws errors is
    // not very useful, because the particular document on which
    // the error was encountered cannot be identified.
    return modifyAndGet(patchSpec, true, false);
  }

  public WriteResult merge(OracleDocument patchSpec)
    throws OracleException
  {
    collection.checkJDBCVersion();
    if (eJSON)
      throw SODAUtils.makeException(SODAMessage.EX_EJSON_CANNOT_BE_USED);

    return modify(patchSpec, true, true);
  }

  @Override
  public boolean mergeOne(OracleDocument patchSpec) throws OracleException
  {
    collection.checkJDBCVersion();
    if (eJSON)
      throw SODAUtils.makeException(SODAMessage.EX_EJSON_CANNOT_BE_USED);

    return modifyOne(patchSpec, true);
  }

  @Override
  public OracleDocument mergeOneAndGet(OracleDocument patchSpec)
    throws OracleException
  {
    collection.checkJDBCVersion();
    if (eJSON)
      throw SODAUtils.makeException(SODAMessage.EX_EJSON_CANNOT_BE_USED);
    // ### This could take an optimized code path as in mergeOne()
    return modifyOneAndGet(patchSpec, true, false);
  }

  public List<OracleDocument> mergeAndGet(OracleDocument patchSpec)
                                          throws OracleException
  {
    if (eJSON)
      throw SODAUtils.makeException(SODAMessage.EX_EJSON_CANNOT_BE_USED);
    return modifyAndGet(patchSpec, true, true);
  }

  private Operation generateOperation(Terminal terminal)
          throws OracleException
  {
    return generateOperation(terminal, null);
  }

  /**
   * Generates SQL for a SODA operation, creates a PreparedStatement
   * for it, and binds the parameters
   *
   * @param terminal the SODA operation
   * @param document new document for a replace operation,
   *                 patch spec if a patch operation,
   *                 <code>null</code> if it's not a replace
   *                 or patch operation
   */
  private Operation generateOperation(Terminal terminal,
                                      OracleDocument document)
    throws OracleException
  {
    StringBuilder sb = new StringBuilder();

    // Make this a callable statement for non-Oracle drivers
    if (returningClause(terminal) && collection.useCallableReturns)
      sb.append("begin\n");

    if (terminal == Terminal.REMOVE)
    {
      return_query = false;
      generateRemove(sb);
    }
    else if (isReplaceOrMergeOrPatch(terminal))
    {
      return_query = false;
      generateUpdate(sb, terminal);
    }
    else
    {
      if (terminal != Terminal.GET_CURSOR || selectStageOfPatch())
      {
        // ### It would also be good to support non-cursor based
        //     operations, such as getOne(). However, we would
        //     need to provide the ability to fetch the SQL
        //     from OracleOperationBuilder (i.e. getLastSQLStatement(...)),
        //     instead of OracleCursor.
        return_query = false;
      }

      if (paginationWorkaround(terminal))
      {
        generatePaginationWorkaround(sb, terminal);
      }
      else
      {
        generateSelect(sb, terminal);
      }
    }

    if (flashback(terminal))
      generateFlashback(sb);

    int numOfFilterSpecKeys = 0;

    if (filterSpec != null)
    {
      if (tree == null)
      {
        throw new IllegalStateException();
      }

      numOfFilterSpecKeys = getNumberOfFilterSpecKeys();
    }

    generateWhere(sb, write(terminal));

    if (!countOrWrite(terminal) && !selectStageOfPatch())
    {
      boolean filterSpecOrderByPresent = false;

      if (hasFilterSpecOrderBy())
      {
        try {
          sb.append(" order by ");
          tree.appendFilterSpecOrderBy(sb,
                                       options.contentColumnName,
                                       options.hasEmbeddedID(),
				       options.hasMaterializedEmbeddedID(),
                                       collection.getInputFormatClause(),
				       options.keyColumnName);
        } catch (QueryException e) {
          throw SODAUtils.makeException(SODAMessage.EX_INVALID_FILTER, e);
        }
        filterSpecOrderByPresent = true;
      }

      if (!paginationWorkaround(terminal))
      {
        generateOrderBy(sb, filterSpecOrderByPresent);
        generateOffsetAndFetchNext(sb);
      }
      if (lockRows) sb.append(" for update");
    }

    if (returningClause(terminal))
    {
      generateReturning(sb);
    }

    // Make this a callable statement for non-Oracle drivers
    if (returningClause(terminal) && collection.useCallableReturns)
      sb.append(";\nend;\n");

    PreparedStatement stmt  = null;
    CallableStatement cstmt = null;

    String sqltext = sb.toString();

    try
    {
      if (return_query)
        beginQueryRecord(sqltext);

      if (OracleLog.isLoggingEnabled())
        log.fine("Query:\n" + sqltext);

      metrics.startTiming();

      if (returningClause(terminal) && collection.useCallableReturns)
        stmt = cstmt = connection.prepareCall(sqltext);
      else
        stmt = connection.prepareStatement(sqltext);

      int parameterIndex = 0;

      // Binds new values for a replace statement
      // (e.g. new content, version, etc).
      if (isReplaceOrMergeOrPatch(terminal))
      {
        parameterIndex = bindUpdate(stmt, document, terminal);
      }

      if (selectPatchedDoc) 
      {
        stmt.setString(++parameterIndex, patchSpec.getContentAsString());
      }
      else if (selectMergedDoc)
      {
        parameterIndex = bindMergePatch(stmt, parameterIndex);
      }
      // If this is a projection based on REDACT, bind the redaction spec
      else if (projection(terminal))
      {
        stmt.setString(++parameterIndex, projString);
      }

      if (flashback(terminal))
      {
        parameterIndex = bindFlashback(stmt, parameterIndex);
      }

      Iterator<String> keysIter = null;

      if (key != null && !(isStartKey && write(terminal)))
      {
        String canonicalKey = collection.canonicalKey(key);

        ((TableCollectionImpl)collection)
            .bindKeyColumn(stmt, ++parameterIndex, canonicalKey);
        if (return_query)
          recordNamedBind("key", canonicalKey);
      }
      else if (likePattern != null)
      {
        ((TableCollectionImpl)collection)
            .bindKeyColumn(stmt, ++parameterIndex, likePattern);

        if (likeEscape != null)
        {
          ((TableCollectionImpl)collection)
              .bindKeyColumn(stmt, ++parameterIndex, likeEscape);
        }
      }
      else if (keys != null)
      {
        keysIter = keys.iterator();
        bindKeys(keysIter, stmt, keys.size(), parameterIndex);

        parameterIndex += keys.size();
      }

      // Bind $id part of the filterSpec
      if ((numOfFilterSpecKeys > 0) && !skipFilterKeys)
      {
        HashSet<String> keysFromFilterSpec = tree.getKeys();
        int numKeys = keysFromFilterSpec.size();
        HashSet<String> canonicalKeysFromFilterSpec = new HashSet<String>(numKeys);

        for (String k : keysFromFilterSpec)
          canonicalKeysFromFilterSpec.add(collection.canonicalKey(k));

        bindKeys(canonicalKeysFromFilterSpec.iterator(),
                 stmt,
                 numKeys,
                 parameterIndex);
        parameterIndex += numKeys;
      }

      parameterIndex = setStartAndEndTime(stmt, parameterIndex);
      parameterIndex = setVersionAndLastModified(stmt, parameterIndex);
      BiFunction<String, ValueTypePair, Void> callback = null;
      if (return_query) 
      {
        callback = new BiFunction<String, ValueTypePair, Void>() {
          @Override
          public Void apply(String name, ValueTypePair value) {
            recordJsonValueBind(name, value);
            return null;
          }
        };
      }
      if (tree != null) 
      {
        parameterIndex = tree.bind(stmt, parameterIndex, callback);
      }
      int returnParameterIndex = -1;
      if (returningClause(terminal))
      {
        if (collection.useCallableReturns)
        {
          returnParameterIndex = parameterIndex;
          parameterIndex = bindReturning(cstmt, parameterIndex);
        }
        else
        {
          parameterIndex = bindReturning(stmt, parameterIndex);
        }
      }

      boolean isSingleKey = ((key != null) && !isStartKey);

      // If it's not a count() or a write operation,
      // reduce round trips associated with fetching.
      if (!countOrWrite(terminal))
      {
        // If it's not a single row operation, set array fetch size
        if (!isSingleKey)
        {
          if (prefetch > 0)
            stmt.setFetchSize(Math.min(prefetch, SODAConstants.BATCH_FETCH_SIZE));
          else
            stmt.setFetchSize(SODAConstants.BATCH_FETCH_SIZE);

          if (rowDataLimit > 0)
            ((oracle.jdbc.internal.OracleStatement)stmt).setRowDataLimit(rowDataLimit);
        }
        // Prefetch LOB data with the row(s)
        collection.db.setLobPrefetchSize(stmt);
      }

      Operation operation = null;
      boolean isFilterSpec = (filterSpec != null);

      operation = new Operation(stmt,
                                sqltext,
                                headerOnly,
                                isFilterSpec,
                                isSingleKey,
                                returnParameterIndex,
                                collection);

      stmt = null;

      return operation;
    }
    catch (SQLException e)
    {
      if (OracleLog.isLoggingEnabled())
        log.severe(e.toString() + "\n" + sqltext);
      throw SODAUtils.makeExceptionWithSQLText(e, sqltext);
    }
    finally
    {
      for (String message : SODAUtils.closeCursor(stmt, null))
      {
        if (OracleLog.isLoggingEnabled())
          log.severe(message);
      }
    }
  }

  private int bindMergePatch(PreparedStatement stmt, int parameterIndex) throws SQLException, OracleException {
    parameterIndex++;
    ((TableCollectionImpl)collection).bindPayloadColumn(stmt, parameterIndex, patchSpec, false, null);
    return parameterIndex;
  }



  private int setVersionAndLastModified(PreparedStatement stmt,
                                        int parameterIndex)
    throws SQLException
  {
    if (version != null)
    {
      if (return_query)
        recordNamedBind("version", version);
      stmt.setString(++parameterIndex, version);
    }

    if (lastModified != null)
    {
      if (return_query)
        recordNamedBind("lastModified", lastModified);
      stmt.setString(++parameterIndex, lastModified);
    }

    return parameterIndex;
  }

  private int setStartAndEndTime(PreparedStatement stmt,
                                 int parameterIndex)
    throws SQLException
  {
    if (since != null)
    {
      if (return_query)
        recordNamedBind("since", since);
      stmt.setString(++parameterIndex, since);
    }
    if (until != null)
    {
      if (return_query)
        recordNamedBind("until", until);
      stmt.setString(++parameterIndex, until);
    }

    return parameterIndex;
  }

  int bindUpdate(PreparedStatement stmt, OracleDocument document, Terminal terminal)
    throws SQLException, OracleException
  {
    int num = 0;

    byte[] dataBytes = OracleCollectionImpl.EMPTY_DATA;

    boolean materializeContent = true;

    if (!collection.payloadBasedVersioning() &&
        collection.admin().isHeterogeneous() &&
        ((OracleDocumentImpl) document).hasStreamContent() &&
        !OracleDocumentImpl.isBinary(document))
    {
      // This means it needs to be streamed without materializing
      ((TableCollectionImpl)collection).setStreamBind(stmt, document, ++num);
      materializeContent = false;
    }
    // ### After discussion with Doug, we do want to materialize even when
    // not content-based versioning due to the LOB layer issue. Leaving
    // a comment to register this fact.
    else
    {
      // This means we need to materialize the payload
      dataBytes = ((TableCollectionImpl)collection)
          .bindPayloadColumn(stmt, ++num, document, eJSON, null);
    }
    
    if (newKey != null && !collection.getDatabase().omitIdProcessing())
      stmt.setString(++num, newKey);
    
    if ((options.versionColumnName != null) &&
            (options.versioningMethod) != CollectionDescriptor.VERSION_NONE)
    {
      switch (options.versioningMethod)
      {
        case CollectionDescriptor.VERSION_SEQUENTIAL:
          // Assumes RETURNING clause
          break;
        case CollectionDescriptor.VERSION_TIMESTAMP:
          OracleDatabaseImpl db = collection.getDatabase();
          long lstamp = db.getDatabaseTimeVersion(db.getDatabaseTime());
          stmt.setLong(++num, lstamp);
          computedVersion = Long.toString(lstamp);
          break;
        case CollectionDescriptor.VERSION_UUID:
          computedVersion = collection.getDatabase().generateKey();
          stmt.setString(++num, computedVersion);
          break;
        default: /* Hashes */
         if (!materializeContent || (terminal == Terminal.MERGE_ONE ||
                                     terminal == Terminal.MERGE_ONE_AND_GET || 
                                     terminal == Terminal.PATCH_ONE ||
                                     terminal == Terminal.PATCH_ONE_AND_GET))
          {
            // Not Feasible
            throw SODAUtils.makeException(SODAMessage.EX_NO_HASH_VERSION,
                    options.uriName,
                    options.getVersioningMethod());
          }
          computedVersion = collection.computeVersion(dataBytes);
          stmt.setString(++num, computedVersion);
          break;
      }
    }

    num = ((TableCollectionImpl)collection)
              .bindMediaTypeColumn(stmt, num, document);
    return num;
  }

  int bindReturning(CallableStatement stmt, int parameterIndex)
    throws SQLException
  {
    if (options.timestampColumnName != null)
    {
      stmt.registerOutParameter(++parameterIndex, Types.VARCHAR);
    }

    if (options.versionColumnName != null &&
         (options.versioningMethod == CollectionDescriptor.VERSION_NONE ||
          options.versioningMethod == CollectionDescriptor.VERSION_SEQUENTIAL))
    {
      stmt.registerOutParameter(++parameterIndex, Types.VARCHAR);
    }

    if (options.creationColumnName != null)
    {
      stmt.registerOutParameter(++parameterIndex, Types.VARCHAR);
    }

    // Register a "flag" column that is never NULL
    // This is needed to detect whether or not a row was actually updated
    // because we can't rely on the other returned values to be non-NULL,
    // and the row count from a PL/SQL execution is always 1.
    stmt.registerOutParameter(++parameterIndex, Types.VARCHAR);

    return (parameterIndex);
  }

  int bindReturning(PreparedStatement stmt, int parameterIndex)
    throws SQLException
  {

    if (options.hasMaterializedEmbeddedID() && !options.isDualityView())
    {
      collection.db.registerReturnString(stmt, ++parameterIndex);	     
    }

    if (options.timestampColumnName != null)
    {
      collection.db.registerReturnTimestamp(stmt, ++parameterIndex);
    }

    if (options.versionColumnName != null &&
         (options.versioningMethod == CollectionDescriptor.VERSION_NONE ||
          options.versioningMethod == CollectionDescriptor.VERSION_SEQUENTIAL))
    {
      if (options.isDualityView())
        collection.db.registerReturnBinary(stmt, ++parameterIndex);
      else
        collection.db.registerReturnString(stmt, ++parameterIndex);

    }

    if (options.creationColumnName != null)
    {
      collection.db.registerReturnTimestamp(stmt, ++parameterIndex);
    }

    return (parameterIndex);
  }

  int bindFlashback(PreparedStatement stmt, int num)
          throws SQLException
  {
    if (asOfScn != null)
      stmt.setLong(++num, asOfScn.longValue());
    else if (asOfTimestamp != null)
      stmt.setString(++num, asOfTimestamp);

    return (num);
  }

  // Helper method for binding multiple keys
  void bindKeys(Iterator<String> keysIterator,
                PreparedStatement stmt,
                int fetchLimit,
                int currentNumBinds)
    throws SQLException, OracleException
  {
    String currentKey = null;

    int numBinds = currentNumBinds;

    int count = 0;

    while (keysIterator.hasNext())
    {
      currentKey = keysIterator.next();

      if (return_query)
        recordQueryKey(count++, currentKey);

      // Bind each key in the set
      ((TableCollectionImpl)collection)
          .bindKeyColumn(stmt, ++numBinds, currentKey);
    }
  }

  private Operation createReplaceStatement(Terminal terminal,
                                           OracleDocument document,
                                           boolean skipKeyCheck)
    throws OracleException
  {
    if ((key == null) || isStartKey)
    {
      if (!skipKeyCheck)
        throw SODAUtils.makeException(SODAMessage.EX_KEY_MUST_BE_SPECIFIED);
    }

    if (document == null)
    {
      throw SODAUtils.makeException(SODAMessage.EX_ARG_CANNOT_BE_NULL,
              "document");
    }

    computedVersion = null;

    return (generateOperation(terminal, document));
  }

  public OracleDocument replaceOneAndGet(OracleDocument document)
    throws OracleException
  {
    collection.checkJDBCVersion();
    collection.writeCheck("replaceOneAndGet");
    if (OracleDocumentImpl.isBinary(document) && eJSON)
      throw SODAUtils.makeException(SODAMessage.EX_EJSON_CANNOT_BE_USED_WITH_BINARY_DOC);
    return replaceOneAndGetWithOptionalMerge(document, Terminal.REPLACE_ONE_AND_GET, false);
  }

  private OracleDocument replaceOneAndGetWithOptionalMerge(OracleDocument document, Terminal terminal, boolean skipKeyCheck)
    throws OracleException
  {
    if((terminal == Terminal.REPLACE_ONE || terminal == Terminal.REPLACE_ONE_AND_GET) 
        && this.patchSpec == null && !collection.getDatabase().omitIdProcessing()) 
    {
      keyCheckInDocumentSpec(document, true, true);
      if (options.hasVarcharEmbeddedID())
        extractKeyWithErrorOnMissing(document);
    }
    
    Operation operation = createReplaceStatement(terminal, document, skipKeyCheck);
    PreparedStatement stmt  = operation.getPreparedStatement();
    CallableStatement cstmt = operation.getCallableStatement();

    String tstamp = null;
    String createdOn = null;

    String newKey = this.newKey == null ? key : this.newKey;

    int count = 0;
    OracleDocumentImpl result = null;

    try
    {
      count = stmt.executeUpdate();
      //
      // This is normally the count of rows updated; however, for a PL/SQL
      // call it's the number of executions of the anonymous block. In
      // such cases, count == 1 even if it didn't update any rows, and
      // we have to use a "flag" column to tell if it worked or not.
      //
      if (count == 0)
      {
        return null;
      }

      // Oracle-specific RETURNING clauses
      if (returningClause(terminal) &&
          (options.timestampColumnName != null ||
            ((TableCollectionImpl) collection).returnVersion() ||
            options.creationColumnName != null ||
	    (options.hasMaterializedEmbeddedID() && !options.isDualityView())))
      {
        int onum = 0;

        if (cstmt == null)
        {
          ResultSet rows = collection.db.getReturnResultSet(stmt);
          if (rows == null)
            return null;
          if (!rows.next())
          {
            rows.close();
            return null;
          }

	  if ((options.hasMaterializedEmbeddedID() && !options.isDualityView())) {
             newKey = rows.getString(++onum);
          }

          if (options.timestampColumnName != null)
          {
            tstamp = OracleDatabaseImpl.getTimestamp(rows, ++onum);
          }

          if (((TableCollectionImpl)collection).returnVersion())
          {
            if (options.isDualityView())
            {
              computedVersion = ByteArray.rawToHex(rows.getBytes(++onum));		     
            }		     
	        else
            {
              computedVersion = rows.getString(++onum);
            }
          }

          if (options.creationColumnName != null)
          {
             createdOn = OracleDatabaseImpl.getTimestamp(rows, ++onum);
          }

          rows.close();
          rows = null;
        }
        else
        {
          onum = operation.getReturnParameterIndex();

          // If this has a RETURNING clause, it's wrapped in a PL/SQL call
          if (onum >= 0)
          {
            if (options.timestampColumnName != null)
            {
              tstamp = OracleDatabaseImpl.getTimestamp(cstmt, ++onum);
            }

            if (((TableCollectionImpl)collection).returnVersion())
            {
              computedVersion = cstmt.getString(++onum);
            }

            if (options.creationColumnName != null)
            {
              createdOn = OracleDatabaseImpl.getTimestamp(cstmt, ++onum);
            }

            // If this was wrapped in a PL/SQL block, the count is always 1
            // because it's the execution count, not the row count. Get the
            // value of a "flag" column to make sure a row was processed.
            String flagValue = cstmt.getString(++onum);
            if ((flagValue == null) || (!flagValue.equals("1")))
            {
              return null;
            }
          }
        }
      }

      stmt.close();
      stmt = null;

      metrics.recordWrites(1, 1);

      // ### When can count ever be > 1 ?
      if (count == 1)
      {
        result = new OracleDocumentImpl(collection.canonicalKey(newKey),
                                        computedVersion,
                                        tstamp);
        result.setCreatedOn(createdOn);
        String ctype = document.getMediaType();

        ((TableCollectionImpl)collection).setContentType(ctype, result);
      }

    }
    catch (SQLException e)
    {
      if (OracleLog.isLoggingEnabled())
        log.severe(e.toString() + "\n" + operation.getSqlText());
      throw SODAUtils.makeExceptionWithSQLText(e, operation.getSqlText());
    }
    finally
    {
      for (String message : SODAUtils.closeCursor(stmt, null))
      {
        if (OracleLog.isLoggingEnabled())
          log.severe(message);
      }
    }

    return result;
  }

  @Override
  public boolean replaceOne(OracleDocument document) throws OracleException {
    collection.checkJDBCVersion();
    collection.writeCheck("replaceOne");
    if (OracleDocumentImpl.isBinary(document) && eJSON)
      throw SODAUtils.makeException(SODAMessage.EX_EJSON_CANNOT_BE_USED_WITH_BINARY_DOC);
    int count = replaceOneWithOptionalMerge(document, Terminal.REPLACE_ONE, false);
    if (count == 0) 
      return false;
    else
      return true;
  }

  public int replace(OracleDocument document) throws OracleException {
    collection.checkJDBCVersion();
    collection.writeCheck("replace");
    if (OracleDocumentImpl.isBinary(document) && eJSON)
      throw SODAUtils.makeException(SODAMessage.EX_EJSON_CANNOT_BE_USED_WITH_BINARY_DOC);
    return replaceOneWithOptionalMerge(document, Terminal.REPLACE_ONE, true);
  }
  
  private int replaceOneWithOptionalMerge(OracleDocument document, Terminal terminal, boolean skipKeyCheck)
    throws OracleException
  {
    int nrows = 0;

    if ((terminal == Terminal.REPLACE_ONE || terminal == Terminal.REPLACE_ONE_AND_GET) 
        && this.patchSpec == null && !collection.getDatabase().omitIdProcessing())
    {
      keyCheckInDocumentSpec(document, true, true);
      if (options.hasVarcharEmbeddedID())
        extractKeyWithErrorOnMissing(document);
    }
    
    Operation operation = createReplaceStatement(terminal, document, skipKeyCheck);
    PreparedStatement stmt = operation.getPreparedStatement();

    try
    {
      nrows = stmt.executeUpdate();

      stmt.close();
      stmt = null;

      metrics.recordWrites(1, 1);
    }
    catch (SQLException e)
    {
      if (OracleLog.isLoggingEnabled())
        log.severe(e.toString() + "\n" + operation.getSqlText());
      throw SODAUtils.makeExceptionWithSQLText(e, operation.getSqlText());
    }
    finally
    {
      for (String message : SODAUtils.closeCursor(stmt, null))
      {
        if (OracleLog.isLoggingEnabled())
          log.severe(message);
      }
    }

    return nrows;
  }

  public int remove() throws OracleException
  {
    collection.checkJDBCVersion();
    collection.writeCheck("remove");

    Operation operation = generateOperation(Terminal.REMOVE);

    PreparedStatement stmt = operation.getPreparedStatement();

    int count = 0;

    try
    {
      count = stmt.executeUpdate();

      stmt.close();
      stmt = null;

      metrics.recordWrites(1, 1);
    }
    catch (SQLException e)
    {
      if (OracleLog.isLoggingEnabled())
        log.severe(e.toString() + "\n" + operation.getSqlText());
      throw SODAUtils.makeExceptionWithSQLText(e, operation.getSqlText());
    }
    finally
    {
      for (String message : SODAUtils.closeCursor(stmt, null))
      {
        if (OracleLog.isLoggingEnabled())
          log.severe(message);
      }
    }

    return count;
  }

  public OracleOperationBuilder orderByKey(boolean order)
  {
    this.orderByKey = order;
    return this;
  }

  public OracleOperationBuilder limit(int limit) throws OracleException
  {
    /* 
     * Typical soda usecase is for operational cases where a single access
     * does not return much data to the application.  Therefore, optimize for
     * response time.  
     */
    firstRowsHint(limit);
    if (limit < 1)
    {
      throw SODAUtils.makeException(SODAMessage.EX_ARG_MUST_BE_POSITIVE,
                                    "limit");
    }
    else if (lockRows)
    {
      throw SODAUtils.makeException(SODAMessage.EX_INCOMPATIBLE_METHODS, "lock()", "limit()");
    }

    this.limit = limit;

    return this;
  }

  public OracleOperationBuilder skip(long skip) throws OracleException
  {
    if (skip < 0L)
    {
      throw SODAUtils.makeException(SODAMessage.EX_ARG_MUST_BE_NON_NEGATIVE,
                                    "skip");
    }
    else if (lockRows)
    {
      throw SODAUtils.makeException(SODAMessage.EX_INCOMPATIBLE_METHODS, "lock()", "skip()");
    }

    this.skip = skip;

    return this;
  }

  public OracleOperationBuilder headerOnly()
  {
    // Override project() setting
    proj = null;

    skipProjErrors = true;

    headerOnly = true;

    return this;
  }

  /* Not part of a public API */
  public OracleOperationBuilder firstRowsHint(int numRows)
  {
    // Hint is ignored if numRows is less than 0
    if (numRows >= 0)
    {
      firstRows = numRows;
    }

    return this;
  }

  /* Not part of a public API */
  public OracleOperationBuilder prefetch(int prefetch) throws OracleException
  {
    if (prefetch < 0L)
    {
      throw SODAUtils.makeException(SODAMessage.EX_ARG_MUST_BE_NON_NEGATIVE,
                                    "prefetch");
    }
    this.prefetch = prefetch;
    return this;
  }

  /* Not part of a public API */
  public OracleOperationBuilder rowDataLimit(long rowDataLimit) throws OracleException
  {
    if (prefetch < 0L)
    {
      throw SODAUtils.makeException(SODAMessage.EX_ARG_MUST_BE_NON_NEGATIVE,
                                    "rowDataLimit");
    }
    this.rowDataLimit = rowDataLimit;
    return this;
  }

  /* Not part of a public API */
  // ### It would be better to provide the plan via logging instead.
  //     Remove this method once that's done.
  public String explainPlan(String format) throws OracleException
  {
    return explainPlan(format, "TEXT");
  }

  public String explainPlan(String format, String type) throws OracleException
  {
    Operation operation = generateOperation(Terminal.EXPLAIN_PLAN);
    // Run 'explain plan...' and close the result.
    getResultSet(operation, true);

    ResultSet         res  = null;
    PreparedStatement stmt = null;

    String plan = null;
    try
    {
      String sqltext = "select dbms_xplan.display_plan('plan_table', null, ?, null, ?) from sys.dual";

      stmt = connection.prepareStatement(sqltext);
      stmt.setString(1, format);
      stmt.setString(2, type);

      res = stmt.executeQuery();

      while (res.next())
      {
        // We get it as a CLOB because it's a temp LOB and we want to free it
        Clob tmp = res.getClob(1);
        plan = tmp.getSubString(1L, (int)tmp.length());
        tmp.free();
      }

      res.close();
      res = null;

      stmt.close();
      stmt = null;

      return plan;
    }
    catch (SQLException e)
    {
      if (OracleLog.isLoggingEnabled())
        log.severe(e.toString() + "\n" + operation.getSqlText());
      throw SODAUtils.makeExceptionWithSQLText(e, operation.getSqlText());
    }
    finally
    {
      for (String message : SODAUtils.closeCursor(stmt, res))
      {
        if (OracleLog.isLoggingEnabled())
          log.severe(message);
      }
    }
  }

  public long count() throws OracleException
  {
    if (skip > 0L || limit > 0)
    {
      throw SODAUtils.makeException(SODAMessage.EX_SKIP_AND_LIMIT_WITH_COUNT);
    }
    else if (lockRows)
    {
      throw SODAUtils.makeException(SODAMessage.EX_INCOMPATIBLE_METHODS, "lock()", "count()");
    }

    Operation operation = generateOperation(Terminal.COUNT);

    long result = 0L;

    PreparedStatement stmt = null;

    try
    {
      metrics.startTiming();

      stmt = operation.getPreparedStatement();
      ResultSet rows = stmt.executeQuery();

      if (rows.next())
        result = rows.getLong(1);

      stmt.close();
      stmt = null;

      metrics.recordReads(1, 1);
    }
    catch (SQLException e)
    {
      if (OracleLog.isLoggingEnabled())
        log.severe(e.toString() + "\n" + operation.getSqlText());
      throw SODAUtils.makeExceptionWithSQLText(e, operation.getSqlText());
    }
    finally
    {
      for (String message : SODAUtils.closeCursor(stmt, null))
      {
        if (OracleLog.isLoggingEnabled())
          log.severe(message);
      }
    }

    return result;
  }

  public OracleDocument getOne() throws OracleException
  {
    collection.checkJDBCVersion();
    Operation operation = generateOperation(Terminal.GET_ONE);
    ResultSet resultSet = getResultSet(operation);

    long prepAndExecTime = metrics.endTiming();

    OracleCursorImpl cursor =
      new OracleCursorImpl(options,
                           metrics,
                           operation,
                           resultSet,
                           // Projection is ignored if the operation is patch.
                           (selectPatchedDoc == true ? false :
                                                       (proj == null ? false : true)),
                           (selectPatchedDoc || selectMergedDoc),
                           eJSON);

    cursor.setElapsedTime(prepAndExecTime);

    OracleDocument doc = null;

    if (cursor.hasNext())
      doc = cursor.next();

    try
    {
      cursor.close();
    }
    catch (IOException e)
    {
      // Unwrap the SQL cause, and set it as the cause
      // of a new OracleException.
      Throwable cause = e.getCause();
      if (cause instanceof SQLException)
      {
        throw SODAUtils.makeExceptionWithSQLText((SQLException) cause,
                                                 operation.getSqlText());
      }

      throw new IllegalStateException();
    }

    return(doc);
  }

  private void appendColumn(StringBuilder sb,
                            String colName)
  {
    appendAliasedColumn(sb, colName, null);
  }

  /**
   * Appends a quoted column name, prepending a table alias if necessary
   */
  private void appendAliasedColumn(StringBuilder sb,
                                   String colName,
                                   String tAlias)
  {
    if (tAlias != null) {
      sb.append(tAlias);
      sb.append(".");
    }

    sb.append("\"");
    sb.append(colName);
    sb.append("\"");
  }

  
  // Figure out the number of keys specified via
  // the filter spec.
  private int getNumberOfFilterSpecKeys()
  {
    int numOfFilterSpecKeys = 0;

    if (filterSpec != null)
    {
      if (tree == null)
      {
        throw new IllegalStateException();
      }

      HashSet<String> keys = tree.getKeys();

      if (keys != null)
      {
        numOfFilterSpecKeys = keys.size();
      }
    }

    return numOfFilterSpecKeys;
  }
  
  private void appendInStaments(StringBuilder sb, int keysSize) {
    final int KEY_LIMIT = 1000;
    int i = 0;
    
    // Since IN statement only supports up to 1000 binds, we
    // need to create a new one every time is needed, that way
    // we can avoid execution errors
    while (i < keysSize)
    {
      // Build an IN statement with the requested number of positions
      // The bindings will need to be in the native form of the key
      // e.g. setLong() for NUMBER, setBytes() for RAW, or setString()
      appendColumn(sb, options.keyColumnName);
      sb.append(" in (");
      for (int j = 1; (j <= KEY_LIMIT && i < keysSize) ; j++) 
      {
        if (++i == keysSize || j == KEY_LIMIT)
          sb.append("?)");
        else
          sb.append("?,");
      }
      
      if (i != keysSize)
        sb.append(" or ");
    }
  }

  private void generateWhere(StringBuilder sb, boolean isWrite) throws OracleException
  {
    boolean append = false;
    int numOfFilterSpecKeys = getNumberOfFilterSpecKeys();

    if (whereClauseRequired(isWrite))
    {
      sb.append(" where ");
    }
    else
    {
      return;
    }

    // morgiyan_bug-37234854. startsWith specified in conjunction with
    // write operation should have no effect. Prior to this bug, it 
    // was incorrectly allowed with startsWith.
    if (key != null && !(isStartKey && isWrite))
    {
      sb.append("(");
      appendColumn(sb, options.keyColumnName);

      if (!isStartKey)
      {
        sb.append(" = ");
      }
      else
      {
        sb.append(startKeyAscending ? " >" : " <");
        sb.append(startKeyInclusive ? "= " : " ");
      }

      ((TableCollectionImpl)collection).addKey(sb);

      append = true;
    }
    else if (likePattern != null)
    {
      sb.append("(");
      appendColumn(sb, options.keyColumnName);

      sb.append(" LIKE ?");

      if (likeEscape != null)
      {
        sb.append(" ESCAPE ?");
      }

      append = true;

    }
    else if (keys != null)
    {
      sb.append(" (");

      appendInStaments(sb, keys.size());

      append = true;
    }

    if ((numOfFilterSpecKeys > 0) && !skipFilterKeys)
    {
      // If key() or keys() is specified, for
      // now just OR together the keys from
      // filterSpec. Once we finalize
      // consistent read issues this will
      // go away (will have one IN clause
      // for all keys)
      if (append)
      {
        if (isStartKey)
        {
          sb.append(" ) and ( ");
        }
        else
        {
          sb.append(" or ");
        }
      }
      else
      {
        sb.append("( ");
      }

      appendInStaments(sb, numOfFilterSpecKeys);

      addCloseParenthesis(sb);
      append = true;
    }
    else if (append)
    {
      addCloseParenthesis(sb);
    }

    if (since != null || until != null)
    {
      appendAnd(sb, append);
      
      sb.append(" ( ");
      
      appendColumn(sb, options.timestampColumnName);
      
      if (since != null)
      {
        sb.append(" >");

        if (timeRangeInclusive)
          sb.append("=");

        OracleDatabaseImpl.addToTimestamp(" ", sb);
      }
      
      if (until != null)
      {
        if (since != null)
        {
          sb.append(" and ");
          appendColumn(sb, options.timestampColumnName);
        }
        OracleDatabaseImpl.addToTimestamp(" <= ", sb);
      }
      
      addCloseParenthesis(sb);
      append = true;
    }

    if (version != null)
    {
      appendAnd(sb, append);
      
      if (errorOnVersionMismatch)
          sb.append("(case ");
          
      if (options.isDualityView())
        sb.append("etag_matches(");
      else
        sb.append(" ( ");
      
      appendColumn(sb, options.versionColumnName);
      
      if (options.isDualityView())
        sb.append(", ");
      else if (errorOnVersionMismatch)
        sb.append(") when (");
      else
        sb.append(" = ");
      
      switch (options.versioningMethod)
      {
        case CollectionDescriptor.VERSION_SEQUENTIAL:
        case CollectionDescriptor.VERSION_TIMESTAMP:
          sb.append("to_number(?)");
          break;
        default:
          if (options.isNative() && !options.isDualityView() && errorOnVersionMismatch)
            sb.append("HEXTORAW(?)");
          else
            sb.append("?");
          break;
      }
      if (options.isDualityView())
        sb.append(" ERROR ON MISMATCH"); 
      
      addCloseParenthesis(sb);
      
      if (errorOnVersionMismatch) {
        if (options.isDualityView())
          sb.append(" when True then HEXTORAW('1') else HEXTORAW('o') end)=HEXTORAW('1')");
        else
          sb.append(" then HEXTORAW('1') else HEXTORAW('o') end)=HEXTORAW('1')");
      }
        
      
      append = true;
    }
    
    if (lastModified != null)
    {
      appendAnd(sb, append);

      sb.append(" ( ");

      appendColumn(sb, options.timestampColumnName);
      OracleDatabaseImpl.addToTimestamp(" = ", sb);

      addCloseParenthesis(sb);
      append = true;
    }

    if (filterSpec != null)
    {
      try
      {
        append = tree.appendFilterSpec(sb, append, options.contentColumnName, collection.getInputFormatClause(), collection.isTreatAsAvailable());
      }
      catch (QueryException e)
      {
        if (OracleLog.isLoggingEnabled())
          log.warning(e.toString());
         throw SODAUtils.makeException(SODAMessage.EX_INVALID_FILTER, e);
      }
    }
  }

  public static void appendAnd(StringBuilder sb, boolean append)
  {
    if (append)
    {
      sb.append(" and ");
    }
  }
  
  private boolean whereClauseRequired(boolean isWrite)
  {
    if ((key != null && !(isStartKey && isWrite)) || (keys != null) ||
        (likePattern != null)      ||
        (since != null)   || (until != null)        ||
        (version != null) || (lastModified != null) ||
        ((filterSpec != null) &&
         (tree.hasJsonExists()     ||
          tree.hasSpatialClause()  ||
          tree.hasContainsClause() ||
          tree.hasSqlJsonClause()  ||
          tree.hasKeys())
         )
        )
    {
      return true;
    }

    return false;
  }

  private boolean hasFilterSpecOrderBy()
  {
    return (filterSpec != null && tree.hasOrderBy());
  }

  private boolean projection(Terminal terminal)
  {
    // ### TODO: projection can be supported with GET_ONE
    if (proj != null &&
        !countOrWrite(terminal) &&
        !selectStageOfPatch())
    {
      return true;
    }

    return false;
  }

  private boolean countOrWrite(Terminal terminal)
  {
    if ((terminal == Terminal.COUNT) || write(terminal))
    {
      return true;
    }

    return false;
  }

  private boolean write (Terminal terminal)
  {
    if ((terminal == Terminal.REMOVE) || isReplaceOrMergeOrPatch(terminal))
    {
      return true;
    }

    return false;
  }

  private boolean selectStageOfPatch()
  {
    // if selectPatchedDoc or selectMergedDoc is true, we are in the
    // "select" stage of the current two stage patch (which first
    // selects patched documents, and then does a replace for each
    // patched document to write it back to the collection).
    //
    // We can't tell we are in the select stage of patch by
    // looking at the terminal (since the latter will be
    // GET_ONE for a single doc patch, or GET_CURSOR for bulk
    // patch). But we can tell by these flags.
    return (selectPatchedDoc || selectMergedDoc);
  }

  private boolean returningClause(Terminal terminal)
  {
    boolean disableReturning = collection.internalDriver;

    // Disable use of DML RETURNING clauses for non-Oracle drivers
    // unless the non-Oracle driver supports callable statements
    // as an alternative.
    if (!collection.oracleDriver && !collection.useCallableReturns)
      disableReturning = true;

    if ((terminal == Terminal.REPLACE_ONE_AND_GET ||
         terminal == Terminal.MERGE_ONE_AND_GET ||
         terminal == Terminal.PATCH_ONE_AND_GET) && !disableReturning)
    {
      if ((options.timestampColumnName == null) &&
          (options.creationColumnName == null) &&
          (!((TableCollectionImpl)collection).returnVersion()) &&
	  (!options.hasMaterializedEmbeddedID() || options.isDualityView()))
      {
        return false;
      }

      return true;
    }

    return false;
  }

  private boolean isReplaceOrMergeOrPatch(Terminal terminal)
  {
    if (terminal == Terminal.REPLACE_ONE_AND_GET ||
        terminal == Terminal.REPLACE_ONE ||
        terminal == Terminal.MERGE_ONE_AND_GET ||
        terminal == Terminal.MERGE_ONE || 
        terminal == Terminal.PATCH_ONE_AND_GET ||
        terminal == Terminal.PATCH_ONE)
    {
      return true;
    }

    return false;
  }

  private boolean flashback(Terminal terminal)
  {
   if (!selectStageOfPatch() &&
       !paginationWorkaround(terminal) &&
       (terminal == Terminal.COUNT ||
        terminal == Terminal.GET_ONE ||
        terminal == Terminal.GET_CURSOR))
    {
      return true;
    }

    return false;
  }

  /**
   * Pagination workaround should be enabled only
   * for a case of whole-table pagination only
   * (no where clause, no filter spec order by).
   */
  private boolean paginationWorkaround(Terminal terminal)
  {
    return (PAGINATION_WORKAROUND &&
            (asOfTimestamp == null) && (asOfScn == null) &&
            (skip > 0L || limit > 0) &&
            (terminal == Terminal.GET_CURSOR ||
             terminal == Terminal.GET_ONE ||
             terminal == Terminal.EXPLAIN_PLAN) &&
             !projection(terminal) &&
             !whereClauseRequired(write(terminal)) &&
             !hasFilterSpecOrderBy() &&
             !selectPatchedDoc && !selectMergedDoc &&
             (collection.options.dbObjectType == CollectionDescriptor.DBOBJECT_TABLE));
  }


  private void addCloseParenthesis(StringBuilder sb)
  {
    sb.append(" ) ");
  }

  private void generateOrderBy(StringBuilder sb,
                               boolean hasFilterSpecOrderBy)
  {
    if (isStartKey && !hasFilterSpecOrderBy)
    {
      sb.append(" order by ");
      appendColumn(sb, options.keyColumnName);

      if (!ascending)
      {
        sb.append(" desc ");
      }
    }
    else if ((since != null || until != null) && !hasFilterSpecOrderBy)
    {
      sb.append(" order by ");

      if (since != null || until != null)
      {
        appendColumn(sb, options.timestampColumnName);
        sb.append(",");
      }

      appendColumn(sb, options.keyColumnName);
    }
    else if (((skip > 0L || limit > 0) && orderByKey) || keyOrdered)
    {
      if (!hasFilterSpecOrderBy)
      {
        sb.append(" order by ");
        appendColumn(sb, options.keyColumnName);
  
        if (!ascending)
        {
          sb.append(" desc");
        }
      }
      // Tree should not be null here (it would be an illegal state).
      // Just checking for null as an extra precaution.
      else if (tree != null && !tree.hasIDColumnRewrite(options.hasEmbeddedID(), options.hasMaterializedEmbeddedID()))
      {
        sb.append(", ");
        appendColumn(sb, options.keyColumnName);
  
        if (!ascending)
        {
          sb.append(" desc");
        }
      }
    }
  }

  private void generateOffsetAndFetchNext(StringBuilder sb)
  {
    if (skip > 0L)
    {
      sb.append(" offset "+Long.toString(skip)+" rows");
    }

    if (limit > 0)
    {
      sb.append(" fetch next "+Integer.toString(limit)+" rows only");
    }
  }

  private void generateFlashback(StringBuilder sb)
  {
    if (asOfScn != null)
      sb.append(" as of scn ?");
    else if (asOfTimestamp != null)
    {
      // ### For reasons that aren't clear, Flashback will fail
      // ### if given UTC times - it insists on using only RDBMS
      // ### zone-specific times. Converting it to a timestamp with
      // ### time zone and then using "AT LOCAL" fixes this by
      // ### forcing a zone conversion to the RDBMS time zone.
      sb.append(" as of timestamp ");
      sb.append("to_timestamp_tz(?,'SYYYY-MM-DD\"T\"HH24:MI:SS.FFTZH:TZM')");
      sb.append(" at local");
    }
  }

  /**
   * Build string ["schema".]"table"
   */
  private void appendTable(StringBuilder sb)
  {
    sb.append("\"");
    if (options.dbSchema != null)
    {
      sb.append(options.dbSchema);
      sb.append("\".\"");
    }
    sb.append(options.dbObjectName);
    sb.append("\"");
  }

  // By default, a whole-table pagination query
  // (i.e. select * from MyTable order by ID
  //       offset X row fetch next Y rows only),
  // generates inefficient plans with Oracle RDBMS 12C.
  // At best, the index on IDs is used, but the table is 
  // accessed for all the X rows of the offset. At worst, 
  // the whole table is sorted. See bug 20689267 for more info.
  // For now, the following workaround is used (once the bug 
  // is fixed, the workaround can be removed):
  //
  //  select /*+ LEADING(TAB1_) USE_NL(TAB2_) */ *
  //  from
  //      (select /*+ INDEX(TAB_)*/ ID
  //       from MyTable TAB_
  //       order by ID
  //       offset X rows fetch next Y rows only) TAB1_,
  //  MyTable TAB2_
  //  where TAB1_.rowid = TAB2_.rowid
  //  order by TAB1_.ID
  //
  // This workaround uses an inline view to first
  // fetch the relevant IDs. Then, probe into the
  // table by rowid, thus only accessing the table
  // rows only for the rows with relevant IDs.
  //
  // Note that this workaround is only applied for the
  // case of whole table pagination (i.e. no where clause).
  //
  private void generatePaginationWorkaround(StringBuilder sb, Terminal terminal)
  {
    sb.setLength(0);

    String tabAlias = "TAB_";
    String tab1Alias = "TAB1_";
    String tab2Alias = "TAB2_";

    // select with leading and use_nl hints
    if (terminal == Terminal.EXPLAIN_PLAN)
    {
      sb.append("explain plan for ");
    }

    // ### How can we determine if a projection is occuring?
    boolean addProjection = projection(terminal);

    sb.append("select ");
    // ### We should extract the MONITOR from hint and insert here.
    sb.append(" /*+ LEADING(" + tab1Alias + ") ");
    sb.append("USE_NL(" + tab2Alias + ") */ ");
    appendTableColumns(sb, tab2Alias, addProjection, terminal);

    sb.append(" from ");

    // outer query selecting only the relevant IDs
    sb.append(" ( select /*+ INDEX(");
    sb.append(tabAlias);
    sb.append(") */ ");
    appendAliasedColumn(sb, options.keyColumnName, tabAlias);
    sb.append(" from ");
    appendTable(sb);
    sb.append(" ");
    sb.append(tabAlias);
    generateOffsetAndFetchNext(sb);
    sb.append(" ) ");
    sb.append(tab1Alias);
    // end outer query selecting only the relevant IDs

    sb.append(", ");
    appendTable(sb);
    sb.append(" ");
    sb.append(tab2Alias);

    // where TAB1_.rowid = TAB2_.rowid
    sb.append(" where ");
    sb.append(tab1Alias);
    sb.append(".rowid = ");
    sb.append(tab2Alias);
    sb.append(".rowid ");

    // orderby
    sb.append(" order by ");
    appendAliasedColumn(sb, options.keyColumnName, tab1Alias);
  }

  private void generateSelect(StringBuilder sb, Terminal terminal)
  {
    sb.setLength(0);

    if (terminal == Terminal.EXPLAIN_PLAN)
    {
      sb.append("explain plan for ");
    }

    sb.append("select ");

    if (firstRows >= 0 || hints != null)
    {
      sb.append("/*+");
      if (firstRows >= 0)
      {
        sb.append(" FIRST_ROWS(");
        sb.append(firstRows);
        sb.append(')');
      }
      if (hints != null)
    	sb.append(" " + hints);  
      sb.append(" */ ");
    }

    boolean addProjection = projection(terminal);

    if (terminal == Terminal.COUNT)
    {
      // Count over the key column is a full index scan (not a table scan)
      sb.append(" count(\"");
      sb.append(options.keyColumnName);
      sb.append("\")");
    }
    else
    {
      appendTableColumns(sb, null, addProjection, terminal);
    }

    sb.append(" from ");

    appendTable(sb);
  }

  private void appendTableColumns(StringBuilder sb,
                                  String tAlias,
                                  boolean addProjection,
                                  Terminal terminal)
  {
    if (selectPatchedDoc)
    {
      appendPatchExpression(sb, tAlias);
      sb.append(",");
    }
    else if (selectMergedDoc)
    {
      appendMergePatchExpression(sb, tAlias);
      sb.append(",");
    }
    else if (addProjection)
    {
      if (options.hasBinaryFormat() || options.hasJsonType())
      {
        // Sanity check
        if (options.contentDataType != CollectionDescriptor.BLOB_CONTENT &&
            options.hasBinaryFormat())
        {
          throw new IllegalStateException();
        }

        // ### json_patch can be used for all collections (not just with binary format),
        //     in 18c or above. Expand later.
        sb.append("json_patch(");
        appendAliasedColumn(sb, options.contentColumnName, tAlias);
        if (options.hasJsonType())
        {
          sb.append(",? project ");
        }
        else
        {
          sb.append(",? project returning blob format oson");
        }

        if (!skipProjErrors)
        {
          sb.append(" error on error),");
        }
        else
        {
          sb.append("),");
        }
      }
      else
      {
        // Use REDACT operation with a bind variable for the redaction spec
        switch (options.contentDataType) {
          case CollectionDescriptor.BLOB_CONTENT:
            sb.append("DBMS_SODA_DOM.JSON_SELECT_B(");
            break;
          case CollectionDescriptor.CLOB_CONTENT:
            sb.append("DBMS_SODA_DOM.JSON_SELECT_C(");
            break;
          case CollectionDescriptor.NCLOB_CONTENT:
            sb.append("DBMS_SODA_DOM.JSON_SELECT_NC(");
            break;
          case CollectionDescriptor.NCHAR_CONTENT:
            sb.append("DBMS_SODA_DOM.JSON_SELECT_N(");
            break;
          case CollectionDescriptor.RAW_CONTENT:
            sb.append("DBMS_SODA_DOM.JSON_SELECT_R(");
            break;
          case CollectionDescriptor.CHAR_CONTENT:
          default:
            sb.append("DBMS_SODA_DOM.JSON_SELECT(");
            break;
        }
        appendAliasedColumn(sb, options.contentColumnName, tAlias);

        sb.append(",?,");
        if (skipProjErrors) {
          // Even if skip project errors is specified,
          // invalid projection spec errors are always reported,
          // and consequently the operation is stopped.
          // There's no point continuing a project operation
          // if the projection spec is broken.
          sb.append("'INVALID_PROJECTION_SPEC'),");
        } else {
          sb.append("'ALL'),");
        }
      }
    }
    else if (!headerOnly)
    {
      if (eJSON)
        sb.append("json_serialize(");
      appendAliasedColumn(sb, options.contentColumnName, tAlias);
      if (eJSON)
        sb.append(" extended)");
      sb.append(",");
    }

    // Key is always returned as a string
    switch (options.keyDataType)
    {
      case CollectionDescriptor.INTEGER_KEY:
        sb.append("to_char(");
        appendAliasedColumn(sb, options.keyColumnName, tAlias);
        sb.append(")");
        break;
      case CollectionDescriptor.RAW_KEY:
        sb.append("rawtohex(");
        appendAliasedColumn(sb, options.keyColumnName, tAlias);
        sb.append(")");
        break;
      case CollectionDescriptor.STRING_KEY:
      case CollectionDescriptor.NCHAR_KEY:
        appendAliasedColumn(sb, options.keyColumnName, tAlias);
        break;
    }

    if (options.doctypeColumnName != null)
    {
      sb.append(",");
      appendAliasedColumn(sb, options.doctypeColumnName, tAlias);
    }

    if ((options.timestampColumnName != null) &&
        !selectPatchedDoc && !selectMergedDoc)
    {
      sb.append(",");
      appendAliasedColumn(sb, options.timestampColumnName, tAlias);
    }

    if ((options.creationColumnName != null) &&
        !selectPatchedDoc && !selectMergedDoc)
    {
      sb.append(",");
      appendAliasedColumn(sb, options.creationColumnName, tAlias);
    }

    
    if ((options.versionColumnName != null) &&
        !selectPatchedDoc && !selectMergedDoc)
    {
      sb.append(",");
      appendAliasedColumn(sb, options.versionColumnName, tAlias);
    }
  }

  private void appendPatchExpression(StringBuilder sb, String tAlias)
  {
    // Sanity check
    if (options.contentDataType != CollectionDescriptor.BLOB_CONTENT &&
            options.hasBinaryFormat())
    {
      throw new IllegalStateException();
    }

    if (options.hasJsonType() || (options.contentDataType == CollectionDescriptor.BLOB_CONTENT))
    {
      // ### json_patch can be used for all collections (not just with binary format),
      //     in 18c or above. Expand later.
      sb.append("json_patch(");
      appendAliasedColumn(sb, options.contentColumnName, tAlias);
      if (options.hasJsonType())
      {
        sb.append(",? ");
      }
      else if (options.hasBinaryFormat())
      {
        sb.append(",oson(? returning blob error on error) returning blob format oson");
      }
      else
      {
        sb.append(",? returning blob");
      }

      if (!patchSpecExceptionOnly)
      {
        sb.append(" error on error)");
      }
      else
      {
        sb.append(")");
      }
    }
    else
    {
      switch (options.contentDataType)
      {
        case CollectionDescriptor.CLOB_CONTENT:
          sb.append("DBMS_SODA_DOM.JSON_PATCH_C(");
          break;
        case CollectionDescriptor.NCLOB_CONTENT:
          sb.append("DBMS_SODA_DOM.JSON_PATCH_NC(");
          break;
        case CollectionDescriptor.NCHAR_CONTENT:
          sb.append("DBMS_SODA_DOM.JSON_PATCH_N(");
          break;
        case CollectionDescriptor.RAW_CONTENT:
          sb.append("DBMS_SODA_DOM.JSON_PATCH_R(");
          break;
        case CollectionDescriptor.CHAR_CONTENT:
        default:
          sb.append("DBMS_SODA_DOM.JSON_PATCH(");
          break;
       }
       appendAliasedColumn(sb, options.contentColumnName, tAlias);
       sb.append(",?");

       if (patchSpecExceptionOnly)
         sb.append(",'INVALID_PATCH_SPEC')");
       else
         sb.append(")");
    }
  }

  private void appendMergePatchExpression(StringBuilder sb, String tAlias)
  {
    // Sanity check
    if (options.contentDataType != CollectionDescriptor.BLOB_CONTENT &&
            options.hasBinaryFormat())
    {
      throw new IllegalStateException();
    }

    if (options.hasJsonType() || (options.contentDataType == CollectionDescriptor.BLOB_CONTENT))
    {
      // ### json_mergepatch can be used for all collections (not just with binary format),
      //     in 18c or above. Expand later.
      sb.append("json_mergepatch(");
      appendAliasedColumn(sb, options.contentColumnName, tAlias);
      if (options.hasJsonType())
      {
        sb.append(",? ");
      }
      else if (options.hasBinaryFormat())
      {
        sb.append(",oson(?  returning blob error on error) returning blob format oson");
      }
      else
      {
        sb.append(",? returning blob");
      }

      if (!patchSpecExceptionOnly)
      {
        sb.append(" error on error)");
      }
      else
      {
        sb.append(")");
      }
    }
    else 
    {
      switch (options.contentDataType)
      {
        case CollectionDescriptor.CLOB_CONTENT:
          sb.append("DBMS_SODA_DOM.JSON_MERGE_PATCH_C(");
          break;
        case CollectionDescriptor.NCLOB_CONTENT:
          sb.append("DBMS_SODA_DOM.JSON_MERGE_PATCH_NC(");
          break;
        case CollectionDescriptor.NCHAR_CONTENT:
          sb.append("DBMS_SODA_DOM.JSON_MERGE_PATCH_N(");
          break;
        case CollectionDescriptor.RAW_CONTENT:
          sb.append("DBMS_SODA_DOM.JSON_MERGE_PATCH_R(");
          break;
        case CollectionDescriptor.CHAR_CONTENT:
        default:
          sb.append("DBMS_SODA_DOM.JSON_MERGE_PATCH(");
          break;
      }
      appendAliasedColumn(sb, options.contentColumnName, tAlias);
      sb.append(",?");

      if (patchSpecExceptionOnly)
        sb.append(",'INVALID_PATCH_SPEC')");
      else
        sb.append(")");
    }
  }

  private void generateUpdate(StringBuilder sb, Terminal terminal)
  {
    sb.append("update ");
    if (hints != null)
      sb.append("/*+ " + hints + " */");
    appendTable(sb);
    sb.append(" set \"");
    sb.append(options.contentColumnName);

    if (terminal == Terminal.MERGE_ONE || terminal == Terminal.MERGE_ONE_AND_GET)
    {
      sb.append("\" = ");
      appendMergePatchExpression(sb, null);
    } 
    else if (terminal == Terminal.PATCH_ONE || terminal == Terminal.PATCH_ONE_AND_GET) 
    {
      sb.append("\" = ");
      appendPatchExpression(sb, null);
    } 
    else 
    {
      boolean omitIdInjection = collection.getDatabase().omitIdProcessing();
      if (eJSON) 
      {
        if (options.hasBinaryFormat())
          sb.append("\" = OSON(? EXTENDED)");
        else if (options.hasJsonType())
          sb.append("\" = JSON(? EXTENDED)");
      }
      else 
      {
        if (options.hasMaterializedEmbeddedID() && !OracleCollectionImpl.NO_JSON_TRANSFORM &&
            !options.isNative() && !options.isDualityView())
        {
          sb.append("\" = json_transform(?, set '$._id' = json_query(\"");  
          sb.append(options.contentColumnName); 
          sb.append("\", '$._id') format oson ignore on existing)");  
        } 
        else {
          sb.append("\" = ");
          if (options.isNative() && collection.getDatabase().isREST() && !eJSON)
            sb.append("json_mergepatch(?,'{\"_id\":null}')");
          else
            sb.append("?");
        }
      }
      
      if (newKey != null && !omitIdInjection)
        sb.append(", ID = ?");
    }

    
    if (options.timestampColumnName != null)
    {
      sb.append(", \"");
      sb.append(options.timestampColumnName);
      sb.append("\" = sys_extract_utc(SYSTIMESTAMP)");
    }

    if ((options.versionColumnName != null) &&
        (options.versioningMethod) != CollectionDescriptor.VERSION_NONE)
    {
      sb.append(", \"");
      sb.append(options.versionColumnName);
      sb.append("\" = ");
      if (options.versioningMethod == CollectionDescriptor.VERSION_SEQUENTIAL)
      {
        sb.append("(\"");
        sb.append(options.versionColumnName);
        sb.append("\" + 1)");
      }
      else
      {
        sb.append("?");
      }
    }

    if (options.doctypeColumnName != null)
    {
      sb.append(", \"");
      sb.append(options.doctypeColumnName);
      sb.append("\" = ?" );
    }
  }

  private void generateReturning(StringBuilder sb)
  {
    TableCollectionImpl tabCollection = (TableCollectionImpl)collection;

    int count = 0;

    sb.append(" returning ");

    if (options.hasMaterializedEmbeddedID() && !options.isDualityView())
    {
      sb.append('"');
      sb.append(options.keyColumnName);
      sb.append('"');
      count++;
    }

    if (options.timestampColumnName != null)
    {
      if (count > 0)
        TableCollectionImpl.addComma(sb, count);
      sb.append('"');
      sb.append(options.timestampColumnName);
      sb.append('"');
      count++;
    }

    if (tabCollection.returnVersion())
    {
      if (count > 0)
        TableCollectionImpl.addComma(sb, count);

      sb.append("\"");
      sb.append(options.versionColumnName);
      sb.append("\"");

      count++;
    }

    if (options.creationColumnName != null)
    {
      if (count > 0)
        TableCollectionImpl.addComma(sb, count);

      sb.append('"');
      sb.append(options.creationColumnName);
      sb.append('"');
      count++;
    }

    // For an anonymous block, add a flag column that is never NULL
    if (collection.useCallableReturns)
    {
      if (count > 0)
        TableCollectionImpl.addComma(sb, count);
      sb.append("'1'");
      count++;
    }

    TableCollectionImpl.addInto(sb, count);
  }

  private void generateRemove(StringBuilder sb)
  {
    sb.append("delete");
    if (hints != null)
      sb.append(" /*+ " + hints + " */");
    sb.append(" from ");
    appendTable(sb);
  }
}
