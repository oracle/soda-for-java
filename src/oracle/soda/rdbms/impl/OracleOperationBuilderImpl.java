/* Copyright (c) 2014, 2017, Oracle and/or its affiliates. 
All rights reserved.*/

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

import java.io.IOException;

import oracle.json.common.MetricsCollector;
import oracle.json.logging.OracleLog;
import oracle.json.util.JsonByteArray;
import oracle.json.parser.AndORTree;
import oracle.json.parser.QueryException;
import oracle.json.parser.Predicate;
import oracle.json.parser.ValueTypePair;
import oracle.json.parser.JsonQueryPath;

import oracle.soda.OracleOperationBuilder;
import oracle.soda.OracleDocument;
import oracle.soda.OracleException;
import oracle.soda.OracleCursor;

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.logging.Logger;

import oracle.jdbc.OracleConnection;
import oracle.jdbc.OraclePreparedStatement;

/**
 * OracleOperationBuilderImpl
 */
public class OracleOperationBuilderImpl implements OracleOperationBuilder
{
  private static final int MAX_NUM_OF_KEYS = 1000;

  private static final Logger log =
    Logger.getLogger(OracleOperationBuilderImpl.class.getName());

  private String key;

  // If isStartKey is true, the value of the key
  // will be stored in the 'key' variable above
  private boolean isStartKey;
  private boolean ascending;
  private boolean startKeyInclusive;

  Set<String> keys;

  private String since;
  private String until;
  private boolean timeRangeInclusive;

  private OracleDocument filterSpec;
  private AndORTree tree;

  private String version;

  private String lastModified;

  private boolean lock;

  private int limit;

  private long skip;

  boolean headerOnly;

  private int firstRows;

  private final OracleCollectionImpl collection;

  private final MetricsCollector metrics;

  private final OracleConnection connection;

  private final CollectionDescriptor options;

  private enum Terminal { COUNT,
                          GET_ONE,
                          GET_CURSOR,
                          REMOVE,
                          REPLACE_ONE_AND_GET,
                          REPLACE_ONE,
                          // Not part of a public API
                          EXPLAIN_PLAN
                        };

  // Stores the value of the new version (unless it's sequential,
  // or none) computed in bindUpdate for a replace operation.
  // Sequential or none come from the database, and thus
  // need a returning clause.
  private String computedVersion;

  private static final String NULL = "null";

  private static final boolean PAGINATION_WORKAROUND = Boolean.valueOf(
    System.getProperty("oracle.soda.rdbms.paginationWorkaround", "true"));

  OracleOperationBuilderImpl(OracleCollectionImpl collection,
                             OracleConnection connection)
  {
    this.collection = collection;

    options = collection.getOptions();

    this.connection = connection;

    ascending = true;

    startKeyInclusive = true;

    timeRangeInclusive = true;

    firstRows = -1;

    metrics = collection.getMetrics();
  }

  // If true, this flag requests that the returned cursor
  // provide the SQL statement that produced the result set.
  private boolean return_query = false;
  private JsonByteArray return_sql_json = null;

  /**
   * Not part of a public API.
   *
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

  private void recordQueryBind(int pos, ValueTypePair item)
  {
    return_sql_json.appendComma();
    return_sql_json.appendValue("B"+Integer.toString(pos));
    return_sql_json.appendColon();

    switch (item.getType())
    {
    case ValueTypePair.TYPE_NUMBER:
      return_sql_json.append(item.getNumberValue().toString());
      break;

    case ValueTypePair.TYPE_STRING:
      return_sql_json.appendValue(item.getStringValue());
      break;

    case ValueTypePair.TYPE_BOOLEAN:
      return_sql_json.append((item.getBooleanValue()) ? "true" : "false");
      break;

    case ValueTypePair.TYPE_NULL:
      // FALLTHROUGH
    default:
      return_sql_json.append("null");
    }
  }

  private void recordQueryKey(int pos, String key)
  {
    return_sql_json.appendComma();
    return_sql_json.appendValue("key_"+Integer.toString(pos));
    return_sql_json.appendColon();
    if (key == null)
      return_sql_json.append("null");
    else
      return_sql_json.appendValue(key);
  }

  public OracleOperationBuilder key(String key) throws OracleException
  {
    this.key = key;

    if (key == null)
    {
      throw SODAUtils.makeException(SODAMessage.EX_ARG_CANNOT_BE_NULL,
                                    "key");
    }

    maxNumberOfKeysCheck();

    // Override keys() and startKey() settings
    keys = null;

    isStartKey = false;

    ascending = true;

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
    this.keys = new HashSet<String>(keys);

    maxNumberOfKeysCheck();

    // Override key() and startKey() settings
    key = null;

    isStartKey = false;

    ascending = true;

    startKeyInclusive = true;

    return this;
  }

  /* Not part of a public API */
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

    key = startKey;

    if (ascending != null)
    {
      this.ascending = ascending;
    }
    else
    {
      this.ascending = true;
    }

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

  public OracleOperationBuilder filter(OracleDocument filterSpec)
    throws OracleException
  {
    if (filterSpec == null)
    {
      throw SODAUtils.makeException(SODAMessage.EX_ARG_CANNOT_BE_NULL,
                                    "filterSpec");
    }

    if (collection.admin().isHeterogeneous())
    {
      throw SODAUtils.makeException(SODAMessage.EX_NO_QBE_ON_HETERO_COLLECTIONS);
    }

    try
    {
      tree = AndORTree.createTree(((OracleDocumentImpl) filterSpec).getContentAsStream());
      tree.generateJsonExists();
    }
    catch (QueryException e)
    {
      if (OracleLog.isLoggingEnabled())
        log.warning(e.toString());
      throw SODAUtils.makeException(SODAMessage.EX_INVALID_FILTER, e);
    }
    
    this.filterSpec = filterSpec;

    maxNumberOfKeysCheck();

    return this;
  }

  // Checks that the maximum number of specified keys is
  // not greater than MAX_NUM_OF_KEYS.
  // Note that this is called from filter(), keys(), and key()
  // as opposed to generateOperation() in
  // order to fail early.
  private void maxNumberOfKeysCheck() throws OracleException
  {
    int totalNumOfKeys = 0;

    if (filterSpec != null)
    {
      totalNumOfKeys += tree.getKeys().size();
    }

    if (keys != null)
    {
      totalNumOfKeys += keys.size();
    }
    else if (key != null && !isStartKey)
    {
      totalNumOfKeys++;
    }

    if (totalNumOfKeys > MAX_NUM_OF_KEYS)
    {
      throw SODAUtils.makeException(SODAMessage.EX_MAX_NUM_OF_KEYS_EXCEEDED,
                                    totalNumOfKeys,
                                    MAX_NUM_OF_KEYS);
    }
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
    PreparedStatement stmt = operation.getPreparedStatement();
    ResultSet resultSet = null;

    try
    {
      resultSet = stmt.executeQuery();
      stmt = null;
    }
    catch (SQLException e)
    {
      if (OracleLog.isLoggingEnabled())
      {
        log.severe(e.toString());
        log.severe(operation.getSqlText());
      }
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

    return resultSet;
  }

  public OracleCursor getCursor() throws OracleException
  {
    Operation operation = generateOperation(Terminal.GET_CURSOR);
    ResultSet resultSet = getResultSet(operation);

    long prepAndExecTime = metrics.endTiming();

    OracleCursorImpl cursor =
      new OracleCursorImpl(options, metrics, operation, resultSet);

    cursor.setElapsedTime(prepAndExecTime);

    // This seems to be the only place where a return query makes sense
    if (return_query)
    {
      return_sql_json.appendCloseBrace();
      cursor.setQuery(return_sql_json.toArray());
    }

    return(cursor);
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
   * @param terminal             the SODA operation
   * @param document             new document for a replace operation,
   *                             <code>null</code> if it's not a replace
   *                             operation
   */
  private Operation generateOperation(Terminal terminal,
                                      OracleDocument document)
                                      throws OracleException
  {
    StringBuilder sb = new StringBuilder();

    if (terminal == Terminal.REMOVE)
    {
      return_query = false;
      generateRemove(sb);
    }
    else if (replace(terminal))
    {
      return_query = false;
      generateUpdate(sb);
    }
    else
    {
      if (terminal != Terminal.GET_CURSOR)
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

    int numOfFilterSpecKeys = 0;

    if (filterSpec != null)
    {
      if (tree == null)
      {
        throw new IllegalStateException();
      }

      numOfFilterSpecKeys = getNumberOfFilterSpecKeys();
    }

    generateWhere(sb);

    boolean filterSpecOrderByPresent = false;

    if (!countOrWrite(terminal))
    {
      if (hasFilterSpecOrderBy())
      {
        generateFilterSpecOrderBy(sb, tree);
        filterSpecOrderByPresent = true;
      }

      if (!paginationWorkaround(terminal))
      {
        generateOrderBy(sb, filterSpecOrderByPresent);
        generateOffsetAndFetchNext(sb);
      }
    }

    if (returningClause(terminal))
    {
      generateReturning(sb);
    }

    PreparedStatement stmt = null;

    String sqltext = sb.toString();

    try
    {
      if (return_query)
        beginQueryRecord(sqltext);

      if (OracleLog.isLoggingEnabled())
        log.fine("Query:\n" + sqltext);

      metrics.startTiming();

      stmt = connection.prepareStatement(sqltext);

      int parameterIndex = 0;

      // Binds new values for a replace statement
      // (e.g. new content, version, etc).
      if (replace(terminal))
      {
        parameterIndex = bindUpdate(stmt, document);
      }

      Iterator<String> keysIter = null;

      if (key != null)
      {
        String canonicalKey = collection.canonicalKey(key);

        ((TableCollectionImpl)collection).bindKeyColumn(stmt,
                                                        ++parameterIndex,
                                                        canonicalKey);
        if (return_query)
          recordNamedBind("key", canonicalKey);
      }
      else if (keys != null)
      {
        keysIter = keys.iterator();
        bindKeys(keysIter, stmt, keys.size(), parameterIndex);

        parameterIndex += keys.size();
      }

      // Bind $id part of the filterSpec
      if (numOfFilterSpecKeys > 0)
      {
        HashSet<String> keysFromFilterSpec = tree.getKeys();
        bindKeys(keysFromFilterSpec.iterator(),
                 stmt,
                 keysFromFilterSpec.size(),
                 parameterIndex);
        parameterIndex += keysFromFilterSpec.size();
      }

      parameterIndex = setStartAndEndTime(stmt, parameterIndex);
      parameterIndex = setVersionAndLastModified(stmt, parameterIndex);

      if (filterSpec != null)
      {
        // Bind JSON exists part of the filterSpec
        parameterIndex = bindJsonExists(stmt, tree, parameterIndex);
      }

      if (returningClause(terminal))
      {
        bindReturning(stmt, parameterIndex);
      }

      // If it's not a count() or a write operation,
      // and if it's not a single key based operation,
      // set fetch size and lob prefetch size.
      boolean isSingleKey = (key != null && !isStartKey);

      if (!countOrWrite(terminal) && !isSingleKey)
      {
        stmt.setFetchSize(SODAConstants.BATCH_FETCH_SIZE);
        ((OraclePreparedStatement)stmt)
                .setLobPrefetchSize(SODAConstants.LOB_PREFETCH_SIZE);
      }

      Operation operation = null;
      boolean isFilterSpec = (filterSpec != null);

      operation = new Operation(stmt,
                                sqltext,
                                headerOnly,
                                isFilterSpec,
                                isSingleKey,
                                collection);

      stmt = null;

      return operation;
    }
    catch (SQLException e)
    {
      if (OracleLog.isLoggingEnabled())
        log.severe(e.toString());
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

  private int bindJsonExists(PreparedStatement stmt,
                             AndORTree tree,
                             int parameterIndex)
                             throws SQLException
  {
    int count = 0;
    for (ValueTypePair item: tree.getValueArray())
    {
      ++parameterIndex;

      if (return_query)
        recordQueryBind(count++, item);

      switch (item.getType())
      {
        case ValueTypePair.TYPE_NUMBER:
          stmt.setBigDecimal(parameterIndex, item.getNumberValue());
          break;

        case ValueTypePair.TYPE_STRING:
          stmt.setString(parameterIndex, item.getStringValue());
          break;

        case ValueTypePair.TYPE_BOOLEAN:
          stmt.setString(parameterIndex,
                         String.valueOf(item.getBooleanValue()));
          break;

        case ValueTypePair.TYPE_NULL:
          stmt.setString(parameterIndex, NULL);
          break;

        default:
          throw new IllegalStateException();
      }
    }

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

  int bindUpdate(PreparedStatement stmt, OracleDocument document)
    throws SQLException, OracleException
  {
    int num = 0;

    byte[] dataBytes  = OracleCollectionImpl.EMPTY_DATA;

    boolean materializeContent = true;

    if (!collection.payloadBasedVersioning() &&
         collection.admin().isHeterogeneous() &&
         ((OracleDocumentImpl) document).hasStreamContent())
    {
      // This means it needs to be streamed without materializing
      ((TableCollectionImpl) collection).setStreamBind(stmt, document, ++num);

      materializeContent = false;
    }
    else
    {
      // This means we need to materialize the payload
      dataBytes = ((TableCollectionImpl) collection).bindPayloadColumn(stmt,
              ++num,
              document);
    }

    if ((options.versionColumnName != null) &&
        (options.versioningMethod) != CollectionDescriptor.VERSION_NONE)
    {
      switch (options.versioningMethod)
      {
        case CollectionDescriptor.VERSION_SEQUENTIAL:
          // Assumes RETURNING clause
          break;
        case CollectionDescriptor.VERSION_TIMESTAMP:
          long lstamp = collection.getDatabase().getDatabaseTime();
          stmt.setLong(++num, lstamp);
          computedVersion = Long.toString(lstamp);
          break;
        case CollectionDescriptor.VERSION_UUID:
          computedVersion = collection.getDatabase().generateKey();
          stmt.setString(++num, computedVersion);
          break;
        default: /* Hashes */
          if (!materializeContent)
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

    num = ((TableCollectionImpl)collection).bindMediaTypeColumn(stmt,
                                                                num,
                                                                document);
    return num;
  }

  void bindReturning(PreparedStatement stmt, int parameterIndex)
    throws SQLException
  {
    OraclePreparedStatement ostmt = (OraclePreparedStatement)stmt;

    if (options.timestampColumnName != null)
    {
      ostmt.registerReturnParameter(++parameterIndex, Types.VARCHAR);
    }

    if (options.versionColumnName != null &&
        (options.versioningMethod == CollectionDescriptor.VERSION_NONE ||
         options.versioningMethod == CollectionDescriptor.VERSION_SEQUENTIAL))
    {
      ostmt.registerReturnParameter(++parameterIndex, Types.VARCHAR);
    }

    if (options.creationColumnName != null)
    {
      ostmt.registerReturnParameter(++parameterIndex, Types.VARCHAR);
    }
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
      ((TableCollectionImpl)collection).bindKeyColumn(stmt, ++numBinds, currentKey);
    }
  }

  private Operation createReplaceStatement(Terminal terminal,
                                           OracleDocument document)
    throws OracleException
  {
    if ((key == null) || isStartKey)
    {
      throw SODAUtils.makeException(SODAMessage.EX_KEY_MUST_BE_SPECIFIED);
    }

    if (document == null)
    {
      throw SODAUtils.makeException(SODAMessage.EX_ARG_CANNOT_BE_NULL,
                                    "document");
    }

    computedVersion = null;

    return(generateOperation(terminal, document));
  }

  public OracleDocument replaceOneAndGet(OracleDocument document)
    throws OracleException
  {
    collection.writeCheck("replaceOneAndGet");

    Operation operation = createReplaceStatement(Terminal.REPLACE_ONE_AND_GET,
                                                 document);
    PreparedStatement stmt = operation.getPreparedStatement();
    OraclePreparedStatement ostmt = (OraclePreparedStatement)stmt;

    ResultSet rows = null;
    String tstamp = null;
    String createdOn = null;

    int count = 0;
    OracleDocumentImpl result = null;

    try
    {
      count = ostmt.executeUpdate();

      if (count == 0)
      {
        return null;
      }

      if (returningClause(Terminal.REPLACE_ONE_AND_GET) &&
          (options.timestampColumnName != null ||
           ((TableCollectionImpl)collection).returnVersion() ||
           options.creationColumnName != null))
      {
        // Oracle-specific RETURNING clause
        rows = ostmt.getReturnResultSet();
        if (rows.next())
        {
          int onum = 0;

          if (options.timestampColumnName != null)
          {
            tstamp = OracleDatabaseImpl.getTimestamp(rows.getString(++onum));
          }

          if (((TableCollectionImpl)collection).returnVersion())
          {
            computedVersion = rows.getString(++onum);
          }

          if (options.creationColumnName != null)
          {
            createdOn = OracleDatabaseImpl.getTimestamp(rows.getString(++onum));
          }
        }
      }

      if (rows != null)
        rows.close();
      rows = null;

      stmt.close();
      stmt = null;

      metrics.recordWrites(1,1);

      if (count == 1)
      {
        result = new OracleDocumentImpl(collection.canonicalKey(key),
                                        computedVersion,
                                        tstamp);
        result.setCreatedOn(createdOn);
        String ctype = document.getMediaType();

        ((TableCollectionImpl)collection).setContentType(ctype,
                                                         result);
      }

    }
    catch (SQLException e)
    {
      if (OracleLog.isLoggingEnabled())
        log.severe(e.toString());
      throw SODAUtils.makeExceptionWithSQLText(e, operation.getSqlText());
    }
    finally
    {
      for (String message : SODAUtils.closeCursor(stmt, rows))
      {
        if (OracleLog.isLoggingEnabled())
          log.severe(message);
      }
    }

    return result;
  }

  public boolean replaceOne(OracleDocument document)
    throws OracleException
  {
    collection.writeCheck("replaceOne");

    Operation operation = createReplaceStatement(Terminal.REPLACE_ONE,
                                                 document);
    PreparedStatement stmt = operation.getPreparedStatement();

    boolean success = false;

    try
    {
      if (stmt.executeUpdate() == 1)
      {
        success = true;
      }

      stmt.close();
      stmt = null;

      metrics.recordWrites(1,1);
    }
    catch (SQLException e)
    {
      if (OracleLog.isLoggingEnabled())
        log.severe(e.toString());
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

    return success;
  }

  public int remove() throws OracleException
  {
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
        log.severe(e.toString());
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

  public OracleOperationBuilder limit(int limit) throws OracleException
  {
    if (limit < 1)
    {
      throw SODAUtils.makeException(SODAMessage.EX_ARG_MUST_BE_POSITIVE,
                                    "limit");
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

    this.skip = skip;

    return this;
  }

  public OracleOperationBuilder headerOnly()
  {
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
  // ### It would be better to provide the plan via logging instead.
  //     Remove this method once that's done.
  public String explainPlan(String level) throws OracleException
  {
    Operation operation = generateOperation(Terminal.EXPLAIN_PLAN);
    ResultSet res = getResultSet(operation);

    Statement stmt = null;

    StringBuilder planOutput = new StringBuilder();

    try
    {
      // First, close the 'explain plan...' query result.
      res.close();
      res = null;

      stmt = connection.createStatement();
      if (level.equalsIgnoreCase("all"))
      {
        level = "all";
      }
      else if (level.equalsIgnoreCase("typical"))
      {
        level = "typical";
      }
      else
      {
        level = "basic";
      }

      res = stmt.executeQuery(
            "select plan_table_output from " +
            "table(dbms_xplan.display('plan_table', null, '" +
             level +
             "'))");

      while (res.next())
      {
        planOutput.append(res.getString(1));
        planOutput.append("\n");
      }

      res.close();
      res = null;

      stmt.close();
      stmt = null;

      return planOutput.toString();
    }
    catch (SQLException e)
    {
      if (OracleLog.isLoggingEnabled())
        log.severe(e.toString());
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
        log.severe(e.toString());
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
    Operation operation = generateOperation(Terminal.GET_ONE);
    ResultSet resultSet = getResultSet(operation);

    long prepAndExecTime = metrics.endTiming();

    OracleCursorImpl cursor =
      new OracleCursorImpl(options, metrics, operation, resultSet);

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
    if (tAlias != null)
    {
      sb.append(tAlias);
      sb.append(".");
    }

    sb.append("\"");
    sb.append(colName);
    sb.append("\"");
  }

  private void generateFilterSpecJsonExists(StringBuilder sb,
                                            AndORTree tree)
  {
    sb.append("JSON_EXISTS(");
    appendColumn(sb, options.contentColumnName);

    ((TableCollectionImpl)collection).addFormat(sb);

    sb.append(",");
    tree.appendJsonExists(sb);

    sb.append(")");
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

  private void generateWhere(StringBuilder sb)
  {
    boolean append = false;
    int numOfFilterSpecKeys = getNumberOfFilterSpecKeys();

    if (whereClauseRequired())
    {
      sb.append(" where ");
    }
    else
    {
      return;
    }

    if (key != null)
    {
      sb.append("(");
      appendColumn(sb, options.keyColumnName);

      if (!isStartKey)
      {
        sb.append(" = ");
      }
      else
      {
        sb.append(ascending ? " >" : " <");
        sb.append(startKeyInclusive ? "= " : " ");
      }

      ((TableCollectionImpl)collection).addKey(sb);

      append = true;
    }
    else if (keys != null)
    {
      sb.append(" (");
      // Build an IN statement with the requested number of positions
      // The bindings will need to be in the native form of the key
      // e.g. setLong() for NUMBER, setBytes() for RAW, or setString()
      appendColumn(sb, options.keyColumnName);
      sb.append(" in (");

      int i = 0;

      while (i < keys.size())
      {
        if (++i == keys.size())
          sb.append("?)");
        else
          sb.append("?,");
      }

      append = true;
    }

    if (numOfFilterSpecKeys > 0)
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

      appendColumn(sb, options.keyColumnName);
      sb.append(" in (");

      int i = 0;

      while (i < numOfFilterSpecKeys)
      {
        if (++i == numOfFilterSpecKeys)
          sb.append("?)");
        else
          sb.append("?,");
      }

      addCloseParenthesis(sb);
      append = true;
    }
    else if (append)
    {
      addCloseParenthesis(sb);
    }

    if (since != null || until != null)
    {
      addAnd(sb, append);
      
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
      addAnd(sb, append);
      
      sb.append(" ( ");
      appendColumn(sb, options.versionColumnName);
      sb.append(" = ");
      switch (options.versioningMethod)
      {
        case CollectionDescriptor.VERSION_SEQUENTIAL:
        case CollectionDescriptor.VERSION_TIMESTAMP:
          sb.append("to_number(?)");
          break;
        default:
          sb.append("?");
          break;
      }
      addCloseParenthesis(sb);
      append = true;
    }
    
    if (lastModified != null)
    {
      addAnd(sb, append);

      sb.append(" ( ");
      
      appendColumn(sb, options.timestampColumnName);
      OracleDatabaseImpl.addToTimestamp(" = ", sb);
      
      addCloseParenthesis(sb);
      append = true;
    }

    if (filterSpec != null && tree.hasJsonExists())
    {
      addAnd(sb, append);
      generateFilterSpecJsonExists(sb, tree);
    }
  }

  private boolean whereClauseRequired()
  {
    if (key != null ||
        keys != null ||
        since != null ||
        until != null ||
        version != null ||
        lastModified != null ||
        (filterSpec != null &&
         (tree.hasJsonExists() ||
          tree.hasKeys()))
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

  private boolean countOrWrite(Terminal terminal)
  {
    if (terminal == Terminal.COUNT ||
        terminal == Terminal.REMOVE ||
        replace(terminal))
    {
      return true;
    }

    return false;
  }

  private boolean returningClause(Terminal terminal)
  {
    boolean disableReturning = collection.internalDriver;

    if (terminal == Terminal.REPLACE_ONE_AND_GET && !disableReturning)
    {
      return true;
    }

    return false;
  }

  private boolean replace(Terminal terminal)
  {
    if (terminal == Terminal.REPLACE_ONE_AND_GET ||
        terminal == Terminal.REPLACE_ONE)
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
            (skip > 0L || limit > 0) &&
            (terminal == Terminal.GET_CURSOR ||
             terminal == Terminal.GET_ONE ||
             terminal == Terminal.EXPLAIN_PLAN) &&
             !whereClauseRequired() &&
             !hasFilterSpecOrderBy());
  }

  private void addAnd(StringBuilder sb, boolean append)
  {
    if (append)
    {
      sb.append(" and ");
    }
  }
  
  private void addCloseParenthesis(StringBuilder sb)
  {
    sb.append(" ) ");
  }

  private void generateFilterSpecOrderBy(StringBuilder sb,
                                         AndORTree tree)
    throws OracleException
  {
    ArrayList<Predicate> orderByArray = tree.getOrderByArray();
    Predicate entry = null;
    for (int i = 0; i < orderByArray.size(); i++)
    {
      entry = orderByArray.get(i);
      if (i == 0)
        sb.append(" order by");
      else
        sb.append(",");
      sb.append(" JSON_VALUE(");
      appendColumn(sb, options.contentColumnName);

      ((TableCollectionImpl)collection).addFormat(sb);

      JsonQueryPath qpath = entry.getQueryPath();

      if (qpath.hasArraySteps())
      {
        throw SODAUtils.makeException(SODAMessage.EX_ARRAY_STEPS_IN_PATH);
      }

      sb.append(", '");
      qpath.toSingletonString(sb);
      sb.append("'");

      String returnType = entry.getReturnType();
      if (returnType != null)
      {
        sb.append(" returning ");
        sb.append(returnType);
      }
      sb.append(")");

      if (entry.getValue().equals("1"))
        sb.append(" asc");
      else
        sb.append(" desc");
    }

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
    else if (skip > 0L || limit > 0)
    {
      if (!hasFilterSpecOrderBy)
      {
        sb.append(" order by ");
      }
      else
      {
        sb.append(", ");
      }

      appendColumn(sb, options.keyColumnName);
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
    sb.append("select ");
    sb.append(" /*+ LEADING(" + tab1Alias + ") ");
    sb.append("USE_NL(" + tab2Alias + ") */ ");
    appendTableColumns(sb, tab2Alias);

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

    if (firstRows >= 0)
    {
      sb.append(" /*+ FIRST_ROWS(");
      sb.append(firstRows);
      sb.append(") */ ");
    }

    if (terminal == Terminal.COUNT)
    {
      // Count over the key column is a full index scan (not a table scan)
      sb.append(" count(\"");
      sb.append(options.keyColumnName);
      sb.append("\")");
    }
    else
    {
      appendTableColumns(sb, null);
    }

    sb.append(" from ");

    appendTable(sb);
  }

  private void appendTableColumns(StringBuilder sb,
                                  String tAlias)
  {
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

    if (!headerOnly)
    {
      sb.append(",");
      appendAliasedColumn(sb, options.contentColumnName, tAlias);
    }

    if (options.timestampColumnName != null)
    {
      sb.append(",to_char(");
      appendAliasedColumn(sb, options.timestampColumnName, tAlias);
      OracleDatabaseImpl.addTimestampSelectFormat(sb);
    }

    if (options.creationColumnName != null)
    {
      sb.append(",to_char(");
      appendAliasedColumn(sb, options.creationColumnName, tAlias);
      OracleDatabaseImpl.addTimestampSelectFormat(sb);
    }

    if (options.versionColumnName != null)
    {
      sb.append(",");
      appendAliasedColumn(sb, options.versionColumnName, tAlias);
    }
  }

  private void generateUpdate(StringBuilder sb)
  {
    sb.append("update ");
    appendTable(sb);
    sb.append(" set \"");
    sb.append(options.contentColumnName);
    sb.append("\" = ?");

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
      sb.append("\" = ?");
    }
  }

  private void generateReturning(StringBuilder sb)
  {
    int count = 0;

    if ((options.timestampColumnName == null) &&
        (options.creationColumnName == null) &&
        (!((TableCollectionImpl)collection).returnVersion()))
      return; // Nothing to do

    sb.append(" returning ");

    if (options.timestampColumnName != null)
    {
      sb.append("to_char(\"");
      sb.append(options.timestampColumnName);
      sb.append('"');
      OracleDatabaseImpl.addTimestampReturningFormat(sb);

      count++;
    }

    if (((TableCollectionImpl)collection).returnVersion())
    {
      ((TableCollectionImpl)collection).addComma(sb, count);

      sb.append("\"");
      sb.append(options.versionColumnName);
      sb.append("\"");

      count++;
    }

    if (options.creationColumnName != null)
    {
      ((TableCollectionImpl)collection).addComma(sb, count);

      sb.append("to_char(\"");
      sb.append(options.creationColumnName);
      sb.append('"');
      OracleDatabaseImpl.addTimestampReturningFormat(sb);

      count++;
    }

    ((TableCollectionImpl)collection).addInto(sb, count);
  }

  private void generateRemove(StringBuilder sb)
  {
    sb.append("delete from ");
    appendTable(sb);
  }
}
