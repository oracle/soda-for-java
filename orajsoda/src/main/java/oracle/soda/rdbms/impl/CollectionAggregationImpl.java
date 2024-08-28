/* Copyright (c) 2014, 2024, Oracle and/or its affiliates. */
/* All rights reserved.*/

/*
   DESCRIPTION
    This is a help class for OracleCollectionImpl. It helps with
    certain aggregation operations against a collection.
 */

/**
 *  This class is not part of the public API, and is
 *  subject to change.
 *
 *  Do not rely on it in your application code.
 *
 *  @author  Doug McMahon
 *  @author  Max Orgiyan
 *  @author  Rahul Kadwe
 */

package oracle.soda.rdbms.impl;

import jakarta.json.JsonObject;
import java.math.BigDecimal;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.logging.Logger;

import oracle.json.common.DistinctValue;
import oracle.json.logging.OracleLog;
import oracle.json.parser.AndORTree;
import oracle.json.parser.JsonQueryPath;
import oracle.json.parser.QueryException;
import oracle.json.parser.ValueTypePair;
import oracle.json.util.Pair;
import oracle.soda.OracleDocument;
import oracle.soda.OracleException;
import oracle.sql.json.OracleJsonValue;

public class CollectionAggregationImpl extends TableCollectionImpl
{
  protected static final Logger log =
    Logger.getLogger(CollectionAggregationImpl.class.getName());
  public CollectionAggregationImpl(OracleCollectionImpl src) {
    super(src.db, src.collectionName, src.options);
  }

  /**
   * Count the number of distinct values for a path. Expands array elements
   * if necessary. Returns the results in a list.
   * This method is is not part of the public API.
   * ### Should this be on the operation builder instead?
   * ### If so, how does it interact with all the other possible
   * ### settings? And what is the return type for the terminal?
   */

  public interface CloseableIterator<T> extends Iterator<T>, AutoCloseable {
  }

  enum CloseableIteratorState {
    /**
     * Indicates the last value has been consumed by next().
     * iter.hasNext() will return false, iter.next() will return null
     */
    DONE,

    /**
     * This either means that the iterator is newly constructed
     * we have returned a current item from iter.next() but not yet
     * called resultSet.next() to see if there is another item.
     */
    BEFORE,

    /**
     * Means that rs.next() has been called and returned true
     * but the corresponding value has not been returned
     * by iter.next().
     */
    ON
  };

  public <T> CloseableIterator<T> distinctStrict(String path, OracleDocument filterSpec, Class<T> returnType)
          throws OracleException
  {
    if (!getStrictMode()) {
      throw SODAUtils.makeException(SODAMessage.EX_23DB_AND_JSON_TYPE_REQUIRED);
    }

    PreparedStatement stmt     = null;
    ResultSet         rows     = null;
    JsonObject        jsonObj  = null;
    JsonQueryPath     jqpath;

    try
    {
      jqpath = new JsonQueryPath(path);
    }
    catch (QueryException e)
    {
      throw SODAUtils.makeException(SODAMessage.EX_NULL_PATH, e);
    }

    if (jqpath.hasArraySteps())
      throw SODAUtils.makeException(SODAMessage.EX_PATH_CONTAINS_ARRAY_STEP);

    sb.setLength(0);
    sb.append("select JT.value");
    sb.append("\nfrom \"");
    sb.append(options.dbSchema);
    sb.append("\".\"");
    sb.append(options.dbObjectName);
    sb.append("\" T1, JSON_TABLE(T1.\"");
    sb.append(options.contentColumnName);
    sb.append("\", '");
    jqpath.toLaxString(sb, true);
    sb.append("' columns ( value json path '$')) JT \n");

    AndORTree tree = null;

    if (filterSpec != null)
    {
      try
      {
        jsonObj = filterSpec.getContentAs(JsonObject.class);
        tree = AndORTree.createTree(jsonObj, getStrictMode(), options.isDualityView());
        tree.checkStringValues(OracleDatabaseImpl.MAX_STRING_BIND_LENGTH);
        tree.generateJsonExists();
      }
      catch (QueryException e)
      {
        if (OracleLog.isLoggingEnabled())
          log.warning(e.toString());
        throw SODAUtils.makeException(SODAMessage.EX_INVALID_FILTER, e);
      }
      // ### We can't support spatial, fulltext, or SQL/JSON clauses
      if (tree.hasSpatialClause()  ||
          tree.hasContainsClause() ||
          tree.hasSqlJsonClause())
        throw SODAUtils.makeException(SODAMessage.EX_UNSUPPORTED_CLAUSE);
      // ### We also can't support naked lists of keys
      if (tree.getKeys().size() > 0)
        throw SODAUtils.makeException(SODAMessage.EX_ID_CLAUSE_NOT_SUPPORTED);

      sb.append(" where JSON_EXISTS(T1.\"");
      sb.append(options.contentColumnName);
      sb.append("\", ");
      tree.appendJsonExists(sb, isTreatAsAvailable());
      sb.append(")\n");
    }
    sb.append(" group by JT.value\n");

    String sqltext = sb.toString();

    ArrayList<OracleJsonValue> results = new ArrayList<OracleJsonValue>();

    try {
      stmt = conn.prepareStatement(sqltext);

      int parameterIndex = 0;

      if (tree != null) {
        parameterIndex = tree.bind(stmt, 0, null);
      }

      rows = stmt.executeQuery();

      final ResultSet finalRows = rows;
      final PreparedStatement finalStmt = stmt;

      return new CloseableIterator<T>() {

        CloseableIteratorState state = CloseableIteratorState.BEFORE;

        @Override
        public boolean hasNext() {
          try {
            switch (state) {
              case BEFORE: {
                boolean result = finalRows.next();
                if (result) {
                  state = CloseableIteratorState.ON;
                  return true;
                } else {
                  state = CloseableIteratorState.DONE;
                  return false;
                }
              }
              case ON: {
                return true;
              }
              case DONE: {
                return false;
              }
              default:
                throw new IllegalStateException();
            }
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        }

        public T next() {
          try {
            switch (state) {
              case BEFORE: {
                boolean result = finalRows.next();
                if (result) {
                  // state stays as before
                  return finalRows.getObject(1, returnType);
                } else {
                  throw new NoSuchElementException();
                }
              }
              case ON: {
                state = CloseableIteratorState.BEFORE;
                return finalRows.getObject(1, returnType);
              }
              case DONE:
                throw new NoSuchElementException();
              default:
                throw new IllegalStateException();
            }
          }
          catch (SQLException e){
            throw new RuntimeException(e);
          }
        }

        public void close() {
          state = CloseableIteratorState.DONE;
          try {
            finalRows.close();
            finalStmt.close();
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        }
      };
    }
    catch (SQLException e)
    {
      throw SODAUtils.makeExceptionWithSQLText(e, sqltext);
    }
  }


  public List<DistinctValue> distinct(String path, OracleDocument filterSpec)
    throws OracleException
  {
    PreparedStatement stmt     = null;
    ResultSet         rows     = null;
    JsonQueryPath     jqpath;

    try
    {
      jqpath = new JsonQueryPath(path);
    }
    catch (QueryException e)
    {
      throw SODAUtils.makeException(SODAMessage.EX_NULL_PATH, e);
    }

    if (jqpath.hasArraySteps())
      throw SODAUtils.makeException(SODAMessage.EX_PATH_CONTAINS_ARRAY_STEP);

    sb.setLength(0);
    sb.append("select JT.STR#, JT.NUM#, JT.BOOL#,");
    sb.append(" to_char(JT.TS#,'YYYY-MM-DD\"T\"HH24:MI:SS.FF\"Z\"'),");
    sb.append(" count(1)\nfrom \"");
    sb.append(options.dbSchema);
    sb.append("\".\"");
    sb.append(options.dbObjectName);
    sb.append("\" T1, JSON_TABLE(T1.\"");
    sb.append(options.contentColumnName);
    sb.append("\", '");
    jqpath.toLaxString(sb, true);
    sb.append("' columns STR# varchar2(255) path '$',");
    sb.append(" BOOL# varchar2(10) path '$.booleanOnly()',");
    sb.append(" NUM# number path '$.numberOnly()',");
    sb.append(" TS# timestamp path '$') JT\n");

    AndORTree tree = null;

    if (filterSpec != null)
    {
      try
      {
        tree = AndORTree.createTree(getDatabase().getJsonFactoryProvider(),
                                    ((OracleDocumentImpl) filterSpec).getContentAsStream(),
				    options.isDualityView());
        tree.checkStringValues(OracleDatabaseImpl.MAX_STRING_BIND_LENGTH);
        tree.generateJsonExists();
      }
      catch (QueryException e)
      {
        if (OracleLog.isLoggingEnabled())
          log.warning(e.toString());
        throw SODAUtils.makeException(SODAMessage.EX_INVALID_FILTER, e);
      }
      // ### We can't support spatial, fulltext, or SQL/JSON clauses
      if (tree.hasSpatialClause()  ||
          tree.hasContainsClause() ||
          tree.hasSqlJsonClause())
        throw SODAUtils.makeException(SODAMessage.EX_UNSUPPORTED_CLAUSE);
      // ### We also can't support naked lists of keys
      if (tree.getKeys().size() > 0)
        throw SODAUtils.makeException(SODAMessage.EX_ID_CLAUSE_NOT_SUPPORTED);

      sb.append(" where JSON_EXISTS(T1.\"");
      sb.append(options.contentColumnName);
      sb.append("\", ");
      tree.appendJsonExists(sb, isTreatAsAvailable());
      sb.append(")\n");
    }
    sb.append(" group by JT.NUM#, JT.BOOL#, JT.TS#, JT.STR#\n");
    sb.append(" order by JT.NUM#, JT.BOOL#, JT.TS#, JT.STR#");

    String sqltext = sb.toString();

    ArrayList<DistinctValue> results = new ArrayList<DistinctValue>();

    // The underlying SQL cannot produce a truly distinct set of values
    // because of two factors:
    //   (1) strings are mirrored to numbers, and don't have a canonical form
    //   (2) booleans are also expressed as numbers (0 and 1)
    // To get around this, the code relies on the SQL to produce an ordered
    // list so it can look back at the prior result and if necessary merge
    // results with it.
    BigDecimal bufferedNumber = null;
    int        bufferedCount  = 0;
    String     bufferedTstamp = null;

    try
    {
      stmt = conn.prepareStatement(sqltext);

      if (tree != null)
      {
        // ### Most of this logic copied from OperationBuilderImpl:
        int parameterIndex = 0;
        for (ValueTypePair item : tree.getValueArray())
        {
          ++parameterIndex;

          switch (item.getValue().getValueType())
          {
          case NUMBER:
            stmt.setBigDecimal(parameterIndex, item.getNumberValue());
            break;

          case STRING:
            stmt.setString(parameterIndex, item.getStringValue());
            break;

          case TRUE:
          case FALSE:
            stmt.setString(parameterIndex,
                           String.valueOf(item.getBooleanValue()));
            break;

          case NULL:
            stmt.setString(parameterIndex, "null");
            break;

          default:
            throw new IllegalStateException();
          }
        }
      }

      rows = stmt.executeQuery();

      while (rows.next())
      {
        int count = rows.getInt(5);

        // Check the timestamp column, if it has a value, this is a date/time
        String sval = rows.getString(4);
        if (sval != null)
        {
          // If there's a previous numeric value, emit it now
          if (bufferedNumber != null)
          {
            results.add(DistinctValue.createNumber(bufferedNumber,
                                                   bufferedCount));
            bufferedNumber = null;
          }

          // If there's a previous timestamp value not yet emitted
          if (bufferedTstamp != null)
          {
            // If it matches this one, merge the count and continue
            if (bufferedTstamp.equals(sval))
            {
              bufferedCount += count;
              continue;
            }
            // Otherwise we can emit the prior value now
            results.add(DistinctValue.createDateTime(bufferedTstamp,
                                                     bufferedCount));
          }

          // Buffer this timestamp value in case there are more matches to it
          bufferedTstamp = sval;
          bufferedCount  = count;

          continue;
        }

        // Check the boolean column
        sval = rows.getString(3);
        // If the string is true or false, then this has to have been
        // a JSON literal mapped as both a string and a 0 or 1.
        if (sval != null)
        {
          if (sval.equals("true"))
          {
            results.add(DistinctValue.createBoolean(true, count));
            continue;
          }
          else if (sval.equals("false"))
          {
            results.add(DistinctValue.createBoolean(false, count));
            continue;
          }
        }

        // Check the number column
        BigDecimal nval = rows.getBigDecimal(2);
        if (nval != null)
        {
          // It must be a number, because we used numberOnly()
          // on the numeric column, excluding strings that would otherwise
          // automatically be mapped as numbers.

          // Classification as int/long/double/decimal is left to the caller

          // If there's a previous timestamp value, emit it now
          if (bufferedTstamp != null)
          {
            results.add(DistinctValue.createDateTime(bufferedTstamp,
                                                     bufferedCount));
            bufferedTstamp = null;
          }

          // If there's a previous numeric value not yet emitted
          if (bufferedNumber != null)
          {
            // If it matches this one, merge the count and continue
            if (bufferedNumber.equals(nval))
            {
              bufferedCount += count;
              continue;
            }
            // Otherwise we can emit the prior value now
            results.add(DistinctValue.createNumber(bufferedNumber,
                                                   bufferedCount));
          }

          // Buffer this numeric value in case there are more matches to it
          bufferedNumber = nval;
          bufferedCount  = count;
          continue;
        }

        //
        // Otherwise it's not a number or date so emit a buffered one
        //
        if (bufferedNumber != null)
        {
          results.add(DistinctValue.createNumber(bufferedNumber,
                                                 bufferedCount));
          bufferedNumber = null;
        }
        else if (bufferedTstamp != null)
        {
          results.add(DistinctValue.createDateTime(bufferedTstamp,
                                                   bufferedCount));
          bufferedTstamp = null;
        }

        // Check the string column
        sval = rows.getString(1);

        // If it's null, it's a JSON null
        // ### Or it might be an empty string and we can't tell
        // ### Unfortunately it might also have been an object or array
        if (sval == null)
          results.add(DistinctValue.createNull(count));
        // Otherwise it's a string
        else
          results.add(DistinctValue.createString(sval, count));
      }

      // Push out any trailing buffered number or date
      if (bufferedNumber != null)
      {
        results.add(DistinctValue.createNumber(bufferedNumber,
                                               bufferedCount));
        bufferedNumber = null;
      }
      else if (bufferedTstamp != null)
      {
        results.add(DistinctValue.createDateTime(bufferedTstamp,
                                                 bufferedCount));
        bufferedTstamp = null;
      }

      rows.close();
      rows = null;
      stmt.close();
      stmt = null;
    }
    catch (SQLException e)
    {
      throw SODAUtils.makeExceptionWithSQLText(e, sqltext);
    }
    finally
    {
      for (String message : SODAUtils.closeCursor(stmt, rows))
      {
        if (OracleLog.isLoggingEnabled())
          log.severe(message);
      }
    }

    return results;
  }

  /**
   * Count the number of rows and also sum up the sizes.
   * This method is is not part of the public API.
   * ### Should this be on the operation builder instead?
   * ### If so, how does it interact with all the other possible
   * ### settings? And what is the return type for the terminal?
   */
  public Pair<Long, Long> summarize(OracleDocument filterSpec)
    throws OracleException
  {
    PreparedStatement stmt     = null;
    ResultSet         rows     = null;
    Pair              result   = null;

    sb.setLength(0);

    sb.append("select count(1) NUM#, ");
    if ((options.contentDataType == CollectionDescriptor.CHAR_CONTENT) ||
        (options.contentDataType == CollectionDescriptor.NCHAR_CONTENT))
      sb.append("sum(lengthb(T1.\"");  // byte length, not character length
    else if ((options.contentDataType == CollectionDescriptor.CLOB_CONTENT) ||
             (options.contentDataType == CollectionDescriptor.NCLOB_CONTENT))
      sb.append("sum(2*length(T1.\""); // assumes UCS2 code point storage
    else
      sb.append("sum(length(T1.\"");   // standard binary storage
    sb.append(options.contentColumnName);
    sb.append("\")) CLEN# from \"");
    sb.append(options.dbSchema);
    sb.append("\".\"");
    sb.append(options.dbObjectName);
    sb.append("\" T1");

    AndORTree tree = null;

    if (filterSpec != null)
    {
      try
      {
        tree = AndORTree.createTree(getDatabase().getJsonFactoryProvider(),
                                    ((OracleDocumentImpl) filterSpec).getContentAsStream(),
				    options.isDualityView());
        tree.checkStringValues(OracleDatabaseImpl.MAX_STRING_BIND_LENGTH);
        tree.generateJsonExists();
      }
      catch (QueryException e)
      {
        if (OracleLog.isLoggingEnabled())
          log.warning(e.toString());
        throw SODAUtils.makeException(SODAMessage.EX_INVALID_FILTER, e);
      }
      // ### We can't support spatial, fulltext, or SQL/JSON clauses
      if (tree.hasSpatialClause()  ||
          tree.hasContainsClause() ||
          tree.hasSqlJsonClause())
        throw SODAUtils.makeException(SODAMessage.EX_UNSUPPORTED_CLAUSE);
      // ### We also can't support naked lists of keys
      if (tree.getKeys().size() > 0)
        throw SODAUtils.makeException(SODAMessage.EX_ID_CLAUSE_NOT_SUPPORTED);

      sb.append(" where JSON_EXISTS(T1.\"");
      sb.append(options.contentColumnName);
      sb.append("\", ");
      tree.appendJsonExists(sb, isTreatAsAvailable());
      sb.append(")\n");
    }

    String sqltext = sb.toString();

    try
    {
      stmt = conn.prepareStatement(sqltext);

      if (tree != null)
      {
        // ### Most of this logic copied from OperationBuilderImpl:
        int parameterIndex = 0;
        for (ValueTypePair item : tree.getValueArray())
        {
          ++parameterIndex;

          switch (item.getValue().getValueType())
          {
          case NUMBER:
            stmt.setBigDecimal(parameterIndex, item.getNumberValue());
            break;

          case STRING:
            stmt.setString(parameterIndex, item.getStringValue());
            break;

          case TRUE:
          case FALSE:
            stmt.setString(parameterIndex,
			   String.valueOf(item.getBooleanValue()));
            break;

          case NULL:
            stmt.setString(parameterIndex, "null");
            break;

          default:
            throw new IllegalStateException();
          }
        }
      }

      rows = stmt.executeQuery();

      if (rows.next())
      {
        Long count     = new Long(rows.getLong(1));
        Long totalSize = new Long(rows.getLong(2));

        result = new Pair<Long, Long>(count, totalSize);
      }

      rows.close();
      rows = null;
      stmt.close();
      stmt = null;
    }
    catch (SQLException e)
    {
      throw SODAUtils.makeExceptionWithSQLText(e, sqltext);
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

}
