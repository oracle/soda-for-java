/* Copyright (c) 2014, 2015, Oracle and/or its affiliates. 
All rights reserved.*/

/*
   DESCRIPTION
    
    SODA Utilities
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

import oracle.soda.OracleBatchException;
import oracle.soda.OracleException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.ArrayList;
import java.util.List;

public final class SODAUtils
{
  // Not part of a public API.
  // ### This is only exposed for OracleRDBMSClient.
  public static OracleException makeException(SODAMessage msg,
                                              Object... params)
  {
    return new OracleException(msg.get(params), msg.getKey());
  }

  static OracleException makeException(SODAMessage msg,
                                       Throwable cause,
                                       Object... params)
  {
    return new OracleException(msg.get(params), cause, msg.getKey());
  }

  static OracleBatchException makeBatchException(SODAMessage msg,
                                                 int processedCount,
                                                 Object... params)
  {
    return new OracleBatchException(msg.get(params),
                                    msg.getKey(),
                                    processedCount);
  }
  static OracleException makeExceptionWithSQLText(Throwable cause,
                                                  String sqlText)
  {
    return makeExceptionWithSQLText(null, cause, sqlText);
  }

  static OracleException makeExceptionWithSQLText(SODAMessage msg,
                                                  Throwable cause,
                                                  String sqlText,
                                                  Object... params)
  {
    class OracleSQLException extends OracleException implements SQLTextCarrier
    {
      String sqlText;

      public OracleSQLException(String message,
                                Throwable cause,
                                int errorCode,
                                String sqlText)
      {
        super(message, cause, errorCode);
        this.sqlText = sqlText;
      }

      public OracleSQLException(Throwable cause, String sqlText)

      {
         super(cause);
         this.sqlText = sqlText;
      }

      public String getSQL()
      {
         return sqlText;
      }
    }

    if (msg != null)
      return new OracleSQLException(msg.get(params), cause, msg.getKey(), sqlText);

    return new OracleSQLException(cause, sqlText);
  };

  static OracleBatchException makeBatchExceptionWithSQLText(Throwable cause,
                                                            int count,
                                                            String sqlText)
  {
    class OracleBatchSQLException extends OracleBatchException implements SQLTextCarrier
    {
      String sqlText;

      public OracleBatchSQLException(Throwable cause, int count, String sqlText)
      {
        super(cause, count);
        this.sqlText = sqlText;
      }

      public String getSQL()
      {
        return sqlText;
      }
    }

    return new OracleBatchSQLException(cause, count, sqlText);
  };

  public static List<String> closeCursor(Statement stmt, ResultSet rows)
  {
    List<String> exceptions = new ArrayList<String>();

    try
    {
      if (rows != null) rows.close();
    }
    catch (SQLException e)
    {
      // During hard-close it's acceptable to just
      // log the message.
      //
      // Save the message with the intention of logging
      // it from the caller.
      exceptions.add(e.getMessage());
    }
    try
    {
      if (stmt != null) stmt.close();
    }
    catch (SQLException e)
    {
      // During hard-close it's acceptable to just
      // log the message.
      //
      // Save the message with the intention of logging
      // it from the caller.
      exceptions.add(e.getMessage());
    }

    return exceptions;
  }

}
