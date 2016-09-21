/* Copyright (c) 2014, 2016, Oracle and/or its affiliates. 
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

import java.sql.SQLException;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Connection;

import java.util.ArrayList;
import java.util.List;

public final class SODAUtils
{
  public enum SQLSyntaxLevel {SQL_SYNTAX_UNKNOWN,
                              SQL_SYNTAX_12_1,
                              SQL_SYNTAX_12_2,
                              SQL_SYNTAX_13_1};

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

  /**
   * Return true if the SQL syntax is known to be below the 12.2 level.
   * If unknown, or if known to be 12.2 or higher, returns false.
   * This routine may be used to exclude features that can't be supported
   * on older database labels.
   */
  public static boolean sqlSyntaxBelow_12_2(SQLSyntaxLevel sqlSyntaxLevel)
  {
    return (sqlSyntaxLevel == SODAUtils.SQLSyntaxLevel.SQL_SYNTAX_12_1);
  }

  public static SQLSyntaxLevel getDatabaseVersion(Connection conn)
    throws SQLException
  {
    DatabaseMetaData dbmd = (conn == null) ? null : conn.getMetaData();
    if (dbmd == null) return SQLSyntaxLevel.SQL_SYNTAX_UNKNOWN;

    int dbMajor = dbmd.getDatabaseMajorVersion();
    int dbMinor = dbmd.getDatabaseMinorVersion();

    if (dbMajor > 12)
    {
      return SQLSyntaxLevel.SQL_SYNTAX_13_1;
    }
    else if (dbMajor > 11)
    {
      if (dbMinor > 1)
        return SQLSyntaxLevel.SQL_SYNTAX_12_2;
      else
        return SQLSyntaxLevel.SQL_SYNTAX_12_1;
    }

    return SQLSyntaxLevel.SQL_SYNTAX_UNKNOWN;
  }
}
