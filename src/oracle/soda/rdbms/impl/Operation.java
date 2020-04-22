/* $Header: xdk/src/java/json/src/oracle/soda/rdbms/impl/Operation.java /st_xdk_soda1/1 2020/01/29 12:44:01 jspiegel Exp $ */

/* Copyright (c) 2014, 2020, Oracle and/or its affiliates. 
All rights reserved.*/

/*
   DESCRIPTION

    Holds the PreparedStatement and the ResultSet representing
    a SODA operation.
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

import oracle.soda.OracleCollection;

import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.CallableStatement;

class Operation
{
  private final CallableStatement plsql_stmt;
  private final PreparedStatement stmt;

  private final boolean headerOnly;

  // Whether the operation involves filter()
  private final boolean filterSpecBased;

  // Whether the operation involved key()
  private final boolean singleKeyBased;

  // The collection on which this operation is performed
  private final OracleCollection collection;

  // SQL text, for subsequent error reporting
  private final String sqlText;

  private final int returnParameterIndex;

  Operation(PreparedStatement stmt,
            String sqlText,
            boolean headerOnly,
            boolean filterSpecBased,
            boolean singleKeyBased,
            int returnParameterIndex,
            OracleCollection collection)
  {
    this.stmt = stmt;
    this.sqlText = sqlText;
    this.headerOnly = headerOnly;
    this.filterSpecBased = filterSpecBased;
    this.singleKeyBased = singleKeyBased;
    this.returnParameterIndex = returnParameterIndex;
    this.collection = collection;
    this.plsql_stmt = ((stmt instanceof CallableStatement) ?
                       (CallableStatement)stmt : null);
  }

  Operation(PreparedStatement stmt,
            String sqlText,
            boolean headerOnly,
            boolean filterSpecBased,
            boolean singleKeyBased,
            OracleCollection collection)
  {
    this(stmt, sqlText, headerOnly, filterSpecBased, singleKeyBased,
         -1, collection);
  }

  Operation(CallableStatement stmt,
            String sqlText,
            boolean filterSpecBased,
            boolean singleKeyBased,
            int returnParameterIndex,
            OracleCollection collection)
  {
    this(stmt, sqlText, false, filterSpecBased, singleKeyBased,
         returnParameterIndex, collection);
  }

  PreparedStatement getPreparedStatement()
  {
    return stmt;
  }

  CallableStatement getCallableStatement()
  {
    return plsql_stmt;
  }

  Statement getStatement()
  {
    return stmt;
  }

  boolean headerOnly()
  {
    return headerOnly;
  }

  boolean isFilterSpecBased()
  {
    return filterSpecBased;
  }

  boolean isSingleKeyBased()
  {
    return singleKeyBased;
  }

  int getReturnParameterIndex()
  {
    return returnParameterIndex;
  }

  OracleCollection getCollection()
  {
    return collection;
  }

  String getSqlText()
  {
    return sqlText;
  }
}
