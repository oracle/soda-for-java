/* $Header: xdk/src/java/json/src/oracle/soda/rdbms/impl/Operation.java /main/10 2015/08/31 12:59:04 dmcmahon Exp $ */

/* Copyright (c) 2014, 2015, Oracle and/or its affiliates. 
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

import java.sql.PreparedStatement;
import java.sql.CallableStatement;

class Operation
{
  private final PreparedStatement stmt;
  private final CallableStatement plsql_stmt;

  private final boolean headerOnly;

  // Whether the operation involves filter()
  private final boolean filterSpecBased;

  // Whether the operation involved key()
  private final boolean singleKeyBased;

  // The collection on which this operation is performed
  private final OracleCollection collection;

  // SQL text, for subsequent error reporting
  private final String sqlText;

  Operation(PreparedStatement stmt,
            String sqlText,
            boolean headerOnly,
            boolean filterSpecBased,
            boolean singleKeyBased,
            OracleCollection collection)
  {
    this.stmt = stmt;
    this.plsql_stmt = null;
    this.sqlText = sqlText;
    this.headerOnly = headerOnly;
    this.filterSpecBased = filterSpecBased;
    this.singleKeyBased = singleKeyBased;
    this.collection = collection;
  }

  Operation(CallableStatement stmt,
            String sqlText,
            boolean filterSpecBased,
            boolean singleKeyBased,
            OracleCollection collection)
  {
    this.stmt = null;
    this.plsql_stmt = stmt;
    this.sqlText = sqlText;
    this.headerOnly = false;
    this.filterSpecBased = filterSpecBased;
    this.singleKeyBased = singleKeyBased;
    this.collection = collection;
  }

  PreparedStatement getPreparedStatement()
  {
    return stmt;
  }

  CallableStatement getCallableStatement()
  {
    return plsql_stmt;
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

  OracleCollection getCollection()
  {
    return collection;
  }

  String getSqlText()
  {
    return sqlText;
  }
}
