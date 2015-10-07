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

class Operation
{
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

  Operation(PreparedStatement stmt,
            String sqlText,
            boolean headerOnly,
            boolean filterSpecBased,
            boolean singleKeyBased,
            OracleCollection collection)
  {
    this.stmt = stmt;
    this.sqlText = sqlText;
    this.headerOnly = headerOnly;
    this.filterSpecBased = filterSpecBased;
    this.singleKeyBased = singleKeyBased;
    this.collection = collection;
  }

  PreparedStatement getPreparedStatement()
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

  OracleCollection getCollection()
  {
    return collection;
  }

  String getSqlText()
  {
    return sqlText;
  }
}
