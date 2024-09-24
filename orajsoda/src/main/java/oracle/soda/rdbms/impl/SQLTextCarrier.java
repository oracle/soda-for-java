/* Copyright (c) 2014, 2015, Oracle and/or its affiliates. 
All rights reserved.*/

/*
   DESCRIPTION
    Provides access to the SQL String. Used for generating exceptions
    that have a SQL String attached.
 */

/**
 *  This class is not part of the public API, and is
 *  subject to change.
 *
 *  Do not rely on it in your application code.
 *
 *  @author  Max Orgiyan
 */

package oracle.soda.rdbms.impl;

public interface SQLTextCarrier
{
  public String getSQL();
}
