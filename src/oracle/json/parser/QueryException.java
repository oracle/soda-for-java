/* Copyright (c) 2014, 2015, Oracle and/or its affiliates. 
All rights reserved.*/

/*
   DESCRIPTION
    Exception thrown by the QBE parser.

    When used from SODA, QueryException might be the cause of a SODA
    OracleException. It's not, however, part of the public API, and is
    subject to change.
 */

/**
 * This class is not part of the public API, and is
 * subject to change.
 *
 * Do not rely on it in your application code.
 *
 *  @author  Doug McMahon
 */

package oracle.json.parser;

import javax.json.JsonException;

public class QueryException
     extends Exception
{
  private static final long serialVersionUID = 1L;

  public QueryException(String message)
  {
    super(message);
  }

  public QueryException(Exception e)
  {
    super(e);
  }

  public QueryException(String message, Exception e)
  {
    super(message, e);
  }

  @Override
  public String getMessage()
  {
    String msg = null;
    Throwable t = this.getCause();

    if (t != null)
      if (t instanceof JsonException)
        msg = ((Exception)t).getMessage();

    if (msg == null)
      msg = super.getMessage();

    return msg;
  }

  static QueryException getSyntaxException(QueryMessage msg, Object... params)
  {
    return new QueryException(msg.get(params));
  }

  static void throwSyntaxException(QueryMessage msg, Object... params)
    throws QueryException
  {
    throw new QueryException(msg.get(params));
  }

  static void throwExecutionException(QueryMessage msg, Object... params)
    throws QueryException
  {
    throw new QueryException(msg.get(params));
  }
}
