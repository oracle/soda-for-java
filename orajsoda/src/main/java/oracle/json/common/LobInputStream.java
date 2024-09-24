/* Copyright (c) 2014, 2024, Oracle and/or its affiliates. */
/* All rights reserved.*/

/*
   DESCRIPTION

     LimitedInputStream based on a LOB. The close method will also
     free the LOB when finished.
 */

/**
 * This class is not part of the public API, and is
 * subject to change.
 *
 * Do not rely on it in your application code.
 *
 * @author   Doug McMahon
 */

package oracle.json.common;

import java.io.IOException;
import java.io.InputStream;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLException;

import java.util.logging.Logger;

import oracle.json.logging.OracleLog;
import oracle.json.util.LimitedInputStream;

public class LobInputStream
     extends LimitedInputStream
{
  private static final Logger log =
    Logger.getLogger(LobInputStream.class.getName());

  private final Blob  blocator;
  private final Clob  clocator;
  private final NClob nlocator;
  private final boolean noLob;

  protected MetricsCollector metrics = null;

  public LobInputStream()
  {
    super();
// BEGIN INFEASIBLE
    blocator = null;
    clocator = null;
    nlocator = null;
    noLob = true;
// END INFEASIBLE
  }

  public LobInputStream(InputStream in, long limit)
  {
    super(in, limit);
    blocator = null;
    clocator = null;
    nlocator = null;
    noLob = true;
  }

  public LobInputStream(InputStream in, int limit)
  {
    super(in, limit);
    blocator = null;
    clocator = null;
    nlocator = null;
    noLob = true;
  }

  public LobInputStream(Blob locator, InputStream in, long limit)
  {
    super(in, limit);
    blocator = locator;
    clocator = null;
    nlocator = null;
    noLob = false;
  }

  public LobInputStream(Clob locator, InputStream in, long limit)
  {
    super(in, limit);
    blocator = null;
    clocator = locator;
    nlocator = null;
    noLob = false;
  }

  public LobInputStream(NClob locator, InputStream in, long limit)
  {
    super(in, limit);
    blocator = null;
    clocator = null;
    nlocator = locator;
    noLob = false;
  }

  public void setMetrics(MetricsCollector metrics)
  {
    this.metrics = metrics;
  }

  @Override
  protected int timedRead(byte[] b, int off, int len)
    throws IOException
  {
    if (metrics == null)
      return(super.timedRead(b, off, len));

    metrics.startTiming();

    int n = super.timedRead(b, off, len);

    if (noLob)
      metrics.recordStreamRead((n > 0) ? n : 0);
    else
      metrics.recordLobReads(0);

    return(n);
  }

  @Override
  public void close()
    throws IOException
  {
    // Ensure that embedded object is closed only once
    if (!closed)
    {
      try
      {
        super.close();
      }
      finally
      {
        try
        {
          // ### This might not be necessary for all LOBs.
          //     It may be needed only for "temporary" LOBs
          //     but we can't tell from the locator itself.
          //     We'd need to use Oracle's BLOB type instead,
          //     which has an isTemporary() method on it.
          if (blocator != null)
            blocator.free();
          if (clocator != null)
            clocator.free();
          if (nlocator != null)
            nlocator.free();
          if (metrics != null)
            metrics.recordLobReads(1);
        } catch (SQLException e) {
          // ### For now ignore the exception
          if (OracleLog.isLoggingEnabled())
            log.severe(e.toString());
        }
      }
    }
  }
}
