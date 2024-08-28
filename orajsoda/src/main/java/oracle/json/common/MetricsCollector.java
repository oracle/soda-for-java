/* Copyright (c) 2014, 2024, Oracle and/or its affiliates. */
/* All rights reserved.*/

/**
 *  DESCRIPTION
 *
 *    Perf metrics
 */

/**
 *  This class is not part of the public API, and is
 *  subject to change.
 *  
 *  Do not rely on it in your application code.
 *  
 *  @author  Doug McMahon
 *  @author  Max Orgiyan
 */

package oracle.json.common;

import oracle.json.logging.OracleLog;

import java.util.Formatter;

import java.util.logging.Level;
import java.util.logging.Logger;

public class MetricsCollector
{
  private static final Logger log =
    Logger.getLogger(MetricsCollector.class.getName());

  private static final int FINE_LOG_LEVEL  = Level.FINE.intValue();
  private static final int currentLogLevel = MetricsCollector.getLogLevel();

  // Database-related metrics
  private int    dbLobReads           = 0; // Count of LOBs read
  private int    dbLobWrites          = 0; // Count of LOBs written
  private int    dbDocReadRoundTrips  = 0; // Doc reads (including LOBs)
  private int    dbDocWriteRoundTrips = 0; // Doc Writes (including LOBs)
  private int    dbCursorReads        = 0; // Number of docs read by cursors
  private int    dbTimestampReads     = 0; // Number of SYSTIMESTAMP reads
  private int    dbDDLs               = 0; // Number of DDL operations
  private int    dbGUIDs              = 0; // Numer of GUID fetches
  private int    dbProcCalls          = 0; // Number of procedure calls
  private int    dbSequenceBatch      = 0; // Number of sequence batch fetches
  private int    checksums            = 0; // Count of checksums
  private int    ioWrites             = 0; // Count of write I/Os
  private int    ioReadBytes          = 0; // Number of bytes read
  private int    ioWriteBytes         = 0; // Number of bytes written

  // IO-related timings (from System.nanoTime)
  private long   dbLobReadNanos       = 0L; // Time reading LOBs
  private long   dbLobWriteNanos      = 0L; // Time writing LOBs
  private long   dbDocReadNanos       = 0L; // Time doing doc reads
  private long   dbDocWriteNanos      = 0L; // Time doing doc writes
  private long   dbConnectNanos       = 0L; // Time doing connects
  private long   dbTransactionNanos   = 0L; // Time doing commit/rollback
  private long   dbTimestampNanons    = 0L; // Time doing timestamp
  private long   dbDDLNanos           = 0L; // Time doing DDLs
  private long   dbGUIDNanos          = 0L; // Time fetching GUIDs
  private long   dbProcCallNanos      = 0L; // Time running PL/SQL procedures
  private long   dbSequenceBatchNanos = 0L; // Time running sequence batch fetches
  private long   checksumNanos        = 0L; // Time computing checksums
  private long   ioReadNanos          = 0L; // Time doing read I/O
  private long   ioWriteNanos         = 0L; // Time doing write I/O

  // Transiently used for timing
  private long          total_time = 0L; // Start of request
  private long          start_time = 0L; // Start of sub-operation

  // Reusable string buffer for printing
  private StringBuilder formattingBuffer = new StringBuilder(30);

  private boolean encounteredNegativeTimeDiff = false;

  public MetricsCollector()
  {
    reset();
  }

  private void reset()
  {
    total_time = getTime();
  }

  public long getTime()
  {
    return(System.nanoTime());
  }

  public long getTimeDiff(long start_time)
  {
    long delta_time = getTime() - start_time;

    // It's possible that the time diff might be negative,
    // for example on systems with multiple CPUs.
    // See http://stackoverflow.com/questions/21339153/system-nanotime-turns-negative
    // and http://stackoverflow.com/questions/510462/is-system-nanotime-completely-useless
    // Set this flag if that's the case, so that later on we can register that
    // this has happened in the logging output. Also, set the time diff to 0.
    if (delta_time < 0L)
    {
      encounteredNegativeTimeDiff = true;
      delta_time = 0L;
    }

    return(delta_time);
  }

  /**
   * Start the internal timer. Since there's only one, this should
   * be used only for low-level operations (e.g. database and other I/Os,
   * DOM parses and serializations, etc.).
   */
  public void startTiming()
  {
    start_time = getTime();
  }

  public long endTiming()
  {
    return(getTimeDiff(start_time));
  }

  public void recordTimestampRead()
  {
    dbTimestampNanons += endTiming();
    ++dbTimestampReads;
  }

  public void recordDDL()
  {
    ++dbDDLs;
    dbDDLNanos += endTiming();
  }

  public void recordWrites(int count, int batchSize)
  {
    int roundTrips = (count + batchSize - 1) / batchSize;
    dbDocWriteNanos += endTiming();
    dbDocWriteRoundTrips += roundTrips;
  }

  public void recordReads(int count, int batchSize)
  {
    int roundTrips = (count + batchSize - 1) / batchSize;
    dbDocReadNanos += endTiming();
    dbDocReadRoundTrips += roundTrips;
  }

  public void recordGUIDS()
  {
    dbGUIDNanos += endTiming();
    ++dbGUIDs;
  }

  public void recordCall()
  {
    dbProcCallNanos += endTiming();
    ++dbProcCalls;
  }

  public void recordsSequenceBatchFetches()
  {
    dbSequenceBatchNanos += endTiming();
    ++dbSequenceBatch;
  }

  public void recordCursorReads(int count, int batchSize, long nanos)
  {
    int roundTrips = (count + batchSize - 1) / batchSize;
    dbDocReadNanos += nanos;
    dbDocReadRoundTrips += roundTrips;
    dbCursorReads += count;
  }

  public void recordLobReads(int count)
  {
    if (count > 0)
      dbLobReads += count;
    else
      dbLobReadNanos += endTiming();
  }

  public void recordLobWrites(int count, int batchSize)
  {
    int roundTrips = (count + batchSize - 1) / batchSize;
    dbLobWriteNanos += endTiming();
    dbDocWriteRoundTrips += roundTrips;
    dbLobWrites += count;
  }

  public void recordStreamRead(int nbytes)
  {
    ioReadNanos += endTiming();
    ioReadBytes += nbytes;
  }

  public void recordStreamWrite(int nbytes)
  {
    ioWriteNanos += endTiming();
    ioWriteBytes += nbytes;
    if (nbytes != 0) ++ioWrites;
  }

  public void recordChecksum()
  {
    checksumNanos += endTiming();
    ++checksums;
  }

  public void recordConnect()
  {
    dbConnectNanos += endTiming();
  }

  public void recordTransaction()
  {
    dbTransactionNanos += endTiming();
  }

  public String nanosToString(long nanos)
  {
    Formatter fmt = new Formatter(formattingBuffer); // ### Assumes default locale is OK

    long seconds = nanos/1000000000L;
    nanos = nanos - (seconds * 1000000000L);

    formattingBuffer.setLength(0);
    fmt.format("%d.%09d", (int)seconds, (int)nanos);
    fmt.close();
    return(formattingBuffer.toString());
  }

  private void logMsgTime(StringBuilder sb, String msg, long t)
  {
    sb.append(msg);
    sb.append(nanosToString(t));
    sb.append("\n");
  }

  private void logMsgCount(StringBuilder sb, String msg, int x)
  {
    sb.append(msg);
    sb.append(Integer.toString(x));
    sb.append("\n");
  }

  private void logMsgCountTime(StringBuilder sb, String msg, int x, long t)
  {
    sb.append(msg);
    sb.append(Integer.toString(x));
    sb.append(" in ");
    sb.append(nanosToString(t));
    sb.append("\n");
  }

  /**
   * Print the metrics to the log file
   */
  public void logResults()
  {
// BEGIN INFEASIBLE

    if (!OracleLog.isLoggingEnabled() || (currentLogLevel > FINE_LOG_LEVEL)) return; // Nothing to do

    boolean chatty_logging = (currentLogLevel < FINE_LOG_LEVEL);

    StringBuilder sb = new StringBuilder();

    sb.append("\n\n");

    if (chatty_logging || (dbConnectNanos > 0L))
      logMsgTime(sb, "JDBC connect in: ", dbConnectNanos);

    if (chatty_logging || (dbTransactionNanos > 0L))
      logMsgTime(sb, "JDBC commit/rollback in: ", dbTransactionNanos);

    if (chatty_logging || (ioReadBytes > 0) || (ioReadNanos > 0L))
      logMsgCountTime(sb, "IO read: ", ioReadBytes, ioReadNanos);

    if (chatty_logging || (ioWrites > 0))
    {
      sb.append("IO writes: ");
      sb.append(Integer.toString(ioWrites));
      sb.append(" of ");
      sb.append(Integer.toString(ioWriteBytes));
      sb.append(" bytes in ");
      sb.append(nanosToString(ioWriteNanos));
      sb.append("\n");
    }

    if ((chatty_logging) || (checksums > 0))
      logMsgCountTime(sb, "Checksums: ", checksums, checksumNanos);

    if (chatty_logging || (dbDDLs > 0))
      logMsgCountTime(sb, "DDLS: ", dbDDLs, dbDDLNanos);

    if (chatty_logging || (dbGUIDs > 0))
      logMsgCountTime(sb, "GUID fetches: ", dbGUIDs, dbGUIDNanos);

    if (chatty_logging || (dbTimestampReads > 0))
      logMsgCountTime(sb, "SYSTIMESTAMP reads: ", dbTimestampReads, dbTimestampNanons);

    if (chatty_logging || (dbSequenceBatch > 0))
      logMsgCountTime(sb, "Sequence batch fetches: ", dbSequenceBatch, dbSequenceBatchNanos);

    if (chatty_logging || (dbCursorReads > 0))
    {
      logMsgCount(sb, "Cursor reads: ", dbCursorReads);
    }

    if (chatty_logging || (dbLobReads > 0) || (dbLobWrites > 0))
    {
      logMsgCountTime(sb, "LOB reads: ",  dbLobReads,  dbLobReadNanos);
      logMsgCountTime(sb, "LOB writes: ", dbLobWrites, dbLobWriteNanos);
    }

    if (chatty_logging || (dbProcCalls > 0))
      logMsgCountTime(sb, "PLSQL calls: ", dbProcCalls, dbProcCallNanos);

    if ((chatty_logging) || (dbDocReadRoundTrips > 0) || (dbDocWriteRoundTrips > 0))
    {
      logMsgCountTime(sb,"Doc read round-trips: ", dbDocReadRoundTrips, dbDocReadNanos);
      logMsgCountTime(sb,"Doc write round-trips: ", dbDocWriteRoundTrips, dbDocWriteNanos);
    }

    if (encounteredNegativeTimeDiff)
    {
      sb.append("Warning: timings might be off, encountered negative time diff!!!\n");
    }

    long elapsed = getTime() - total_time;
    logMsgTime(sb, "\nElapsed total: ", elapsed);

    log.fine(sb.toString());
// END INFEASIBLE
  }

  /**
   * This code is needed because the java.util.logging facility won't
   * report an inherited level, forcing the caller to walk the parents
   * until one is found.
   */
  private static int getLogLevel()
  {
    Logger plog = MetricsCollector.log;
    Level  lvl  = null;

    while (plog != null)
    {
      lvl = plog.getLevel();
      if (lvl != null) break;
      // If lvl is null we need to try the parent - ugh!
      plog = plog.getParent();
    }

    return((lvl == null) ? 0 : lvl.intValue());
  }
}
