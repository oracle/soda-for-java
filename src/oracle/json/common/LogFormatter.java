/* Copyright (c) 2014, 2015, Oracle and/or its affiliates. 
All rights reserved.*/

/*
   DESCRIPTION
    Format and write messages to a log file.
    This wrappers an instance of a Logger and provides formatted
    output methods that are also sensitive to the logging level
    for the Logger.
 */

/**
 * This class is not part of the public API, and is
 * subject to change.
 *
 * Do not rely on it in your application code.
 *
 *  @author  Doug McMahon
 */

package oracle.json.common;

import java.util.Formatter;

import java.util.logging.Level;
import java.util.logging.Logger;

public class LogFormatter
{
  // Reusable buffer for log messages
  private StringBuilder formatBuffer = new StringBuilder(100);

  private static String NO_MESSAGE = "";

  private final Level   level;

  /**
   * Create a log formatter around the global logger.
   */
  public LogFormatter()
  {
    this(null);
  }

  /**
   * Create a log formatter around a given logger.
   * If null, uses the global logger.
   */
  public LogFormatter(Logger log)
  {
    if (log == null)
      log = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    Level lvl  = null;

    while (log != null)
    {
      lvl = log.getLevel();

      // If we found a logger that reports a level, use it
      if (lvl != null) break;

      // If lvl is null we need to try the parent - ugh!
      log = log.getParent();
    };

    // If we found a setting, use it, otherwise default to WARN
    if (lvl == null) lvl = Level.WARNING;

    this.level = lvl;
  }

  private String format(String msg, Object... params)
  {
    Formatter fmt = new Formatter(formatBuffer);
    formatBuffer.setLength(0);
    fmt.format(msg, params);
    fmt.close();
    return(formatBuffer.toString());
  }

  /**
   * Output a formatted message with arguments
   * at the severe or error level.
   */
  public String error(String msg, Object... params)
  {
    if (level.intValue() < Level.SEVERE.intValue()) return(NO_MESSAGE);
    return(format(msg, params));
  }

  /**
   * Output a formatted message with arguments
   * at the warning level.
   */
  public String warn(String msg, Object... params)
  {
    if (level.intValue() < Level.WARNING.intValue()) return(NO_MESSAGE);
    return(format(msg, params));
  }

  /**
   * Output a formatted message with arguments
   * at the info level.
   */
  public String info(String msg, Object... params)
  {
    if (level.intValue() < Level.INFO.intValue()) return(NO_MESSAGE);
    return(format(msg, params));
  }

  /**
   * Output a formatted message with arguments
   * at the debug or fine level.
   */
  public String debug(String msg, Object... params)
  {
    if (level.intValue() < Level.FINE.intValue()) return(NO_MESSAGE);
    return(format(msg, params));
  }
}
