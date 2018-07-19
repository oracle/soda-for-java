/* Copyright (c) 2014, 2018, Oracle and/or its affiliates. 
All rights reserved.*/

/*
   DESCRIPTION
    Parser for JSON paths found in QBE documents, index specifications, etc.

   NOTES
    A path consists of a series of steps. There are two kinds of steps:
      - field steps
      - array steps
    A path may start with a field step or an array step.
    A field step is a series of allowed characters, or a string enclosed
    in backquotes. Certain characters (such as path step syntactic characters)
    must be enclosed in backquotes. A * is a special field step that is
    treated as a wildcard.
    A dot (.) must be followed by a field step, and field steps after
    the first step must be preceded by a dot.
    An array step is delimited by square brackets.
    An array step may follow a field step or another array step.
 */

/**
 * This class is not part of the public API, and is
 * subject to change.
 *
 * Do not rely on it in your application code.
 *
 *  @author  Rahul Kadwe
 *  @author  Doug McMahon
 */

package oracle.json.parser;

import java.util.ArrayList;
import java.util.Formatter;

public class PathParser
{
  private static final char STEP_SEPARATOR = '.';  // period
  private static final char SEG_DELIMITER  = '`';  // backquote
  private static final char SQL_DELIMITER  = '"';  // double quotes
  private static final char ESCAPE_CHAR    = '\\'; // backslash
  private static final char ARRAY_START    = '[';
  private static final char ARRAY_STOP     = ']';
  private static final char WILD_STEP      = '*';
  private static final char ROOT_STEP      = '$';
  private static final char CONTEXT_STEP   = '@';

  private final String pathString;

  private static final String ALPHA_NUM_UNDER =
    "_abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  private static final int    IS_DIGIT_POS = ALPHA_NUM_UNDER.indexOf('0');

  /**
   * Create a path parser for the given path string.
   */
  public PathParser(String pathString)
  {
    this.pathString = pathString;
  }

  private String buildStep(StringBuilder sb, int pos,
                           boolean needsQuotes, boolean quotedStep)
    throws QueryException
  {
    String result;

    if ((needsQuotes) && (!quotedStep))
    {
      // Unquoted empty steps are not allowed
      if (sb.length() == 1)
        throwPathException(QueryMessage.EX_EMPTY_PATH_STEP, pos);

      if (sb.length() == 2)
      {
        char ch = sb.charAt(1);

        // Naked $ or @ sign not allowed
        if ((ch == ROOT_STEP) || (ch == CONTEXT_STEP))
          throwPathException(QueryMessage.EX_PATH_SYNTAX_ERROR, pos);

        // This strips the quotes from a singleton * step
        if (ch == WILD_STEP)
          needsQuotes = false;
      }
    }

    if (!needsQuotes)
      result = sb.substring(1);
    else
    {
      sb.append(SQL_DELIMITER);
      result = sb.toString();
    }
    sb.setLength(1);
    return(result);
  }

  private String buildArrayStep(StringBuilder sb)
  {
    String result;
    sb.append(ARRAY_STOP);
    result = sb.toString();
    sb.setLength(0);
    sb.append(SQL_DELIMITER);
    return(result);
  }

  private void throwException(QueryMessage msg, Object... params)
    throws QueryException
  {
    throw new QueryException(msg.get(params));
  }

  private void throwPathException(QueryMessage msg, int position)
    throws QueryException
  {
    throwException(msg, pathString, Integer.toString(position));
  }

  private void throwArrayException(String arrStep)
    throws QueryException
  {
    throwException(QueryMessage.EX_BAD_ARRAY_SUBSCRIPT,
                   pathString, arrStep);
  }

  /*
  ** Allowed forms:
  **   [*]            wildcard (must be the only entry)
  **   [123]          single entry
  **   [1 to 4]       range of entries
  **   [1,2,3]        series of entries
  **   [1, 3 to 5, 8] series of entries and ranges
  */
  private void validateArrayStep(String arrStep)
    throws QueryException
  {
    boolean wildAllowed  = true;    // * is allowed initially
    boolean digitAllowed = true;    // Digit is allowed as next char
    boolean commaAllowed = false;   // Comma is allowed as next char

    boolean afterDigit    = false;  // Last non-space was a digit
    boolean toAllowed     = false;  // Any space after digit allows "to"
    boolean afterTo       = false;  // After "to" expecting range end
    boolean toInProgress  = false;  // Prior char was 't' in "to"
    boolean spaceRequired = false;  // A whitespace is required (after "to")
    boolean digitRequired = false;  // Digit required after comma or "to"

    for (int i = 1; i < arrStep.length() - 1; ++i)
    {
      char currentChar = arrStep.charAt(i);

      if (currentChar == '*')
      {
        if (!wildAllowed)
          throwArrayException(arrStep);

        wildAllowed  = false;  // We've seen the only allowed wildcard
        digitAllowed = false;  // Only whitespace is allowed afterward
      }
      else if (currentChar == ',')
      {
        if (!commaAllowed)
          throwArrayException(arrStep);

        commaAllowed  = false;
        toAllowed     = false;
        afterDigit    = false;
        afterTo       = false;
        digitRequired = true;  // Next non-space must be a digit
      }
      else if ("0123456789".indexOf(currentChar) >= 0)
      {
        if (!digitAllowed)
          throwArrayException(arrStep);

        wildAllowed   = false; // Wildcard no longer allowed
        commaAllowed  = true;
        afterDigit    = true;
        digitRequired = false;
      }
      else if (" \t\n\r".indexOf(currentChar) >= 0)
      {
        // Whitespace not allowed when parsing "to"
        if (toInProgress)
          throwArrayException(arrStep);

        if (afterDigit)
        {
          // Last non-space was a digit - next non-space is "to" or comma
          digitAllowed = false;
          toAllowed    = !afterTo;
          commaAllowed = true;
        }
        else if (spaceRequired)
        {
          // This is the whitespace required after "to"
          digitAllowed  = true;
          spaceRequired = false;
          digitRequired = true;  // At least one digit must follow
        }
      }
      else if (currentChar == 't')
      {
        if (!toAllowed)
          throwArrayException(arrStep);

        toInProgress = true;  // Next char must be the 'o' in "to"
        commaAllowed = false;
        afterDigit   = false;
      }
      else if (currentChar == 'o')
      {
        if (!toInProgress)
          throwArrayException(arrStep);

        toInProgress  = false;
        toAllowed     = false;
        afterTo       = true;
        spaceRequired = true;  // "to" must be followed by whitespace
      }
      else
      {
        // Invalid character
        throwArrayException(arrStep);
      }
    }

    // Empty array or only whitespace found
    if (wildAllowed)
      throwArrayException(arrStep);

    // Incomplete "to" or comma sequence at end of subscript
    if (toInProgress || spaceRequired || digitRequired)
      throwArrayException(arrStep);
  }

  /**
   * Parse the path into an array of steps.
   * The steps are suitable for assembly into a single
   * single-quoted literal, meaning that any necessary
   * escaping and/or double quoting has been done.
   * Returns null if the path cannot be parsed properly.
   */
  public String[] splitAndSQLEscape()
    throws QueryException
  {
    if (pathString == null)
      throwException(QueryMessage.EX_EMPTY_PATH);

    return(splitSteps(true));
  }

  /**
   * Parse the path into an array of steps.
   * The steps are suitable for assembly into a single
   * single-quoted literal, meaning that any necessary
   * escaping and/or double quoting has been done.
   * Returns null if the path cannot be parsed properly.
   */
  public String[] splitSteps(boolean doSqlEscape)
    throws QueryException
  {
    if (pathString == null)
      throwException(QueryMessage.EX_EMPTY_PATH);

    char [] pathChars = pathString.toCharArray();

    int pathLen = pathChars.length;


    ArrayList<String> result = new ArrayList<String>(10);

    StringBuilder sb = new StringBuilder(Math.max(pathLen * 2 + 2, 128));

    boolean inQuotes    = false; // currently inside quotes
    boolean inArray     = false; // currently inside array
    boolean needsQuotes = false; // segment needs to be quoted (SQL)
    boolean allowArray  = true;  // array step is allowed
    boolean afterArray  = false; // preceding step was an array
    boolean quotedStep  = false; // Current step was backquoted
    int     pos         = 0;     // Position in string

    // Put a leading double-quote on the step in case it's needed
    sb.append(SQL_DELIMITER);

    if (pathLen == 0)
    {
      allowArray = false;
      needsQuotes = doSqlEscape;
    }

    while (pos < pathChars.length)
    {
      char currentChar = pathChars[pos];

      switch (currentChar)
      {
      case SEG_DELIMITER:

        // Array cannot be followed by a backquote without a dot
        // A backquote cannot immediately follow a quoted step without a dot
        if ((afterArray) || (quotedStep))
          throwPathException(QueryMessage.EX_MISSING_STEP_DOT, pos);

        // If not currently in a quoted step
        if (!inQuotes)
        {
          // If this is not the first character
          if (sb.length() > 1)
            throwPathException(QueryMessage.EX_BAD_BACKQUOTE, pos);

          // This begins a quoted step
          inQuotes = true;
          ++pos;
          break;
        }

        // Inside a quoted step
        ++pos;

        // If not at the end of the string
        if (pos < pathChars.length)
        {
          // Peek at the next character
          currentChar = pathChars[pos];

          // If it's another quote, this is an "escaped" quote
          if (currentChar == SEG_DELIMITER)
          {
            sb.append(currentChar);
            ++pos;
            needsQuotes = doSqlEscape;
            break;
          }
        }

        // If it's an empty step, it needs quotes
        if (sb.length() == 1)
          needsQuotes = doSqlEscape;

        // Otherwise this quote ends the quoted step
        inQuotes = false;
        quotedStep = true;

        break;

      case ARRAY_STOP:

        // If currently in an array, we've found the end
        if (inArray)
        {
          inArray = false;

          String arrStep = buildArrayStep(sb);
          validateArrayStep(arrStep);

          result.add(arrStep);
          afterArray = true;

          ++pos;
          break;
        }
        else if (!inQuotes)
        {
          if (currentChar == ARRAY_STOP) // Naked array close
            throwPathException(QueryMessage.EX_PATH_SYNTAX_ERROR, pos);
        }

        /* FALLTHROUGH */

      case ARRAY_START:
      case STEP_SEPARATOR:

        // If not already in quotes or an array, this ends the prior step
        if ((!inQuotes) && (!inArray))
        {
          inArray = (currentChar == ARRAY_START);

          // If the current step isn't empty, it's a field step - append it 
          if ((sb.length() > 1) || quotedStep)
          {
            result.add(buildStep(sb, pos, needsQuotes, quotedStep));
          }
          // Else if the current step is an empty dot
          else if (!inArray)
          {
            if (!afterArray)
            {
              result.add(buildStep(sb, pos, true, quotedStep));
            }
          }
          // Otherwise the current step starts an array
          else
          {
            if (!allowArray)
            {
              result.add(buildStep(sb, pos, true, quotedStep));
            }
          }

          quotedStep = false;   // Quoted step (if any) consumed
          allowArray = inArray; // Array not allowed right after a dot
          needsQuotes = false;
          afterArray = false;
          ++pos;

          // Inside an array step, set the first character to the [
          if (inArray)
          {
            sb.setLength(0);
            sb.append(ARRAY_START);
          }
          // Else this must be a dot
          else
          {
            if (pos >= pathChars.length)
              needsQuotes = doSqlEscape;
          }

          break;
        }

        /* FALLTHROUGH */

      default:

        // Characters can't immediately follow an array or quoted step
        if ((afterArray) || (quotedStep))
          throwPathException(QueryMessage.EX_MISSING_STEP_DOT, pos);

        allowArray = true;

        if (currentChar == '\'')
        {
          // Double-escape the single quote for SQL
          if (doSqlEscape) sb.append(currentChar);
          sb.append(currentChar);
          needsQuotes = doSqlEscape;
        }
        else if ((currentChar == SQL_DELIMITER) || (currentChar == ESCAPE_CHAR))
        {
          needsQuotes = doSqlEscape;
          // Character needs to be escaped with a backslash for SQL
          if (doSqlEscape) sb.append(ESCAPE_CHAR);
          sb.append(currentChar);
        }
        // Control characters need escaping for SQL
        else if (doSqlEscape &&
                 ((currentChar < ' ') || (currentChar == '\177')))
        {
          sb.append(ESCAPE_CHAR);

          switch (currentChar)
          {
          case '\n': sb.append('n'); break;
          case '\r': sb.append('r'); break;
          case '\t': sb.append('t'); break;
          case '\b': sb.append('b'); break;
          case '\f': sb.append('f'); break;
          default:
            // Character needs to be escaped as "uXXXX"
            Formatter fmt = new Formatter(sb); // ### Assumes it will append
            fmt.format("u%04x", (int)currentChar);
            fmt.close();
            break;
          }
          needsQuotes = doSqlEscape;
        }
        else
        {
          if (!needsQuotes && doSqlEscape)
          {
            int idx = ALPHA_NUM_UNDER.indexOf(currentChar);
            // The SQL JSON path parser is fussy about any non-alphanumerics
            if (idx < 0)
              needsQuotes = true;
            // It also doesn't allow leading digits
            else if ((sb.length() == 1) &&  (idx >= IS_DIGIT_POS))
              needsQuotes = true;
          }

          // Ordinary character
          sb.append(currentChar);
        }
        ++pos;
        break;
      }
    }

    if (inArray)
      throwException(QueryMessage.EX_UNCLOSED_STEP,
                     "array", pathString);
    if (inQuotes)
      throwException(QueryMessage.EX_UNCLOSED_STEP,
                     "quote", pathString);

    // Add the last step (if any)
    if ((sb.length() > 1) || !allowArray || quotedStep)
    {
      // Disallow empty steps unless in quotes
      if ((sb.length() == 1) && !quotedStep)
        needsQuotes = true;

      result.add(buildStep(sb, pos, needsQuotes, quotedStep));
    }

    String[] stepArr = new String[result.size()];
    return(result.toArray(stepArr));
  }
}
