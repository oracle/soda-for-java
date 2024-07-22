/* Copyright (c) 2014, 2022, Oracle and/or its affiliates.*/
/* All rights reserved.*/

/*
   DESCRIPTION
     QBE JsonValue clause

   PRIVATE CLASSES
  
   NOTES
    Simple carrier of information needed to generate a JSON_VALUE clause
      - Operation (inequality or IN operator)
      - Not flag (should use NOT wrapper)
      - Path within row source (e.g. "$.address.location")
      - Modifier function (e.g. upper/lower/ceil/floor/abs)
      - Comparator function (e.g. INSTR(...) = 1)

   MODIFIED    (MM/DD/YY)
    dmcmahon    02/24/17 - Creation
 */

/**
 * SqlJsonClause.java
 *
* Copyright (c) 2014, 2022, Oracle and/or its affiliates.
 *
 * @author Doug McMahon
 */ 

package oracle.json.parser;

import java.lang.NumberFormatException;

import java.util.ArrayList;
import java.math.BigDecimal;

import jakarta.json.JsonValue;
import jakarta.json.JsonArray;

import oracle.json.parser.Evaluator.EvaluatorCode;

public class SqlJsonClause
{
  private static final String INSTR_WRAPPER = "instr";
  private static final String REGEX_WRAPPER = "regexp_like";

  private final JsonQueryPath qPath;
  private final String comparator;

  private String compFunc     = null;
  private String convFunc     = null;

  private boolean isNumber    = false; // LHS needs/produces a NUMBER
  private boolean isString    = false; // LHS needs/produces a VARCHAR2
  private boolean isDate      = false; // LHS produces a DATE
  private boolean isTimestamp = false; // LHS produces a TIMESTAMP
  private boolean bindString  = false; // RHS must be a VARCHAR2
  private boolean bindNumber  = false; // RHS must be a NUMBER

  private boolean existsFlag  = false;
  private boolean notFlag     = false;

  private final ArrayList<ValueTypePair> binds = new ArrayList<ValueTypePair>();

  SqlJsonClause(EvaluatorCode oper, JsonQueryPath qPath)
  {
    this(oper, qPath, false);
  }

  SqlJsonClause(EvaluatorCode oper, JsonQueryPath qPath, boolean isNumberOperand)
  {
    this.qPath = qPath;

    String sqlComparator = null;

    if (AndORNode.restrictedSqlJson)
    {
      isNumber = isNumberOperand;
      bindNumber = isNumberOperand;

      switch (oper) 
      {
        case $eq:
          sqlComparator = "=";
          break;
        default:
          isNumber = isNumberOperand;
          bindNumber = isNumberOperand;
          sqlComparator = Evaluator.Operator.get(oper);
      }
    }
    else 
    {
      switch (oper) 
      {
        case $eq:
          sqlComparator = "=";
          break;
        case $ne:
          sqlComparator = "<>";
          break;
        case $instr:
        case $hasSubstring:
          sqlComparator = "> 0"; // Requires special INSTR() construct
          compFunc = INSTR_WRAPPER;
          bindString = true;
          break;
        case $startsWith:
          sqlComparator = "= 1"; // Requires special INSTR() construct
          compFunc = INSTR_WRAPPER;
          bindString = true;
          break;
        case $regex:
          sqlComparator = null; // Requires special REGEXP_LIKE() construct
          compFunc = REGEX_WRAPPER;
          bindString = true;
          break;
        case $exists:
          sqlComparator = "is not null";
          compFunc = null;
          existsFlag = true;
          break;
        case $like:
          bindString = true;
      /* FALLTHROUGH */
        case $in:                // Requires (...binds...)
        default:
          sqlComparator = Evaluator.Operator.get(oper);
      }
    }

    this.comparator = sqlComparator;
  }
  
  void setNot(boolean notFlag)
    throws QueryException
  {
    this.notFlag = notFlag;
  }

  void addModifier(EvaluatorCode modifier, String key)
    throws QueryException
  {
    if (modifier == null)
      return;

    // Check for supported modifiers with JSON_VALUE
    switch (modifier)
    {
    case $double:
    case $number:
    case $ceiling:
    case $floor:
    case $abs:
      isNumber = true;
      break;

    case $date:
      isDate = true;
      break;

    case $timestamp:
      isTimestamp = true;
      break;

    case $length:
      // RETURNING string but bind variable is number
      // However if the comparator demands a string, ignore it
      bindNumber = !bindString;
    case $upper:
    case $lower:
    case $string:
      isString = true;
      break;

    default:
      // ### is this an appropriate error message?
      QueryException.throwSyntaxException(QueryMessage.EX_OPERATOR_NOT_ALLOWED,
                                          key);
    }

    // SQL versions of the conversion functions
    switch (modifier)
    {
    case $ceiling: convFunc = "ceil";   break;
    case $floor:   convFunc = "floor";  break;
    case $abs:     convFunc = "abs";    break;
    case $length:  convFunc = "length"; break;
    case $upper:   convFunc = "upper";  break;
    case $lower:   convFunc = "lower";  break;
    default:
      break;
    }
  }

  void addBind(JsonValue itemValue, String key)
    throws QueryException
  {
    ValueTypePair bval = AndORTree.createBindValue(itemValue, key,
                                                   (compFunc == null));

    if (existsFlag)
    {
      // Exists always has 0 binds
      if (AndORNode.isReversedExists(bval))
        notFlag = !notFlag;
      return;
    }

    if (compFunc == REGEX_WRAPPER)
    {
      String oldval = null;
      String newval = null;

      if (bval.isString())
        newval = oldval = bval.getStringValue();
      else if (bval.isNumber())
        newval = bval.getNumberValue().toString();

      //
      // The SQL regexp_like() operator doesn't behave exactly like
      // QBE $regex, which is mapped (in JSON_EXISTS) to eq_regex,
      // not to like_regex. Therefore, we need to put leading and
      // trailing anchors onto the expression, i.e. to transform the
      // input string from "___" to "^___$".
      //
      if (newval != null)
      {
        int slen = newval.length();

        if (slen > 0)
        {
          if (newval.charAt(slen-1) != '$')
            newval = newval+'$';
          if (newval.charAt(0) != '^')
            newval = '^'+newval;
        }

        if (newval != oldval)
          bval = new ValueTypePair(newval);
      }
    }
    // Do whatever type coersion we can to avoid the need for SQL to do it
    else if (isNumber || bindNumber)
    {
      if (bval.isString())
      {
        try
        {
          BigDecimal dec = new BigDecimal(bval.getStringValue());
          bval = new ValueTypePair(dec);
        }
        catch (NumberFormatException e)
        {
          // This means it will probably fail if SQL attempts to convert it
          throw(new QueryException(e));
        }
      }
    }
    else if (isDate)
    {
      bval = ValueTypePair.makeTemporal(bval, false);
    }
    else if (isTimestamp)
    {
      bval = ValueTypePair.makeTemporal(bval, true);
    }
    else if (bindString || isString)
    {
      // Do the conversion here to avoid SQL format issues
      // We can't easily control the radix character in SQL
      // without also forcing limits on the digits, and without
      // compromising the exponential formats.
      if (bval.isNumber())
      {
        String sval = bval.getNumberValue().toString();
        bval = new ValueTypePair(sval);
      }
    }
    // Else it defaults to string from JSON_VALUE
    // However it's not clear we want a forced conversion here,
    // because SQL will favor numbers. So for now we will allow
    // SQL to do the conversion if this operator isn't clearly
    // in the string domain.

    binds.add(bval);
  }

  void addBindArray(JsonArray arr, String key)
    throws QueryException
  {
    for (JsonValue val : arr)
      this.addBind(val, key);
  }

  /*
   * The path that's the source expression for JSON_VALUE
   */
  public JsonQueryPath getPath()
  {
    return(qPath);
  }

  /*
   * Number of additional arguments to pass to the wrapper function
   */
  public int getArgCount()
  {
    return (compFunc == null) ? 0 : 1;
  }

  /*
   * Number of binds to appear after the comparator.
   * If > 1, they must be parenthesized and comma-separated.
   * This reports 0 for special-case functions with no comparand.
   */
  public int getBindCount()
  {
    // For the INSTR wrapper, the comparand becomes
    // an argument to the function.
    if (compFunc != null) return 0;

    return binds.size();
  }

  public ValueTypePair getValue(int pos)
  {
    return(binds.get(pos));
  }

  /*
   * Returns the comparison operator to use between the
   * (possibly wrappered) source and the comparand(s).
   */
  public String getComparator()
  {
    // Rewrite IN of a singleton using =
    if (comparator != null)
      if (comparator.equals("in"))
        if (binds.size() <= 1)
          return("=");

    return(comparator);
  }

  /*
   * Returns true if the expression should be surrounded
   * by a NOT() in SQL.
   */
  public boolean isNot()
  {
    return(notFlag);
  }

  /*
   * String to use in the RETURNING clause of JSON_VALUE.
   * If none, accept the default.
   */
  public String getReturningType()
  {
    if (isNumber)    return("number");
    if (isDate)      return("date");
    if (isTimestamp) return("timestamp");
/***
    if (isString)   return("varchar2(4000)");
***/
    return(null);
  }

  /*
   * See if this is an exists query
   */
  public boolean isExists()
  {
    return(existsFlag);
  }

  /*
   * Conversion function to apply, or null if none.
   */
  public String getConversionFunction()
  {
    return convFunc;
  }

  /*
   * Special compare function to apply, or null if none.
   */
  public String getCompareFunction()
  {
    return compFunc;
  }

  boolean useNumberBinding()
  {
    // If the comparator demands a number on the RHS (e.g. length),
    // or if the modifier produces a numeric value (e.g. number, abs)
    // then consider this operator to prefer a numeric binding.
    return (bindNumber || isNumber);
    // ### This may miss some cases where an odd mixture of the
    // ### modifier and the operator are used, for example
    // ###   ... {"$ceiling" : { "$regex" : ... } } ...
  }

  boolean useStringBinding()
  {
    // If the comparator demands a string on the RHS (e.g. regex, like,
    // etc.) then we have to bind a string even if the LHS produces
    // a numeric or date/timestamp value.
    return(bindString);
  }
  
  boolean useDateWrapper()
  {
    // If the comparator is an ordinary inequality, we should use
    // a wrapper around DATE types which are bound as strings.
    return(!bindString && isDate);
  }
  
  boolean useTimestampWrapper()
  {
    // If the comparator is an ordinary inequality, we should use
    // a wrapper around TIMESTAMP types which are bound as strings.
    return(!bindString && isTimestamp);
  }
}
