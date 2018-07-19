/* Copyright (c) 2014, 2017, Oracle and/or its affiliates. 
All rights reserved.*/

/*
   DESCRIPTION
     Maps QBE operators to SQL/JSON operators
 */

/**
 * This class is not part of the public API, and is
 * subject to change.
 *
 * Do not rely on it in your application code.
 *
 * @author Rahul Manohar Kadwe
 */

package oracle.json.parser;

import java.util.HashMap;

class Evaluator
{
  static final String MODIFIER_NOT = "not";

  static enum EvaluatorCode
  {
    $and,
    $or,
    $exists,
    $eq,
    $ne,
    $gt,
    $lt,
    $gte,
    $lte,
    $ge,
    $le,
    $in,
    $nin,
    $all,
    $not,
    $nor,
    $between,
    $startsWith,
    $regex,
    $hasSubstring,
    $instr,
    $like,
    $id,
    // SQL JSON operators
    $sqlJson,
    // Path field type coersions
    $double,    // to-double
    $number,    // to-decimal-number
    $string,    // to-string
    $date,      // to-date
    $timestamp, // to-date/time
    $boolean,   // to-boolean
    // Numeric field operators
    $ceiling,   // ceiling
    $floor,     // floor
    $abs,       // absolute value
    // Text field operators
    $upper,     // to-upper
    $lower,     // to-lower
    // Path field information extractors
    $type,      // field data type
    $length,    // text field byte length
    $size,      // array size
    //
    // $near
    //   $geometry : {...geoJson...}
    //   $maxDistance : <distance in meters>,
    //   $minDistance : <distance in meters>
    //
    // $within (replaced by $geoWithin)
    //   $geometry : {...geoJson...}
    //
    // $intersects (replaced by $geoIntersects)
    //   $geometry : {...geoJson...}
    //
    $near,
    $within,
    $intersects,
    //
    // JSON_TextContains(column, path, value)
    //
    $contains,
    $orderby
  }

  static HashMap<EvaluatorCode, String> Operator =
     new HashMap<EvaluatorCode, String>();

  static HashMap<EvaluatorCode, String> Modifier =
     new HashMap<EvaluatorCode, String>();

  /**
   * This returns true if the input operator is detectably a new 12.2-only
   * operator. It returns false otherwise.
   */
  public static boolean requires_12_2(EvaluatorCode code)
  {
    if (code == null) return(false);

    switch (code)
    {
    // These string operators were added in 12.2
    case $hasSubstring:
    case $instr:
    case $like:
    // These type conversions were added in 12.2
    case $double:
    case $number:
    case $string:
    case $date:
    case $timestamp:
    case $boolean:
    // These trailing functions were added in 12.2
    case $ceiling:
    case $floor:
    case $abs:
    case $upper:
    case $lower:
    // These introspection operators were added in 12.2
    case $type:
    case $length:
    case $size:
    // Spatial operators were added in 12.2
    case $near:
    case $within:
    case $intersects:
      return(true);

    // ### Assumes $contains -> JSON_TextContains() work in 12.1

    default:
      break;
    }

    return(false);
  }

  /**
   * This returns true if the input operator is detectably a new 12.2-only
   * operator. It returns false otherwise (even if the operator is completely
   * unrecognized).
   */
  public static boolean requires_12_2(String op)
  {
    EvaluatorCode code = null;

    if (op == null) return(false);

    try
    {
      code = EvaluatorCode.valueOf(op);
    }
    catch (IllegalArgumentException e)
    {
      code = null;
    }

    return(Evaluator.requires_12_2(code));
  }

  // Maps QBE operator to SQL/JSON operator
  static {
    Operator.put(EvaluatorCode.$gt,  ">");
    Operator.put(EvaluatorCode.$lt,  "<");
    Operator.put(EvaluatorCode.$gte, ">=");
    Operator.put(EvaluatorCode.$lte, "<=");
    Operator.put(EvaluatorCode.$ge,  ">="); // Synonym for $gte
    Operator.put(EvaluatorCode.$le,  "<="); // Synonym for $lte
    Operator.put(EvaluatorCode.$eq,  "==");
    Operator.put(EvaluatorCode.$ne,  "!="); // ### Note: it's not correct to
                                            // use != for $ne. This mapping is
                                            // ignored generating JSON_EXISTS,
                                            // and the !(field == value) form
                                            // is generated instead.
    Operator.put(EvaluatorCode.$exists,       "exists");
    Operator.put(EvaluatorCode.$in,           "in");
    Operator.put(EvaluatorCode.$nin,          "not_in");
    Operator.put(EvaluatorCode.$between,      "between");
    Operator.put(EvaluatorCode.$startsWith,   "starts with");
    Operator.put(EvaluatorCode.$regex,        "eq_regex");
    Operator.put(EvaluatorCode.$hasSubstring, "has substring");
    Operator.put(EvaluatorCode.$instr,        "has substring"); // synonym
    Operator.put(EvaluatorCode.$like,         "like");
    Operator.put(EvaluatorCode.$id,           "id");

    // Wrapper
    Modifier.put(EvaluatorCode.$not,       MODIFIER_NOT);
    // Data type casting
    Modifier.put(EvaluatorCode.$double,    "double");
    Modifier.put(EvaluatorCode.$number,    "number");
    Modifier.put(EvaluatorCode.$string,    "string");
    Modifier.put(EvaluatorCode.$date,      "date");
    Modifier.put(EvaluatorCode.$timestamp, "timestamp");
    Modifier.put(EvaluatorCode.$boolean,   "boolean");
    // Numeric targets only
    Modifier.put(EvaluatorCode.$ceiling,   "ceiling");
    Modifier.put(EvaluatorCode.$floor,     "floor");
    Modifier.put(EvaluatorCode.$abs,       "abs");
    // String targets only
    Modifier.put(EvaluatorCode.$upper,     "upper");
    Modifier.put(EvaluatorCode.$lower,     "lower");
    // Metadata
    Modifier.put(EvaluatorCode.$type,      "type");
    Modifier.put(EvaluatorCode.$length,    "length");
    Modifier.put(EvaluatorCode.$size,      "size");
  }
}
