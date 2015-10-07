/* Copyright (c) 2014, 2015, Oracle and/or its affiliates. 
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
    $startsWith,
    $regex,
    $id,
    $orderby
  }

  static HashMap<EvaluatorCode, String> Operator =
     new HashMap<EvaluatorCode, String>();

  // Maps QBE operator to SQL/JSON operator
  static {
    Operator.put(EvaluatorCode.$gt, ">");
    Operator.put(EvaluatorCode.$lt, "<");
    Operator.put(EvaluatorCode.$gte, ">=");
    Operator.put(EvaluatorCode.$lte, "<=");
    Operator.put(EvaluatorCode.$ge,  ">="); // Synonym for $gte
    Operator.put(EvaluatorCode.$le,  "<="); // Synonym for $lte
    Operator.put(EvaluatorCode.$eq, "==");
    Operator.put(EvaluatorCode.$ne, "!=");  // ### Note: it's not correct to
                                            // use != for $ne. This mapping is
                                            // ignored at JSON_EXISTS generation time,
                                            // and the !(field == value) form is
                                            // generated instead.
    Operator.put(EvaluatorCode.$exists, "exists");
    Operator.put(EvaluatorCode.$in, "in");
    Operator.put(EvaluatorCode.$nin, "not_in");
    Operator.put(EvaluatorCode.$startsWith, "starts with");
    Operator.put(EvaluatorCode.$regex, "eq_regex");
    Operator.put(EvaluatorCode.$id, "id");
  }
}
