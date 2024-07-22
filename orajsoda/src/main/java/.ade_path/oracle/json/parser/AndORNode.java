/* Copyright (c) 2014, 2024, Oracle and/or its affiliates.*/
/* All rights reserved.*/

/*
   DESCRIPTION
     Translate QBE to SQL
 */

/**
 * This class is not part of the public API, and is
 * subject to change.
 *
 * Do not rely on it in your application code.
 *
 * @author Rahul Manohar Kadwe
 * @author Doug McMahon
 * @author Max Orgiyan
 */ 

package oracle.json.parser;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import jakarta.json.JsonString;
import jakarta.json.JsonValue;
import jakarta.json.JsonValue.ValueType;
import oracle.json.parser.Evaluator.EvaluatorCode;

class AndORNode
{
  private final static int KEY_TYPE_OPERATOR = 1;
  private final static int KEY_TYPE_AND      = 2;
  private final static int KEY_TYPE_OR       = 3;
  private final static int KEY_TYPE_LEAF     = 4;
  private final static int KEY_TYPE_ID       = 5;
  private final static int KEY_TYPE_NOR      = 6;
  private final static int KEY_TYPE_SPATIAL  = 7;
  private final static int KEY_TYPE_FULLTEXT = 8;
  private final static int KEY_TYPE_MODIFIER = 9;
  private final static int KEY_TYPE_SQLJSON  = 10;

  private EvaluatorCode           eval;
  private Predicate               predicate;
  private List<AndORNode>         children = new ArrayList<AndORNode>();
  private final AndORNode         parent;
  private int                     numVals = 0;
  private JsonQueryPath           downScopePath = null;
  private boolean                 notOp = false;

  private boolean strictTypeMode = false;

  // ### Implementation of sqlJson includes various operators
  // that are similar to regular QBEs but have different semantics
  // (because they are based on json_value/json_query operators,
  // which are different from json_exists).
  //
  // For now, restrict supported sqlJson operators to
  // ones that can pickup indexes on 12.1.0.2: $eq, $le, $ge, $lt, $gt,
  // $lte, $gte. These can be used in conjunction with regular QBEs to
  // force index pickup.
  //
  // The rest of sqlJson operators can't pick up indexes on
  // 12.1.0.2, so users can use regular QBEs instead.
  static final boolean    restrictedSqlJson = true;

  /**
   * ### Might use a native SQL not_in the future,
   *     if the RDBMS supports it. Set to false
   *     for now.
   */
  private static final boolean useSqlNotIn  = false;

  /**
   * ### Might want to generate $nin during JSON_EXISTS
   *     generation in the future. Currently we don't do this.
   *     Instead, we rewrite $nin to $not($in(...)) prior
   *     to the JSON_EXISTS generation phase.
   */
  private static final boolean generateNIN = false;

  /**
   * Not part of a public API, and is subject to change.
   * @param parent
   */
  public AndORNode(AndORNode parent)
  {
    this.parent = parent;
  }

  EvaluatorCode getEval()
  {
    return eval;
  }

  AndORNode getParent()
  {
    return parent;
  }

  int getNumVals()
  {
    return numVals;
  }

  String getDownScopePath()
  {
    if (downScopePath != null)
      return downScopePath.toQueryString();
    return null;
  }

  Predicate getPredicate()
  {
    return predicate;
  }

  boolean getNotOperation()
  {
    return notOp;
  }
  
  void setEval(EvaluatorCode eval)
  {
    this.eval = eval;
  }

  private void setPredicatePath(String path)
    throws QueryException
  {
    downScopePath = new JsonQueryPath(path);
  }

  private void incrementNumVals()
  {
    ++numVals;
  }

  void setPredicate(Predicate predicate)
  {
    this.predicate = predicate;
  }

  private void setNotOperation()
  {
    notOp = true;
  }

  private void clearNotOperation()
  {
    notOp = false;
  }

  private static EvaluatorCode codeFor(String op)
    throws QueryException
  {
    EvaluatorCode code = null;
    try
    {
      code = EvaluatorCode.valueOf(op);
    }
    catch (IllegalArgumentException e)
    {
      QueryException.throwSyntaxException(QueryMessage.EX_NOT_AN_OPERATOR, op);
    }
    return(code);
  }

  private static ValueType checkScalarType(EvaluatorCode evc,
                                     String itemKey,
                                     ValueTypePair vpair)
    throws QueryException
  {
    ValueType vtype = vpair.getValue().getValueType();

    if ((evc == EvaluatorCode.$startsWith)   ||
        (evc == EvaluatorCode.$hasSubstring) ||
        (evc == EvaluatorCode.$instr)        ||
        (evc == EvaluatorCode.$like)         ||
        (evc == EvaluatorCode.$regex)        ||
        (evc == EvaluatorCode.$likeRegex)    ||
        (evc == EvaluatorCode.$ciRegex)      ||
        (evc == EvaluatorCode.$ciLikeRegex)  ||
	(evc == EvaluatorCode.$nlRegex)      ||
	(evc == EvaluatorCode.$nlLikeRegex)  ||
	(evc == EvaluatorCode.$nlciRegex)    ||
	(evc == EvaluatorCode.$nlciLikeRegex))
    {
      if (vtype != ValueType.STRING)
        QueryException.throwSyntaxException(QueryMessage.EX_MUST_BE_STRING,
                                            itemKey);
    }
    else if ((evc != EvaluatorCode.$eq) &&
             (evc != EvaluatorCode.$ne) &&
             (evc != EvaluatorCode.$exists))
    {
      if ((vtype == ValueType.TRUE) || (vtype == ValueType.FALSE) ||
          (vtype == ValueType.NULL))
        QueryException.throwSyntaxException(QueryMessage.EX_OPERAND_INVALID,
                                            itemKey,
                                            vtype.toString());
    }

    return (vtype);
  }

  /*
   * Check if the "value" after $exists indicates reversal
   */
  static boolean isReversedExists(ValueTypePair bval)
  {
    boolean reversed = false;

    // If the value is false, null, or 0, it's a not(exists())
    if (bval.isBoolean())
    {
      if (!bval.getBooleanValue())
        reversed = true;
    }
    else if (bval.getValue() == JsonValue.NULL)
    {
      reversed = true;
    }
    else if (bval.isNumber())
    {
      if (BigDecimal.ZERO.equals(bval.getNumberValue()))
        reversed = true;
    }

    return(reversed);
  }

  static int getKeyType(String key, boolean strictTypeMode)
    throws QueryException
  {
    int keyType = 0;    

    if (key.length() == 0)
      keyType = KEY_TYPE_LEAF;
    else if (key.charAt(0) != '$')
      keyType = KEY_TYPE_LEAF;
    else if (key.equalsIgnoreCase("$and"))
      keyType = KEY_TYPE_AND;
    else if (key.equalsIgnoreCase("$or"))
      keyType = KEY_TYPE_OR;
    else if (key.equalsIgnoreCase("$nor"))
      keyType = KEY_TYPE_NOR;
    else if (key.equalsIgnoreCase("$sqlJson"))
      keyType = KEY_TYPE_SQLJSON;
    else if (key.equalsIgnoreCase("$id"))
      keyType = KEY_TYPE_ID;
    else if (key.equalsIgnoreCase("$near")         ||
             key.equalsIgnoreCase("$within")       ||
             key.equalsIgnoreCase("$intersects"))
      keyType = KEY_TYPE_SPATIAL;
    else if (key.equalsIgnoreCase("$value"))
    {
      if (strictTypeMode)
        keyType = KEY_TYPE_MODIFIER;
      else
        QueryException.throwSyntaxException(QueryMessage.EX_23C_AND_JSON_TYPE_REQUIRED);
    }
    else if (key.equalsIgnoreCase("$contains"))
      keyType = KEY_TYPE_FULLTEXT;
    else if (key.equalsIgnoreCase("$gt")           ||
             key.equalsIgnoreCase("$gte")          ||
             key.equalsIgnoreCase("$ge")           ||
             key.equalsIgnoreCase("$lt")           ||
             key.equalsIgnoreCase("$lte")          ||
             key.equalsIgnoreCase("$le")           ||
             key.equalsIgnoreCase("$eq")           ||
             key.equalsIgnoreCase("$ne")           ||
             key.equalsIgnoreCase("$in")           ||
             key.equalsIgnoreCase("$nin")          ||
             key.equalsIgnoreCase("$all")          ||
             key.equalsIgnoreCase("$between")      ||
             key.equalsIgnoreCase("$regex")        ||
             key.equalsIgnoreCase("$likeRegex")    ||
             key.equalsIgnoreCase("$ciRegex")      ||
             key.equalsIgnoreCase("$ciLikeRegex")  ||
             key.equalsIgnoreCase("$nlRegex")      ||
             key.equalsIgnoreCase("$nlLikeRegex")  ||
             key.equalsIgnoreCase("$nlciRegex")    ||
             key.equalsIgnoreCase("$nlciLikeRegex")||
             key.equalsIgnoreCase("$exists")       ||
             key.equalsIgnoreCase("$hasSubstring") ||
             key.equalsIgnoreCase("$instr")        ||
             key.equalsIgnoreCase("$like")         ||
             key.equalsIgnoreCase("$startsWith"))
      keyType = KEY_TYPE_OPERATOR;
    else if (key.equalsIgnoreCase("$double")       ||
             key.equalsIgnoreCase("$number")       ||
             key.equalsIgnoreCase("$string")       ||
             key.equalsIgnoreCase("$date")         ||
             key.equalsIgnoreCase("$timestamp")    ||
             key.equalsIgnoreCase("$boolean")      ||
             key.equalsIgnoreCase("$binary")       ||
             key.equalsIgnoreCase("$numberOnly")   ||
             key.equalsIgnoreCase("$stringOnly")   ||
             key.equalsIgnoreCase("$booleanOnly")  ||
             key.equalsIgnoreCase("$binaryOnly")   ||
             key.equalsIgnoreCase("$dateTimeOnly") ||
             key.equalsIgnoreCase("$idOnly")       ||
             key.equalsIgnoreCase("$ceiling")      ||
             key.equalsIgnoreCase("$floor")        ||
             key.equalsIgnoreCase("$abs")          ||
             key.equalsIgnoreCase("$upper")        ||
             key.equalsIgnoreCase("$lower")        ||
             key.equalsIgnoreCase("$type")         ||
             key.equalsIgnoreCase("$length")       ||
             key.equalsIgnoreCase("$size")         ||
             key.equalsIgnoreCase("$not")          ||
             key.equalsIgnoreCase("$mod"))
      keyType = KEY_TYPE_MODIFIER;
    else
      QueryException.throwSyntaxException(QueryMessage.EX_NOT_AN_OPERATOR, key);

    return keyType;
  }

  private void addArrayValues(AndORTree tree,
                              JsonValue memberValue, String keyString,
                              boolean allowEmpty)
    throws QueryException
  {
    JsonArray jsonArr = (JsonArray)memberValue;

    if (!allowEmpty && (jsonArr.size() == 0))
      QueryException.throwSyntaxException(QueryMessage.EX_CANNOT_BE_EMPTY,
                                          keyString);

    Iterator<JsonValue> arrIter = jsonArr.iterator();

    while (arrIter.hasNext())
    {
      JsonValue arrElem = arrIter.next();
      if (AndORTree.isJSONObject(arrElem))
      {
        incrementNumVals();
        parseComplexValue(tree, arrElem.asJsonObject(), true);
      }
      else
      {
        incrementNumVals();
        tree.addToValueArray(arrElem, keyString);
      }
    }
  }

  /*
  ** Add the values for a $between, returns:
  **   -1 lower bound only, equivalent to $gte
  **   +1 upper bound only, equivalent to $lte
  **    0 both bounds, needs a $and
  */
  private int addBetweenValues(AndORTree tree,
                               JsonValue memberValue, String keyString)
    throws QueryException
  {
    if (!AndORTree.isJSONArray(memberValue))
      QueryException.throwSyntaxException(QueryMessage.EX_MUST_BE_ARRAY,
                                          keyString);

    JsonArray jsonArr = (JsonArray)memberValue;

    if (jsonArr.size() != 2)
    {
      QueryException.throwSyntaxException(QueryMessage.EX_BETWEEN_ARGUMENT,
                                          keyString);
    }

    JsonValue lowerBound = jsonArr.get(0);
    JsonValue upperBound = jsonArr.get(1);

    if ((lowerBound == null) || (upperBound == null))
    {
      QueryException.throwSyntaxException(QueryMessage.EX_BETWEEN_ARGUMENT,
                                          keyString);
    }
    else
    {
      JsonValue.ValueType lowerVal = lowerBound.getValueType();
      JsonValue.ValueType upperVal = upperBound.getValueType();

      if (lowerVal == JsonValue.ValueType.NULL)
      {
        if (upperVal == JsonValue.ValueType.NULL)
        {
          QueryException.throwSyntaxException(QueryMessage.EX_BETWEEN_ARGUMENT,
                                              keyString);
        }

        // This is equivalent to $lte
        tree.addToValueArray(upperBound, keyString);

        return(1);
      }
      else if (upperVal == JsonValue.ValueType.NULL)
      {
        // This is equivalent to $gte
        tree.addToValueArray(lowerBound, keyString);

        return(-1);
      }

      tree.addToValueArray(lowerBound, keyString);
      tree.addToValueArray(upperBound, keyString);
    }

    return(0);
  }

  /*
  ** Add the values for a $between, returns:
  **   -1 lower bound only, equivalent to $gte
  **   +1 upper bound only, equivalent to $lte
  **    0 both bounds, needs a $and
  */
  private int addBetweenValues(AndORTree tree, JsonQueryPath qPath,
                               EvaluatorCode modifier, boolean isNot,
                               JsonValue memberValue, String keyString)
    throws QueryException
  {
    if (!AndORTree.isJSONArray(memberValue))
      QueryException.throwSyntaxException(QueryMessage.EX_MUST_BE_ARRAY,
                                          keyString);

    SqlJsonClause sjClause;
    JsonArray jsonArr = (JsonArray)memberValue;

    if (jsonArr.size() != 2)
    {
      QueryException.throwSyntaxException(QueryMessage.EX_BETWEEN_ARGUMENT,
                                          keyString);
    }

    JsonValue lowerBound = jsonArr.get(0);
    JsonValue upperBound = jsonArr.get(1);

    if ((lowerBound == null) || (upperBound == null))
    {
      QueryException.throwSyntaxException(QueryMessage.EX_BETWEEN_ARGUMENT,
                                          keyString);
    }
    else
    {
      JsonValue.ValueType lowerVal = lowerBound.getValueType();
      JsonValue.ValueType upperVal = upperBound.getValueType();

      if (lowerVal == JsonValue.ValueType.NULL)
      {
        if (upperVal == JsonValue.ValueType.NULL)
        {
          QueryException.throwSyntaxException(QueryMessage.EX_BETWEEN_ARGUMENT,
                                              keyString);
        }

        sjClause = new SqlJsonClause(EvaluatorCode.$lte, qPath);
        sjClause.addModifier(modifier, keyString);
        sjClause.setNot(isNot);
        sjClause.addBind(upperBound, keyString);
        tree.addSqlJsonOperator(sjClause);

        return(1);
      }
      else if (upperVal == JsonValue.ValueType.NULL)
      {
        sjClause = new SqlJsonClause(EvaluatorCode.$gte, qPath);
        sjClause.addModifier(modifier, keyString);
        sjClause.setNot(isNot);
        sjClause.addBind(lowerBound, keyString);
        tree.addSqlJsonOperator(sjClause);

        return(-1);
      }

      sjClause = new SqlJsonClause(EvaluatorCode.$gte, qPath);
      sjClause.addModifier(modifier, keyString);
      sjClause.setNot(isNot);
      sjClause.addBind(lowerBound, keyString);
      tree.addSqlJsonOperator(sjClause);

      sjClause = new SqlJsonClause(EvaluatorCode.$lte, qPath);
      sjClause.addModifier(modifier, keyString);
      sjClause.setNot(isNot);
      sjClause.addBind(upperBound, keyString);
      tree.addSqlJsonOperator(sjClause);
    }

    return(0);
  }

  private void parseSpatialOperator(AndORTree tree, String key,
                                    boolean notFlag,
                                    JsonQueryPath memberPath,
                                    JsonValue memberValue)
    throws QueryException
  {
    if (this.parent != null)
    {
      // We only allow spatial clause at the outermost level
      QueryException.throwSyntaxException(QueryMessage.EX_SPATIAL_MISPLACED);
    }

    if (!AndORTree.isJSONObject(memberValue))
      QueryException.throwSyntaxException(QueryMessage.EX_MUST_BE_OBJECT,
                                          key);

    JsonObject geoObj = (JsonObject)memberValue;

    Set<Entry<String, JsonValue>> geoMembers = geoObj.entrySet();
    Iterator<Entry<String, JsonValue>> geoIter = geoMembers.iterator();

    tree.checkCompatibility(AndORNode.codeFor(key));

    boolean isNear = key.equals(SpatialClause.NEAR);
    String  sdoOperator = SpatialClause.sdoOperatorFor(key);

    String geo  = null;        // Required
    String dist = null;        // Used only for $near
    String excludeDist = null; // Used only for $near
    String unit = null;        // Used only for $near
    Boolean lax = false;
    Boolean scalarRequired = false;
    String errorClause;

    while (geoIter.hasNext())
    {
      Entry<String, JsonValue> geoItem = geoIter.next();
      String    geoKey   = geoItem.getKey();
      JsonValue geoValue = geoItem.getValue();
      JsonValue.ValueType valueType = geoValue.getValueType();

      if (geoKey.equals(SpatialClause.GEOMETRY_FIELD_NAME))
      {
        if (valueType != JsonValue.ValueType.OBJECT)
          QueryException.throwSyntaxException(QueryMessage.EX_MUST_BE_OBJECT,
                                              geoKey);
        //
        // Note that for Oracle the $geometry is not a full GeoJSON,
        // but instead is just the "geometry" portion of a GeoJSON.
        //
        // ### Should we fix that? The underlying SQL can't support it,
        // ### but we could peek into itemValue and extract the
        // ### geometry member. For now, don't do that because it
        // ### just makes extra work for the caller. The main reason
        // ### to do it would be competitor compatibility, and/or possibly
        // ### standards adherence.
        //
        geo = geoValue.toString();
        // ### Revisit to check that this conversion works as expected
      }
      else if (geoKey.equals(SpatialClause.DISTANCE_FIELD_NAME))
      {
        if (!isNear)
          QueryException.throwSyntaxException(QueryMessage.EX_OP_FIELD_NOT_ALLOWED,
                                              geoKey, key);
        else if (valueType != JsonValue.ValueType.NUMBER)
          QueryException.throwSyntaxException(QueryMessage.EX_MUST_BE_NUMBER,
                                              geoKey);

        dist = geoValue.toString();
      }
      else if (geoKey.equals(SpatialClause.EXCLUDE_FIELD_NAME))
      {
        if (!isNear)
          QueryException.throwSyntaxException(QueryMessage.EX_OP_FIELD_NOT_ALLOWED,
                                              geoKey, key);
        else if (valueType != JsonValue.ValueType.NUMBER)
          QueryException.throwSyntaxException(QueryMessage.EX_MUST_BE_NUMBER,
                                              geoKey);

        excludeDist = geoValue.toString();
      }
      else if (geoKey.equals(SpatialClause.UNIT_FIELD_NAME))
      {
        if (!isNear)
          QueryException.throwSyntaxException(QueryMessage.EX_OP_FIELD_NOT_ALLOWED,
                                              geoKey, key);
        else if (valueType != JsonValue.ValueType.STRING)
          QueryException.throwSyntaxException(QueryMessage.EX_MUST_BE_STRING,
                                              geoKey);

        unit = ((JsonString)geoValue).getString();
      }
      else if (geoKey.equals(SpatialClause.SCALAR_REQ))
      {
        if (valueType.equals(JsonValue.ValueType.TRUE))
        {
          scalarRequired = true;
        }
        else if (!valueType.equals(JsonValue.ValueType.FALSE))
        {
          QueryException.throwSyntaxException(QueryMessage.EX_BAD_PROP_TYPE,
            key, geoKey, "boolean");
        }
      }
      else if (geoKey.equals(SpatialClause.LAX))
      {
        if (valueType.equals(JsonValue.ValueType.TRUE))
        {
          lax = true;
        }
        else if (!valueType.equals(JsonValue.ValueType.FALSE))
        {
          QueryException.throwSyntaxException(QueryMessage.EX_BAD_PROP_TYPE,
            key, geoKey, "boolean");
        }
      }
      else
      {
        QueryException.throwSyntaxException(QueryMessage.EX_OP_FIELD_UNKNOWN,
                                            geoKey, key);
      }
    }

    if (lax && scalarRequired)
    {
      QueryException.throwSyntaxException(QueryMessage.EX_SCALAR_AND_LAX);
    }

    if (scalarRequired)
    {
      errorClause = AndORTree.ERROR_ON_ERROR;
    }
    else if (lax)
    {
      errorClause = AndORTree.NULL_ON_ERROR;
    }
    else
    {
      errorClause = AndORTree.ERROR_ON_ERROR_NULL_ON_EMPTY;
    }

    //
    // The $geometry is a bind variable to the SQL spatial operator
    //
    if (geo == null)
    {
      QueryException.throwSyntaxException(QueryMessage.EX_OP_FIELD_REQUIRED,
                                          key, SpatialClause.GEOMETRY_FIELD_NAME);
    }

    String geoDistanceClause = null;

    //
    // The $near operator requires a distance (unit is optional?)
    //
    if (isNear)
    {
      if (dist == null)
        QueryException.throwSyntaxException(QueryMessage.EX_SYNTAX_ERROR);

      geoDistanceClause = SpatialClause.buildDistance(dist, excludeDist, unit);
    }

    //
    // Record the information needed to build the WHERE clause component
    //
    SpatialClause geoClause = new SpatialClause(sdoOperator, notFlag,
                                                memberPath,
                                                geo, geoDistanceClause,
                                                errorClause);
    tree.addSpatialOperator(geoClause);
  }

  private void parseComplexValue(AndORTree tree,
                                 String operKey,
                                 JsonValue memberValue)
          throws QueryException
  {
    if (operKey.equals("$value"))
    {
      if (!tree.getStrictTypeMode())
        QueryException.throwSyntaxException(QueryMessage.EX_23C_AND_JSON_TYPE_REQUIRED);

      tree.addToValueArray(memberValue);
      return;
    }
    else
      QueryException.throwSyntaxException(QueryMessage.EX_UNEXPECTED_OPERATOR, operKey);
  }

  private boolean parseComplexValue(AndORTree tree, JsonObject obj, boolean allowPaths)
          throws QueryException
  {
    String    key = null;
    JsonValue item = null;

    if (obj.size() != 1) return false;
    for (Entry<String, JsonValue> entry : obj.entrySet())
    {
      key  = entry.getKey();
      item = entry.getValue();
    }

    if (!key.equals("$value"))
      return false;

    parseComplexValue(tree, key, item);
    return true;
  }

  private void parseFullTextOperator(AndORTree tree, String key,
                                     boolean notFlag, String fieldStr,
                                     JsonQueryPath memberPath,
                                     JsonValue memberValue)
    throws QueryException
  {
    if (this.parent != null)
    {
      // We only allow full text clause at the outermost level
      QueryException.throwSyntaxException(QueryMessage.EX_FULLTEXT_MISPLACED);
    }

    if (memberValue.getValueType() != JsonValue.ValueType.STRING)
      QueryException.throwSyntaxException(QueryMessage.EX_MUST_BE_STRING,
                                          fieldStr);

    tree.checkCompatibility(AndORNode.codeFor(key));

    String searchString = ((JsonString)memberValue).getString();

    tree.addContainsClause(new ContainsClause(searchString, notFlag, memberPath));
  }

  private void parseModifiedSqlJson(AndORTree tree, String key,
                                    EvaluatorCode modifier, boolean isNot,
                                    JsonQueryPath qPath, JsonObject subObj)
    throws QueryException
  {
    Set<Entry<String, JsonValue>> items = subObj.entrySet();
    Iterator<Entry<String, JsonValue>> inIter = items.iterator();

    if (restrictedSqlJson) {
      SqlJsonClause sjClause;

      // Empty objects are disallowed
      if (!inIter.hasNext()) {
        QueryException.throwSyntaxException(QueryMessage.EX_CANNOT_BE_EMPTY,
                                            key);
      }
      
      while (inIter.hasNext()) {
        Entry<String, JsonValue> item = inIter.next();
        String itemKey = item.getKey();
        JsonValue itemValue = item.getValue();

        // All restricted sqlJson operators need primitive scalars
        if (AndORTree.isJSONArray(itemValue))
          QueryException.throwSyntaxException(QueryMessage.EX_CONTAINER_NOT_ALLOWED,
                  itemKey, "array");
        else if (AndORTree.isJSONObject(itemValue))
          QueryException.throwSyntaxException(QueryMessage.EX_CONTAINER_NOT_ALLOWED,
                  itemKey, "object");

         EvaluatorCode evc = AndORNode.codeFor(itemKey);

        tree.checkCompatibility(evc);

        // Block all sqlJson operators except for the following
        // (these can be used to pick up the index on 12.1.0.2,
        // unlike the others).
        if (evc != EvaluatorCode.$eq &&
                evc != EvaluatorCode.$le &&
                evc != EvaluatorCode.$gt &&
                evc != EvaluatorCode.$lte &&
                evc != EvaluatorCode.$gte &&
                evc != EvaluatorCode.$lt &&
                evc != EvaluatorCode.$gt) {
          QueryException.throwSyntaxException(QueryMessage.EX_UNSUPPORTED_SQLJSON_OP,
                  evc.toString());
        }

        // If RHS is a number, force number bind and 'returning number'
        // json_value. This might not be correct for all operators, but
        // it's correct for the limited set of operators supported by
        // restricted sqlJson ($eq, $lt, $gt, $gt, $lt, $gte, $lte).
        // If set of supported operators is ever expanded, revisit this code
        // to make sure forcing numeric comparisons here is correct for
        // other operators.
        boolean numberArg = false;
        if (itemValue.getValueType() == JsonValue.ValueType.NUMBER) {
          numberArg = true;
        }

        // Must be a JSON scalar
        sjClause = new SqlJsonClause(evc, qPath, numberArg);
        sjClause.addBind(itemValue, itemKey);
        tree.addSqlJsonOperator(sjClause);
      }
    }
    else {
      if (modifier != null) {
        if (modifier == EvaluatorCode.$not) {
          if (isNot)
            // Chained $not modifiers is not allowed
            QueryException.throwSyntaxException(QueryMessage.EX_MOD_IS_NOT_ALLOWED,
                    key, modifier.toString());

          // We allow one level of $not at the outer level
          isNot = true;
          modifier = null;
        }
      }

      // Empty objects are disallowed
      if (!inIter.hasNext()) {
        QueryException.throwSyntaxException(QueryMessage.EX_CANNOT_BE_EMPTY,
                key);
      }

      SqlJsonClause sjClause;

      while (inIter.hasNext()) {
        Entry<String, JsonValue> item = inIter.next();
        String itemKey = item.getKey();
        JsonValue itemValue = item.getValue();

        // This is $in, $nin, $all op salary:{$in:[1,2,3]}
        // Path/object pair where the object contains operators such as
        //   "salary":{"$gt":10000}
        //   "salary":{"$in":[10000,20000,30000]}
        //   "salary":{"$not": {"$lt" : 10000} }

        int operatorType = getKeyType(itemKey, strictTypeMode);

        if (operatorType == KEY_TYPE_MODIFIER) {
          if (modifier != null) {
            // Chaining modifiers is not currently supported
            QueryException.throwSyntaxException(QueryMessage.EX_MOD_IS_NOT_ALLOWED,
                    itemKey, modifier.toString());
          }

          JsonObject modifierObj = null;

          // Target of modifier must be an object or scalar
          if (AndORTree.isJSONArray(itemValue)) {
            QueryException.throwSyntaxException(QueryMessage.EX_MUST_BE_OBJECT,
                    itemKey);
          } else if (AndORTree.isJSONObject(itemValue)) {
            // Modifier is versus an object so it will be distributed
            modifierObj = (JsonObject) itemValue;
          }

          EvaluatorCode subModifier = AndORNode.codeFor(itemKey);
          boolean subIsNot = isNot;

          if (subModifier == EvaluatorCode.$not) {
            if (isNot)
              // Chained $not modifiers is not allowed
              QueryException.throwSyntaxException(QueryMessage.EX_MOD_IS_NOT_ALLOWED,
                      key, modifier.toString());
            subIsNot = true;
            subModifier = null;
          }

          tree.checkCompatibility(subModifier);

          if (modifierObj != null) {
            // Special handling for modifier clauses
            parseModifiedSqlJson(tree, key, subModifier, subIsNot,
                                 qPath, modifierObj);
          } else {
            // Modifier against implied $eq using scalar item
            EvaluatorCode joper = (isNot) ? EvaluatorCode.$ne : EvaluatorCode.$eq;

            sjClause = new SqlJsonClause(joper, qPath);
            sjClause.addModifier(subModifier, key);
            sjClause.addBind(itemValue, key);
            tree.addSqlJsonOperator(sjClause);
          }
        } else if (operatorType == KEY_TYPE_OPERATOR) {
          EvaluatorCode evc = AndORNode.codeFor(itemKey);

          tree.checkCompatibility(evc);

          switch (evc) {
            case $all:
              QueryException.throwSyntaxException(QueryMessage.EX_OPERATOR_NOT_ALLOWED,
                      itemKey);

            case $between:

              int hi_lo = addBetweenValues(tree, qPath, modifier, isNot,
                                           itemValue, itemKey);
              break;

            case $in:
            case $nin:

              // These operators take an array
              if (!AndORTree.isJSONArray(itemValue))
                QueryException.throwSyntaxException(QueryMessage.EX_MUST_BE_ARRAY,
                        itemKey);

              JsonArray barr = (JsonArray) itemValue;
              if (barr.size() == 0)
                QueryException.throwSyntaxException(QueryMessage.EX_CANNOT_BE_EMPTY,
                        itemKey);

              sjClause = new SqlJsonClause(EvaluatorCode.$in, qPath);
              sjClause.addModifier(modifier, key);
              if (evc == EvaluatorCode.$nin)
                sjClause.setNot(!isNot);
              else
                sjClause.setNot(isNot);
              sjClause.addBindArray(barr, itemKey);
              tree.addSqlJsonOperator(sjClause);

              break;

            default:

              // All other operators need primitive scalars

              if (AndORTree.isJSONArray(itemValue))
                QueryException.throwSyntaxException(QueryMessage.EX_CONTAINER_NOT_ALLOWED,
                        itemKey, "array");
              else if (AndORTree.isJSONObject(itemValue))
                QueryException.throwSyntaxException(QueryMessage.EX_CONTAINER_NOT_ALLOWED,
                        itemKey, "object");

              // Must be a JSON scalar
              sjClause = new SqlJsonClause(evc, qPath, false);
              sjClause.addModifier(modifier, key);
              sjClause.setNot(isNot);
              sjClause.addBind(itemValue, itemKey);
              tree.addSqlJsonOperator(sjClause);

          }
        } else {
          QueryException.throwSyntaxException(QueryMessage.EX_OPERATOR_NOT_ALLOWED,
                  itemKey);
        }
      }
    }
  }

  /*
   * Parse a $sqlJson clause, which is an object containing field/clause
   * pairs. It has restrictions:
   *  - downscoping is not allowed
   *  - $not is not supported (yet) as a modifier
   *  - $all is not supported
   */
  private void parseSqlJson(AndORTree tree, JsonObject sqlObj)
    throws QueryException
  {
    Set<Entry<String, JsonValue>> members = sqlObj.entrySet();
    Iterator<Entry<String, JsonValue>> memberIter = members.iterator();

    while (memberIter.hasNext())
    {
      Entry<String, JsonValue> entry = memberIter.next();

      JsonValue memberValue = entry.getValue();
      String    key = entry.getKey();
      int       keyType = getKeyType(key, strictTypeMode);

      // JSON_VALUE must be a simple path/expression pair
      if (keyType != KEY_TYPE_LEAF)
        QueryException.throwSyntaxException(QueryMessage.EX_UNEXPECTED_OPERATOR,
                                            key);

      JsonQueryPath qPath = new JsonQueryPath(key);

      SqlJsonClause sjClause;

      // "salary"
      if (AndORTree.isJSONPrimitive(memberValue))
      {
        boolean numberArg = false;
        if (memberValue.getValueType() == JsonValue.ValueType.NUMBER)
        {
          numberArg = true;
        }

        sjClause = new SqlJsonClause(EvaluatorCode.$eq, qPath, numberArg);
        sjClause.addBind(memberValue, key);
        tree.addSqlJsonOperator(sjClause);
      }
      // Path/array pair interpreted as $in, such as:
      //   "salary":[10000,20000,30000]
      else if (!restrictedSqlJson && AndORTree.isJSONArray(memberValue))
      {
        JsonArray barr = (JsonArray)memberValue;
        if (barr.size() == 0)
          QueryException.throwSyntaxException(QueryMessage.EX_CANNOT_BE_EMPTY,
                                              key);

        sjClause = new SqlJsonClause(EvaluatorCode.$in, qPath);
        sjClause.addBindArray(barr, "$eq");
        tree.addSqlJsonOperator(sjClause);
      }
      // Path/object inequalities or grouped inequalities, such as:
      //   "salary":{"$gt":10, "$lt":100}
      //   "salary":{"$startsWith":"Joe"}
      //   "salary":{"$exists": true}
      else if (AndORTree.isJSONObject(memberValue))
      {
        parseModifiedSqlJson(tree, key, null, false, qPath,
                             (JsonObject)memberValue);
      }
      else
      {
        // Should never happen
        // The above three cases cover all possible
        // types of "memberValue" (primitive, array, and object).
        QueryException.throwSyntaxException(QueryMessage.EX_SYNTAX_ERROR);
      }
    }
  }

  private void parseModifiedOperators(AndORTree tree,
                                      String pushDown, String itemKey,
                                      JsonQueryPath memberPath, JsonObject modifierObj)
    throws QueryException
  {
    EvaluatorCode evc = AndORNode.codeFor(itemKey);
    boolean       isNot = (evc == EvaluatorCode.$not);
    String        modifier = pushDown;

    // Iterate over the members
    Set<Entry<String, JsonValue>> modMember = modifierObj.entrySet();
    Iterator<Entry<String, JsonValue>> modIter = modMember.iterator();

    if (!modIter.hasNext())
      QueryException.throwSyntaxException(QueryMessage.EX_CANNOT_BE_EMPTY,
                                          itemKey);

    // The $not modifier is handled as a special-case (using wrappers)
    // It's allowed to follow another modifier, which is "pushed down"
    if (!isNot)
    {
      modifier = Evaluator.Modifier.get(evc);

      // Bookmark this set of descendants for special formatting
      if (evc != null)
        tree.setValBookmark(evc);
    }

    while (modIter.hasNext())
    {
      Entry<String, JsonValue> modEntry = modIter.next();

      AndORNode node = new AndORNode(this);
      Predicate modifiedPred = new Predicate(memberPath, modifier);
      this.children.add(node);
      node.predicate = modifiedPred;
      if (isNot) node.setNotOperation();

      String operKey = modEntry.getKey();
      EvaluatorCode opcode = AndORNode.codeFor(operKey);

      JsonValue opEntryValue = modEntry.getValue();

      tree.checkCompatibility(opcode);

      node.setEval(opcode);

      switch (opcode)
      {
      case $all:
        if (!AndORTree.isJSONArray(opEntryValue))
          QueryException.throwSyntaxException(QueryMessage.EX_MUST_BE_ARRAY,
                                              operKey);
        // Convert empty array $all into $in
        if (((JsonArray)opEntryValue).size() == 0)
        {
          opcode = EvaluatorCode.$in;
          node.setEval(opcode);
        }
        // FALLTHROUGH
      case $in:
      case $nin:
        if (!AndORTree.isJSONArray(opEntryValue))
          QueryException.throwSyntaxException(QueryMessage.EX_MUST_BE_ARRAY,
                                              operKey);
        node.addArrayValues(tree, opEntryValue, operKey, true);
        break;

      case $near:
      case $within:
      case $intersects:
        // Special handling for spatial clauses
        parseSpatialOperator(tree, operKey, isNot,
                             memberPath, opEntryValue);
        // Since this is handled by special logic, remove the node
        this.children.remove(node);
        break;

      case $contains:
        // Special handling for full text clauses
        parseFullTextOperator(tree, operKey, isNot, "$contains",
                              memberPath, opEntryValue);
        // Since this is handled by special logic, remove the node
        this.children.remove(node);
        break;

      case $between:
        int hi_lo = node.addBetweenValues(tree, opEntryValue,
                                          memberPath.toQueryString());

        if (hi_lo < 0)
        {
          node.setEval(EvaluatorCode.$gte);
        }
        else if (hi_lo > 0)
        {
          node.setEval(EvaluatorCode.$lte);
        }
        else
        {
          AndORNode subParent = this;

          // Normally $between is distributed as an AND of two comparisons.
          // We can't do this underneath an OR node, so the caller is obliged
          // to ensure an AND node prior to calling this routine to process the
          // modifier. The exception is if the modifier is $not, which may
          // be under an AND node (if $between is the only operator), or
          // may be under an OR (created so that the $not could be distributed).
          // Normally "!((X >= A) && (X <= B))" is distributed via an OR node as
          //* "!(X >= A) || !(X <= B)". However, if we are under an AND node and
          //* trying to distribute $not for between, we need to introduce an OR
          // node.
          if ((isNot) && (parent.eval != EvaluatorCode.$or))
          {
            node.setEval(EvaluatorCode.$or);
            node.clearNotOperation();
            node.numVals = 0;
            subParent = node;

            node = new AndORNode(subParent);
            subParent.children.add(node);
            node.predicate = modifiedPred;
            if (isNot) node.setNotOperation();
          }

          // Make the first node $gte
          node.setEval(EvaluatorCode.$gte);
          node.numVals = 1;

          // Make a second node for $lte
          node = new AndORNode(subParent);
          subParent.children.add(node);
          node.setEval(EvaluatorCode.$lte);
          node.numVals = 1;
          node.predicate = modifiedPred;
          if (isNot) node.setNotOperation();
        }
        break;

      case $double:
      case $number:
      case $string:
      case $date:
      case $timestamp:
      case $boolean:
      case $binary:
      case $numberOnly:
      case $stringOnly:
      case $booleanOnly:
      case $binaryOnly:
      case $dateTimeOnly:
      case $idOnly:
      case $ceiling:
      case $floor:
      case $abs:
      case $upper:
      case $lower:
      case $type:
      case $length:
      case $size:

        // Chaining modifiers is not currently supported
        QueryException.throwSyntaxException(QueryMessage.EX_MOD_IS_NOT_ALLOWED,
                                            itemKey, operKey);
        break;

      // Currently don't allow modifiers around $and, $or, $nor, $not,
      // but might allow that in the future
      case $not:

        // We don't allow $not around another $not
        if (isNot)
          QueryException.throwSyntaxException(QueryMessage.EX_NOT_IS_NOT_ALLOWED,
                                              operKey);

        if (!AndORTree.isJSONObject(opEntryValue))
          QueryException.throwSyntaxException(QueryMessage.EX_MUST_BE_OBJECT,
                                              operKey);

        AndORNode modifierParent = this;

        // If necessary add an ORing node because it's NOTed
        if (this.eval != EvaluatorCode.$or)
        {
          node.setEval(EvaluatorCode.$or);
          node.clearNotOperation();
          modifierParent = node;
        }
        else
        {
          this.children.remove(node);
        }

        modifierParent.parseModifiedOperators(tree, modifier, operKey, memberPath,
                                              (JsonObject)opEntryValue);

        break;
      case $mod:
        
        BigDecimal divisor = validateModOperator(tree, opEntryValue, operKey);
        node.predicate = new Predicate(memberPath, Evaluator.Modifier.get(EvaluatorCode.$mod), divisor.toString());
        node.setEval(EvaluatorCode.$eq);
        break;
      case $and:
      case $or:
      case $nor:
        // modifiers don't make sense around $orderby/$id
      case $orderby:
      case $id:
        QueryException.throwSyntaxException(QueryMessage.EX_MOD_IS_NOT_ALLOWED,
                                            itemKey, operKey);
        break;

      // Rewrite $not($ne) to $eq, which will avoid
      // generating !(!(field == value)).
      case $ne:
        if (isNot)
        {
          node.setEval(EvaluatorCode.$eq);
          node.clearNotOperation();
        }
        // FALLTHROUGH

      default:

        boolean isNotOperatorWithValue = false;
        if (AndORTree.isJSONObject(opEntryValue))
          isNotOperatorWithValue = parseComplexValue(tree, (JsonObject)opEntryValue, true);
        if (!isNotOperatorWithValue)
          AndORNode.checkScalarType(opcode, operKey,
                                  tree.addToValueArray(opEntryValue, operKey));

        break;
      }

      //
      // Additional rewrites for $in and $nin
      //
      switch (opcode)
      {
      case $nin:
        // $not($nin) is equivalent to $in
        if ((!useSqlNotIn) && (isNot))
        {
          // If there's no native $nin, use $in
          node.setEval(EvaluatorCode.$in);
          node.clearNotOperation();
        }
        break;

      case $in:
        // $not($in) is equivalent to $nin
        if ((useSqlNotIn) && (isNot))
        {
          // Use $nin if it's available or if we're expanding
          // the clauses anyway (the $in work-around)
          node.setEval(EvaluatorCode.$nin);
          node.clearNotOperation();
        }
        break;

      default:
        break;
      }
    }

    tree.endValBookmark();
  }

  private void adjustPredicate(JsonQueryPath memberPath, JsonValue memberValue)
    throws QueryException
  {
    JsonValue.ValueType mval = memberValue.getValueType();
    if (mval == JsonValue.ValueType.NUMBER)
    {
      this.predicate = new Predicate(memberPath, "numberOnly");
    }
    else if ((mval == JsonValue.ValueType.TRUE) ||
             (mval == JsonValue.ValueType.FALSE))
    {
      this.predicate = new Predicate(memberPath, "booleanOnly");
    }
    // ### We can't reasonably assert stringOnly() without support
    // ### for the date/time, binary, and ID types in the JSON DOM.
  }

  void addNode(AndORTree tree, Entry<String, JsonValue> entry, boolean strictTypeMode)
    throws QueryException
  {
    AndORNode node = null;
    String key = entry.getKey();
    JsonValue memberValue;

    int keyType = AndORNode.getKeyType(key, strictTypeMode);

    // This takes {$or/$and/$nor: [{key1:value1}, {key2:value2}, ...,
    //                             {keyN: valueN}]} as input
    switch (keyType)
    {
    case KEY_TYPE_OR:
    case KEY_TYPE_AND:
    case KEY_TYPE_NOR:

      AndORNode thisParent = this;

      if (keyType == KEY_TYPE_AND)
      {
        // If not under an existing $and parent, add a sub-node
        if (getEval() != EvaluatorCode.$and)
        {
          AndORNode andNode = new AndORNode(this);
          andNode.setEval(EvaluatorCode.$and);
          children.add(andNode);
          thisParent = andNode;
        }
      }
      else if (keyType == KEY_TYPE_OR)
      {
        // If not under an existing $or parent, add a sub-node
        if ((getEval() != EvaluatorCode.$or) || (getNotOperation()))
        {
          AndORNode orNode = new AndORNode(this);
          orNode.setEval(EvaluatorCode.$or);
          children.add(orNode);
          thisParent = orNode;
        }
      }
      else if (keyType == KEY_TYPE_NOR)
      {
        // If not under an existing $nor parent, add a sub-node
        if ((getEval() != EvaluatorCode.$or) || (!getNotOperation()))
        {
          // $nor is synthesized as not(or(...))
          AndORNode orNode = new AndORNode(this);
          orNode.setEval(EvaluatorCode.$or);
          children.add(orNode);
          thisParent = orNode;
          orNode.setNotOperation();
        }
      }
      // ### Might want to add $nand operator in the future

      if (!AndORTree.isJSONArray(entry.getValue()))
      {
        QueryException.throwSyntaxException(QueryMessage.EX_MUST_BE_ARRAY,
                                            key);
      }
      JsonArray jArr = (JsonArray)entry.getValue();
      Iterator<JsonValue> iter = jArr.iterator();

      if (!iter.hasNext())
      {
        QueryException.throwSyntaxException(QueryMessage.EX_CANNOT_BE_EMPTY,
                                            key);
      }

      while (iter.hasNext()) // for each object in the array
      {
        JsonValue jElem = iter.next();
        // This node must be an object
        if (jElem.getValueType() != JsonValue.ValueType.OBJECT)
          QueryException.throwSyntaxException(QueryMessage.EX_MUST_BE_OBJECT,
                                              key);
        JsonObject jObj = (JsonObject)jElem;
        Set<Entry<String, JsonValue>> members = jObj.entrySet();
        Iterator<Entry<String, JsonValue>> memberIter = members.iterator();

        // By default the clauses are parented under the current container
        AndORNode clauseParent = thisParent;
        if (clauseParent.getEval() != EvaluatorCode.$and)
        {
          // However, if this isn't found under a $and node
          // create a sub-node (because by default conditions are $and)
          clauseParent = new AndORNode(thisParent);
          clauseParent.setEval(EvaluatorCode.$and);
          thisParent.children.add(clauseParent);
          // ### This implies an extra level of parentheses
          //     in simple cases where there's a single clause to follow.
          //     Ideally, we'd construct the child and then count the
          //     nodes within it before deciding whether or not to include
          //     this node, but the current structure of this code makes that
          //     difficult.
          //     Instead, at JSON_EXISTS clause generation time, we will avoid adding
          //     parens around an AND that has only one descendant.
        }

        // Empty objects are disallowed
        if (!memberIter.hasNext())
        {
          QueryException.throwSyntaxException(QueryMessage.EX_CANNOT_BE_EMPTY,
                                              key);
        }

        while (memberIter.hasNext()) 
        {
          clauseParent.addNode(tree, memberIter.next(), strictTypeMode);
        }
      }

      break;

    case KEY_TYPE_SQLJSON:

      if (this.parent != null)
      {
        // We only allow the $sqlJson clause at the outermost level
        QueryException.throwSyntaxException(QueryMessage.EX_SQL_JSON_MISPLACED,
                                            key);
      }

      memberValue = entry.getValue();

      if (!AndORTree.isJSONObject(memberValue))
        QueryException.throwSyntaxException(QueryMessage.EX_MUST_BE_OBJECT,
                                            key);

      parseSqlJson(tree, (JsonObject)memberValue);

      break;

    case KEY_TYPE_ID:

      if (this.parent != null)
      {
        // We only allow the $id clause at the outermost level
        QueryException.throwSyntaxException(QueryMessage.EX_ID_MISPLACED);
      }

      if (tree.hasKeys())
      {
        // Disallow multiple $id conditions
        QueryException.throwSyntaxException(
                       QueryMessage.EX_MULTIPLE_ID_CLAUSES);
      }

      // Special processing for $id since it is a separate where clause
      memberValue = entry.getValue();
      if (AndORTree.isJSONPrimitive(memberValue))
      {
        String keyString = tree.getScalarKey(memberValue);
        tree.addToKeys(keyString);
      }
      else if (AndORTree.isJSONArray(memberValue))
      {
        JsonArray jsonArr = (JsonArray)memberValue;
        Iterator<JsonValue> iterArr = jsonArr.iterator();

        while (iterArr.hasNext()) 
        {
          JsonValue arrElem = iterArr.next();

          String keyString = tree.getScalarKey(arrElem);
          tree.addToKeys(keyString);
        }

        if (tree.getKeys().size() <= 0)
        {
          QueryException.throwSyntaxException(QueryMessage.EX_CANNOT_BE_EMPTY,
                                              key);
        }
      }
      else
      {
        QueryException.throwSyntaxException(QueryMessage.EX_NON_SCALAR_KEY);
      }

      break;

    case KEY_TYPE_SPATIAL:
    case KEY_TYPE_FULLTEXT:
    case KEY_TYPE_OPERATOR:
    case KEY_TYPE_MODIFIER:

      QueryException.throwSyntaxException(QueryMessage.EX_UNEXPECTED_OPERATOR,
                                          key);

    default:

      // This takes key:value as input

      String memberKey = key;
      memberValue = entry.getValue();

      // Simple path/value pairs interpreted as equality matches, such as:
      //   "salary":10
      if (AndORTree.isJSONPrimitive(memberValue)) 
      {
        node = new AndORNode(this);
        node.predicate = new Predicate(new JsonQueryPath(memberKey));
        node.setEval(EvaluatorCode.$eq);
        tree.addToValueArray(memberValue, memberKey);

        if (tree.strictTypeMatching)
          node.adjustPredicate(new JsonQueryPath(memberKey), memberValue);

        this.children.add(node);
      }
      // Path/object inequalities or grouped inequalities, such as:
      //   "salary":{"$gt":10, "$lt":100}
      //   "salary":{"$startsWith":"Joe"}
      //   "salary":{"$exists": true}
      else if (AndORTree.isJSONObject(memberValue))
      {
        JsonObject jObj = (JsonObject)memberValue;
        Set<Entry<String, JsonValue>> members = jObj.entrySet();
        Iterator<Entry<String, JsonValue>> inIter = members.iterator();
        int nmembers = members.size();

        // Empty objects are disallowed
        if (nmembers == 0)
        {
          QueryException.throwSyntaxException(QueryMessage.EX_CANNOT_BE_EMPTY,
                                              memberKey);
        }

        JsonQueryPath memberPath = new JsonQueryPath(memberKey);
        JsonQueryPath originalPath = memberPath;

        boolean notDownscoped = false;
        boolean isFirstIter = true;

        AndORNode newParent = this;

        while (inIter.hasNext())
        {
          Entry<String, JsonValue> item = inIter.next();
          String    itemKey   = item.getKey();
          JsonValue itemValue = item.getValue();

          // This is $in, $nin, $all op salary:{$in:[1,2,3]}
          // Path/object pair where the object contains operators such as
          //   "salary":{"$gt":10000}
          //   "salary":{"$in":[10000,20000,30000]}
          //   "salary":{"$not": {"$lt" : 10000} }

          int operatorType = getKeyType(itemKey, strictTypeMode);

          JsonObject nestedObj = null;

          // Bug 32931540 - do not distribute field names
          if (isFirstIter)
          {
            isFirstIter = false;

            if (operatorType != KEY_TYPE_LEAF)
            {
              boolean distributePath = (nmembers == 1);
              if (distributePath)
              {
                if (itemKey.equals("$between")) distributePath = false;
                else if (getKeyType(itemKey, strictTypeMode) == KEY_TYPE_MODIFIER &&
                    jObj.get(itemKey).getValueType() == ValueType.OBJECT)
                {
                  JsonObject objectAfterModifier = jObj.getJsonObject(itemKey);
                  // This handles the following case:
                  // path : {$someModifier : {operand object has more than one key}}
                  // In this case, we don't want to distribute the operand object.
                  // Instead, we keep it all under the predicate following "path".
                  if (objectAfterModifier.size() > 1)
                  {
                    distributePath = false;
                  }
                  // This handles the following case:
                  // path : {$someModifier : {$not : {operand object has more than one key}}
                  // In this case, we don't want to distribute the operand object.
                  // Instead, we keep it all under the predicate following "path".
                  else if (objectAfterModifier.containsKey("$not"))
                  {
                     JsonObject objectAfterModifierAndNot = objectAfterModifier.getJsonObject("$not");
                     if (objectAfterModifierAndNot.size() > 1)
                     {
                       distributePath = false;
                     }
                  }
                }
              }
              if (!distributePath)
              {
                newParent = new AndORNode(this);
                newParent.setEval(EvaluatorCode.$and);
                this.children.add(newParent);
                newParent.downScopePath = memberPath;
                memberPath = new JsonQueryPath();
              }
            }
          }

          if (operatorType == KEY_TYPE_MODIFIER)
          {
            // ### For now, disallow mixing ordinary comparisons/$exists here.
            //     We can't get here after a downscope because it consumes
            //     the rest of the iterator. But we could reach this point
            //     prior to a clause that's interpreted as a downscope.
            //
            //     Example:
            //       "address":{"$gt":123, "state":"MA", "$lt":456}
            //
            //     For now, set a flag indicating that we should not
            //     expect an ordinary field which would be downscoped.
            notDownscoped = true;

            JsonObject modifierObj = null;
            JsonArray  modifiedArr = null;

            // Rewrite an array as an implied $and or $eq
            if (AndORTree.isJSONArray(itemValue) && !itemKey.equals("$value"))
              modifiedArr = (JsonArray)itemValue;

            // Target of modifier must be an object or scalar
            else if (AndORTree.isJSONObject(itemValue))
            {
              // Modifier is versus an object so it will be distributed
              modifierObj = (JsonObject)itemValue;
            }

            EvaluatorCode evc = AndORNode.codeFor(itemKey);

            tree.checkCompatibility(evc);

            AndORNode clauseParent = newParent;

            // If $not has more than one clause we need an ORing parent
            if (evc == EvaluatorCode.$not)
            {
              // $not must be followed by an object
              if (modifierObj == null)
                QueryException.throwSyntaxException(QueryMessage.EX_MUST_BE_OBJECT,
                                                    itemKey);

              if (modifierObj.size() > 1)
              {
                clauseParent = new AndORNode(newParent);
                clauseParent.setEval(EvaluatorCode.$or);
                newParent.children.add(clauseParent);
              }
            }
            // Otherwise we must have an ANDing parent
            else if (newParent.getEval() != EvaluatorCode.$and)
            {
              if (modifierObj != null)
              {
                clauseParent = new AndORNode(newParent);
                clauseParent.setEval(EvaluatorCode.$and);
                newParent.children.add(clauseParent);
              }
            }

            if (evc == EvaluatorCode.$value)
            {
              AndORNode complexNode = new AndORNode(clauseParent);
              clauseParent.children.add(complexNode);
              complexNode.parseComplexValue(tree, itemKey, itemValue);
              complexNode.setEval(EvaluatorCode.$eq);
              complexNode.predicate = new Predicate(memberPath,
                                                    Evaluator.Modifier.get(evc));
            } 
            else if (evc == EvaluatorCode.$mod){
              BigDecimal divisor = validateModOperator(tree, modifiedArr, itemKey);
              
              AndORNode eqNode = new AndORNode(clauseParent);
              clauseParent.children.add(eqNode);
              eqNode.predicate = new Predicate(memberPath,
                  Evaluator.Modifier.get(evc), divisor.toString());
              eqNode.setEval(EvaluatorCode.$eq);
            } 
            else if (modifierObj != null)
            {
              // Special handling for modifier clauses (including $not)
              clauseParent.parseModifiedOperators(tree, null, itemKey,
                                                  memberPath, modifierObj);
            }
            else if (modifiedArr != null)
            {
              // ### To-do: allow this as an implied $all
              QueryException.throwSyntaxException(QueryMessage.EX_MUST_BE_OBJECT,
                                                  itemKey);
            }
            else
            {
              // Modifier against implied $eq using scalar item
              AndORNode eqNode = new AndORNode(clauseParent);
              clauseParent.children.add(eqNode);
              eqNode.predicate = new Predicate(memberPath,
                                               Evaluator.Modifier.get(evc));
              eqNode.setEval(EvaluatorCode.$eq);

              tree.setValBookmark(evc);
              AndORNode.checkScalarType(eqNode.getEval(),
                                        itemKey,
                                        tree.addToValueArray(itemValue,
                                        Evaluator.Modifier.get(eqNode.getEval())));
              tree.endValBookmark();
            }
          }
          else if (operatorType == KEY_TYPE_OPERATOR)
          {
            EvaluatorCode evc = AndORNode.codeFor(itemKey);

            tree.checkCompatibility(evc);

            // ### For now, disallow mixing ordinary comparisons/$exists here.
            //     We can't get here after a downscope because it consumes
            //     the rest of the iterator. But we could reach this point
            //     prior to a clause that's interpreted as a downscope.
            //
            //     Example:
            //       "address":{"$gt":123, "state":"MA", "$lt":456}
            //
            //     For now, set a flag indicating that we should not
            //     expect an ordinary field which would be downscoped.

            notDownscoped = true;

            node = new AndORNode(newParent);
            newParent.children.add(node);

            switch (evc)
            {
            case $between:

              int hi_lo = node.addBetweenValues(tree, itemValue, itemKey);

              JsonArray onlyArray = (JsonArray)itemValue;

              if (hi_lo < 0)
              {
                node.setEval(EvaluatorCode.$gte);
                node.predicate = new Predicate(memberPath);
                if (tree.strictTypeMatching)
                  node.adjustPredicate(memberPath, onlyArray.get(0));
              }
              else if (hi_lo > 0)
              {
                node.setEval(EvaluatorCode.$lte);
                node.predicate = new Predicate(memberPath);
                if (tree.strictTypeMatching)
                  node.adjustPredicate(memberPath, onlyArray.get(1));
              }
              else
              {
                // Make an AND of the conditions
                // ### In theory we could avoid this if already under a $and
                node.setEval(EvaluatorCode.$and);
                node.numVals = 0;

                AndORNode boundaryNode;

                boundaryNode = new AndORNode(node);
                node.children.add(boundaryNode);
                boundaryNode.setEval(EvaluatorCode.$gte);
                boundaryNode.predicate = new Predicate(memberPath);
                boundaryNode.numVals = 1;
                if (tree.strictTypeMatching)
                  boundaryNode.adjustPredicate(memberPath, onlyArray.get(0));

                boundaryNode = new AndORNode(node);
                node.children.add(boundaryNode);
                boundaryNode.setEval(EvaluatorCode.$lte);
                boundaryNode.predicate = new Predicate(memberPath);
                boundaryNode.numVals = 1;
                if (tree.strictTypeMatching)
                  boundaryNode.adjustPredicate(memberPath, onlyArray.get(1));
              }

              break;

            case $in:
            case $nin:
            case $all:

              // These operators all take arrays
              if (!AndORTree.isJSONArray(itemValue))
                QueryException.throwSyntaxException(QueryMessage.EX_MUST_BE_ARRAY,
                                                    itemKey);
              else if (evc == EvaluatorCode.$all)
              {
                // Convert empty array $all into $in
                if (((JsonArray)itemValue).size() == 0)
                  evc = EvaluatorCode.$in;
              }
              node.addArrayValues(tree, itemValue, itemKey, true);

              // If there's no native SQL not_in operator,
              // but there is a native in operator,
              // rewrite $nin as $not($in(<expr>))

              if (!useSqlNotIn && (evc == EvaluatorCode.$nin))
              {
                node.setEval(EvaluatorCode.$in);
                node.setNotOperation();
              }
              else
              {
                node.setEval(evc);
              }

              node.predicate = new Predicate(memberPath);
              break;
            case $eq:
            case $ne:
            case $gt:
            case $lt:
            case $gte:
            case $lte:
              if (AndORTree.isJSONObject(itemValue))
              {
                if (parseComplexValue(tree, (JsonObject)itemValue, true))
                {
                  node.predicate = new Predicate(memberPath);
                  node.setEval(evc);
                  break;
                }
              }

            default:

              // All other operators need primitive scalars

              if (AndORTree.isJSONArray(itemValue))
                QueryException.throwSyntaxException(QueryMessage.EX_CONTAINER_NOT_ALLOWED,
                                                    itemKey, "array");
              else if (AndORTree.isJSONObject(itemValue))
                QueryException.throwSyntaxException(QueryMessage.EX_CONTAINER_NOT_ALLOWED,
                                                    itemKey, "object");

              // Must be a JSON scalar

              node.predicate = new Predicate(memberPath);
              node.setEval(evc);

              if (tree.strictTypeMatching)
                node.adjustPredicate(memberPath, itemValue);

              AndORNode.checkScalarType(evc, itemKey,
                                        tree.addToValueArray(itemValue, itemKey));
            }
          }
          // Path/object pair where the object contains ordinary fields
          // interpreted as a downscoped operation, such as:
          //   "address":{"city":"Boston","state":"MA"}
          //
          // Mixing downscoped operation with regular operation not allowed
          else if ((getKeyType(itemKey, strictTypeMode) == KEY_TYPE_LEAF) && (!notDownscoped))
          {
            node = new AndORNode(newParent);
            newParent.children.add(node);

            node.setEval(EvaluatorCode.$and);
            node.downScopePath = memberPath;

            // add the already parsed item
            node.addNode(tree, item, strictTypeMode);

            // loop and recurse over the remaining items
            while (inIter.hasNext())
              node.addNode(tree, inIter.next(), strictTypeMode);
          }
          //
          // Spatial extension
          //
          else if (operatorType == KEY_TYPE_SPATIAL)
          {
            // ### For now, disallow spatial in a distribution
            if (newParent != this)
              QueryException.throwSyntaxException(QueryMessage.EX_OPERATOR_NOT_ALLOWED, itemKey);
            // Special handling for spatial clauses
            parseSpatialOperator(tree, itemKey, false,
                                 originalPath, itemValue);
          }
          //
          // Full Text extension
          //
          else if (operatorType == KEY_TYPE_FULLTEXT)
          {
            // ### For now, disallow $contains in a distribution
            if (newParent != this)
              QueryException.throwSyntaxException(QueryMessage.EX_OPERATOR_NOT_ALLOWED, itemKey);
            // Special handling for full text clauses
            parseFullTextOperator(tree, itemKey, false, memberKey,
                                  originalPath, itemValue);
          }
          else
          {
            // ### The EX_OPERATOR_NOT_ALLOWED exception message says that the only
            //     allowed keys here are:
            //
            //     comparison operators
            //     $exists
            //     $not
            //
            //     If more possibilities are allowed here in the
            //     future, the exception message needs to be adjusted
            //     accordingly.
            QueryException.throwSyntaxException(QueryMessage.EX_OPERATOR_NOT_ALLOWED, itemKey);
          }
        }
      }
      // Path/array pair interpreted as $all, such as:
      //   "salary":[10000,20000,30000]
      else if (AndORTree.isJSONArray(memberValue))
      {
        JsonQueryPath memberPath = new JsonQueryPath(memberKey);
        node = new AndORNode(this);
        node.addArrayValues(tree, memberValue, memberKey, true);
        if (((JsonArray)memberValue).size() == 0)
          node.setEval(EvaluatorCode.$in);
        else
          node.setEval(EvaluatorCode.$all);
        node.predicate = new Predicate(memberPath);
        if (tree.strictTypeMatching)
        {
          JsonArray onlyArray = (JsonArray)memberValue;
          boolean nonNumeric = (onlyArray.size() == 0);
          boolean nonBoolean = (onlyArray.size() == 0);
          for (JsonValue jv : onlyArray)
          {
            switch (jv.getValueType())
            {
            case NUMBER:
              nonBoolean = true;
              break;
            case TRUE:
            case FALSE:
              nonNumeric = true;
              break;
            default:
              nonBoolean = nonNumeric = false;
            }
            if (nonBoolean && nonNumeric) break;
          }
          if (!nonNumeric)
            node.predicate = new Predicate(memberPath, "numberOnly");
          else if (!nonBoolean)
            node.predicate = new Predicate(memberPath, "booleanOnly");
        }
        this.children.add(node);
      }
      else
      {
        // Should never happen
        // The above three cases cover all possible
        // types of "memberValue" (primitive, array, and object).
        QueryException.throwSyntaxException(QueryMessage.EX_SYNTAX_ERROR);
      }
      break;
    }
  }
  
private BigDecimal validateModOperator(AndORTree tree, JsonValue input, String itemKey) throws QueryException
{
  
  if (input == null || input.getValueType() != JsonValue.ValueType.ARRAY)
    QueryException.throwSyntaxException(QueryMessage.EX_INVALID_MOD_INPUT);
  
  JsonArray array = input.asJsonArray();
  
  if (array.size() != 2)
    QueryException.throwSyntaxException(QueryMessage.EX_INVALID_MOD_INPUT);
  
  JsonValue divisor = array.get(0), remainder = array.get(1);
  
  if (divisor.getValueType() != JsonValue.ValueType.NUMBER ||
      remainder.getValueType() != JsonValue.ValueType.NUMBER)
    QueryException.throwSyntaxException(QueryMessage.EX_REMAINDER_OR_DIVISOR_NOT_A_NUMBER);
  
  BigDecimal roundDivisor = null, roundRemainder = null;
  
  roundRemainder = validateModNumericValue(remainder);
  roundDivisor   = validateModNumericValue(divisor);
  
  if (roundDivisor.equals(BigDecimal.ZERO))
    QueryException.throwSyntaxException(QueryMessage.EX_DIVISOR_CANNOT_BE_ZERO);
  
  tree.addToValueArray(roundRemainder);
  
  return roundDivisor;
}
  
  private BigDecimal validateModNumericValue(JsonValue val) throws QueryException {
    BigDecimal roundValue = null;
    try {
      double num = Double.parseDouble(val.toString());
      num = Math.floor(num);
      if (num < Long.MIN_VALUE || num > Long.MAX_VALUE || num == Double.NaN)
        QueryException.throwSyntaxException(QueryMessage.EX_OUT_OF_RANGE_MOD_NUMERIC_VALUE);
      roundValue = new BigDecimal(num);
    } catch (NumberFormatException e) {
      QueryException.throwSyntaxException(QueryMessage.EX_REMAINDER_OR_DIVISOR_NOT_A_NUMBER);
    }
    
    return roundValue;
    
  }

  private void appendPathOp(String predChar, StringBuilder sb,
                            String predPath, String op)
  {
    sb.append(predChar);
    sb.append(predPath);
    if (op != null)
    {
      sb.append(" ");
      sb.append(op);
    }
  }

  private void appendBind(StringBuilder sb, int bindnum)
    throws QueryException
  {
    sb.append(" $B");
    sb.append(bindnum);
  }

  private void appendBind(StringBuilder sb, AndORTree tree)
    throws QueryException
  {
    appendBind(sb, tree.getNextBind());
  }

  private void prependNotWrapper(StringBuilder sb)
  {
    sb.append("(!");
  }

  private void appendNotWrapper(StringBuilder sb)
  {
    sb.append(")");
  }

  StringBuilder generateJsonExists(AndORTree tree) throws QueryException
  {
    StringBuilder sb = new StringBuilder();
    generateJsonExists(tree, sb);

    return sb;
  }

  private void generateJsonExists(AndORTree tree, StringBuilder sb)
    throws QueryException
  {
    if (eval == null)
    {
      QueryException.throwSyntaxException(QueryMessage.EX_SYNTAX_ERROR);
    }

    String  predPath  = null;
    String  operator  = Evaluator.Operator.get(eval);
    String  connector = " && ";
    boolean useExists = false;
    boolean useParens = false;
    int     nvals;

    if (predicate != null)
      if (predicate.path != null)
        predPath = predicate.path.toQueryString(predicate.getValue(), predicate.getLiteralFuncParam());

    switch (eval)
    {
    case $all:

      if (getNotOperation())
        prependNotWrapper(sb);

      //
      // Synthesize $all with an ANDed series of ==
      //
      operator = Evaluator.Operator.get(EvaluatorCode.$eq);

      nvals = getNumVals();
      if (nvals > 1) sb.append("(");

      for (int i = 0 ; i < nvals; ++i)
      {
        if (i > 0) sb.append(connector);
        sb.append("(");
        appendPathOp(tree.getPredChar(), sb, predPath, operator);
        appendOrInlineBind(tree, sb);
        sb.append(")");
      }

      if (nvals > 1) sb.append(")");

      if (getNotOperation())
        appendNotWrapper(sb);

      break;

    case $nin:

      // ### This case doesn't run right now, because the generateNIN flag
      //     is set to false above. The reason is that $nin is rewritten to
      //     $not($in(<expr>)), prior to this JSON_EXISTS generation code.
      //     Keeping it around in case we want to directly generate $nin
      //     in the future.
      if (generateNIN)
      {
        if (getNotOperation())
          prependNotWrapper(sb);

        sb.append("(");

        nvals = getNumVals();

        // $nin with a single value inside the arg array, i.e.
        // field : { $nin : [value] }, is equivalent
        // to !(field == value).
        if (nvals == 1) {
          operator = Evaluator.Operator.get(EvaluatorCode.$eq);

          sb.append("!(");
          appendPathOp(tree.getPredChar(), sb, predPath, operator);
          appendOrInlineBind(tree, sb);
          sb.append(")");
        } else if (useSqlNotIn) {
          appendPathOp(tree.getPredChar(), sb, predPath, operator);
          sb.append(" (");
          for (int i = 0; i < nvals; ++i) {
            if (i > 0) sb.append(",");
            appendOrInlineBind(tree, sb);
          }
          sb.append(")");
        } else {
          // Synthesize $nin with an ANDed series of !(field == value)
          operator = Evaluator.Operator.get(EvaluatorCode.$eq);

          for (int i = 0; i < nvals; ++i) {
            if (i > 0) sb.append(connector);

            prependNotWrapper(sb);
            sb.append("(");
            appendPathOp(tree.getPredChar(), sb, predPath, operator);
            appendOrInlineBind(tree, sb);
            sb.append(")");
            appendNotWrapper(sb);
          }
        }

        sb.append(")");

        if (getNotOperation())
          appendNotWrapper(sb);
      }
      else
      {
        // This exception should never be thrown.
        QueryException.throwExecutionException(QueryMessage.EX_UNSUPPORTED_OP);
      }

      break;

    case $in:

      if (getNotOperation())
        prependNotWrapper(sb);

      sb.append("(");

      nvals = getNumVals();
      if (nvals == 1)
      {
        operator = Evaluator.Operator.get(EvaluatorCode.$eq);
        appendPathOp(tree.getPredChar(), sb, predPath, operator);
        appendOrInlineBind(tree, sb);
      }
      else
      {
        appendPathOp(tree.getPredChar(), sb, predPath, operator);
        sb.append(" (");
        for (int i = 0 ; i < nvals; ++i)
        {
          if (i > 0) sb.append(",");
          appendOrInlineBind(tree, sb);
        }
        sb.append(")");
      }
      sb.append(")");

      if (getNotOperation())
        appendNotWrapper(sb);

      break;

    case $exists:
    {

      // Remember whether the operation is not(exists())
      boolean notExists = getNotOperation();

      // Peek at the bind value, and be sure to "consume" the bind position
      int bindnum = tree.getNextBind();
      ValueTypePair bval = tree.getValueArray().get(bindnum);

      // If the value is false, null, or 0, it's a not(exists())
      if (AndORNode.isReversedExists(bval)) notExists = !notExists;

      if (notExists)
        prependNotWrapper(sb);

      predPath = predicate.path.toQueryString();

      sb.append("(exists(");
      appendPathOp(tree.getPredChar(), sb, predPath, null);
      sb.append(")) ");

      if (notExists)
        appendNotWrapper(sb);

      // Remove the in-lined bind, so that an unnecessary
      // "passing" clause is not generated later.
      tree.removeBind(bindnum);

      break;

    }
    // ### There's no $nor case because it's rewritten as $not($or(<expr>))
    case $or:
      connector = " || ";
      // FALLTHROUGH

    case $and:

      // Top level $and with no children (corresponds to the empty QBE, i.e. {}),
      // so just return without generating JSON_EXISTS.
      //
      // Note: children can never be null currently, because the
      // children array is allocated at AndORNode creation.
      if ((children == null) || (children.size() == 0))
        return;

      if (getNotOperation())
        prependNotWrapper(sb);

      // Only an $and node can have a downscope path
      String scopePath = getDownScopePath();
      if (scopePath != null)
      {
        // A downscoped clause will have an exists wrapper
        useExists = true;
        sb.append("( exists(");

        sb.append(tree.getPredChar());
        sb.append(scopePath);
        sb.append("?");
        useParens = true; // Must use parens for a downscope operation
      }

      int numChildren = children.size();
      // Must use parens around multiple children
      if (numChildren > 1)
        useParens = true;

      if (useParens)
        sb.append("( ");

      for (int i = 0; i < numChildren; ++i)
      {
        if (i > 0)
          sb.append(connector);

        AndORNode child = children.get(i);
        child.generateJsonExists(tree, sb);
      }

      if (useParens)
        sb.append(" )");
      
      // close the exists()
      if (useExists)
        sb.append(") )");

      if (getNotOperation())
        appendNotWrapper(sb);

      break;

    case $ne:

      // There could be $not in front of $ne,
      // so we need to prepend/append not wrapper
      // around it if that's the case
      if (getNotOperation())
        prependNotWrapper(sb);

      // $ne is generated as !(field == value),
      // so that it has the semantics of a negation of $eq
      //
      // Note: we cannot generate (field != value) for $ne,
      // as then $ne would not be the negation of $eq
      // (in SQL/JSON "!=" is not the negation of "==")
      prependNotWrapper(sb);
      sb.append("(");
      appendPathOp(tree.getPredChar(), sb, predPath, Evaluator.Operator.get(EvaluatorCode.$eq));
      appendOrInlineBind(tree, sb);
      sb.append(")");
      appendNotWrapper(sb);

      if (getNotOperation())
        appendNotWrapper(sb);
      break;

    case $near:
    case $within:
    case $intersects:
    case $contains:
      // No clause to generate, it's done out-of-band
      break;

    default:

      // This is regular leaf node
      if (getNotOperation())
        prependNotWrapper(sb);

      sb.append("(");

      appendPathOp(tree.getPredChar(), sb, predPath, operator);

      nvals = getNumVals();

      if (nvals <= 1)
      {
        appendOrInlineBind(tree, sb);
      }
      else
      {
        sb.append("(");
        for (int i = 0; i < nvals; ++i)
        {
          if (i > 0) sb.append(", ");
          appendBind(sb, tree);
        }
        sb.append(")");
      }

      sb.append(")");

      if (getNotOperation())
        appendNotWrapper(sb);
    }
  }

  /**
   * Appends a bind variable, or in-lines it as a constant in the
   * case of null, true, false, "null", "true", and "false" values.
   *
   * These constants are in-lined, because they cannot be
   * correctly bound thru JDBC. For example, if we bind null
   * as a string thru JDBC, it will match both the JSON null
   * and the JSON string "null". When in-lined, on the other hand,
   * the desired correct result is produced.
   */
  private void appendOrInlineBind(AndORTree tree, StringBuilder sb)
    throws QueryException
  {
    boolean skipBind = false;

    // Get and consume the next bind position
    int bindnum = tree.getNextBind();
    // Peek at the bind value
    ValueTypePair bval = tree.getValueArray().get(bindnum);

    // If the value is a boolean, in-line it and avoid the bind
    if (bval.isBoolean())
    {
      sb.append(bval.getBooleanValue() ? " true" : " false");
      skipBind = true;
    }
    // If the value is a null, in-line it and avoid the bind
    else if (bval.getValue() == JsonValue.NULL)
    {
      sb.append(" null");
      skipBind = true;
    }
    // If the value is a string that happens to be a keyword
    else if (bval.isString())
    {
      String sval = bval.getStringValue();
      if (sval != null)
        // migsilva_bug-36689907 changes empty string to be an in-lined constant in
        // json_exists as opposed to a bind value (as it was before this bug fix). This fix
        // is needed, at least temporarily, to get correct behavior for queries with empty
        // string value, both with and without the index.
        if (sval.isEmpty()) 
        {
          sb.append("\"\"");
          skipBind = true;
        }
        else if (sval.equals("true")  ||
            sval.equals("false") ||
            sval.equals("null"))
        {
          sb.append(" \"");
          sb.append(sval);
          sb.append("\"");
          skipBind = true;
        }
    }

    // Otherwise bind it normally
    if (!skipBind)
      appendBind(sb, bindnum);
    else
      // Remove the in-lined bind, so that an unnecessary
      // "passing" clause is not generated later.
      tree.removeBind(bindnum);
  }
}
