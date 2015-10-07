/* Copyright (c) 2014, 2015, Oracle and/or its affiliates. 
All rights reserved.*/

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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;

import java.math.BigDecimal;

import javax.json.JsonValue;
import javax.json.JsonObject;
import javax.json.JsonArray;

import oracle.json.parser.Evaluator.EvaluatorCode;

class AndORNode
{
  private EvaluatorCode           eval;
  private Predicate               predicate;
  private List<AndORNode>         children = new ArrayList<AndORNode>();
  private final AndORNode         parent;
  private int                     numVals = 0;
  private JsonQueryPath           downScopePath = null;
  private boolean                 notOp = false;

  private final static int KEY_TYPE_OPERATOR = 1;
  private final static int KEY_TYPE_AND      = 2;
  private final static int KEY_TYPE_OR       = 3;
  private final static int KEY_TYPE_LEAF     = 4;
  private final static int KEY_TYPE_ID       = 5;
  private final static int KEY_TYPE_NOR      = 6;

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
      return downScopePath.toQueryString(false); // Do not append [*]
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

  private void setNotEval(EvaluatorCode eval)
  {
    boolean inverseOp = true;

    // See if there's a simple inverse of the evaluator
    switch (eval)
    {
    case $eq:
      eval = EvaluatorCode.$ne;
      inverseOp = false;
      break;

    case $ne:
      eval = EvaluatorCode.$eq;
      inverseOp = false;
      break;

    case $gt:
      eval = EvaluatorCode.$lte;
      inverseOp = false;
      break;

    case $lt:
      eval = EvaluatorCode.$gte;
      inverseOp = false;
      break;

    case $gte:
    case $ge:
      eval = EvaluatorCode.$lt;
      inverseOp = false;
      break;

    case $lte:
    case $le:
      eval = EvaluatorCode.$gt;
      inverseOp = false;
      break;

    default:
      inverseOp = true;
    }

    this.eval = eval;
    this.notOp = inverseOp;
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

  private static int checkScalarType(EvaluatorCode evc,
                                     String itemKey,
                                     ValueTypePair vpair)
    throws QueryException
  {
    int vtype = vpair.getType();

    if ((evc == EvaluatorCode.$startsWith) ||
        (evc == EvaluatorCode.$regex))
    {
      if (vtype != ValueTypePair.TYPE_STRING)
        QueryException.throwSyntaxException(QueryMessage.EX_MUST_BE_STRING,
                                            itemKey);
    }
    else if ((evc != EvaluatorCode.$eq) &&
             (evc != EvaluatorCode.$ne) &&
             (evc != EvaluatorCode.$exists))
    {
      if ((vtype == ValueTypePair.TYPE_BOOLEAN) ||
          (vtype == ValueTypePair.TYPE_NULL))
        QueryException.throwSyntaxException(QueryMessage.EX_NULL_BOOLEAN_INVALID,
                                            itemKey,
                                            ValueTypePair.getStringType(vtype));
    }

    return (vtype);
  }

  static int getKeyType(String key)
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
    else if (key.equalsIgnoreCase("$id"))
      keyType = KEY_TYPE_ID;
    else if (key.equalsIgnoreCase("$gt")  ||
             key.equalsIgnoreCase("$gte") ||
             key.equalsIgnoreCase("$ge") ||
             key.equalsIgnoreCase("$lt")  ||
             key.equalsIgnoreCase("$lte") ||
             key.equalsIgnoreCase("$le") ||
             key.equalsIgnoreCase("$eq")  ||
             key.equalsIgnoreCase("$ne")  ||
             key.equalsIgnoreCase("$in")  ||
             key.equalsIgnoreCase("$nin") ||
             key.equalsIgnoreCase("$all") ||
             key.equalsIgnoreCase("$not") ||
             key.equalsIgnoreCase("$regex")  ||
             key.equalsIgnoreCase("$exists") ||
             key.equalsIgnoreCase("$startsWith"))
      keyType = KEY_TYPE_OPERATOR;
    else
      QueryException.throwSyntaxException(QueryMessage.EX_NOT_AN_OPERATOR, key);

    return keyType;
  }

  private void addArrayValues(AndORTree tree,
                              JsonValue memberValue, String keyString)
    throws QueryException
  {
    JsonArray jsonArr = (JsonArray)memberValue;
    Iterator<JsonValue> arrIter = jsonArr.iterator();

    if (!arrIter.hasNext())
      QueryException.throwSyntaxException(QueryMessage.EX_CANNOT_BE_EMPTY,
                                          keyString);

    while (arrIter.hasNext()) 
    {
      JsonValue arrElem = arrIter.next();
      if (tree.isJSONObject(arrElem)) // For $in, $nin, $all, or field values
        QueryException.throwSyntaxException(QueryMessage.EX_MUST_BE_SCALAR,
                                            keyString);
      incrementNumVals();
      tree.addToValueArray(arrElem, keyString);
    }
  }

  void addNode(AndORTree tree, Entry<String, JsonValue> entry)
    throws QueryException
  {
    AndORNode node = null;
    String key = entry.getKey();
    JsonValue memberValue;

    int keyType = AndORNode.getKeyType(key);

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
        if ((getEval() != EvaluatorCode.$or) ||
            (getNotOperation()))
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
        if ((getEval() != EvaluatorCode.$or) ||
            (!getNotOperation()))
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

      if (!tree.isJSONArray(entry.getValue()))
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
          clauseParent.addNode(tree, memberIter.next());
        } 
      }

      break;

    case KEY_TYPE_ID:

      if (this.parent != null)
      {
        // We only allow the $id clause at the outermost level
        QueryException.throwSyntaxException(QueryMessage.EX_ID_MISPLACED);
      }

      // Special processing for $ID since it is a separate where clause
      memberValue = entry.getValue();
      if (tree.isJSONPrimitive(memberValue))
      {
        String keyString = tree.getScalarKey(memberValue);
        tree.addToKeys(keyString);
      }
      else if (tree.isJSONArray(memberValue))
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

    case KEY_TYPE_OPERATOR:

      QueryException.throwSyntaxException(QueryMessage.EX_UNEXPECTED_OPERATOR,
                                          key);

    default:

      // This takes key:value as input

      String memberKey = key;
      memberValue = entry.getValue();

      // Simple path/value pairs interpreted as equality matches, such as:
      //   "salary":10
      if (tree.isJSONPrimitive(memberValue)) 
      {
        node = new AndORNode(this);
        node.predicate = new Predicate(new JsonQueryPath(memberKey),
                                       memberValue.toString());
        node.setEval(EvaluatorCode.$eq);
        tree.addToValueArray(memberValue, memberKey);
        this.children.add(node);
      }
      // Path/object inequalities or grouped inequalities, such as:
      //   "salary":{"$gt":10, "$lt":100}
      //   "salary":{"$startsWith":"Joe"}
      //   "salary":{"$exists": true}
      else if (tree.isJSONObject(memberValue))
      {
        JsonObject jObj = (JsonObject)memberValue;
        Set<Entry<String, JsonValue>> members = jObj.entrySet(); 
        Iterator<Entry<String, JsonValue>> inIter = members.iterator();

        boolean notDownscoped = false;

        // Empty objects are disallowed
        if (!inIter.hasNext())
        {
          QueryException.throwSyntaxException(QueryMessage.EX_CANNOT_BE_EMPTY,
                                              memberKey);
        }

        while (inIter.hasNext())
        {
          Entry<String, JsonValue> item = inIter.next();
          String itemKey = item.getKey();
          JsonValue itemValue = item.getValue();
          node = new AndORNode(this);
          this.children.add(node);

          // This is $in, $nin, $all op salary:{$in:[1,2,3]}
          // Path/object pair where the object contains operators such as
          //   "salary":{"$gt":10000}
          //   "salary":{"$in":[10000,20000,30000]}
          //   "salary":{"$not": {"$lt" : 10000} }
          if (getKeyType(itemKey) == KEY_TYPE_OPERATOR)
          {
            EvaluatorCode evc = AndORNode.codeFor(itemKey);

            // ### For now, disallow mixing ordinary comparisons/$exists here.
            //     We can't get here after a downscope because it consumes
            //     the rest of the iterator. But we could reach this point
            //     prior to a clause that's interpreted as a downscope.
            //
            //     Example:
            //       "address"{"$gt":123, "state":"MA", "$lt":456}
            //
            //     For now, set a flag indicating that we should not
            //     expect an ordinary field which would be downscoped.

            notDownscoped = true;

            switch (evc)
            {
            case $not:

              // $not must be followed by an object
              if (!tree.isJSONObject(itemValue))
                QueryException.throwSyntaxException(QueryMessage.EX_MUST_BE_OBJECT,
                                                    itemKey);

              JsonObject notObj = (JsonObject)itemValue;
              Set<Entry<String, JsonValue>> notMember = notObj.entrySet();

              Iterator<Entry<String, JsonValue>> notIter = notMember.iterator();

              // $not requires one entry
              if (!notIter.hasNext())
                QueryException.throwSyntaxException(QueryMessage.EX_CANNOT_BE_EMPTY,
                                                    itemKey);

              Entry<String, JsonValue> notOpEntry = notIter.next();

              // $not cannot have more than one target
              if (notIter.hasNext())
              {
                QueryException.throwSyntaxException(QueryMessage.EX_NUMBER_OF_MEMBERS,
                                                    Integer.toString(notMember.size()),
                                                    itemKey);
              }

              JsonValue notOpEntryValue = notOpEntry.getValue();

              node.predicate = new Predicate(new JsonQueryPath(memberKey));

              String operKey = notOpEntry.getKey();
              EvaluatorCode opcode = AndORNode.codeFor(operKey);

              switch (opcode)
              {
              case $in:
              case $nin:
              case $all:
                if (!tree.isJSONArray(notOpEntryValue))
                  QueryException.throwSyntaxException(QueryMessage.EX_MUST_BE_ARRAY,
                                                      operKey);
                node.addArrayValues(tree, notOpEntryValue, operKey);
                break;

              // Currently don't allow $not around $and, $or, $nor, $not,
              // but might allow it in the future
              case $and:
              case $or:
              case $nor:
              case $not:
              // $not doesn't make sense around $orderby/$id
              case $orderby:
              case $id:
                QueryException.throwSyntaxException(QueryMessage.EX_NOT_IS_NOT_ALLOWED, operKey);
                break;

              default:
                checkScalarType(opcode,
                        operKey,
                        tree.addToValueArray(notOpEntryValue, operKey));
                break;
              }

              switch (opcode)
              {
              case $nin:
                // $not($nin) is equivalent to $in
                if (!useSqlNotIn)
                {
                  // If there's no native $nin, use $in
                  node.setEval(EvaluatorCode.$in);
                }
                else
                {
                  node.setEval(opcode);
                  node.setNotOperation();
                }
                break;

              case $in:
                // $not($in) is equivalent to $nin
                if (useSqlNotIn)
                {
                  // Use $nin if it's available or if we're expanding
                  // the clauses anyway (the $in work-around)
                  node.setEval(EvaluatorCode.$nin);
                }
                else
                {
                  node.setEval(opcode);
                  node.setNotOperation();
                }
                break;

              // Rewrite $not($ne) to $eq, which will avoid
              // generating !(!(field == value)).
              case $ne:
                node.setEval(EvaluatorCode.$eq);
                break;

              default:
                node.setEval(opcode);
                node.setNotOperation();
              }

              break;

            case $in:
            case $nin:
            case $all:

              // These operators all take arrays
              if (!tree.isJSONArray(itemValue))
                QueryException.throwSyntaxException(QueryMessage.EX_MUST_BE_ARRAY,
                                                    itemKey);
              node.addArrayValues(tree, itemValue, itemKey);

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

              node.predicate = new Predicate(new JsonQueryPath(memberKey));
              break;

            default:

              // All other operators need primitive scalars

              if (tree.isJSONArray(itemValue))
                QueryException.throwSyntaxException(QueryMessage.EX_CONTAINER_NOT_ALLOWED,
                                                    itemKey, "array");
              else if (tree.isJSONObject(itemValue))
                QueryException.throwSyntaxException(QueryMessage.EX_CONTAINER_NOT_ALLOWED,
                                                    itemKey, "object");

              // Must be a JSON scalar

              node.predicate = new Predicate(new JsonQueryPath(memberKey),
                                             itemValue.toString());
              node.setEval(evc);


              AndORNode.checkScalarType(evc, itemKey,
                                   tree.addToValueArray(itemValue, itemKey));
            }
          }
          // Path/object pair where the object contains ordinary fields
          // interpreted as a downscoped operation, such as:
          //   "address":{"city":"Boston","state":"MA"}
          //
          // Mixing downscoped operation with regular operation not allowed
          else if (getKeyType(itemKey) == KEY_TYPE_LEAF && (!notDownscoped))
          {
            node.setEval(EvaluatorCode.$and);
            node.setPredicatePath(memberKey);

            // add the already parsed item
            node.addNode(tree, item);

            // loop and recurse over the remaining items
            while (inIter.hasNext())
              node.addNode(tree, inIter.next());
          }
          else
          {
            // ### The EX_KEY_NOT_ALLOWED exception message says that the only
            //     allowed keys here are:
            //
            //     comparison operators
            //     $exists
            //     $not
            //
            //     If more possibilities are allowed here in the
            //     future, the exception message needs to be adjusted
            //     accordingly.
            QueryException.throwSyntaxException(QueryMessage.EX_KEY_NOT_ALLOWED, itemKey);
          }
        }
      }
      // Path/array pair interpreted as $all, such as:
      //   "salary":[10000,20000,30000]
      else if (tree.isJSONArray(memberValue))
      {
        node = new AndORNode(this);
        node.addArrayValues(tree, memberValue, memberKey);
        node.setEval(EvaluatorCode.$all);
        node.predicate = new Predicate(new JsonQueryPath(memberKey));
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
        predPath = predicate.path.toQueryString(true);

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

      // ### This case doesn't run right now, because the generateNIN flag is set to
      //     false above. The reason is that $nin is rewritten to $not($in(<expr>)),
      //     prior to this JSON_EXISTS generation code. Keeping it around in
      //     case we want to directly generate $nin in the future.
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
      if (bval.getType() == ValueTypePair.TYPE_BOOLEAN)
      {
        if (!bval.getBooleanValue())
          notExists = !notExists;
      }
      else if (bval.getType() == ValueTypePair.TYPE_NULL)
      {
        notExists = !notExists;
      }
      else if (bval.getType() == ValueTypePair.TYPE_NUMBER)
      {
        if (BigDecimal.ZERO.equals(bval.getNumberValue()))
          notExists = !notExists;
      }

      if (notExists)
        prependNotWrapper(sb);

      // Get the path without the trailing [*]
      predPath = predicate.path.toQueryString(false);

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
    if (bval.getType() == ValueTypePair.TYPE_BOOLEAN)
    {
      sb.append(bval.getBooleanValue() ? " true" : " false");
      skipBind = true;
    }
    // If the value is a null, in-line it and avoid the bind
    else if (bval.getType() == ValueTypePair.TYPE_NULL)
    {
      sb.append(" null");
      skipBind = true;
    }
    // If the value is a string that happens to be a keyword
    else if (bval.getType() == ValueTypePair.TYPE_STRING)
    {
      String sval = bval.getStringValue();
      if (sval != null)
        if (sval.equals("true")  ||
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
