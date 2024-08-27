/* Copyright (c) 2014, 2024, Oracle and/or its affiliates.*/
/* All rights reserved.*/

/*
   DESCRIPTION

     Provides access to externalized strings.

     Example usage:

        throw new MyException(Message.MY_MESSAGE.get(p1, p2));

     Where p1 and p2 are parameters specific to MY_MESSAGE
 */

/**
 * This class is not part of the public API, and is
 * subject to change.
 *
 * Do not rely on it in your application code.
 *
 * @author  Doug McMahon
 * @author  Josh Spiegel
 * @author  Max Orgiyan
 */

package oracle.json.parser;

import java.util.ResourceBundle;

import oracle.json.common.Message;

import javax.management.Query;

public final class QueryMessage extends Message
{
  /***********************************************************/
  /* Note: 5000 to 5499 range is reserved for syntax errors. */
  /***********************************************************/

  static final QueryMessage EX_SYNTAX_ERROR            = create(5000);
  static final QueryMessage EX_MUST_BE_SCALAR          = create(5001);
  static final QueryMessage EX_MUST_BE_ARRAY           = create(5002);
  static final QueryMessage EX_NON_SCALAR_KEY          = create(5003);
  static final QueryMessage EX_UNEXPECTED_OPERATOR     = create(5004);
  static final QueryMessage EX_NUMBER_OF_MEMBERS       = create(5005);
  static final QueryMessage EX_INDEX_PROP_WRONG_TYPE   = create(5006);
  static final QueryMessage EX_INDEX_PROP_MISSING      = create(5007);
  static final QueryMessage EX_INDEX_ILLEGAL_PATH      = create(5008);
  static final QueryMessage EX_INVALID_INDEX_SPEC      = create(5009);
  static final QueryMessage EX_INVALID_PROJECTION      = create(5010);
  static final QueryMessage EX_INVALID_INDEX_DTYPE     = create(5011);
  static final QueryMessage EX_INVALID_INDEX_DLEN      = create(5012);
  static final QueryMessage EX_INVALID_INDEX_LANG      = create(5013);
  static final QueryMessage EX_MUST_BE_STRING          = create(5014);
  static final QueryMessage EX_OPERAND_INVALID         = create(5015);
  static final QueryMessage EX_CONTAINER_NOT_ALLOWED   = create(5016);
  static final QueryMessage EX_MUST_BE_OBJECT          = create(5017);
  static final QueryMessage EX_NOT_AN_OPERATOR         = create(5018);
  static final QueryMessage EX_EXTRA_INPUT             = create(5019);
  static final QueryMessage EX_INVALID_ARRAY_KEY       = create(5020);
  static final QueryMessage EX_CANNOT_BE_EMPTY         = create(5021);
  static final QueryMessage EX_ARRAY_STEP_DISALLOWED   = create(5022);
  static final QueryMessage EX_BAD_PATH                = create(5023);
  static final QueryMessage EX_EMPTY_PATH              = create(5024);
  static final QueryMessage EX_EMPTY_PATH_STEP         = create(5025);
  static final QueryMessage EX_BAD_ARRAY_SUBSCRIPT     = create(5026);
  static final QueryMessage EX_BAD_BACKQUOTE           = create(5027);
  static final QueryMessage EX_UNCLOSED_STEP           = create(5028);
  static final QueryMessage EX_MISSING_STEP_DOT        = create(5029);
  static final QueryMessage EX_MISSING_FIELD_NAME      = create(5030);
  static final QueryMessage EX_PATH_SYNTAX_ERROR       = create(5031);
  static final QueryMessage EX_MULTIPLE_OPS            = create(5032);
  static final QueryMessage EX_BAD_OP_VALUE            = create(5033);
  static final QueryMessage EX_QUERY_WITH_OTHER_OPS    = create(5034);
  static final QueryMessage EX_BAD_ORDERBY_PATH_VALUE  = create(5035);
  static final QueryMessage EX_BAD_ORDERBY_PATH_VALUE2 = create(5036);
  static final QueryMessage EX_NOT_IS_NOT_ALLOWED      = create(5037);
  static final QueryMessage EX_OPERATOR_NOT_ALLOWED    = create(5038);
  static final QueryMessage EX_ID_MISPLACED            = create(5039);
  static final QueryMessage EX_LENGTH_NOT_ALLOWED      = create(5040);
  static final QueryMessage EX_FIELDS_CANNOT_BE_EMPTY  = create(5041);
  static final QueryMessage EX_FIELDS_REQUIRED         = create(5042);
  static final QueryMessage EX_LANGUAGE_NOT_EXPECTED   = create(5043);
  static final QueryMessage EX_WRONG_ORDER             = create(5044);
  static final QueryMessage EX_BETWEEN_ARGUMENT        = create(5045);
  static final QueryMessage EX_INVALID_PATCH           = create(5046);
  static final QueryMessage EX_MUST_BE_NUMBER          = create(5047);
  static final QueryMessage EX_OP_FIELD_NOT_ALLOWED    = create(5048);
  static final QueryMessage EX_OP_FIELD_UNKNOWN        = create(5049);
  static final QueryMessage EX_OP_FIELD_REQUIRED       = create(5050);
  static final QueryMessage EX_SPATIAL_MISPLACED       = create(5051);
  static final QueryMessage EX_FULLTEXT_MISPLACED      = create(5052);
  static final QueryMessage EX_MOD_IS_NOT_ALLOWED      = create(5053);
  static final QueryMessage EX_ORDERBY_PATH_REQUIRED   = create(5054);
  static final QueryMessage EX_ORDERBY_UNKNOWN_PROP    = create(5055);
  static final QueryMessage EX_ORDERBY_INVALID_PROP    = create(5056);
  static final QueryMessage EX_BAD_PROP_TYPE           = create(5057);
  static final QueryMessage EX_SCALAR_AND_LAX          = create(5058);
  static final QueryMessage EX_INCOMPATIBLE_FIELDS     = create(5059);
  static final QueryMessage EX_BAD_DATAGUIDE_VALUE     = create(5060);
  static final QueryMessage EX_BAD_SEARCH_ON_VALUE     = create(5061);
  static final QueryMessage EX_MUST_NOT_BE_LITERAL     = create(5062);
  static final QueryMessage EX_SQL_JSON_MISPLACED      = create(5063);
  static final QueryMessage EX_MULTIPLE_ID_CLAUSES     = create(5064);
  static final QueryMessage EX_ENVELOPE_WITH_OTHER_OPS = create(5065);
  static final QueryMessage EX_FIELDS_OR_SPATIAL_REQUIRED
                                                       = create(5066);
  static final QueryMessage EX_UNSUPPORTED_SQLJSON_OP  = create(5067);
  static final QueryMessage EX_STRING_BIND_TOO_LONG    = create(5068);
  static final QueryMessage EX_ARRAY_STEPS_IN_PATH     = create(5069);
  static final QueryMessage EX_23C_AND_JSON_TYPE_REQUIRED        = create(5070);
  static final QueryMessage EX_SPATIAL_NOT_SUPPORTED_ON_DUALITY  = create(5071);
  static final QueryMessage EX_CONTAINS_NOT_SUPPORTED_ON_DUALITY = create(5072);
  static final QueryMessage EX_NOT_SUPPORTED_ON_DUALITY          = create(5073);
  static final QueryMessage EX_INVALID_MOD_INPUT                 = create(5074);
  static final QueryMessage EX_REMAINDER_OR_DIVISOR_NOT_A_NUMBER = create(5075);
  static final QueryMessage EX_OUT_OF_RANGE_MOD_NUMERIC_VALUE    = create(5076);
  static final QueryMessage EX_DIVISOR_CANNOT_BE_ZERO            = create(5077);
    
  /************************************************************************/
  /* Note: 5500 to 5999 range is reserved for SQL phase "run-time" errors */
  /************************************************************************/
  static final QueryMessage RR_BIND_MISMATCH           = create(5500);
  static final QueryMessage EX_UNSUPPORTED_OP          = create(5501);

  /************************************************************************/
  /* Note: 6000 to 6499 range is reserved for SQL phase "run-time" errors */
  /************************************************************************/
  static final QueryMessage EX_SQL_JSON_UNSUPPORTED    = create(6000);
  static final QueryMessage EX_UNSUPPORTED_BIND_TOKEN  = create(6001);
  static final QueryMessage EX_NEG_BIND_TOKEN          = create(6002);

  /**
   * Load the ResourceBundle using the default Locale.
   *  
   * @see xdk/src/java/json/resources/oracle/json/parser/Messages.properties 
   */
  private static final ResourceBundle MESSAGES =
    ResourceBundle.getBundle(QueryMessage.class.getPackage().getName() +
                             ".Messages");

  private QueryMessage(int key)
  {
    super(key);
  }  
  
  private static QueryMessage create(int i)
  {
    return new QueryMessage(i);
  }

  @Override
  protected ResourceBundle getBundle()
  {
    return(MESSAGES); 
  }
}
