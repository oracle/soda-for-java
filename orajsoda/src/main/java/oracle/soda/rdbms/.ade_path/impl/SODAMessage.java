/* $Header: xdk/src/java/json/orajsoda/src/main/java/oracle/soda/rdbms/impl/SODAMessage.java /st_xdk_soda1/21 2024/07/16 22:56:04 vemahaja Exp $ */

/* Copyright (c) 2014, 2024, Oracle and/or its affiliates. */
/* All rights reserved.*/

/*
   DESCRIPTION
      SODA Messages
 */

/**
 *  This class is not part of the public API, and is
 *  subject to change.
 *
 *  Do not rely on it in your application code.
 *
 *  @author  Max Orgiyan
 *  @author  Doug McMahon
 *  @author  Rahul Kadwe
 *  @author  Josh Spiegel
 */

package oracle.soda.rdbms.impl;

import java.util.ResourceBundle;

import oracle.json.sodacommon.Message;

public final class SODAMessage extends Message
{
  // These messages are package-private, unless stated otherwise below.

  /**************************************************************/
  /* Note: 2000 to 2499 range is reserved for SODA user errors. */
  /* These are errors that result from bad user input.          */
  /**************************************************************/

  static final SODAMessage EX_READ_ONLY                            = create(2000);
  static final SODAMessage EX_NO_TIMESTAMP                         = create(2001);
  static final SODAMessage EX_INVALID_FILTER                       = create(2002);
  static final SODAMessage EX_MISMATCHED_DESCRIPTORS               = create(2003);
  static final SODAMessage EX_ARG_CANNOT_BE_NULL                   = create(2004);
  static final SODAMessage EX_SET_IS_EMPTY                         = create(2005);
  static final SODAMessage EX_SINCE_AND_UNTIL_CANNOT_BE_NULL       = create(2006);
  static final SODAMessage EX_NO_VERSION                           = create(2007);
  static final SODAMessage EX_ARG_MUST_BE_POSITIVE                 = create(2008);
  static final SODAMessage EX_MAX_NUM_OF_KEYS_EXCEEDED             = create(2009);
  static final SODAMessage EX_ITERATOR_RETURNED_NULL_ELEMENT       = create(2010);
  static final SODAMessage EX_INVALID_ARG_VALUE                    = create(2011);
  static final SODAMessage EX_MAX_LEN_LOB_TYPE                     = create(2012);
  static final SODAMessage EX_SECURE_FILE_NOT_LOB                  = create(2013);
  static final SODAMessage EX_LAST_MODIFIED_COL                    = create(2014);
  static final SODAMessage EX_KEY_COL_SEQ                          = create(2015);
  static final SODAMessage EX_VERSION_METHOD                       = create(2016);
  static final SODAMessage EX_KEY_TYPE_BAD_LENGTH                  = create(2017);
  static final SODAMessage EX_KEY_TYPE_BAD_LENGTH2                 = create(2018);
  static final SODAMessage EX_MEDIA_TYPE_COLUMN_NOT_SUP            = create(2019);
  static final SODAMessage EX_MEDIA_TYPE_NOT_JSON                  = create(2020);
  static final SODAMessage EX_SET_CONTAINS_NULL                    = create(2021);
  static final SODAMessage EX_KEY_MUST_BE_SPECIFIED                = create(2022);
  static final SODAMessage EX_ARG_MUST_BE_NON_NEGATIVE             = create(2023);
  static final SODAMessage EX_INVALID_INDEX_CREATE                 = create(2024);
  static final SODAMessage EX_INVALID_INDEX_DROP                   = create(2025);
  static final SODAMessage EX_METADATA_DOC_HAS_NO_CONTENT          = create(2026);
  static final SODAMessage EX_METADATA_DOC_INVALID_JSON            = create(2027);
  static final SODAMessage 
    EX_METADATA_DOC_INVALID_JSON_CANT_DETERMINE_ENCODING           = create(2028);
  static final SODAMessage EX_METADATA_DOC_IO_EXCEPTION            = create(2029);
  static final SODAMessage EX_UNEXPECTED_IN_COL_SPEC               = create(2030);
  static final SODAMessage EX_COL_SPEC_NOT_OBJECT                  = create(2031);
  static final SODAMessage EX_COL_SPEC_NOT_EXPECTED                = create(2032);
  static final SODAMessage EX_INPUT_DOC_HAS_KEY                    = create(2033);
  static final SODAMessage EX_ITERATOR_RETURNED_DOC_WITH_KEY       = create(2034);
  static final SODAMessage EX_SCHEMA_NAME_IS_NULL                  = create(2035);
  static final SODAMessage EX_INVALID_PROJ_SPEC                    = create(2036);
  static final SODAMessage EX_INVALID_KEY                          = create(2037);
  static final SODAMessage EX_TOO_MANY_COLUMNS                     = create(2038);
  static final SODAMessage EX_UNSUPPORTED_ENCRYPTED_INDEX_CREATE   = create(2039);
  static final SODAMessage EX_CANT_CALL_NEXT_ON_CLOSED_CURSOR      = create(2040);
  static final SODAMessage EX_COMMIT_MIGHT_BE_NEEDED               = create(2041);
  static final SODAMessage EX_CANT_DISABLE_AUTOCOMMIT              = create(2042);
  static final SODAMessage EX_SPEC_HAS_NO_CONTENT                  = create(2043);
  static final SODAMessage EX_PROJ_SPEC_MIXED                      = create(2045);
  static final SODAMessage EX_ARRAY_STEPS_IN_PATH                  = create(2046);
  // Not part of a public API.
  // ### Public for OracleRDBMSClient
  public static final SODAMessage EX_NOT_ORACLE_CONNECTION         = create(2047);

  static final SODAMessage EX_LANG_NOT_SUPPORTED_WITH_121_TEXT_INDEX
                                                                   = create(2048);
  static final SODAMessage EX_NULL_ON_EMPTY_NOT_SUPPORTED          = create(2049);
  static final SODAMessage EX_INVALID_PARAM_121_INDEX              = create(2050);
  static final SODAMessage EX_OPERATION_REQUIRES_TXN_MANAGEMENT    = create(2051);
  static final SODAMessage EX_KEY_LIKE_CANNOT_BE_USED              = create(2052);
  static final SODAMessage EX_INDEX_ALREADY_EXISTS                 = create(2053);
  static final SODAMessage EX_SKIP_AND_LIMIT_WITH_COUNT            = create(2054);
  static final SODAMessage EX_TEXT_INDEX_WITH_LANG_NOT_SUPPORTED   = create(2055);
  static final SODAMessage EX_ARRAY_STEPS_NOT_ALLOWED_IN_PROJ      = create(2056);
  static final SODAMessage EX_OVERLAPPING_PATHS_NOT_ALLOWED_IN_PROJ
                                                                   = create(2057);
  static final SODAMessage EX_INCOMPATIBLE_METHODS                 = create(2058);
  static final SODAMessage EX_TO_BINARY_CONVERSION_ERROR
                                                                   = create(2059);
  static final SODAMessage EX_FROM_BINARY_CONVERSION_ERROR         = create(2060);
  // Not part of a public API.
  // ### Public for OracleRDBMSClient
  public static final SODAMessage EX_UNABLE_TO_FETCH_USER_NAME     = create(2061);
  static final SODAMessage EX_VALIDATION_INVALID_FOR_JSON_TYPE     = create(2064);
  static final SODAMessage EX_INVALID_TYPE_MAPPING                 = create(2065);
  static final SODAMessage EX_INVALID_HINT                         = create(2066);  
  static final SODAMessage EX_JDBC_JAR_HAS_NO_OSON_SUPPORT         = create(2067);
  static final SODAMessage EX_JDBC_JAR_HAS_NO_JSON_TYPE_SUPPORT    = create(2068);
  static final SODAMessage EX_PURGE_AND_DROP_MAPPED_NOT_SUPPORTED  = create(2069);
  static final SODAMessage EX_INVALID_BATCH_SIZE                   = create(2070);
  static final SODAMessage EX_INVALID_PATH                         = create(2071);
  static final SODAMessage EX_PATH_EXTRACT_FAILED                  = create(2072);
  static final SODAMessage EX_PATH_INSERT_FAILED                   = create(2073);
  static final SODAMessage EX_JDBC_196_REQUIRED                    = create(2074);
  static final SODAMessage EX_JDBC_211_REQUIRED                    = create(2075);
  static final SODAMessage EX_INVALID_FORMAT                       = create(2076);  
  static final SODAMessage EX_EJSON_CANNOT_BE_USED                 = create(2077);  
  static final SODAMessage EX_EJSON_CANNOT_BE_USED_WITH_BINARY_DOC = create(2078);  
  static final SODAMessage EX_EJSON_NOT_SUPPORTED                  = create(2079);  
  static final SODAMessage EX_EJSON_MUST_BE_AN_OBJECT              = create(2080);  
  static final SODAMessage EX_STRING_NOT_A_NUMBER                  = create(2081);  
  static final SODAMessage EX_INVALID_EJSON_VALUE                  = create(2082);        
  static final SODAMessage EX_INVALID_EJSON_KEY                    = create(2083);        
  static final SODAMessage EX_INVALID_EJSON_FOR_ID                 = create(2084);
  static final SODAMessage EX_UNSUPPORTED_CLAUSE                   = create(2089);
  static final SODAMessage EX_ID_CLAUSE_NOT_SUPPORTED              = create(2090);
  static final SODAMessage EX_PATH_CONTAINS_ARRAY_STEP             = create(2091);
  static final SODAMessage EX_NULL_PATH                            = create(2092);
  static final SODAMessage EX_FILTER_IS_NOT_JSON_OBJECT            = create(2093);
  static final SODAMessage EX_23DB_AND_JSON_TYPE_REQUIRED          = create(2094);
  static final SODAMessage EX_TTL_INDEX_NOT_SUPPORTED              = create(2095);
  static final SODAMessage EX_INSERT_AND_GET_REQUIRES_23C_JDBC     = create(2096);
  static final SODAMessage EX_KEY_SET_WITH_EMBEDDED_OID            = create(2097);
  static final SODAMessage EX_UNSUPPORTED_METHOD_FOR_DUALV         = create(2098);
  static final SODAMessage EX_23DB_AND_JSON_TYPE_REQUIRED_FOR_INDEX= create(2099);
  static final SODAMessage EX_ONLY_SINGLE_PATH_SUPPORTED           = create(2100);
  static final SODAMessage EX_INCOMPATIBLE_FIELDS                  = create(2101);
  
  static final SODAMessage EX_SPEC_IN_OP_HAS_INCORRECT_TYPE        = create(2102);
  static final SODAMessage EX_PATCH_OPERATION_NOT_AN_OBJECT        = create(2103);
  static final SODAMessage EX_MODIFYING_ID_NOT_SUPPORTED_FOR_OP    = create(2104);
  static final SODAMessage EX_ID_MISSING_IN_REPLACE_OP             = create(2105);
  static final SODAMessage EX_ID_CANT_BE_REMOVED                   = create(2106);

  /****************************************************************************/
  /* Note: 2500 to 2999 range is reserved for SODA internal errors. These     */
  /* are mostly errors that should never occur, unless there's a bug          */
  /* in the SODA code.                                                        */
  /****************************************************************************/

  static final SODAMessage EX_UNSUPPORTED_MODE                     = create(2500);
  static final SODAMessage EX_NO_HASH_VERSION                      = create(2501);
  static final SODAMessage EX_SAVE_FAILED                          = create(2502);
  static final SODAMessage EX_INSERT_FAILED                        = create(2503);
  static final SODAMessage EX_UNABLE_TO_CREATE_UUID                = create(2504);
  static final SODAMessage EX_2G_SIZE_LIMIT_EXCEEDED               = create(2505);
  static final SODAMessage EX_IDENTITY_ASSIGN_RETURNING            = create(2506);
  static final SODAMessage EX_UNABLE_TO_GET_DB_VERSION             = create(2507);
  static final SODAMessage EX_JSON_FACTORY_MISSING_IN_JDBC         = create(2508);

  /***************************************************************************/
  /* Note: 3000 to 3499 range is reserved for unimplemented (or unsupported) */
  /* errors. These are error that are due to unimplemented or unsupported    */
  /* functionality.                                                          */
  /***************************************************************************/

  static final SODAMessage EX_TRUNCATE_NOT_SUPP                    = create(3000);
  static final SODAMessage EX_UNSUPPORTED_INDEX_CREATE             = create(3001);
  static final SODAMessage EX_UNSUPPORTED_INDEX_CREATE2            = create(3002);
  static final SODAMessage EX_MD5_NOT_SUPPORTED                    = create(3003);
  static final SODAMessage EX_SHA256_NOT_SUPPORTED                 = create(3004);
  static final SODAMessage EX_UNIMPLEMENTED_FEATURE                = create(3005);
  static final SODAMessage EX_NO_SPATIAL_INDEX_ON_HETERO_COLLECTIONS   
                                                                   = create(3006);
  static final SODAMessage EX_NO_TEXT_INDEX_ON_HETERO_COLLECTIONS    
                                                                   = create(3007);
  static final SODAMessage EX_NO_FUNC_INDEX_ON_HETERO_COLLECTIONS    
                                                                   = create(3008);
  static final SODAMessage EX_NO_QBE_ON_HETERO_COLLECTIONS         = create(3009);
  static final SODAMessage EX_UNSUPPORTED_FOR_JSON_DUALITY_VIEW    = create(3010);

  /**
   * Load the ResourceBundle using the default Locale.
   *  
   * @see xdk/src/java/json/resources/oracle/soda/rdbms/exceptions/Messages.properties 
   */
  private static final ResourceBundle MESSAGES =
    ResourceBundle.getBundle("oracle.soda.rdbms.exceptions.Messages");

  private SODAMessage(int key)
  {
    super(key);
  }  

  private static SODAMessage create(int i)
  {
    return new SODAMessage(i);
  }

  @Override
  protected ResourceBundle getBundle()
  {
    return(MESSAGES);
  }

  @Override
  protected String getPrefix()
  {
    return("SODA");
  }
}
