/* Copyright (c) 2014, 2015, Oracle and/or its affiliates. 
All rights reserved.*/

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

import oracle.json.common.Message;

public final class SODAMessage extends Message
{
  // These messages are package-private, unless stated otherwise below.

  /**************************************************************/
  /* Note: 2000 to 2499 range is reserved for SODA user errors. */
  /* These are errors that result from bad user input.          */
  /**************************************************************/

  static final SODAMessage EX_READ_ONLY                          = create(2000);
  static final SODAMessage EX_NO_TIMESTAMP                       = create(2001);
  static final SODAMessage EX_INVALID_FILTER                     = create(2002);
  static final SODAMessage EX_MISMATCHED_DESCRIPTORS             = create(2003);
  static final SODAMessage EX_ARG_CANNOT_BE_NULL                 = create(2004);
  static final SODAMessage EX_SET_IS_EMPTY                       = create(2005);
  static final SODAMessage EX_SINCE_AND_UNTIL_CANNOT_BE_NULL     = create(2006);
  static final SODAMessage EX_NO_VERSION                         = create(2007);
  static final SODAMessage EX_ARG_MUST_BE_POSITIVE               = create(2008);
  static final SODAMessage EX_MAX_NUM_OF_KEYS_EXCEEDED           = create(2009);
  static final SODAMessage EX_ITERATOR_RETURNED_NULL_ELEMENT     = create(2010);
  static final SODAMessage EX_INVALID_ARG_VALUE                  = create(2011);
  static final SODAMessage EX_MAX_LEN_LOB_TYPE                   = create(2012);
  static final SODAMessage EX_SECURE_FILE_NOT_LOB                = create(2013);
  static final SODAMessage EX_LAST_MODIFIED_COL                  = create(2014);
  static final SODAMessage EX_KEY_COL_SEQ                        = create(2015);
  static final SODAMessage EX_VERSION_METHOD                     = create(2016);
  static final SODAMessage EX_KEY_TYPE_BAD_LENGTH                = create(2017);
  static final SODAMessage EX_KEY_TYPE_BAD_LENGTH2               = create(2018);
  static final SODAMessage EX_MEDIA_TYPE_COLUMN_NOT_SUP          = create(2019);
  static final SODAMessage EX_MEDIA_TYPE_NOT_JSON                = create(2020);
  static final SODAMessage EX_SET_CONTAINS_NULL                  = create(2021);
  static final SODAMessage EX_KEY_MUST_BE_SPECIFIED              = create(2022);
  static final SODAMessage EX_ARG_MUST_BE_NON_NEGATIVE           = create(2023);
  static final SODAMessage EX_INVALID_INDEX_CREATE               = create(2024);
  static final SODAMessage EX_INVALID_INDEX_DROP                 = create(2025);
  static final SODAMessage EX_METADATA_DOC_HAS_NO_CONTENT        = create(2026);
  static final SODAMessage EX_METADATA_DOC_INVALID_JSON          = create(2027);
  static final SODAMessage 
    EX_METADATA_DOC_INVALID_JSON_CANT_DETERMINE_ENCODING         = create(2028);
  static final SODAMessage EX_METADATA_DOC_IO_EXCEPTION          = create(2029);
  static final SODAMessage EX_UNEXPECTED_IN_COL_SPEC             = create(2030);
  static final SODAMessage EX_COL_SPEC_NOT_OBJECT                = create(2031);
  static final SODAMessage EX_COL_SPEC_NOT_EXPECTED              = create(2032);
  static final SODAMessage EX_INPUT_DOC_HAS_KEY                  = create(2033);
  static final SODAMessage EX_ITERATOR_RETURNED_DOC_WITH_KEY     = create(2034);
  // Not part of a public API.
  // ### Public for OracleRDBMSClient
  public static final SODAMessage EX_SCHEMA_NAME_IS_NULL         = create(2035);
  static final SODAMessage EX_INVALID_PROJ_SPEC                  = create(2036);
  static final SODAMessage EX_INVALID_KEY                        = create(2037);
  static final SODAMessage EX_TOO_MANY_COLUMNS                   = create(2038);
  static final SODAMessage EX_UNSUPPORTED_ENCRYPTED_INDEX_CREATE = create(2039);
  static final SODAMessage EX_CANT_CALL_NEXT_ON_CLOSED_CURSOR    = create(2040);

  /****************************************************************************/
  /* Note: 2500 to 2999 range is reserved for SODA internal errors. These     */
  /* are mostly errors that should never occur, unless there's a bug          */
  /* in the SODA code.                                                        */
  /****************************************************************************/

  static final SODAMessage EX_UNSUPPORTED_MODE                   = create(2500);
  static final SODAMessage EX_NO_HASH_VERSION                    = create(2501);
  static final SODAMessage EX_SAVE_FAILED                        = create(2502);
  static final SODAMessage EX_INSERT_FAILED                      = create(2503);
  static final SODAMessage EX_UNABLE_TO_CREATE_UUID              = create(2504);
  static final SODAMessage EX_2G_SIZE_LIMIT_EXCEEDED             = create(2505);

  /***************************************************************************/
  /* Note: 3000 to 3499 range is reserved for unimplemented (or unsupported) */
  /* errors. These are error that are due to unimplemented or unsupported    */
  /* functionality.                                                          */
  /***************************************************************************/

  static final SODAMessage EX_TRUNCATE_NOT_SUPP                  = create(3000);
  static final SODAMessage EX_UNSUPPORTED_INDEX_CREATE           = create(3001);
  static final SODAMessage EX_UNSUPPORTED_INDEX_CREATE2          = create(3002);
  static final SODAMessage EX_MD5_NOT_SUPPORTED                  = create(3003);
  static final SODAMessage EX_SHA256_NOT_SUPPORTED               = create(3004);

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
