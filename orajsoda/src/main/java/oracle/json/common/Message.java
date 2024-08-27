/* Copyright (c) 2014, 2024, Oracle and/or its affiliates. */
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
 *  @author  Josh Spiegel
 */

package oracle.json.common;

import java.text.MessageFormat;
import java.util.ResourceBundle;
import java.util.MissingResourceException;

public class Message
{
  public static final Message EX_ILLEGAL_DATE_TIME     = create(1011);
  public static final Message EX_UNKNOWN_ENCODING      = create(1013);
  public static final Message EX_BAD_HEX_VALUES        = create(1014);
  public static final Message EX_UNSUPPORTED_MEDIA     = create(1015);
  public static final Message EX_INVALID_CONFIG        = create(1016);
  public static final Message EX_UNKNOWN_ACCOUNT       = create(1017);
  public static final Message EX_DUPLICATE_KEY         = create(1019);
  public static final Message EX_NO_INPUT_DOCUMENT     = create(1020);
  public static final Message EX_KEY_PATH_EMPTY        = create(1021);
  public static final Message EX_DOCUMENT_NOT_CLOSED   = create(1022);
  public static final Message EX_DOCUMENT_NOT_OBJECT   = create(1023);
  public static final Message EX_KEY_MUST_BE_STRING    = create(1024);
  public static final Message EX_KEY_MISMATCH          = create(1025);
  public static final Message EX_KEY_DUPLICATE_STEP    = create(1026);
  public static final Message EX_UNIMPLEMENTED_FORMAT  = create(1027);
  public static final Message EX_UNSUPPORTED_DATA_TYPE = create(1028);
  public static final Message EX_REKEY_PENDING         = create(1030);
  public static final Message EX_KEY_PATH_NOT_SET      = create(1031);
  public static final Message EX_OPER_NOT_ALLOWED      = create(1032);
  public static final Message EX_JSON_OPERATION_FAILED = create(1033);
  public static final Message EX_UNSUPPORTED_DOC_TYPE  = create(1034);

  /**
   * Load the ResourceBundle using the default Locale.
   *  
   * @see xdk/src/java/json/resources/oracle/json/common/Messages.properties 
   */
  private static final ResourceBundle MESSAGES =
    ResourceBundle.getBundle(Message.class.getPackage().getName() +
                             ".Messages");

  private int key;
  
  protected Message(int key)
  {
    this.key = key;
  }  
  
  public int getKey()
  {
    return key;
  }

  public String get(Object... params)
  {
    return MessageFormat.format(getMessageTemplate(), params);
  }  

  private static Message create(int i)
  {
    return new Message(i);
  }

  protected String getMessageTemplate()
  {
    String keyString = String.valueOf(this.getKey());
    String prefix = getPrefix();
    try
    {
      return(getBundle().getString(keyString));
    }
    catch (MissingResourceException e)
    {
      return(prefix+"-"+keyString);
    }
  }

  protected ResourceBundle getBundle()
  {
    return(MESSAGES);
  }

  protected String getPrefix()
  {
    return("JSON");
  }
}
