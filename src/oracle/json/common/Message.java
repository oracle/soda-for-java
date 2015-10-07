/* Copyright (c) 2014, 2015, Oracle and/or its affiliates. 
All rights reserved.*/

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
  public static final Message EX_NO_BOUND             = create(1001); 
  public static final Message EX_NO_LINE_BREAK        = create(1002);
  public static final Message EX_CHARSET_CONVERSION   = create(1003);
  public static final Message EX_QUERY_STR_CONVERSION = create(1004);
  public static final Message EX_NO_LINE_TERMINATOR   = create(1005);
  public static final Message EX_FIELD_NAME_NOT_FOUND = create(1006);
  public static final Message EX_FIELD_NAME_NOT_VALID = create(1007);
  public static final Message EX_NO_CLOSING_QUOTE     = create(1008);
  public static final Message EX_NO_START_OF_FIELD    = create(1009);
  public static final Message EX_NO_END_OF_FIELD      = create(1010);
  public static final Message EX_ILLEGAL_DATE_TIME    = create(1011);
  public static final Message EX_QBE_SYNTAX_ERROR     = create(1012);
  public static final Message EX_UNKNOWN_ENCODING     = create(1013);
  public static final Message EX_BAD_HEX_VALUES       = create(1014);
  public static final Message EX_UNSUPPORTED_MEDIA    = create(1015);
  public static final Message EX_INVALID_CONFIG       = create(1016);
  public static final Message EX_UNKNOWN_ACCOUNT      = create(1017);
  public static final Message EX_ILLEGAL_DATE_TIME2   = create(1018);

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
