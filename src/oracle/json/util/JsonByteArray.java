/* Copyright (c) 2014, 2015, Oracle and/or its affiliates. 
All rights reserved.*/

/*
   DESCRIPTION
    A byte array builder for serializing a JSON content return.
 */

/**
 * This class is not part of the public API, and is
 * subject to change.
 *
 * Do not rely on it in your application code.
 *
 *  @author  Doug McMahon
 */

package oracle.json.util;

import java.nio.charset.Charset;

public final class JsonByteArray extends ByteArray
{
  private static final Charset UTF8    = Charset.forName("UTF-8");
  private static final Charset UTF16   = Charset.forName("UTF-16");
  private static final Charset UTF16BE = Charset.forName("UTF-16BE");
  private static final Charset UTF16LE = Charset.forName("UTF-16LE");

  private static final String  UTF32   = "UTF-32";
  private static final String  UTF32BE = "UTF-32BE";
  private static final String  UTF32LE = "UTF-32LE";

  private static byte[] DOUBLE_QUOTE    = {'"'};
  private static byte[] OPEN_BRACKET    = {'['};
  private static byte[] CLOSE_BRACKET   = {']'};
  private static byte[] OPEN_BRACE      = {'{'};
  private static byte[] CLOSE_BRACE     = {'}'};
  private static byte[] COLON           = {':'};
  private static byte[] COMMA           = {','};
  private static byte[] BACKSLASH       = {'\\'};
  private static byte[] UNICODE         = {'\\', 'u'};
  private static byte[] NEWLINE         = {'\\', 'n'};
  private static byte[] HORIZONTAL_TAB  = {'\\', 't'};
  private static byte[] CARRIAGE_RETURN = {'\\', 'r'};

  private byte[] hextmp = new byte[4];

  public JsonByteArray(int initSize)
  {
    super(initSize);
  }

  public JsonByteArray()
  {
    super();
  }

  public void append(String str)
  {
    append(str.getBytes(DEFAULT_CHARSET));
  }

  public void appendOpenArray()
  {
    append(OPEN_BRACKET);
  }

  public void appendCloseArray()
  {
    append(CLOSE_BRACKET);
  }

  public void appendOpenBrace()
  {
    append(OPEN_BRACE);
  }

  public void appendCloseBrace()
  {
    append(CLOSE_BRACE);
  }

  public void appendColon()
  {
    append(COLON);
  }

  public void appendComma()
  {
    append(COMMA);
  }

  public void appendDoubleQuote()
  {
    append(DOUBLE_QUOTE);
  }

  /**
   * Append a string adding surrounding double-quotes and doing
   * any necessary escaping of control characters, double-quotes,
   * and backslashes.
   */
  public void appendValue(String str)
  {
    appendDoubleQuote();

    byte[] data = str.getBytes(DEFAULT_CHARSET);
    int pos = 0;
    for (int i = 0; i < data.length; ++i)
    {
      int ch = data[i] & 0xFF;

      // If this character requires escaping
      // (control characters, double-quote, and backslash
      if ((ch < ' ') || (ch == '"') || (ch == '\\'))
      {
        // Append all data to this point
        append(data, pos, i - pos);

        // Adjust the position
        pos = i + 1;

        if (ch == '\n')
        {
          append(NEWLINE);
        }
        else if (ch == '\t')
        {
          append(HORIZONTAL_TAB);
        }
        else if (ch == '\r')
        {
          append(CARRIAGE_RETURN);
        }
        else if (ch < ' ') // All other control characters
        {
          append(UNICODE); // Introduce unicode character
          int nibble = 0xF000;
          int shift = 12;
          for (int j = 0; j < 4; ++j)
          {
            int xch = (ch & nibble) >> shift;
            if (xch < 10) xch += '0';
            else          xch = (xch - 10) + 'A';
            hextmp[j] = (byte)xch;
            shift -= 4;
            nibble >>= 4;
          }
          append(hextmp);
        }
        else // Character must be preceeded by a backslash
        {
          append(BACKSLASH); // Prepend the backlash
          --pos;             // Back up to include this byte
        }
      }
    }
    // Append any trailing fragment
    append(data, pos, data.length - pos);

    appendDoubleQuote();
  }

  /**
   * Attempt to auto-detect the character set of a JSON byte array.
   */
  public static Charset getJsonCharset(byte[] b)
  {
    // If BOM is present, use it to determine the encoding
    // http://www.unicode.org/faq/utf_bom.html#BOM
    if (b.length >= 3 && b[0] == (byte)0xef && b[1] == (byte)0xbb && b[2] == (byte)0xbf) 
    {      
      return UTF8; // ef bb bf 
    }
    else if (b.length >= 2)
    {
      if (b[0] == (byte)0xff && b[1] == (byte)0xfe)
      {
        if (b.length >= 4 && b[2] == 0 && b[3] == 0)
        {
          return Charset.forName(UTF32); // ff fe 00 00
        }
        else
        {
          return UTF16; // ff fe 
        }
      }
      else if (b[0] == (byte)0xfe && b[1] == (byte)0xff) 
      {
        return UTF16; // fe ff 
      }
      else if (b.length >= 4 && b[0] == 0 && b[1] == 0 && b[2] == (byte)0xfe && b[3] == (byte)0xff)
      {
        return Charset.forName(UTF32); // 00 00 fe ff
      }
    }
    
    // RFC 4627
    // "
    // Since the first two characters of a JSON text will always be ASCII
    // characters [RFC0020], it is possible to determine whether an octet
    // stream is UTF-8, UTF-16 (BE or LE), or UTF-32 (BE or LE) by looking
    // at the pattern of nulls in the first four octets.
    //   00 00 00 xx  UTF-32BE
    //   00 xx 00 xx  UTF-16BE
    //   xx 00 00 00  UTF-32LE
    //   xx 00 xx 00  UTF-16LE
    //   xx xx xx xx  UTF-8
    // "
    // But this does not work in RFC 7159, as first two characters aren't necessarily ASCII.
    // A string is valid JSON-text and may contain unicode characters directly.
    // Use this pattern instead that satisfies both RFCs:
    //
    //   00 00 00 xx  UTF-32BE     * Can't be two UTF-16 chars or four UTF-8 
    //   xx 00 00 00  UTF-32LE       chars because null doesn't occur directly in JSON
    
    //   00 xx xx xx  UTF-16BE     * Can't be UTF-32 because the first character is ASCII range
    //   xx 00 xx xx  UTF-16LE       Can't be four UTF-8 chars because null doesn't occur directly

    //   xx xx xx xx  UTF-8        * Must be UTF-8 because the first char is ASCII and there would be nulls
    //                               if it wasn't UTF-8
    
    
    if (b.length >= 4) {
      if (b[0] == 0 && b[1] == 0 && b[2] == 0)
      {
        return Charset.forName(UTF32BE); // 00 00 00 xx
      }
      else if (b[1] == 0 && b[2] == 0 && b[3] == 0)
      {
        return Charset.forName(UTF32LE); // xx 00 00 00
      }
    }
    
    if (b.length >= 2)
    {
      if (b[0] == 0)
      {
        return UTF16BE; // 00 xx 
      }
      else if (b[1] == 0)
      {
        return UTF16LE; // xx 00
      }
    }
    
    return UTF8; 
  }
}
