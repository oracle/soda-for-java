/* Copyright (c) 2014, 2024, Oracle and/or its affiliates. */
/* All rights reserved.*/

/*
   DESCRIPTION
    Utility functions for processing URI components.
 */

/**
 * This class is not part of the public API, and is
 * subject to change.
 *
 * Do not rely on it in your application code.
 *
 *  @author  Doug McMahon
 */

package oracle.json.sodautil;

// ### Avoid these utilities because they appear to be geared
// ### only to URI arguments and not to path steps.
//import java.net.URLDecoder;
//import java.net.URLEncoder;

import java.nio.charset.CharacterCodingException;

import java.util.ArrayList;

public final class UriTools
{
  /*
  ** Notes:
  **   Primary reserved characters    :/?#[]@
  **   Secondary reserved characters  !$&'()*+,;=
  **   Escaping character             %
  **
  ** Valid characters in:
  **  Path step                       /:@-._~!$&'()*+,=
  **  Path parameter name             :@-._~!$&'()*+,
  **  Path parameter value            :@-._~!$&'()*+,==
  **  Query parameter name            /?:@-._~!$'()* ,;
  **  Query parameter value           /?:@-._~!$'()* ,;==
  **  Fragment                        /?:@-._~!$&'()*+,;=
  */

  private static final String URI_STEP_ALLOWED = ".-_~+";
  private static final String URI_ARG_ALLOWED  = ".-_~*";

  public static final String[] EMPTY_STRING_ARRAY = new String[0];

  private UriTools()
  {
  }

  private static final char[] hexDigits = "0123456789ABCDEFabcdef".toCharArray();

  private static String encode(String path, String allowedChars)
  {
    byte          arr[] = path.getBytes(ByteArray.DEFAULT_CHARSET);
    ByteArray     out   = new ByteArray(arr.length);
    byte[]        hex   = new byte[3];
    int           i, j;

    hex[0] = (byte)'%';

    j = 0; // Starting position of current fragment
    for (i = 0; i < arr.length; ++i)
    {
      int ch = arr[i] & 0xFF;

      if ((ch >= 'a') && (ch <= 'z')) continue;
      if ((ch >= 'A') && (ch <= 'Z')) continue;
      if ((ch >= '0') && (ch <= '9')) continue;

      if ((ch >= ' ') && (ch <= '~'))
        if (allowedChars.indexOf((char)ch) >= 0)
          continue;

      // Append the bytes found up to this point
      if (i > j) out.append(arr, j, (i - j));

      j = i + 1; // Advance fragment position past this byte

      // ### For arguments we could encode spaces as + signs

      hex[1] = (byte)hexDigits[(ch >> 4) & 0xF];
      hex[2] = (byte)hexDigits[ch & 0xF];

      // Append hex characters for this byte
      out.append(hex);
    }

    // Return the original string if no escapes were neccessary
    if (j == 0)
      return(path);

    // Append any remaining bytes
    if (i > j) out.append(arr, j, (i - j));

    // Convert the bytes to a string in UTF-8
    return(out.getString());
  }

  /**
   * Encode a URL path step.
   * This escapes some characters to %XX sequences using UTF-8 bytes.
   * 
   */
  public static String encodePath(String path)
  {
    return(encode(path, URI_STEP_ALLOWED));
  }

  /**
   * Encode a URL argument name or value.
   * This escapes some characters to %XX sequences using UTF-8 bytes.
   */
  public static String encodeArg(String path)
  {
    return(encode(path, URI_ARG_ALLOWED));
  }

  private static int digitToNibble(int digit)
    throws CharacterCodingException
  {
    int k;

    for (k = 0; k < hexDigits.length; ++k)
      if (hexDigits[k] == digit)
      {
        if (k >= 16) k -= 6;
        return(k);
      }
    throw new CharacterCodingException(); // Invalid hex sequence
  }

  private static String decode(String path, boolean isArgument)
    throws CharacterCodingException
  {
    byte          arr[] = path.getBytes(ByteArray.DEFAULT_CHARSET);
    ByteArray     out   = new ByteArray(arr.length);
    byte[]        temp  = new byte[1];
    int           i, j;

    j = 0; // Starting position of current fragment
    for (i = 0; i < arr.length; ++i)
    {
      int ch = arr[i] & 0xFF;

      if ((ch != '+') || !isArgument)
        if (ch != '%')
          continue;

      // Append the bytes found up to this point
      if (i > j) out.append(arr, j, (i - j));

      ++i;

      if (ch == '+')
        temp[0] = ' ';
      else
      {
        if ((arr.length - i) < 2)
          throw new CharacterCodingException(); //Fragmented hex sequence

        // Compute single byte for this hex sequence
        int hex = UriTools.digitToNibble(arr[i++] & 0xFF) << 4;
        hex |= UriTools.digitToNibble(arr[i++] & 0xFF);
        temp[0] = (byte)hex;
      }

      // Append byte for this hex sequence
      out.append(temp);

      j = i--; // Advance fragment position past this sequence
    }

    // Return the original string if no unescapes were neccessary
    if (j == 0)
      return(path);

    // Append any remaining bytes
    if (i > j) out.append(arr, j, (i - j));

    // Convert the bytes to a string in UTF-8
    return(ByteArray.bytesToString(out));
  }

  /**
   * Decode a URL path portion.
   * This unescapes %XX sequences and converts "+" signs to spaces.
   * The sequences must be valid UTF-8.
   */
  public static String decodePath(String path)
    throws CharacterCodingException
  {
    return(UriTools.decode(path, false));
  }

  /**
   * Parse a path consisting of a series of / delimited steps.
   * The / characters are removed. If the string doesn't begin with
   * a /, the component up to the first / is discarded.
   * The path steps may optionally be decoded after being parsed.
   */
  public static String[] parsePath(String path)
    throws CharacterCodingException
  {
    return parsePath(path, true);
  }

  /**
   * Parse a path consisting of a series of / delimited steps.
   * The / characters are removed. If the string doesn't begin with
   * a /, the component up to the first / is discarded.
   * The path steps are decoded after being parsed.
   */
  public static String[] parsePath(String path, boolean decodeSteps)
    throws CharacterCodingException
  {
    ArrayList<String> arr = new ArrayList<String>();

    int firstPos = path.indexOf('/');

    while (firstPos >= 0)
    {
      String step;
      int    nextPos = path.indexOf('/', firstPos + 1);

      if (nextPos < 0)
        step = path.substring(firstPos + 1);
      else
        step = path.substring(firstPos + 1, nextPos);

      if (decodeSteps)
        step = UriTools.decodePath(step);

      arr.add(step);

      firstPos = nextPos;
    }

    int sz = arr.size();
    if (sz == 0)
      return(EMPTY_STRING_ARRAY);
    return(arr.toArray(new String[sz]));
  }

  /**
   * Convert a string to init-cap format. This ensures that we can match
   * HTTP-style naming e.g. "Accept-Charset".
   */
  public static String makeInitCap(String name)
  {
    int           i;
    StringBuilder icap = new StringBuilder(name);
    int           nlen = icap.length();
    boolean       cap_flag = true;

    for (i = 0; i < nlen; ++i)
    {
      char c = icap.charAt(i);

      if (Character.isLetterOrDigit(c))
      {
        if (Character.isLowerCase(c))
        {
          if (cap_flag)
            icap.setCharAt(i, Character.toUpperCase(c));
        }
        else if (Character.isUpperCase(c))
        {
          if (!cap_flag)
            icap.setCharAt(i, Character.toLowerCase(c));
        }
        cap_flag = false;
      }
      else
      {
        cap_flag = true;
      }
    }

    return(icap.toString());
  }

  /**
   * Ensure that a header string doesn't contain characters that
   * enable cross-site scripting attacks. For now this means
   * limiting header values to Latin-1 characters and disallowing
   * all ASCII control characters.
   * ### Possibly should be restricted to plain ASCII?
   */
  private static final char MIN_ALLOWED_CHARVAL = ' ';
  private static final char MAX_ALLOWED_CHARVAL = '\u00FF';

  public static String sanitizeHeader(String hdrval)
  {
    int len = hdrval.length();

    for (int i = 0; i < len; ++i)
    {
      char ch = hdrval.charAt(i);

      // If any character is outside the allowed bounds
      if ((ch < MIN_ALLOWED_CHARVAL) || (ch > MAX_ALLOWED_CHARVAL))
      {
        // Do a character-by-character conversion
        char[] arr = new char[len];
        hdrval.getChars(0, len, arr, 0);

        for (int j = i; j < len; ++j)
        {
          ch = arr[j];
          if ((ch < MIN_ALLOWED_CHARVAL) || (ch > MAX_ALLOWED_CHARVAL))
            arr[j] = '\u00BF';
        }

        // Return the sanitized string
        return(new String(arr));
      }
    }

    // Most strings won't need sanitization
    return(hdrval);
  }
}
