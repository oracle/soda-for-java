/* Copyright (c) 2014, 2024, Oracle and/or its affiliates. */
/* All rights reserved.*/

/*
   DESCRIPTION
    Utility functions for byte arrays.
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

import java.io.IOException;
import java.io.InputStream;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.CharacterCodingException;

import java.util.Arrays;

public class ByteArray
{
  private static final String hexDigits = "0123456789ABCDEFabcdef";

  // ### Possibly these constants should be centralized
  //     along with other constants somewhere.
  public static final String      DEFAULT_ENCODING    = "utf-8";
  public static final Charset     DEFAULT_CHARSET     =
                                  Charset.forName(DEFAULT_ENCODING);

  private static final String     EMPTY_STRING        = "";
  private static final byte[]     EMPTY_BYTES         = new byte[0];

  private static final int DEFAULT_SIZE = 256;

  private byte[] buffer;
  private int    position = 0;

  public ByteArray(int initSize)
  {
    int sz = DEFAULT_SIZE;
    while (sz < initSize) sz <<= 1;
    buffer = new byte[sz];
  }

  public ByteArray()
  {
    this(DEFAULT_SIZE);
  }

  protected void extend(int sz)
  {
    int newsz = buffer.length;
    while (newsz < (position + sz))
      newsz <<= 1;
    if (newsz > buffer.length)
      buffer = Arrays.copyOf(buffer, newsz);
  }

  protected int capacity()
  {
    return(buffer.length - position);
  }

  /**
   * Return a copy of the internal array, sized to the exact
   * number of actual bytes.
   */
  public byte[] toArray()
  {
    return(Arrays.copyOf(buffer, position));
  }

  /**
   * Return a pointer to the internal array.
   */
  public byte[] getArray()
  {
    return(buffer);
  }

  /**
   * Return the current length of the content of the internal array.
   */
  public int getLength()
  {
    return(position);
  }

  public void reset()
  {
    position = 0;
  }

  public void append(byte[] arr)
  {
    append(arr, 0, arr.length);
  }

  public void append(byte[] arr, int offset, int length)
  {
    if ((length > 0) && (arr != null))
    {
      extend(length);
      System.arraycopy(arr, offset, buffer, position, length);
      position += length;
    }
  }

  public void append(InputStream in)
    throws IOException
  {
    while (in != null)
    {
      if (this.capacity() < DEFAULT_SIZE)
        this.extend(DEFAULT_SIZE);
      int nbytes = in.read(buffer, position, this.capacity()-100);
      if (nbytes < 0)
      {
        in.close();
        break;
      }
      position += nbytes;
    }
  }

  /**
   * Convert the bytes into a string in the specified character set
   */
  public String getString(Charset cs)
  {
    if (cs == null)
      cs = DEFAULT_CHARSET;

    if (position == 0)
      return(EMPTY_STRING);

    return(new String(buffer, 0, position, cs));
  }

  public String getString()
  {
    return(getString(null));
  }

  public static ByteArray loadStream(InputStream in)
    throws IOException
  {
    ByteArray arr = new ByteArray(in.available());
    arr.append(in);
    return(arr);
  }

  /**
   * Render the byte array as a hexadecimal string.
   */
  public static String rawToHex(byte[] buf, int offset, int len)
  {
    char[] hexval = new char[len * 2];
    int    x;
    int    i, j, k;

    j = 0;
    for (i = offset; i < (len + offset); ++i)
    {
      x = (int)(buf[i]);
      k = ((x >> 4) & 0xF);
      hexval[j++] = (char)((k < 10) ? ('0' + k) : ('A' + k - 10));
      k = (x & 0xF);
      hexval[j++] = (char)((k < 10) ? ('0' + k) : ('A' + k - 10));
    }

    // ### Would be best to find a way to do this without copying
    //     the char array
    return(new String(hexval));
  }

  /**
   * Render the byte array as a hexadecimal string.
   */
  public static String rawToHex(byte[] buf)
  {
    return(ByteArray.rawToHex(buf, 0, buf.length));
  }

  public static String rawToLowerHex(byte[] buf, int offset, int len)
  {
    char[] hexval = new char[len * 2];
    int    x;
    int    i, j, k;

    j = 0;
    for (i = offset; i < (len + offset); ++i)
    {
      x = (int)(buf[i]);
      k = ((x >> 4) & 0xF);
      hexval[j++] = (char)((k < 10) ? ('0' + k) : ('a' + k - 10));
      k = (x & 0xF);
      hexval[j++] = (char)((k < 10) ? ('0' + k) : ('a' + k - 10));
    }

    // ### Would be best to find a way to do this without copying
    //     the char array
    return(new String(hexval));
  }

  /**
   * Render the byte array as a hexadecimal string.
   */
  public static String rawToLowerHex(byte[] buf)
  {
    return(ByteArray.rawToLowerHex(buf, 0, buf.length));
  }

  /*
  ** Convert a string from hexadecimal to a byte array
  */
  public static boolean isHex(String hexValue)
  {
    if (hexValue == null) return(false);
    if (hexValue.length() == 0) return(false);
    char hexarray[] = hexValue.toCharArray();
    for (int i = 0; i < hexarray.length; ++i)
      if (hexDigits.indexOf(hexarray[i]) < 0)
        return(false);
    return(true);
  }

  /*
  ** Convert a string from hexadecimal to a byte array
  */
  public static byte[] hexToRaw(String hexValue)
  {
    int    slen = hexValue.length();
    int    nbytes = (slen + 1)/2;
    byte[] result;
    int    x;
    int    val = 0;
    boolean push = ((slen & 1) == 1);
    int    i, n;

    if (nbytes == 0) nbytes = 1;
    result = new byte[nbytes];

    i = n = 0;
    while (i < slen)
    {
      x = hexDigits.indexOf(hexValue.charAt(i++));
      if (x < 0) break;
      if (x > 15) x -= 6;
      val <<= 4;
      val |= x;
      if (push)
      {
        result[n++] = (byte)val;
        val = 0;
      }
      push = !push;
    }
    while (n < nbytes) result[n++] = (byte)0;

    return(result);
  }

  /**
   * Compare two byte arrays for binary order, returning -1, 0, or 1
   * if the first array is less than, equal to, or greater than the
   * second array.
   */
  public static int compareBytes(byte[] buffer1, int offset1, int length1,
                                 byte[] buffer2, int offset2, int length2)
  {
    int idx1 = offset1;
    int idx2 = offset2;
    int mlen = (length1 > length2) ? length2 : length1;
    int diff;

    if ((buffer1 == null) || (buffer2 == null))
    {
      if (buffer1 != null)
        return(1);
      if (buffer2 != null)
        return(-1);
      return(0);
    }

    while (mlen > 0)
    {
      diff = (int)(buffer1[idx1++]) - (int)(buffer2[idx2++]);
      if (diff != 0)
        return(diff);
      --mlen;
    }

    diff = length1 - length2;

    return(diff);
  }

  public static int compareBytes(byte[] buf1, byte[] buf2)
  {
    if (buf1 == buf2)
      return(0);
    int len1 = (buf1 == null) ? 0 : buf1.length;
    int len2 = (buf2 == null) ? 0 : buf2.length;
    return(compareBytes(buf1, 0, len1, buf2, 0, len2));
  }

  /**
   * Scan a byte buffer for a byte pattern.
   *
   * @return the offset of the first occurrence of the pattern,
   *         or -1 if not found
   */
  public static int findPattern(byte[] buffer,
                                int startPosition, int buflen,
                                byte[] pattern, int patlen)
  {
    int midx;
    int plen = patlen;
    int mlen = buflen - startPosition;

    if ((buffer != null) && (pattern != null) && (plen > 0))
      for (midx = startPosition; mlen >= plen; --mlen, ++midx)
        if (compareBytes(buffer, midx, plen, pattern, 0, plen) == 0)
          return(midx);
    return(-1);
  }

  public static String bytesToString(byte[] data,
                                     int offset, int len,
                                     Charset cs)
    throws CharacterCodingException
  {
    if (cs == null)
      cs = DEFAULT_CHARSET;

    if (len == 0)
      return(EMPTY_STRING);

    // Create a decoder that will throw exceptions on errors
    CharsetDecoder decoder = cs.newDecoder()
      .onUnmappableCharacter(CodingErrorAction.REPORT)
      .onMalformedInput(CodingErrorAction.REPORT);

    // Wrap the bytes in a buffer so they can be decoded
    ByteBuffer bb = ByteBuffer.allocate(len);
    bb.put(data, offset, len);
    bb.flip();

    // Do the decode and return the result.
    return(decoder.decode(bb).toString());
  }

  public static String bytesToString(byte[] data)
    throws CharacterCodingException
  {
    return(ByteArray.bytesToString(data, DEFAULT_CHARSET));
  }

  public static String bytesToString(byte[] data, Charset cs)
    throws CharacterCodingException
  {
    if (data == null) return(EMPTY_STRING);
    return(ByteArray.bytesToString(data, 0, data.length, cs));
  }

  public static String bytesToString(byte[] data, int offset, int len)
    throws CharacterCodingException
  {
    return(ByteArray.bytesToString(data, offset, len, DEFAULT_CHARSET));
  }

  public static String bytesToString(ByteArray barr)
    throws CharacterCodingException
  {
    return(ByteArray.bytesToString(barr.buffer, 0, barr.position));
  }

  private static final char[] BASE64DIGITS =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".toCharArray();

  public static String encode64(byte[] buf, int offset, int length)
  {
    if (length == 0) return EMPTY_STRING;

    int    rlen = ((length+2)/3)*4;
    char[] result = new char[rlen];
    int    olen = 0;
    int    havebits = 0;
    int    oldval = 0;
    int    val;

    for (int i = offset; i < (offset + length); ++i)
    {
      val = (int)(buf[i] & 0xFF);

      oldval = (oldval << 8) | val;
      havebits += 8;

      if (havebits == 12)
      {
        val = oldval >> 6;
        result[olen++] = BASE64DIGITS[val];
        val = oldval & 0x3F;
        result[olen++] = BASE64DIGITS[val];
        oldval = 0;
        havebits = 0;
      }
      else if (havebits == 10)
      {
        val = oldval >> 4;
        result[olen++] = BASE64DIGITS[val];
        oldval &= 0xF;
        havebits -= 6;
      }
      else if (havebits == 8)
      {
        val = oldval >> 2;
        result[olen++] = BASE64DIGITS[val];
        oldval &= 0x3;
        havebits -= 6;
      }
    }
    if (havebits > 0)
    {
      val = oldval << (6 - havebits);
      result[olen++] = BASE64DIGITS[val];
    }
    while (olen < rlen)
      result[olen++] = '=';

    return new String(result);
  }

  public static String encode64(byte[] buf)
  {
    return(ByteArray.encode64(buf, 0, buf.length));
  }

  public static byte[] decode64(String str)
  {
    int    slen = str.indexOf('=');
    int    olen = 0;
    int    havebits = 0;
    int    oldval = 0;

    if (slen < 0) slen = str.length();

    if (slen == 0) return EMPTY_BYTES;

    int    rlen = (3 * slen)/4;
    byte[] result = new byte[rlen];

    for (int i = 0; i < slen; ++i)
    {
      char ch = str.charAt(i);
      int  val = 0;

      if ((ch >= 'A') && (ch <= 'Z'))
        val = (ch - 'A');
      else if ((ch >= 'a') && (ch <= 'z'))
        val = (ch - 'a') + 26;
      else if ((ch >= '0') && (ch <= '9'))
        val = (ch - '0') + 26 + 26;
      else if (ch == '+')
        val = 10 + 26 + 26;
      else if (ch == '/')
        val = 11 + 26 + 26;
      else if (ch == '=')
        break;

      oldval = (oldval << 6) | val;
      havebits += 6;

      if (havebits == 8)
      {
        result[olen++] = (byte)oldval;
        oldval = 0;
        havebits = 0;
      }
      else if (havebits == 10)
      {
        result[olen++] = (byte)(oldval >> 2);
        oldval &= 0x3;
        havebits -= 8;
      }
      else if (havebits == 12)
      {
        result[olen++] = (byte)(oldval >> 4);
        oldval &= 0xF;
        havebits -= 8;
      }
    }

    return result;
  }
}
