/* Copyright (c) 2014, 2020, Oracle and/or its affiliates. 
All rights reserved.*/

/**
 * DESCRIPTION
 *  Soda Utils
 */

/**
 * @author  Jianye Wang
 */ 

package oracle.json.testharness;

import java.nio.charset.Charset;

import java.io.IOException;
import java.io.FileInputStream;

import java.util.Arrays;

public class SodaUtils {

  private static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");

  public static byte[] slurpFileAsByteArray(String fname)
    throws IOException
  {
    FileInputStream fin = new FileInputStream(fname);
    if (fin == null) return(null);

    int        bufsize = 4096;
    int        offset = 0;
    byte[]     temp = new byte[bufsize];

    while (true)
    {
      int remaining = bufsize - offset;
      if (remaining == 0)
      {
        remaining = bufsize;
        bufsize += remaining;
        temp = Arrays.copyOf(temp, bufsize);
      }

      int nbytes = fin.read(temp, offset, remaining);
      if (nbytes < 0) break;

      offset += nbytes;
    }

    fin.close();

    return temp;
  }

  public static String slurpFile(String fname)
    throws IOException
  {
    FileInputStream fin = new FileInputStream(fname);
    if (fin == null) return(null);

    int        bufsize = 4096;
    int        offset = 0;
    byte[]     temp = new byte[bufsize];

    while (true)
    {
      int remaining = bufsize - offset;
      if (remaining == 0)
      {
        remaining = bufsize;
        bufsize += remaining;
        temp = Arrays.copyOf(temp, bufsize);
      }

      int nbytes = fin.read(temp, offset, remaining);
      if (nbytes < 0) break;

      offset += nbytes;
    }

    fin.close();

    return(new String(temp, 0, offset, DEFAULT_CHARSET));
  }
} 
