/* Copyright (c) 2014, 2015, Oracle and/or its affiliates. 
All rights reserved.*/

/*
   DESCRIPTION
    Computes a SHA-1 hash on an input string.
 */

/**
 * This class is not part of the public API, and is
 * subject to change.
 *
 * Do not rely on it in your application code.
 *
 *  @author  Doug McMahon
 */

// ### Other functions may be added in the future
//     In particular it may be useful to produce etags via java.util.zip.CRC32

package oracle.json.util;

import oracle.json.logging.OracleLog;

import java.util.logging.Logger;

import java.nio.charset.Charset;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import java.util.UUID;

public final class HashFuncs
{
  // ### Possibly these constants should be centralized
  //     along with other constants somewhere.
  public static final Charset DEFAULT_CHARSET = JsonByteArray.DEFAULT_CHARSET;

  private static final String DEFAULT_ALGORITHM = "SHA-256";
  private static final String MD5_ALGORITHM     = "MD5";

  private static final Logger log = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

  public static byte[] SHA256(String p_input) throws NoSuchAlgorithmException
  {
    byte[] data = p_input.getBytes(HashFuncs.DEFAULT_CHARSET);
    return (SHA256(data));
  }

  public static byte[] MD5(byte[] p_input) throws NoSuchAlgorithmException
  {
    MessageDigest md = MessageDigest.getInstance(MD5_ALGORITHM);
    return (md.digest(p_input));
  }

  public static byte[] SHA256(byte[] p_input) throws NoSuchAlgorithmException
  {
    MessageDigest md = MessageDigest.getInstance(DEFAULT_ALGORITHM);
    md.update(p_input, 0, p_input.length);
    return (md.digest());
  }

  public static byte[] getRandom()
  {
    UUID randval = UUID.randomUUID();
    long lsb = randval.getLeastSignificantBits();
    long msb = randval.getMostSignificantBits();
    byte[] data = new byte[16];
    data[7] = (byte) (msb & 0xFF);
    data[6] = (byte) ((msb >> 8) & 0xFF);
    data[5] = (byte) ((msb >> 16) & 0xFF);
    data[4] = (byte) ((msb >> 24) & 0xFF);
    data[3] = (byte) ((msb >> 32) & 0xFF);
    data[2] = (byte) ((msb >> 40) & 0xFF);
    data[1] = (byte) ((msb >> 48) & 0xFF);
    data[0] = (byte) ((msb >> 56) & 0xFF);
    data[15] = (byte) (lsb & 0xFF);
    data[14] = (byte) ((lsb >> 8) & 0xFF);
    data[13] = (byte) ((lsb >> 16) & 0xFF);
    data[12] = (byte) ((lsb >> 24) & 0xFF);
    data[11] = (byte) ((lsb >> 32) & 0xFF);
    data[10] = (byte) ((lsb >> 40) & 0xFF);
    data[9] = (byte) ((lsb >> 48) & 0xFF);
    data[8] = (byte) ((lsb >> 56) & 0xFF);
    return (data);
  }

  public static byte[] getTimeRandom() {
// BEGIN INFEASIBLE
    long msb = System.nanoTime();
    long lsb = System.currentTimeMillis();
    byte[] data = new byte[16];
    data[7] = (byte) (msb & 0xFF);
    data[6] = (byte) ((msb >> 8) & 0xFF);
    data[5] = (byte) ((msb >> 16) & 0xFF);
    data[4] = (byte) ((msb >> 24) & 0xFF);
    data[3] = (byte) ((msb >> 32) & 0xFF);
    data[2] = (byte) ((msb >> 40) & 0xFF);
    data[1] = (byte) ((msb >> 48) & 0xFF);
    data[0] = (byte) ((msb >> 56) & 0xFF);
    data[15] = (byte) (lsb & 0xFF);
    data[14] = (byte) ((lsb >> 8) & 0xFF);
    data[13] = (byte) ((lsb >> 16) & 0xFF);
    data[12] = (byte) ((lsb >> 24) & 0xFF);
    data[11] = (byte) ((lsb >> 32) & 0xFF);
    data[10] = (byte) ((lsb >> 40) & 0xFF);
    data[9] = (byte) ((lsb >> 48) & 0xFF);
    data[8] = (byte) ((lsb >> 56) & 0xFF);
    return (data);
// END INFEASIBLE
  }

  private final SecureRandom rand = new SecureRandom();

  public HashFuncs()
  {
    try
    {
      rand.setSeed(System.nanoTime());
    }
    catch (RuntimeException e)
    {
      if (OracleLog.isLoggingEnabled())
        log.warning(e.toString());
      throw e;
    }
  }

  public byte[] getSecureRandom()
  {
    byte[] data = new byte[16];
    rand.nextBytes(data);
    return (data);
  }
}
