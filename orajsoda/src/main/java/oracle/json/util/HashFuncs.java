/* Copyright (c) 2014, 2024, Oracle and/or its affiliates. */
/* All rights reserved.*/

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
//    In particular it may be useful to produce etags via java.util.zip.CRC32

package oracle.json.util;

import oracle.json.logging.OracleLog;

import java.util.Arrays;
import java.util.logging.Logger;

import java.lang.Thread;

/***
// ### From Java 9:
import java.lang.ProcessHandle;
***/

import java.nio.charset.Charset;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;

import javax.crypto.Mac;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

import java.util.UUID;

// ### From Java 7:
import java.util.concurrent.ThreadLocalRandom;

public final class HashFuncs
{
  // ### Possibly these constants should be centralized
  // ### along with other constants somewhere.
  public static final Charset DEFAULT_CHARSET = ByteArray.DEFAULT_CHARSET;

  public static final String SHA256_ALGORITHM = "SHA-256";
  public static final String SHA1_ALGORITHM   = "SHA-1";
  public static final String MD5_ALGORITHM    = "MD5";

  private static final Logger log = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

  // Reusable message digest objects
  private MessageDigest sha256_md;
  private MessageDigest sha1_md;
  private MessageDigest md5_md;
  private Mac hmac256;
  private Mac hmac1;
  private SecretKeyFactory sha256_factory;
  private SecretKeyFactory sha1_factory;

  private byte[] computeDigest(MessageDigest md, byte[] p_input)
  {
    md.reset();
    return md.digest(p_input);
  }

  public byte[] SHA256(String p_input)
    throws NoSuchAlgorithmException
  {
    byte[] data = p_input.getBytes(HashFuncs.DEFAULT_CHARSET);
    return (SHA256(data));
  }

  public byte[] MD5(String p_input)
    throws NoSuchAlgorithmException
  {
    byte[] data = p_input.getBytes(HashFuncs.DEFAULT_CHARSET);
    return (MD5(data));
  }

  public byte[] SHA1(String p_input)
    throws NoSuchAlgorithmException
  {
    byte[] data = p_input.getBytes(HashFuncs.DEFAULT_CHARSET);
    return (SHA1(data));
  }

  public byte[] SHA256(byte[] p_input)
    throws NoSuchAlgorithmException
  {
    if (sha256_md == null)
      sha256_md = MessageDigest.getInstance(SHA256_ALGORITHM);

    return computeDigest(sha256_md, p_input);
  }

  public byte[] MD5(byte[] p_input)
    throws NoSuchAlgorithmException
  {
    if (md5_md == null)
      md5_md = MessageDigest.getInstance(MD5_ALGORITHM);

    return computeDigest(md5_md, p_input);
  }

  public byte[] SHA1(byte[] p_input)
    throws NoSuchAlgorithmException
  {
    if (sha1_md == null)
      sha1_md = MessageDigest.getInstance(SHA1_ALGORITHM);

    return computeDigest(sha1_md, p_input);
  }

  private MessageDigest getDigester(String algorithm)
    throws NoSuchAlgorithmException
  {
    if (algorithm.equals(MD5_ALGORITHM))
    {
      if (md5_md == null)
        md5_md = MessageDigest.getInstance(MD5_ALGORITHM);
      return md5_md;
    }
    else if (algorithm.equals(SHA1_ALGORITHM))
    {
      if (sha1_md == null)
        sha1_md = MessageDigest.getInstance(SHA1_ALGORITHM);
      return sha1_md;
    }
    else if (algorithm.equals(SHA256_ALGORITHM))
    {
      if (sha256_md == null)
        sha256_md = MessageDigest.getInstance(SHA256_ALGORITHM);
      return sha256_md;
    }
    throw new NoSuchAlgorithmException(algorithm);
  }

  public byte[] computeDigest(String algorithm, byte[] p_input)
    throws NoSuchAlgorithmException
  {
    MessageDigest md = getDigester(algorithm);
    return computeDigest(md, p_input);
  }

  private static long mix64(long x)
  {
    x = (x ^ (x >>> 33)) * 0xFF51AFD7ED558CCDL;
    x = (x ^ (x >>> 33)) * 0xC4CEB9FE1A85EC53L;
    return(x ^ (x >>> 33));
  }

  public static byte[] getRandomUUID()
  {
    byte[] data = new byte[16];
    UUID randval = UUID.randomUUID();
    long lsb = randval.getLeastSignificantBits();
    long msb = randval.getMostSignificantBits();
    data[7]  = (byte) (msb & 0xFF);
    data[6]  = (byte) ((msb >> 8) & 0xFF);
    data[5]  = (byte) ((msb >> 16) & 0xFF);
    data[4]  = (byte) ((msb >> 24) & 0xFF);
    data[3]  = (byte) ((msb >> 32) & 0xFF);
    data[2]  = (byte) ((msb >> 40) & 0xFF);
    data[1]  = (byte) ((msb >> 48) & 0xFF);
    data[0]  = (byte) ((msb >> 56) & 0xFF);
    data[15] = (byte) (lsb & 0xFF);
    data[14] = (byte) ((lsb >> 8) & 0xFF);
    data[13] = (byte) ((lsb >> 16) & 0xFF);
    data[12] = (byte) ((lsb >> 24) & 0xFF);
    data[11] = (byte) ((lsb >> 32) & 0xFF);
    data[10] = (byte) ((lsb >> 40) & 0xFF);
    data[9]  = (byte) ((lsb >> 48) & 0xFF);
    data[8]  = (byte) ((lsb >> 56) & 0xFF);
    return (data);
  }

  //
  // This uses a SecureRandom generator instead of the standard
  // Random generator. The Random generator may use as few as 48
  // bits from the setSeed() method, and has no ability to
  // accumulate entropy from multiple sources beyond the single
  // long integer seed.
  // Since we don't actually need security, we could get away with
  // an alternate implementation of a Random generator that had
  // more internal state. For now, we'll just use a SecureRandom
  // and pay the performance cost. At least with this technique,
  // there won't be any need for synchronization across database
  // instances, as there is with UUID.randomUUID().
  //
  private final SecureRandom rand = new SecureRandom();
  
  private void initRandom(byte[] macAddress)
  {
    // To help ensure that no two generators running on different
    // machines or even on the same machine will end up seeding their
    // generators in exactly same way, regardless of how their clocks
    // are set and regardless of whether any true entropy was available
    // from the hardware, we should add entropy from system-specific
    // values.
    //   1. Add entropy from a local randomness source (if available)
    //   2. Add entropy from the exact time on the local clock
    //   3. Add information from the network MAC address (cross-machine entropy)
    //   4. Add PID information from the OS (cross-process entropy)
    //   5. Add TID information from the VM (cross-thread entropy)

    // 1) Start with some entropy from a local randomness source
    //    If this is of sufficient quality, it's probably good enough
    //    without any other entropy.

    // Seed the random generator with entropy from the thread
    // local random source. Since this source was seeded from
    // a SecureRandom initial source, it should have sufficient
    // entropy by itself, but will avoid synchronization.
    ThreadLocalRandom trand = ThreadLocalRandom.current();
    rand.setSeed(trand.nextLong());
    rand.setSeed(trand.nextLong());
    rand.setSeed(trand.nextLong());
    rand.setSeed(trand.nextLong());
/***
    // ### Work around the fact that ThreadLocalRandom won't compile yet
    // ### due to the JDK 1.6 limitation. This accepts one synchronization
    // ### operation to get some initial entropy for the independent
    // ### SecureRandom generator here. Note that this is done instead
    // ### of creating a new SecureRandom and then using generateSeed().
    // ### Creating a new SecureRandom and then attempting to get entropy
    // ### without seeding it may block waiting for entropy bits from a
    // ### device (e.g. /dev/random on Unix).
    UUID randval = UUID.randomUUID();
    rand.setSeed(HashFuncs.mix64(randval.getLeastSignificantBits()));
    rand.setSeed(HashFuncs.mix64(randval.getMostSignificantBits()));
***/
    // 2) Add some entropy from the local clock
    //    This exploits the fact that it's extremely unlikely that
    //    any two machines around the world have their clocks synchronized
    //    to the nanosecond, and that two VMs are executing this code
    //    at exactly the same time to the nanosecond.
    rand.setSeed(HashFuncs.mix64(System.currentTimeMillis()));
    rand.setSeed(HashFuncs.mix64(System.nanoTime()));

    // 3) If possible add entropy from network MAC address
    //    This is useful to ensure generators across the world aren't
    //    seeding the same state even if their clocks are exactly in sync.
    if (macAddress != null) rand.setSeed(macAddress);
/***
    // 4. Add entropy from the OS process ID
    //    There isn't much entropy here but it will break ties if
    //    multiple VM instances are running on the same machine, sharing
    //    the same MAC address. It's also better than nothing for
    //    cross-machine situations where the MAC address is unavailable.
    try
    {
      long pid = ProcessHandle.current().pid();
      rand.setSeed(HashFuncs.mix64(pid));
    }
    catch (UnsupportedOperationException e)
    {
      // Ignore errors
    }
***/
    // 5) Add entropy from the thread ID
    //    There may be no useful entropy here, but it will break ties
    //    if threads are racing to seed generators on the same VM instance.
    long tid = Thread.currentThread().getId();
    rand.setSeed(HashFuncs.mix64(tid));
  }

  public HashFuncs(byte[] macAddress)
  {
    try
    {
      initRandom(macAddress);
    }
    catch (RuntimeException e)
    {
      if (OracleLog.isLoggingEnabled())
        log.warning(e.toString());
      throw e;
    }
  }

  public HashFuncs()
  {
    this(null);
  }

  public long getRandomLong()
  {
    return rand.nextLong();
  }

  public void fillRandomBytes(byte[] data)
  {
    rand.nextBytes(data);
  }

  public byte[] getRandom()
  {
    byte[] data = new byte[16];
    fillRandomBytes(data);
    // Make sure the random data is usable as a UUID
    data[6] &= 0x0f;
    data[6] |= 0x40;
    data[8] &= 0x3f;
    data[8] |= 0x80; 
    return (data);
  }

  /*
  ** These values are exposed in case a caller needs intermediate products
  */
  public int num_iters = 0;
  public byte[] salt;

  public byte[] salted_hash;
  public byte[] client_key;
  public byte[] server_key;
  public byte[] stored_key;

  /**
   * Compute a SCRAM credential suitable for use with the SASL
   * protocol.
   *   p = string to use as a credential
   *   method = "SHA-1" or "SHA-256"
   *   iters = number of iterations (default to 10000 for SHA1, 15000 for SHA256, minimal 5000)
   *   client_string = "Client Key" in UTF-8 encoding
   *   server_string = "Server Key" in UTF-8 encoding
   *
   * Output:
   *   1  byte length of salt (0 implies 16-byte default length)
   *   3  bytes iteration count (big-endian integer)
   *   M  bytes salt (random)
   *   N  bytes stored_key
   *   N  bytes server_key
  * @throws GeneralSecurityException 
   */
  public byte[] computeScramCredential(String p,
                                       String method,
                                       int iters,
                                       int salt_len,
                                       byte[] client_string,
                                       byte[] server_string)
    throws GeneralSecurityException
  {
    salt = new byte[salt_len];
    fillRandomBytes(salt);

    return computeScramCredential(p, method, iters, salt,
                                  client_string, server_string);
  }

  public byte[] computeScramCredential(String p,
                                       String method,
                                       int iters,
                                       byte[] salt,
                                       byte[] client_string,
                                       byte[] server_string)
    throws GeneralSecurityException
  {
    MessageDigest md = getDigester(method);
    num_iters = iters;

    salted_hash = hi(method, p, salt, num_iters);
    client_key  = hmac(method, salted_hash, client_string);
    server_key  = hmac(method, salted_hash, server_string);
    stored_key  = computeDigest(md, client_key);
    int l = 4 + salt.length + stored_key.length + server_key.length;
    byte[] result = new byte[l];

    l = 0;
    result[l++] = (byte)salt.length;
    result[l++] = (byte)((num_iters >> 16) & 0xFF);
    result[l++] = (byte)((num_iters >> 8) & 0xFF);
    result[l++] = (byte)(num_iters & 0xFF);
    System.arraycopy(salt, 0, result, l, salt.length);
    l += salt.length;
    System.arraycopy(stored_key, 0, result, l, stored_key.length);
    l += stored_key.length;
    System.arraycopy(server_key, 0, result, l, server_key.length);

    return result;
  }

  /**
   * Unpack the SCRAM credential consisting of:
   *   1  byte length of salt (0 defaults to 16 for older records)
   *   3  bytes iteration count (big-endian integer)
   *   M  bytes salt
   *   N  bytes stored_key
   *   N  bytes server_key
   */
  public void parseScramCredential(String credValue)
  {
    byte[] credential = ByteArray.decode64(credValue);

    int l = 0;

    int salt_len = ((int)credential[l++] & 0xFF);
    if (salt_len == 0) salt_len = 16; // Prop up older records

    num_iters = 0;
    num_iters |= ((int)credential[l++] & 0xFF) << 16;
    num_iters |= ((int)credential[l++] & 0xFF) << 8;
    num_iters |= ((int)credential[l++] & 0xFF);

    l = (credential.length - 4 - salt_len)/2;

    stored_key = new byte[l];
    server_key = new byte[l];

    l = 4;

    salt = new byte[salt_len];

    System.arraycopy(credential, l, salt, 0, salt.length);
    l += salt.length;
    System.arraycopy(credential, l, stored_key, 0, stored_key.length);
    l += stored_key.length;
    System.arraycopy(credential, l, server_key, 0, server_key.length);
  }

  private byte[] hi(final String algo, final String password,
                    final byte[] salt, final int iterations) 
    throws NoSuchAlgorithmException, InvalidKeySpecException
  {
      PBEKeySpec spec;
      SecretKeyFactory keyFactory;
      
      if (algo.equals(SHA256_ALGORITHM))
      {
         spec = new PBEKeySpec(password.toCharArray(), salt, iterations, 32 * 8);
         if (sha256_factory == null)
            sha256_factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
         keyFactory = sha256_factory;
      }
      else if (algo.equals(SHA1_ALGORITHM))
      {
          spec = new PBEKeySpec(password.toCharArray(), salt, iterations, 20 * 8);
          if (sha1_factory == null)
             sha1_factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
          keyFactory = sha1_factory;
      }
      else 
          throw new NoSuchAlgorithmException(algo);   
      return keyFactory.generateSecret(spec).getEncoded();
  }

  public byte[] hmac(final String algo, final byte[] bytes, final byte[] key)
    throws NoSuchAlgorithmException, InvalidKeyException
  {
      SecretKeySpec signingKey;
      Mac mac;
      if (algo.equals(SHA256_ALGORITHM))
      {
          signingKey = new SecretKeySpec(bytes, "HmacSHA256");
          if (hmac256 == null)
             hmac256 = Mac.getInstance("HmacSHA256");
          mac = hmac256;
      }
      else if (algo.equals(SHA1_ALGORITHM))
      {
          signingKey = new SecretKeySpec(bytes, "HmacSHA1");
          if (hmac1 == null)
             hmac1 = Mac.getInstance("HmacSHA1");
          mac = hmac1;
      }
      else 
          throw new NoSuchAlgorithmException(algo);
      mac.init(signingKey);
      return mac.doFinal(key);
  }

  public static final byte[] CLIENT_KEY =
    "Client Key".getBytes(ByteArray.DEFAULT_CHARSET);
  public static final byte[] SERVER_KEY =
    "Server Key".getBytes(ByteArray.DEFAULT_CHARSET);

  public byte[] computeScramCredential(String p,
                                       String method,
                                       int min_iters,
                                       int salt_len)
    throws GeneralSecurityException
  {
    return computeScramCredential(p, method, min_iters, salt_len,
                                  CLIENT_KEY, SERVER_KEY);
  }
}
