/* Copyright (c) 2014, 2015, Oracle and/or its affiliates. 
All rights reserved.*/

/*
   DESCRIPTION
    Input stream that enforces a byte limit. Useful for request content
    bodies where the underlying socket may remain open across requests,
    but a specific request carries a header such as Content-Length.
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

import java.io.IOException;
import java.io.FilterInputStream;
import java.io.ByteArrayInputStream;
import java.io.InputStream;

public class LimitedInputStream
     extends FilterInputStream
{
  private static final byte[] EMPTY_CONTENT = new byte[0];

  private final int limit;
  private       int total = 0;
  private       int markPos  = -1;

  private byte[]  single = new byte[1];

  protected boolean closed = false;

  public LimitedInputStream(InputStream in, int limit)
  {
    super(in);
    this.limit = limit;
  }

  public LimitedInputStream()
  {
    this(new ByteArrayInputStream(EMPTY_CONTENT), 0);
  }

  @Override
  public boolean markSupported()
  {
    return(super.markSupported());
  }

  @Override
  public void mark(int readLimit)
  {
    super.mark(readLimit);
    markPos = total;
  }

  @Override
  public void reset()
    throws IOException
  {
    super.reset();
    if (markPos >= 0)
      total = markPos;
  }

  @Override
  public int available()
  {
    return(limit - total);
  }

  @Override
  public long skip(long n)
    throws IOException
  {
    long nbytes = (long)available();
    if (nbytes > n) nbytes = n;
    nbytes = super.skip(nbytes);
    if (nbytes > 0L)
      total += (int)nbytes;
    return(nbytes);
  }

  @Override
  public int read()
  throws IOException
  {
    int n = read(single);
    if (n == 1)
      return((int)(single[0] & 0xFF));
    return(n);
  }

  @Override
  public int read(byte[] b)
  throws IOException
  {
    return(read(b, 0, b.length));
  }

  /**
   * Override this method to time all read I/O operations
   */
  protected int timedRead(byte[] b, int off, int len)
    throws IOException
  {
    return(in.read(b, off, len));
  }

  @Override
  public int read(byte[] b, int off, int len)
  throws IOException
  {
    int n = -1;

    if (len == 0)
      n = 0;
    else
    {
      int nbytes = available();
      if (nbytes > len)
        nbytes = len;
      if (nbytes > 0)
      {
        n = timedRead(b, off, nbytes);
        if (n > 0)
          total += n;
        else if (n < 0)
          total = limit; // Stream is exhausted
      }
    }

    return(n);
  }

  @Override
  public void close()
    throws IOException
  {
    // Ensure that embedded object is closed only once
    if (!closed)
    {
      closed = true;
      super.close();
    }
  }
}
