/* Copyright (c) 2014, 2015, Oracle and/or its affiliates. 
All rights reserved.*/

/*
   DESCRIPTION
    This is the RDBMS-specific implementation of OracleDocumentFragment
 */

/**
 *  This class is not part of the public API, and is
 *  subject to change.
 *
 *  Do not rely on it in your application code.
 *
 *  @author  Doug McMahon
 *  @author  Max Orgiyan
 */

package oracle.soda.rdbms.impl;

import java.io.InputStream;

public class OracleDocumentFragmentImpl extends OracleDocumentImpl
{
  private long offset = 0L;
  private long totalLength = -1L;

  OracleDocumentFragmentImpl(String docid,
                     String version,
                     String tstamp,
                     InputStream payloadStream,
                     String contentType)
  {
    super(docid, version, tstamp, payloadStream, contentType);
  }
  
  OracleDocumentFragmentImpl(String docid, String version, String tstamp, byte[] payload)
  {
    super(docid, version, tstamp, payload);
  }

  void setFragmentInfo(long offset, long totalLength)
  {
    this.offset = offset;
    this.totalLength = totalLength; // this.len is just the fragment size
  }

  /**
   * Returns the offset of the range of bytes represented by
   * this document.
   * Returns 0L for documents that are not range transfer results.
   */
  public long getOffset()
  {
    return(offset);
  }

  /**
   * Returns the total size of the base document of a range transfer.
   * Returns -1L for documents that are not range transfer results.
   */
  public long getTotalDatabaseObjectLength()
  {
    return(totalLength);
  }

}
