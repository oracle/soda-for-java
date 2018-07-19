/* $Header: xdk/src/java/json/src/oracle/soda/rdbms/impl/OracleDocumentImpl.java /main/21 2015/12/24 12:19:36 morgiyan Exp $ */

/* Copyright (c) 2014, 2015, Oracle and/or its affiliates. 
All rights reserved.*/

/*
   DESCRIPTION
    Container to hold a JSON row object. The types of object are:
     - Row fetched from the database with content
     - Row fetched from the database without the content (keys-only)
     - Row insert candidate formed by caller (without key)
     - Row insert/update candidate formed by caller (with key)

   NOTES
    ### For now, content will be represented as a byte array.
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

import java.nio.charset.Charset;
import java.nio.charset.CharacterCodingException;

import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayInputStream;

import java.util.logging.Logger;

import javax.activation.MimeType;
import javax.activation.MimeTypeParseException;

import oracle.json.logging.OracleLog;

import oracle.soda.OracleDocument;
import oracle.soda.OracleException;

import oracle.json.util.ByteArray;
import oracle.json.util.JsonByteArray;
import oracle.json.util.LimitedInputStream;

public class OracleDocumentImpl implements OracleDocument
{
  static final String APPLICATION_JSON = "application/json";
  private static final int UNKNOWN_LENGTH = -1;
    
  private static final Logger log =
    Logger.getLogger(OracleDocumentImpl.class.getName());
  
  private final String docid;
  private final String tstamp;
  private final String version;
 
  private byte[] payload;
  private InputStream payloadStream;

  private String ctype = APPLICATION_JSON;
  private String creationTime;
  private int len = UNKNOWN_LENGTH;

  OracleDocumentImpl(String docid,
                     String version,
                     String tstamp,
                     byte[] payload,
                     String contentType)
  {
    this.docid   = docid;
    this.version = version;
    this.tstamp  = tstamp;
    this.payload = payload;

    if (contentType != null)
      this.ctype = contentType;

    if (payload != null)
      this.len = payload.length;
  }

  OracleDocumentImpl(String docid,
                     String version,
                     String tstamp,
                     InputStream payloadStream,
                     String contentType)
  {
    this.docid   = docid;
    this.version = version;
    this.tstamp  = tstamp;
    this.payloadStream = payloadStream;

    if (contentType != null)
      this.ctype = contentType;

    if (payloadStream != null)
    {
      if ((payloadStream instanceof LimitedInputStream) ||
          (payloadStream instanceof ByteArrayInputStream))
      {
        try
        {
          this.len = payloadStream.available();
        }
        catch (IOException e)
        {
          this.len = UNKNOWN_LENGTH;
        }
      }
    } 
  }
  
  OracleDocumentImpl(String docid, String version, String tstamp, byte[] payload)
  {
    this(docid, version, tstamp, payload, null);
  }

  OracleDocumentImpl(String docid,
                     String version,
                     String tstamp,
                     String payload,
                     String contentType)
  {
    this(docid, version, tstamp,
         (payload == null) ? (byte[])null :
         payload.getBytes(ByteArray.DEFAULT_CHARSET), contentType);
  }

  OracleDocumentImpl(String docid, String version, String tstamp)
  {
    this(docid, version, tstamp, (byte[])null, null);
  }

  OracleDocumentImpl(String docid, String payload)
  {
    this(docid, null, null, 
            (payload == null) ? (byte[])null :
                payload.getBytes(ByteArray.DEFAULT_CHARSET), null);
  }

  OracleDocumentImpl(String payload)
  {
    this(null, payload);
  }

  OracleDocumentImpl(String docid, byte[] payload)
  {
    this(docid, null, null, payload, null);
  }

  OracleDocumentImpl(String docid, InputStream payload)
    throws IOException
  {
    this(docid, payload, null);
  }
  
  OracleDocumentImpl(String docid, InputStream payload,
                            String contentType)
  throws IOException
  {
    this(docid, null, null, payload, contentType);
  }

  OracleDocumentImpl(byte[] payload)
  {
    this(null, null, null, payload, null);
  }

  public String getKey()
  {
    return(docid);
  }

  public String getLastModified()
  {
    return(tstamp);
  }

  public String getCreatedOn()
  {
    return(creationTime);
  }

  public String getVersion()
  {
    return(version);
  }

  public byte[] getContentAsByteArray() throws OracleException
  {
    // If it's an InputStream, convert to byteArray
    if (payloadStream != null)
    {
      try
      {
        payload = JsonByteArray.loadStream(payloadStream).toArray();
      }
      catch (IOException e)
      {
        if (OracleLog.isLoggingEnabled())
          log.severe(e.toString());
        // ### Revisit classification of this exception
        throw new OracleException(e);
      }
      // Destroy the InputStream
      payloadStream = null;
    }
    // Return byte[] payload
    return(payload);
  }

  // ### Not part of a public API.
  public InputStream getContentAsStream()
  {
    if (payloadStream != null)
      // ### We could be handing out a stream already consumed
      return(payloadStream);

    // Convert byte[] to InputStream
    return((payload == null) ? null : new ByteArrayInputStream(payload));
  }

  // ### Not part of a public API.
  public boolean hasStreamContent()
  {
    // ### We could be returning true for a stream already consumed
    if (payloadStream != null)
      return(true);

    return(false);
  }

  public String getContentAsString() throws OracleException
  {
    return(getContentAsString(false));
  }

  public String getContentAsString(boolean checked)
    throws OracleException
  {
    // ### What is the media type is null but the content
    // is really JSON? Such a document could be present in a heterogeneous
    // collection on an existing table. Currently, it will fail this
    // check. Do we want to support this case?
    if (!isJSON())
    {
      throw SODAUtils.makeException(SODAMessage.EX_MEDIA_TYPE_NOT_JSON);
    }

    byte[] bytes = getContentAsByteArray();
    if (bytes == null) return(null);
    Charset cs = JsonByteArray.getJsonCharset(bytes);

    // Unchecked conversion is faster but replaces bad bytes with Unicode
    // replacement characters (a questionable semantic, but OK for this API).
    if (!checked)
      return new String(bytes, cs);

    // Otherwise the bytes need to be validated as they are converted
    try
    {
      return ByteArray.bytesToString(bytes, cs);
    }
    catch (CharacterCodingException e)
    {
      throw new OracleException(e);
    }
  }

  void setContent(byte[] content)
  {
     payload = content;
  }

  private boolean isJSON(String ctype)
  {
    if (ctype == null)
    {
      return false;
    }
    if ((ctype == APPLICATION_JSON) || (ctype.equals(APPLICATION_JSON)))
    {
      return true; // optimization
    }
    try {
      MimeType mimeType = new MimeType(ctype);
      return "application".equals(mimeType.getPrimaryType()) &&
             "json".equals(mimeType.getSubType());
    } catch (MimeTypeParseException e) {
      // gulp
      return false;
    }
  }

  public boolean isJSON()
  {
    return isJSON(ctype);
  }

  public String getMediaType()
  {
    return(ctype);
  }

  /**
   * Returns the content length (if known), or -1 if not known.
   */
  public int getContentLength()
  {
    return(len);
  }

  void setContentType(String contentType)
  {
    this.ctype = contentType;
  }

  void setContentTypeJson()
  {
    setContentType(APPLICATION_JSON);
  }

  void setCreatedOn(String createdOn)
  {
    this.creationTime = createdOn;
  }
 
  @Override
  public String toString()
  {
    return(docid);
  }
}
