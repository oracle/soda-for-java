/* $Header: xdk/src/java/json/orajsoda/src/main/java/oracle/json/sodautil/MimeTypeLookup.java /st_xdk_soda1/1 2024/07/16 22:56:04 vemahaja Exp $ */

/* Copyright (c) 2014, 2024, Oracle and/or its affiliates.*/

/*
   DESCRIPTION
    Interface providing access to a conversion function that can select
    a mime type based on the file name (extension).

   PRIVATE CLASSES

   NOTES

   MODIFIED    (MM/DD/YY)
    dmcmahon    06/17/14 - Creation
 */

/**
 *  @version $Header: xdk/src/java/json/orajsoda/src/main/java/oracle/json/sodautil/MimeTypeLookup.java /st_xdk_soda1/1 2024/07/16 22:56:04 vemahaja Exp $
 *  @author  dmcmahon
 *  @since   release specific (what release of product did this appear in)
 */

package oracle.json.sodautil;

public interface MimeTypeLookup
{
  public String getMimeType(String fileName);
}
