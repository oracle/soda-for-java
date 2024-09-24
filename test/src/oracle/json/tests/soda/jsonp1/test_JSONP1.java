/* $Header: xdk/test/txjjson/src/oracle/json/tests/soda/jsonp1/test_JSONP1.java /st_xdk_soda1/1 2021/08/25 00:17:25 morgiyan Exp $ */

/* Copyright (c) 2021, Oracle and/or its affiliates. */

/*
   MODIFIED    (MM/DD/YY)
    morgiyan    06/17/21 - Creation
 */
package oracle.json.tests.soda.jsonp1;

/**
 *  @version $Header: xdk/test/txjjson/src/oracle/json/tests/soda/jsonp1/test_JSONP1.java /st_xdk_soda1/1 2021/08/25 00:17:25 morgiyan Exp $
 *  @author  morgiyan
 */
import java.util.ArrayList;
import java.util.List;

import javax.json.Json;
import javax.json.JsonObject;

import oracle.json.testharness.SodaTestCase;
import oracle.soda.OracleCollection;
import oracle.soda.OracleCursor;
import oracle.soda.OracleDocument;
import oracle.soda.OracleException;
import oracle.soda.OracleOperationBuilder;
import oracle.soda.rdbms.OracleRDBMSMetadataBuilder;
import oracle.soda.rdbms.impl.OracleOperationBuilderImpl;

/**
 * @version $Header: xdk/test/txjjson/src/oracle/json/tests/soda/jsonp1/test_JSONP1.java /st_xdk_soda1/1 2021/08/25 00:17:25 morgiyan Exp $
 * @author Josh Spiegel [josh.spiegel@oracle.com]
 */
public class test_JSONP1 extends SodaTestCase {

  public static void testClasspath() {
    assertTrue(Json.class != null);
  }
  
}

