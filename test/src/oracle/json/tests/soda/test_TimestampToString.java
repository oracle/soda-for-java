

/* $Header: xdk/test/txjjson/src/oracle/json/tests/soda/test_TimestampToString.java /st_xdk_soda1/1 2020/01/29 12:44:01 jspiegel Exp $ */

/* Copyright (c) 2019, 2020, Oracle and/or its affiliates. 
All rights reserved.*/
package oracle.json.tests.soda;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;

import oracle.json.testharness.SodaTestCase;
import oracle.soda.rdbms.impl.OracleDatabaseImpl;

/**
 * Tests conversion between SQL timestamp and string
 * used for last modified and creation time properties
 */
public class test_TimestampToString extends SodaTestCase {
  
  private static int C = 10000;

  public void testYears() {
    timestampTest("01-Jan-1970 00:00:00.000000", "interval '1' year ", 7000);
  }
  
  public void testMonths() {
    timestampTest("01-Jan-1970 00:00:00.000000", "interval '1' month ", 7000);
  }
  
  public void testDays() {
    timestampTest("01-Jan-1970 00:00:00.000000", "interval '1' day ", C);
  }
  
  public void testMinutes() {
    timestampTest("01-Jan-2000 00:00:00.000000", "interval '1' minute ", C);
  }

  public void testSeconds() {
    timestampTest("01-Jan-2000 00:00:00.000000", "interval '1' second ", C);
  }
  
  public void testMilliseconds() {
    timestampTest("01-Jan-2000 00:00:00.000000", "interval '0.1' second ", C);
    timestampTest("01-Jan-2000 00:00:00.000000", "interval '0.01' second ", C);
    timestampTest("01-Jan-2000 00:00:00.000000", "interval '0.001' second ", C);
  }

  public void testMicroseconds() {
    timestampTest("01-Jan-2000 00:00:00.000000", "interval '0.0001' second ", C);
    timestampTest("01-Jan-2000 00:00:00.000000", "interval '0.00001' second ", C);
    timestampTest("01-Jan-2000 00:00:00.000000", "interval '0.000001' second ", C);
  }

  public void testNanoseconds() {
    timestampTest("01-Jan-2000 00:00:00.000000", "interval '0 00:00:00.0000001' day to second(9)", C);
    timestampTest("01-Jan-2000 00:00:00.000000", "interval '0 00:00:00.00000001' day to second(9)", C);
    timestampTest("01-Jan-2000 00:00:00.000000", "interval '0 00:00:00.000000001' day to second(9)", C);
  }

  private void timestampTest(String start, String interval, int count) {
    try {
      Statement stmt = this.conn.createStatement();
      ResultSet rs = stmt.executeQuery("select ts, to_char(ts, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF\"Z\"') from "
          + "(select TO_TIMESTAMP ('" + start + "', 'DD-Mon-YYYY HH24:MI:SS.FF') + (" + interval
          + " * level) ts from dual connect by level <= " + count + ")");
      while (rs.next()) {
        LocalDateTime ldt = rs.getObject(1, LocalDateTime.class);
        String str = rs.getObject(2, String.class);
        String str2 = OracleDatabaseImpl.localDateTimeToString(ldt);
        assertEquals(str, str2);
      }
      stmt.close();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
  
}