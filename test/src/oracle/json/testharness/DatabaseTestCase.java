/* Copyright (c) 2014, 2015, Oracle and/or its affiliates. 
All rights reserved.*/

/**
 *  DESCRIPTION
 *   DatabaseTestCase
 */

/**
 *  @author  Josh Spiegel
 */

package oracle.json.testharness;

import oracle.jdbc.OracleConnection;
import java.sql.DatabaseMetaData;

public abstract class DatabaseTestCase extends JsonTestCase {

    public static final int PATCH1 = 1;
    public static final int PATCH2 = 2;

    public static final Integer PATCH_VERSION = Integer.getInteger("patch.version");

    protected OracleConnection conn;

    @Override
        protected void setUp() throws Exception {
        conn = ConnectionFactory.createConnection();
    }

    public boolean isPatch(Integer... patches) {
        if (patches == null || patches.length == 0) {
            throw new IllegalArgumentException();
        }
        for (Integer p : patches) {
            if (p.equals(PATCH_VERSION)) {
                return true;
            }
        }
        return false;
    }

}
