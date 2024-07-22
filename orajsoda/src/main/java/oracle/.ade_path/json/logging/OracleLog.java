/* Copyright (c) 2014, 2015, Oracle and/or its affiliates. 
All rights reserved.*/

/*
   DESCRIPTION

    Controls logging
 */

/**
 *  This class is not part of the public API, and is
 *  subject to change.
 *
 *  Do not rely on it in your application code.
 *
 *  @author  Max Orgiyan
 */

package oracle.json.logging;

import java.security.AccessController;
import java.security.PrivilegedAction;

public class OracleLog
{
    private static final String TRACE_PROPERTY = "oracle.soda.trace";

    private static volatile boolean loggingEnabled = false;

    // Call initialize() on class load to read the System properties.
    // Put this after static variable initializers.
    static
    {
        initialize();
    }

    static private void initialize()
    {
        try {
            String propStr = getSystemProperty(TRACE_PROPERTY);
            if (propStr != null && propStr.equalsIgnoreCase("true"))
                loggingEnabled = true;
        }
        catch (SecurityException e) {
            // Nothing to do: couldn't read the property
        }
    }

    // Must be private. Otherwise anyone can call it
    // and read system properties with permissions
    // given to SODA.
    static private String getSystemProperty(final String str)
    {
        // This PrivilegedAction mechanism will allow SODA
        // to read the system property if it has the appropriate
        // permission (without requiring the SODA caller to
        // have the permission).
        //
        // ### Need to make sure this is OK if running inside
        //     the RDBMS
        final String p = AccessController.doPrivileged(new PrivilegedAction<String>() {
            public String run() {
                return System.getProperty(str, null);
            }
        });

        return p;
    }

    static public boolean isLoggingEnabled()
    {
        return loggingEnabled;
    }

    static public void enableLogging() { loggingEnabled = true; }
}
