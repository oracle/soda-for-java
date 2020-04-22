/* $Header: xdk/src/java/json/src/oracle/json/common/Configuration.java /main/17 2016/05/07 08:27:02 dmcmahon Exp $ */

/* Copyright (c) 2014, 2016, Oracle and/or its affiliates. 
All rights reserved.*/

/*
   DESCRIPTION
    The Configuration class holds parameter values for an internally-defined
    set of keys. This isolates callers from the exact nature of the
    configuration information. Typically this comes from servlet or context
    configuration information, e.g. in a web.xml file, but may come from
    system environment variables to support command-line invocations.

    List of important properties:
      log4j properties file location
      Database URI (or equivalent component info)
      Database user name
      Database pass word
      Async polling interval
      Session cookie name
      Default character set

   PRIVATE CLASSES

   NOTES

   MODIFIED    (MM/DD/YY)
    dmcmahon    06/17/14 - Creation
 */

/**
 *  @version $Header: xdk/src/java/json/src/oracle/json/common/Configuration.java /main/17 2016/05/07 08:27:02 dmcmahon Exp $
 *  @author  dmcmahon
 *  @since   release specific (what release of product did this appear in)
 */

package oracle.json.common;

import java.nio.charset.Charset;
import java.util.Properties;

//import org.apache.log4j.Logger;

import java.util.logging.Logger;

public class Configuration
{
  /** Settable from ORDS or XDB deployment */
  public static final String PARAM_CACHING_ENABLED = "soda.cachingEnabled";
  public static final String PARAM_DROP_DISABLED = "soda.disableDropCollection";
  public static final String PARAM_MAX_LIMIT = "soda.maxLimit";
  public static final String PARAM_DEFAULT_LIMIT = "soda.defaultLimit";

  // ### Should these be in a central location?
  // ### Note: this is currently replicated in util/ByteArray.java
  public static final String  DEFAULT_ENCODING    = "utf-8";
  public static final Charset DEFAULT_CHARSET     =
                              Charset.forName(DEFAULT_ENCODING);

  public static final String ENV_XAP_HOME    = "XAPHOME";
  public static final String ENV_CONFIG_FILE = "XAP_CONFIG";
  public static final String ENV_DATABASE    = "XAP_DBNAME";
  public static final String ENV_USERPASS    = "XAP_DBCONN";

  // ### This one exposed so that the servlet context listener can hack it
  public static final String LOG_PROPERTIES_FILE = "log_properties";

  // List of all XAP configuration parameters. These are typically
  // taken from web.xml, but may also be set by other means, e.g.
  // environment variables for a command-line interface.
  // Each parameter has a name, a datatype, and a default value
  // They're centralized here for ease of maintenance.

  // Database connection information including integer pool size
  private static final String JDBC_DATABASE = "db_uri";
  private static final String JDBC_USERNAME = "db_username";
  private static final String JDBC_PASSWRD  = "db_password";
  private static final String POOL_SIZE = "pool_size";
  private static final String CONTAINER_TYPE = "container_type";

  private static final int DEFAULT_CONNECTIONS = 4;

  private String dbUri = null;
  private String dbUser = null;
  private String dbPasswrd = null;
  private int poolSize = DEFAULT_CONNECTIONS;
  private String containerType = null;

  // Asynchronous processing parameters (numeric)
  private static final String POLL_INTERVAL = "poll_interval";
  private static final String MAX_THREADS = "max_threads";

  private static final long DEFAULT_INTERVAL = 1000L; // 1 second
  private static final int DEFAULT_THREADS = 4;
  private static final long MIN_POLL_INTERVAL = 100L;
  private static final long MAX_POLL_INTERVAL = 10000L;
  private static final int MIN_NUM_THREADS = 1;
  private static final int MAX_NUM_THREADS = 64;

  private long pollInterval = DEFAULT_INTERVAL;
  private int numThreads = DEFAULT_THREADS;
  private boolean debugMode = false;
  private boolean cacheEnabled = false;
  private boolean dropDisabled = false;

  // ORDS service linkage

  public static final String SERVICE_NAME    = "service_name";
  public static final String SERVICE_VERSION = "service_version";

  private static final String DEBUG_SETTING = "debug";

  private String serviceName = null;
  private String serviceVersion = null;

  // Session
  private static final String PARAM_COOKIE_NAME = "session_cookie";
  private static final String PARAM_COOKIE_VALUE = "cookie_value";

  private String cookieName = null;
  private String cookieValue = null;

  public static final int MAXIMUM_NROWS     = 1000;
  public static final int DEFAULT_NROWS     = 100;
  public static final int UNLIMITED_ROWS    = -1;

  private int limitMax     = MAXIMUM_NROWS;
  private int limitDefault = DEFAULT_NROWS;

  private final Properties params;

  private static final Logger log =
    Logger.getLogger(Configuration.class.getName());

  public Configuration(Properties params)
  {
    if (params == null)
      this.params = new Properties();
    else
      this.params = params;

    if (params != null)
      importParameters(params);
    else
      importEnvironment();
  }

  private long getLongParameter(String value, long defaultVal)
  {
    long lvalue;
    if (value == null)
      return (defaultVal);
    try
    {
      lvalue = Long.parseLong(value);
    }
    catch (NumberFormatException e)
    {
      log.warning(e.getMessage());
      lvalue = defaultVal;
    }
    return (lvalue);
  }

  private int convertLimitValue(String value, int defaultVal)
  {
     if (defaultVal < 0)
       throw new IllegalArgumentException();

     if ("unlimited".equalsIgnoreCase(value)) {
       return UNLIMITED_ROWS;
     }

     int result = getIntParameter(value, defaultVal);
     if (result < 0) {
       log.warning("Invalid value: " + result);
       return defaultVal;
     }
     return result;
  }

  private int getIntParameter(String value, int defaultVal)
  {
    int ivalue;
    if (value == null)
      return (defaultVal);
    try
    {
      ivalue = Integer.parseInt(value);
    }
    catch (NumberFormatException e)
    {
      log.warning(e.getMessage());
      ivalue = defaultVal;
    }
    return (ivalue);
  }

  /*
   * Process configuration parameters into variables. Use the default if a
   * variable is unavailable. Perform any type conversions and range checks.
   */
  private synchronized void importParameters(Properties p_params)
  {
    dbUri = p_params.getProperty(JDBC_DATABASE);
    dbUser = p_params.getProperty(JDBC_USERNAME);
    dbPasswrd = p_params.getProperty(JDBC_PASSWRD);
    poolSize = getIntParameter(p_params.getProperty(POOL_SIZE), poolSize);

    containerType = p_params.getProperty(CONTAINER_TYPE);

    pollInterval = getLongParameter(p_params.getProperty(POLL_INTERVAL),
        pollInterval);
    if (pollInterval < MIN_POLL_INTERVAL)
      pollInterval = MIN_POLL_INTERVAL;
    else if (pollInterval > MAX_POLL_INTERVAL)
      pollInterval = MAX_POLL_INTERVAL;

    numThreads = getIntParameter(p_params.getProperty(MAX_THREADS),
        numThreads);
    if (numThreads < MIN_NUM_THREADS)
      numThreads = MIN_NUM_THREADS;
    else if (numThreads > MAX_NUM_THREADS)
      numThreads = MAX_NUM_THREADS;

    cookieName = p_params.getProperty(PARAM_COOKIE_NAME);
    cookieValue = p_params.getProperty(PARAM_COOKIE_VALUE);

    serviceName = p_params.getProperty(SERVICE_NAME);
    serviceVersion = p_params.getProperty(SERVICE_VERSION);

    String debugString = p_params.getProperty(DEBUG_SETTING);
    if (debugString != null)
      if (debugString.equalsIgnoreCase("true"))
        debugMode = true;

    setLimitDefaultParam(p_params.getProperty(PARAM_DEFAULT_LIMIT));
    setLimitMaxParam(p_params.getProperty(PARAM_MAX_LIMIT));
    setCacheEnabledParam(p_params.getProperty(PARAM_CACHING_ENABLED));
    setDropDisabledParam(p_params.getProperty(PARAM_DROP_DISABLED));
  }

  /** Warning: Called from ORDS plugin */
  public void setLimitMaxParam(String value)
  {
      if (value == null)
          return;

      setLimitMax(convertLimitValue(value, MAXIMUM_NROWS));
  }

  /** Warning: Called from ORDS plugin */
  public void setLimitDefaultParam(String value)
  {
      if (value == null)
          return;

      setLimitDefault(convertLimitValue(value, DEFAULT_NROWS));
  }

  /** Warning: Called from ORDS plugin */
  public void setCacheEnabledParam(String value)
  {
     if (value == null)
         return;

     setCacheEnabled(Boolean.valueOf(value));
  }

  /** Warning: Called from ORDS plugin */
  public void setDropDisabledParam(String value)
  {
     if (value == null)
         return;

     setDropDisabled(Boolean.valueOf(value));
  }

  public void setCacheEnabled(boolean value)
  {
    cacheEnabled = value;
  }

  public void setDropDisabled(boolean value)
  {
    dropDisabled = value;
  }

  public void setLimitMax(int max) {
    limitMax = max > 0 ? max : UNLIMITED_ROWS;
  }

  public void setLimitDefault(int def) {
    limitDefault = def > 0 ? def : UNLIMITED_ROWS;
  }

  /*
   * Load environment variables and process them into configuration variables.
   */
  private synchronized void importEnvironment()
  {
    String connectString = System.getProperty(ENV_USERPASS, null);

    dbUri = System.getProperty(ENV_DATABASE, null);

    if (connectString != null)
    {
      int split = connectString.indexOf('/');
      if (split < 0)
      {
        dbUser = connectString;
        dbPasswrd = "";
      }
      else
      {
        dbUser = connectString.substring(0, split);
        if (split == connectString.length())
          dbPasswrd = "";
        else
          dbPasswrd = connectString.substring(split + 1);
      }
    }

    if (dbUri != null)
      params.setProperty(JDBC_DATABASE, dbUri);
    if (dbUser != null)
      params.setProperty(JDBC_USERNAME, dbUser);
    if (dbPasswrd != null)
      params.setProperty(JDBC_PASSWRD, dbPasswrd);

    // ### For now, let all other parameters default
  }

  public long getPollInterval()
  {
    return (pollInterval);
  }

  public int getNumThreads()
  {
    return (numThreads);
  }

  public String getCookieName()
  {
    return (cookieName);
  }

  public String getCookieValue()
  {
    return (cookieValue);
  }

  public String getServiceName()
  {
    return (serviceName);
  }

  public String getServiceVersion()
  {
    return (serviceVersion);
  }

  public boolean getDebugSetting()
  {
    return (debugMode);
  }

  public boolean isCacheEnabled()
  {
    return (cacheEnabled);
  }

  public boolean isDropDisabled()
  {
    return (dropDisabled);
  }

  public String getContainerType()
  {
    return (containerType);
  }

  public int getLimitMax() {
    return (limitMax);
  }

  public int getLimitDefault() {
    return (limitDefault);
  }

  public void setServiceInfo(String serviceName, String serviceVersion)
  {
    if ((this.serviceName == null) && (serviceName != null))
      this.serviceName = serviceName;
    if ((this.serviceVersion == null) && (serviceVersion != null))
      this.serviceVersion = serviceVersion;
  }

  /**
   * Get any other parameter not specifically processed. Fails over to
   * environment variables if not found (the name is uppercased when accessing
   * the environment).
   */
  public String getParameter(String name)
  {
    String value = null;
    if (params != null)
      value = params.getProperty(name, null);
    if (value == null)
      value = System.getProperty(name.toUpperCase());
    return (value);
  }

  protected ConnectionPool createConnectionPool()
  {
    ConnectionPool pool;
    if (poolSize <= 0)
      pool = new ConnectionPool(dbUri, dbUser, dbPasswrd);
    else
      pool = new ConnectionPool(dbUri, dbUser, dbPasswrd, poolSize);
    return (pool);
  }

  static
  {
    // ### Any initialization?
  }
}
