/* Copyright (c) 2014, 2015, Oracle and/or its affiliates. 
All rights reserved.*/

/*
   DESCRIPTION
    This is the RDBMS-specific implementation of OracleDatabase.
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

import oracle.jdbc.OracleCallableStatement;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.OracleTypes;

import oracle.json.common.MetricsCollector;

import oracle.json.logging.OracleLog;
import oracle.json.util.ComponentTime;
import oracle.json.util.HashFuncs;
import oracle.json.util.ByteArray;

import oracle.soda.rdbms.impl.cache.DescriptorCache;
import oracle.soda.rdbms.impl.CollectionDescriptor.Builder;

import oracle.soda.OracleDatabase;
import oracle.soda.OracleCollection;
import oracle.soda.OracleException;
import oracle.soda.OracleDocument;
import oracle.soda.OracleDatabaseAdmin;

import oracle.sql.Datum;

import java.util.logging.Logger;

import java.io.InputStream;

import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class OracleDatabaseImpl implements OracleDatabase
{
  static ArrayList<String> EMPTY_LIST = new ArrayList<String>();

  private static final String SELECT_GUID =
    "select SYS_GUID() from SYS.DUAL";

  private static final String SELECT_GUID_BATCH =
    "declare\n"+
    "  type VCTAB is table of varchar2(255) index by binary_integer;\n"+
    "  N number;\n"+
    "  X varchar2(255);\n"+
    "  K vctab;\n"+
    "begin\n"+
    "  N := ?;\n"+
    "  for I in 1..N loop\n"+
    "    select rawtohex(SYS_GUID()) into X from SYS.DUAL;\n"+
    "    K(I) := X;\n"+
    "  end loop;\n"+
    "  ? := K;\n"+
    "end;";

  private static final String COLLECTION_CREATION_MODE = "DDL";
  
  private static final Logger log =
    Logger.getLogger(OracleDatabaseImpl.class.getName());

  private final OracleConnection conn;

  // Shared cache of CollectionDescriptor objects
  private final DescriptorCache sharedDescriptorCache;

  // Local cache of CollectionDescriptor objects
  private final HashMap<String, CollectionDescriptor> localDescriptorCache;

  // Local cache of OracleCollection objects
  private final HashMap<String, OracleCollectionImpl> localCollectionCache;

  private MetricsCollector metrics;

  private boolean metadataTableExists = true;

  private OracleDatabaseAdmin admin;

  // ### Work-around for JDBC bug with the SQL "returning" clause
  // The internal bug is 14496772. The failure occurs if colons are
  // present in a "returning" expression. The bug is fixed,
  // but leaving th workaround code in just in case for now.
  static final boolean JDBC_WORKAROUND = false;

  // Length of the JSON collection metadata descriptor (in bytes),
  // as stored in the DB and returned by DBMS_SODA_ADMIN
  private static final int DESC_LENGTH = 4000;

  // Length of the creation timestamp (in bytes)
  // as returned by DBMS_SODA_ADMIN
  private static final int CREATION_TIMESTAMP_LENGTH = 255;

  /**
   * Not part of a public API.
   *
   * Used internally by the REST component.
   *
   * Construct a database implementation for the specified connection.
   * Provide a metrics collector and a collection metadata descriptor
   * cache.
   */
  public OracleDatabaseImpl(OracleConnection conn,
                            DescriptorCache descriptorCache,
                            MetricsCollector metrics,
                            boolean localCaching)
  {
    this.conn = conn;
    this.sharedDescriptorCache = descriptorCache;
    this.metrics = metrics;

    if (localCaching)
    {
      localDescriptorCache = new HashMap<String, CollectionDescriptor>();
      localCollectionCache = new HashMap<String, OracleCollectionImpl>();
    }
    else
    {
      localDescriptorCache = null;
      localCollectionCache = null;
    }
  }

  private CollectionDescriptor putDescriptorIntoCaches(CollectionDescriptor desc)
  {
    String collectionName = desc.getName();

    if (localDescriptorCache != null)
    {
      localDescriptorCache.put(collectionName, desc);
      if (OracleLog.isLoggingEnabled())
        log.fine("Put " + collectionName + " descriptor into local cache");
    }

    if (sharedDescriptorCache != null)
    {
      // "putIfAbsent" is used as opposed to "put" here
      // because eventually the loading of sharedDescriptorCache
      // can be optimized by using the Future interface, instead of
      // using the descriptor directly. "putIfAbsent" will
      // be useful then. The idea is to avoid potentially
      // unnecessary trips to the database for the
      // same descriptor from multiple threads.
      CollectionDescriptor existingDesc = sharedDescriptorCache.putIfAbsent(desc);
      if (existingDesc != null)
      {
        if (OracleLog.isLoggingEnabled())
          log.fine(collectionName + " descriptor already exists in shared cache");

        // If local descriptor cache is not used,
        // return the descriptor existing in the shared cache.
        if (localCollectionCache == null)
        {
          return existingDesc;
        }
      }
      else
      {
        if (OracleLog.isLoggingEnabled())
          log.fine("Put " + collectionName +" descriptor into shared cache");
      }
    }

    return desc;
  }


  private CollectionDescriptor getDescriptorFromCaches(String collectionName)
  {
    CollectionDescriptor result = null;

    if (localDescriptorCache != null)
    {
      result = localDescriptorCache.get(collectionName);
      if (result != null)
      {
        if (OracleLog.isLoggingEnabled())
          log.fine("Got " + collectionName + " descriptor from local cache");
        
        if (sharedDescriptorCache != null &&
            !sharedDescriptorCache.containsDescriptor(collectionName))
        {
          // "putIfAbsent" is used as opposed to "put" here,
          // since it'll be useful in the future. See
          // the comment for the other putIfAbsent use
          // in putDescriptorIntoCaches(...) method for
          // more info.
          sharedDescriptorCache.putIfAbsent(result);
        }
      }
    }

    if (result == null && sharedDescriptorCache != null)
    {
      result = sharedDescriptorCache.get(collectionName);
      if (result != null)
      {
        if (OracleLog.isLoggingEnabled())
          log.fine("Got " + collectionName + " descriptor from shared cache");

        // Since the descriptor can get evicted from
        // from the shared cache, store it in the local cache.
        // The local cache doesn't ever evict descriptors.
        if (localDescriptorCache != null)
        {
          localDescriptorCache.put(collectionName, result);
        }
      }

    }

    if (result == null && ((localCollectionCache != null) ||
                           (sharedDescriptorCache != null)))
    {
      if (OracleLog.isLoggingEnabled())
        log.fine("Descriptor not found in caches");
    }

    return result;
  }

  void setMetricsCollector(MetricsCollector metrics)
  {
    this.metrics = metrics;
  }

  private ArrayList<CollectionDescriptor> loadCollections(Integer limit,
                                                          int offset)
    throws OracleException
  {
    if (!metadataTableExists) return(null);

    return(callListCollections(null, limit, offset));
  }

  private ArrayList<CollectionDescriptor> loadCollections(Integer limit,
                                                          String startName)
    throws OracleException
  {
    if (!metadataTableExists) return(null);

    return(callListCollections(startName, limit, 0));
  }

    /**
     * Create a collection with the specified name.
     * This will use default options for the creation.
     * If the collection already exists, it's returned.
     */
    private OracleCollection createCollection(String collectionName)
            throws OracleException
    {
      return(createCollection(collectionName, (CollectionDescriptor)null));
    }

    /**
     * Create a collection with the specified name and options.
     * @throws OracleException 
     */
    private OracleCollection createCollection(String collectionName,
                                              CollectionDescriptor options) 
      throws OracleException
    {
      return createCollection(collectionName, options, COLLECTION_CREATION_MODE);
    }
    
    /**
     * Not part of a public API.
     *
     * Used internally by REST. REST uses the "NEW" collectionCreateMode
     * which doesn't support mapped collections and raises an error if
     * the dbObject already exists.
     */
    public OracleCollection createCollection(String collectionName,
                                             CollectionDescriptor options,
                                             String collectionCreateMode)
      throws OracleException
    {
      OracleCollection result = openCollection(collectionName,
                                               options);
      if (result != null)
        return(result);

      // Create a default descriptor if necessary
      if (options == null)
        options = CollectionDescriptor.createStandardBuilder().buildDescriptor(collectionName);

      options = callCreatePLSQL(collectionName, options, collectionCreateMode);

      // Now load the metadata from the database and return it
      return(openCollection(collectionName, options));
    }

    /**
     * Open a collection with the specified name. If the collection
     * doesn't exist, returns null.
     */
    public OracleCollection openCollection(String collectionName)
            throws OracleException
    {
        return(openCollection(collectionName, (CollectionDescriptor)null));
    }

    /**
     * Open a collection with the specified name and metadata.
     */
    private OracleCollection openCollection(String collectionName,
                                            CollectionDescriptor options)
      throws OracleException
    {
        if (collectionName == null)
        {
          throw SODAUtils.makeException(SODAMessage.EX_ARG_CANNOT_BE_NULL,
                                        "collectionName");
        }

        // See if this collection has already been opened/created
        OracleCollectionImpl coll = null;

        if (localCollectionCache != null)
        {
          coll = localCollectionCache.get(collectionName);
        }

        if (coll != null)
        {
          if (options != null && !coll.matches(options))
          {
            throw SODAUtils.makeException(SODAMessage.EX_MISMATCHED_DESCRIPTORS);
          }

          return(coll);
        }

        // If not, see if the metadata is available
        CollectionDescriptor desc = getDescriptorFromCaches(collectionName);

        // If not, attempt to load it from the database
        if (desc == null)
        {
          desc = loadCollection(collectionName);
        }

        boolean createFlag = false;

        // If found, create a new collection object and cache it
        if (desc != null)
        {
          if (options != null && !desc.matches(options))
          {
            throw SODAUtils.makeException(SODAMessage.EX_MISMATCHED_DESCRIPTORS);
          }

          createFlag = true;
        }
        // Otherwise it's not found

        if (createFlag)
        {
          if (desc.dbObjectType == CollectionDescriptor.DBOBJECT_PACKAGE)
          {
            throw new IllegalStateException();
          }
          else // TableCollectionImpl is used for views and tables
          {
            coll = new TableCollectionImpl(this, collectionName, desc);
          }

          if (localCollectionCache != null)
          {
            localCollectionCache.put(collectionName, coll);
          }
        }

        return(coll);
  }

  private OracleCollection createCollection(String collectionName,
                                            OracleDocument collectionMetadata)
    throws OracleException
  {
    if (collectionMetadata == null)
    {
      return createCollection(collectionName);
    }

    CollectionDescriptor descriptor = 
            createCollectionDescriptor(collectionName, collectionMetadata);
    return createCollection(collectionName, descriptor); 
  }

  private CollectionDescriptor createCollectionDescriptor(String name,
                                                          OracleDocument metadata)
    throws OracleException
  {
    String meta = metadata.getContentAsString();

    if (meta == null || meta.isEmpty())
    {
      throw SODAUtils.makeException(SODAMessage.EX_METADATA_DOC_HAS_NO_CONTENT);
    }

    Builder builder = CollectionDescriptor.jsonToBuilder(meta);

    String defaultSchema = null;

    try
    {
      defaultSchema = conn.getCurrentSchema();
    }
    catch (SQLException e)
    {
      // gulp
      if (OracleLog.isLoggingEnabled())
        log.severe(e.getMessage());
    }

    return builder.buildDescriptor(name, defaultSchema);
  }

  /**
   * Drop collection with the specified name.
   */
  void dropCollection(String collectionName) throws OracleException
  {
    if (collectionName == null)
    {
      throw SODAUtils.makeException(SODAMessage.EX_ARG_CANNOT_BE_NULL, "collectionName");
    }

    callDropPLSQL(collectionName);

    if (sharedDescriptorCache != null)
    {
      sharedDescriptorCache.remove(collectionName);
    }

    if (localDescriptorCache != null)
    {
      localDescriptorCache.remove(collectionName);
    }

    if (localCollectionCache != null)
    {
      localCollectionCache.remove(collectionName);
    }
  }

  /**
   * Get a list of the names of all collections in the database.
   */
  private List<String> getCollectionNames()
    throws OracleException
  {
    return(getCollectionNames(null, 0));
  }

  /**
   * Get a list of the names of collections in the database with a
   * limit on the number returned.
   *
   * This method implies that the list is ordered.
   */
  private List<String> getCollectionNames(int limit)
    throws OracleException
  {
    return(getCollectionNames(limit, 0));
  }

  /**
   * Get a list of the names of collections in the database with a
   * limit on the number returned, starting at a specific offset in
   * the list.
   *
   * This method implies that the list is ordered.
   */
  private List<String> getCollectionNames(int limit, int skip)
    throws OracleException
  {
    if (limit < 1)
    {
      throw SODAUtils.makeException(SODAMessage.EX_ARG_MUST_BE_POSITIVE,
                                    "limit");
    }

    if (skip < 0)
    {
      throw SODAUtils.makeException(SODAMessage.EX_ARG_MUST_BE_NON_NEGATIVE,
                                    "skip");
    }

    return getCollectionNames(Integer.valueOf(limit), skip);
  }

  private List<String> getCollectionNames(Integer limit, int skip)
    throws OracleException
  {

    ArrayList<CollectionDescriptor> arr = loadCollections(limit, skip);
    int sz = arr.size();
    ArrayList<String> results = EMPTY_LIST;
    if (sz > 0)
    {
      results = new ArrayList<String>(sz);
      for (CollectionDescriptor desc : arr)
        results.add(desc.uriName);
    }
    return(results);
  }

  /**
   * Get a list of the names of collections in the database with a
   * limit on the number returned, starting at the first name greater
   * than startName. If a null or empty string is passed as startName,
   * it's equivalent to starting at the first collection in sequence.
   *
   * This method implies that the list is ordered.
   */
  private List<String> getCollectionNames(int limit, String startName)
    throws OracleException
  {
    if (limit < 1)
    {
      throw SODAUtils.makeException(SODAMessage.EX_ARG_MUST_BE_POSITIVE,
                                    "limit");
    }

    if (startName == null)
    {
      throw SODAUtils.makeException(SODAMessage.EX_ARG_CANNOT_BE_NULL,
                                    "startName");
    }

    ArrayList<CollectionDescriptor> arr = loadCollections(limit, startName);
    int sz = arr.size();
    ArrayList<String> results = EMPTY_LIST;
    if (sz > 0)
    {
      results = new ArrayList<String>(sz);
      for (CollectionDescriptor desc : arr)
        results.add(desc.uriName);
    }
    return(results);
  }

  /**
   * Create new document from InputStream methods
   */

  // Not part of a public API.
  public OracleDocument createDocumentFromStream(InputStream content)
  {
    // ### Currently swallows up IOException.
    //     Revisit if stream-based createDocument methods ever become public.
    return(new OracleDocumentImpl(null, null, null, content, null));
  }

  // Not part of a public API.
  public OracleDocument createDocumentFromStream(String key,
                                                 InputStream content)
  {
    // ### Currently swallows up IOException.
    //     Revisit if stream-based createDocument methods ever become public.
    return(new OracleDocumentImpl(key, null, null, content, null));
  }

  // Not part of a public API.
  public OracleDocument createDocumentFromStream(String key,
                                                 InputStream content,
                                                 String contentType)
  {
    return(new OracleDocumentImpl(key, null, null, content, contentType));
  }

  /**
   * Create new document from String methods
   */

  public OracleDocument createDocumentFromString(String content)
  {
    return(new OracleDocumentImpl(null, content));
  }

  public OracleDocument createDocumentFromString(String key,
                                                 String content)
  {
    return(new OracleDocumentImpl(key, content));
  }

  public OracleDocument createDocumentFromString(String key,
                                                 String content,
                                                 String contentType)
  {
    return(new OracleDocumentImpl(key, null, null, content, contentType));
  }

  /**
   * Create a new document from byte[] methods
   */

  public OracleDocument createDocumentFromByteArray(byte[] content)
  {
    return(new OracleDocumentImpl(null, content));
  }

  public OracleDocument createDocumentFromByteArray(String key,
                                                    byte[] content)
  {
    return(new OracleDocumentImpl(key, content));
  }

  public OracleDocument createDocumentFromByteArray(String key,
                                                    byte[] content,
                                                    String contentType)
  {
    return(new OracleDocumentImpl(key, null, null, content, contentType));
  }

  MetricsCollector getMetrics()
  {
    return(metrics);
  }

  public OracleConnection getConnection()
  {
    return(conn);
  }

  /**
   * Internal database timekeeper. This fetches a SYSTIMESTAMP from
   * the database and keeps a cached value for it. Repeated requests
   * will attempt to avoid SQL round-trips by comparing clock differences
   * locally and providing an estimate in lieu of the actual DB time.
   * When a maximum elapsed time is reached for the possibly stale time
   * estimate, a refresh SQL is done. To cope with possible clock skew,
   * the time will only advance, so if the SQL clock is behind the
   * current estimate, the current estimate is simply advanced one tick.
   */

  // Refresh database time after 100 milliseconds (in nanoseconds)
  private static final long TIME_REFRESH_INTERVAL = 100000000L;

  // Note: no time zone on the format, string is consumed/parsed internally
  private static String SELECT_DB_TIMESTAMP =
    "select to_char(sys_extract_utc(SYSTIMESTAMP)," +
                   "'YYYY-MM-DD\"T\"HH24:MI:SS.FF' ) from SYS.DUAL";

  // Current time in component-long format
  private long   currentTimestamp = 0L;
  // Most recent refresh (in local nanoseconds)
  private long   updateNanos = 0L;

  /**
   * Get the current timestamp from the database;
   * internally this may simply adjust the mirrored time using the local
   * clock to avoid a round-trip, until the elapsed local time exceeds
   * a threshold (which should still be sub-second).
   */
  long getDatabaseTime()
    throws SQLException
  {
    long currentNanos = System.nanoTime();
    long deltaNanos;

    if (updateNanos == 0L)
      deltaNanos = -1L;
    else
      deltaNanos = currentNanos - updateNanos;

    // If too much time has elapsed or the local clock rolled over
    if ((deltaNanos > TIME_REFRESH_INTERVAL) || (deltaNanos < 0L))
    {
      // Refresh with a real SQL operation
      refreshDatabaseTime(currentNanos);
    }
    // Otherwise adjust the time by the elapsed time on the local clock
    else
    {
      // Increment the timestamp by the elapsed time
      long elapsedmicros = deltaNanos / 1000L;
      if (elapsedmicros <= 0L)
        elapsedmicros = 1L;
      long newTimestamp = ComponentTime.increment(currentTimestamp,
                                                  elapsedmicros);
      // If that can't be done without a rollover, refresh from SQL
      if (newTimestamp < 0L)
        refreshDatabaseTime(currentNanos);
      else
        currentTimestamp = newTimestamp;
    }

    return(currentTimestamp);
  }

  /**
   * This sets the database time from a string (presumably from a SQL
   * statement). The clock will only be moved forward; if clock skew
   * is detected, the clock will advance by one tick.
   */
  private void setDatabaseTime(long currentNanos, String strtime)
  {
    if (strtime.length() > 26)
      strtime = strtime.substring(0, 26);

    ComponentTime newComponentTime = new ComponentTime(strtime);
    long          newTime = newComponentTime.getValue();

    // If the time stamp from the database is lagging we must have clock skew
    if (newTime <= currentTimestamp)
    {
      // Force the current time ahead one tick
      newTime = ComponentTime.plus(currentTimestamp, +1L);
    }

    // Mark the local update time as precisely as possible
    updateNanos = currentNanos;

    // Update the timestamp
    currentTimestamp = newTime;
  }

  private void refreshDatabaseTime(long currentNanos)
    throws SQLException
  {
    PreparedStatement stmt = null;
    ResultSet         rows = null;

    String sqltext = SELECT_DB_TIMESTAMP;

    try
    {
      metrics.startTiming();

      stmt = conn.prepareStatement(sqltext);
      stmt.setFetchSize(1);
      rows = stmt.executeQuery();

      if (rows.next())
        setDatabaseTime(currentNanos, rows.getString(1));

      // These calls to close the objects can throw exceptions
      rows.close();
      rows = null;
      stmt.close();
      stmt = null;

      metrics.recordTimestampRead();
    }
    finally
    {
      // Exceptions aren't thrown inside a finally block
      // These close operations are just to clean up from an exception
      for (String message : SODAUtils.closeCursor(stmt, rows))
      {
        if (OracleLog.isLoggingEnabled())
          log.severe(message);
      }
    }
  }

  /**
   * Internal GUID cache
   * This keeps a small block of GUIDs assigned by the database
   * in a memory cache. When the cache is exhausted, a new set of
   * GUIDs is fetched from the database in a single round trip.
   */

  private static final int GUID_BATCH_SIZE = 10;
  private final String[] guidCache = new String[GUID_BATCH_SIZE];
  private       int      guidCachePos = GUID_BATCH_SIZE;

  String nextGuid()
    throws OracleException
  {
    if (guidCachePos >= guidCache.length)
      fetchGuids();
    return(guidCache[guidCachePos++]);
  }

  private void fetchGuids()
    throws OracleException
  {
    OracleCallableStatement stmt = null;
    String sqltext = SELECT_GUID_BATCH;

    int count = GUID_BATCH_SIZE;

    try
    {
      Datum[] vcarr = null;

      metrics.startTiming();

      stmt = (OracleCallableStatement)conn.prepareCall(sqltext);
      stmt.setInt(1, count);
      stmt.registerIndexTableOutParameter(2, count, OracleTypes.VARCHAR, 255);

      stmt.execute();

      vcarr = stmt.getOraclePlsqlIndexTable(2);

      count = vcarr.length;
      if (count > 0)
      {
        for (int i = 0; i < count; ++i)
          guidCache[i] = vcarr[i].stringValue();
      }
      guidCachePos -= count;

      stmt.close();
      stmt = null;

      metrics.recordGUIDS();
    }
    catch (SQLException e)
    {
      throw SODAUtils.makeExceptionWithSQLText(e, sqltext);
    }
    finally
    {
      for (String message : SODAUtils.closeCursor(stmt, null))
      {
        if (OracleLog.isLoggingEnabled())
          log.severe(message);
      }
    }
  }

  /**
   * Fetch a single GUID from the database
   * This is used as a back-up for UUID generation.
   */
  private byte[] fetchGuid()
  {
    PreparedStatement stmt = null;
    ResultSet         rows = null;
    String            sqltext = SELECT_GUID;

    byte[] data = null;

    try
    {
      metrics.startTiming();

      stmt = conn.prepareStatement(sqltext);

      rows = stmt.executeQuery();
      if (rows.next())
      {
        data = rows.getBytes(1);
      }

      rows.close();
      rows = null;
      stmt.close();
      stmt = null;

      metrics.recordGUIDS();

    }
    catch (SQLException e)
    {
      if (OracleLog.isLoggingEnabled())
        log.severe(e.toString());
    }
    finally
    {
      for (String message : SODAUtils.closeCursor(stmt, rows))
      {
        if (OracleLog.isLoggingEnabled())
          log.severe(message);
      }
    }

    return(data);
  }

  /**
   * Generate a UUID key string
   */
  String generateKey() throws OracleException
  {
    byte[] data = null;

    // ### The following fails inside Oracle RDBMS internal
    // driver.
    try
    {
      // Get a UUID-based key (this should be most efficient)
      data = HashFuncs.getRandom();
    }
    catch (Exception e)
    {
      if (OracleLog.isLoggingEnabled())
        log.warning(e.toString());
    }
    // ### If the above method failed, select SYS_GUID
    if (data == null)
    {
      data = fetchGuid();
    }
    // ### If fetching SYS_GUID failed as well, give up.
    if (data == null)
    {
      throw SODAUtils.makeException(SODAMessage.EX_UNABLE_TO_CREATE_UUID);
    }

    return(ByteArray.rawToHex(data));
  }

  public OracleDatabaseAdmin admin()
  {
    if (admin == null)
    {
      admin = new OracleDatabaseAdministrationImpl();
    }
    return admin;
  }

  /**
   * Internal call to PL/SQL DDL drop.
   */
  private void callDropPLSQL(String collectionName) throws OracleException
  {
    OracleCallableStatement stmt = null;
    String sqltext = "begin\n"+
            " DBMS_SODA_ADMIN.DROP_COLLECTION(P_URI_NAME => ?);\n"+
            "end;";

    try
    {
      metrics.startTiming();

      stmt = (OracleCallableStatement)conn.prepareCall(sqltext);

      stmt.setNString(1, collectionName);

      stmt.execute();
      if (OracleLog.isLoggingEnabled())
        log.info("Dropped collection "+collectionName);

      stmt.close();
      stmt = null;

      metrics.recordDDL();
    }
    catch (SQLException e)
    {
      if (OracleLog.isLoggingEnabled())
        log.severe(e.toString());
      
      // ### Temporary workaround for the cryptic
      // "ORA-00054: resource busy and acquire with NOWAIT specified or timeout
      // expired" exception. This exception occurs when a collection with
      // uncommitted writes is attempted to be dropped. We wrap ORA-00054
      // in an OracleException with a message telling the user to commit.
      // DBMS_SODA_ADMIN will eventually be modified to output a custom 
      // exception with the same message, instead of the ORA-00054. 
      // Then this workaround can be removed.
      if (e.getErrorCode() == 54)
        throw SODAUtils.makeExceptionWithSQLText(SODAMessage.EX_COMMIT_MIGHT_BE_NEEDED,
                                                 e,
                                                 sqltext);

      throw SODAUtils.makeExceptionWithSQLText(e, sqltext);
    }
    finally
    {
      for (String message : SODAUtils.closeCursor(stmt, null))
      {
        if (OracleLog.isLoggingEnabled())
          log.severe(message);
      }
    }
  }

  private CollectionDescriptor callCreatePLSQL(String collectionName,
                                               CollectionDescriptor options,
                                               String collectionCreateMode)
          throws OracleException
  {
    OracleCallableStatement stmt = null;
    String sqltext = "begin\n"                                   +
                     "  DBMS_SODA_ADMIN.CREATE_COLLECTION(\n"    +
                     "                   P_URI_NAME    => ?,\n"  +
                     "                   P_CREATE_MODE => ?,\n"  +
                     "                   P_DESCRIPTOR  => ?,\n"  +
                     "                   P_CREATE_TIME => ?);\n" +
                     "end;";

    String jsonDescriptor = options.getDescription();
    if (OracleLog.isLoggingEnabled())
      log.info("Create collection:\n" + jsonDescriptor);
    CollectionDescriptor newDescriptor = null;

    try
    {
      metrics.startTiming();

      stmt = (OracleCallableStatement)conn.prepareCall(sqltext);

      stmt.setNString(1, collectionName);
      stmt.setString(2, collectionCreateMode);
      stmt.setString(3, jsonDescriptor);

      // Creation time (unused)
      stmt.registerOutParameter(3, OracleTypes.VARCHAR, DESC_LENGTH);
      stmt.registerOutParameter(4, OracleTypes.VARCHAR, CREATION_TIMESTAMP_LENGTH);

      stmt.execute();
      if (OracleLog.isLoggingEnabled())
        log.info("Created collection "+collectionName);

      String createTime = stmt.getString(4);

      jsonDescriptor = stmt.getString(3);

      stmt.close();
      stmt = null;

      metrics.recordDDL();

      if (createTime != null)
      {
        Builder builder = CollectionDescriptor.jsonToBuilder(jsonDescriptor);
        newDescriptor = builder.buildDescriptor(collectionName);
      }
    }
    catch (SQLException e)
    {
      if (OracleLog.isLoggingEnabled())
        log.severe(e.toString());
      throw SODAUtils.makeExceptionWithSQLText(e, sqltext);
    }
    finally
    {
      for (String message : SODAUtils.closeCursor(stmt, null))
      {
        if (OracleLog.isLoggingEnabled())
          log.severe(message);
      }
    }

    return(newDescriptor);
  }

  /**
   * Load the metadata for a collection from the table.
   */
  private CollectionDescriptor loadCollection(String collectionName)
    throws OracleException
  {
    if (!metadataTableExists)
    {
      return null;
    }

    OracleCallableStatement stmt = null;

    String sqltext = "begin\n"+
                     "  DBMS_SODA_ADMIN.DESCRIBE_COLLECTION(\n"  +
                     "                   P_URI_NAME   => ?,\n"   +
                     "                   P_DESCRIPTOR => ?);\n"  +
                     "end;";

    CollectionDescriptor desc = null;

    try
    {
      metrics.startTiming();

      stmt = (OracleCallableStatement)conn.prepareCall(sqltext);

      stmt.setNString(1, collectionName);

      stmt.registerOutParameter(2, OracleTypes.VARCHAR, DESC_LENGTH);

      stmt.execute();

      String jsonDescriptor = stmt.getString(2);

      desc = getDescriptorFromCaches(collectionName);

      if ((desc == null) && (jsonDescriptor != null))
      {
        Builder builder = CollectionDescriptor.jsonToBuilder(jsonDescriptor);
        desc = putDescriptorIntoCaches(builder.buildDescriptor(collectionName));
      }

      stmt.close();
      stmt = null;

      metrics.recordCall();
    }
    catch (SQLException e)
    {
      if (OracleLog.isLoggingEnabled())
        log.severe(e.toString());
      throw SODAUtils.makeExceptionWithSQLText(e, sqltext);
    }
    finally
    {
      for (String message : SODAUtils.closeCursor(stmt, null))
      {
        if (OracleLog.isLoggingEnabled())
          log.severe(message);
      }
    }

    return(desc);
  }

  /**
   * Internal call to PL/SQL metadata bulk load.
   */
  private ArrayList<CollectionDescriptor> callListCollections(String startName,
                                                              Integer limit,
                                                              int offset)
    throws OracleException
  {
    OracleCallableStatement stmt = null;
    ResultSet               rows = null;
    String sqltext = "begin\n"                                  +
                     "  DBMS_SODA_ADMIN.LIST_COLLECTIONS(\n"   +
                     "                   P_START_NAME => ?,\n"  +
                     "                   P_RESULTS    => ?);\n" +
                     "end;";

    boolean resultFull = false;

    ArrayList<CollectionDescriptor> results =
      new ArrayList<CollectionDescriptor>();

    try
    {
      int rowCount = 0;

      metrics.startTiming();

      stmt = (OracleCallableStatement)conn.prepareCall(sqltext);

      if (startName == null)
        stmt.setNull(1, Types.VARCHAR);
      else
        stmt.setNString(1, startName);

      stmt.registerOutParameter(2, OracleTypes.CURSOR);

      stmt.execute();

      rows = stmt.getCursor(2);

      if (OracleLog.isLoggingEnabled())
        log.fine("Loaded collections");

      while (rows.next())
      {
        // Get the name of the next collection
        String uriName        = rows.getNString(1);
        String jsonDescriptor = rows.getString(2);
        String createTime     = rows.getString(3);
        // See if it's already in the cache
        CollectionDescriptor desc = getDescriptorFromCaches(uriName);

        if (desc == null)
        {
          Builder builder = CollectionDescriptor.jsonToBuilder(jsonDescriptor);
          desc = builder.buildDescriptor(uriName);
          desc = putDescriptorIntoCaches(desc);
        }

        if ((rowCount >= offset) && (!resultFull))
          results.add(desc);

        ++rowCount;

        // Stop filling results after limit rows are retrieved.
        // If null is passed in for limit, that means unlimited
        // number of results.
        if (limit != null && limit > 0)
        {
          if (rowCount >= (offset + limit))
          {
            if (rowCount > SODAConstants.BATCH_FETCH_SIZE)
              break;
            resultFull = true;
          }
        }
      }

      rows.close();
      rows = null;

      stmt.close();
      stmt = null;

      metrics.recordCall();
    }
    catch (SQLException e)
    {
      if (OracleLog.isLoggingEnabled())
        log.severe(e.toString());
      throw SODAUtils.makeExceptionWithSQLText(e, sqltext);
    }
    finally
    {
      for (String message : SODAUtils.closeCursor(stmt, rows))
      {
        if (OracleLog.isLoggingEnabled())
          log.severe(message);
      }
    }

    return(results);
  }

  /**
   * Add the standard formatting for convering a bind string to a DB
   * timestamp (this must never have a time zone).
   */
  static void addToTimestamp(String operation, StringBuilder sb)
  {
    if (operation != null) sb.append(operation);
    sb.append("to_timestamp(?,'YYYY-MM-DD\"T\"HH24:MI:SS.FF TZH:TZM')");
  }

  /**
   * Use for specifying timestamp format, with colon workaround if required
   */
  static private void addTimestampFormat(StringBuilder sb, boolean avoidColons)
  {
    if (avoidColons) // Used only by RETURNING clauses as a work-around
      sb.append(",'YYYY-MM-DD\"T\"HH24.MI.SS.FF\"Z\"')");
    else
      sb.append(",'YYYY-MM-DD\"T\"HH24:MI:SS.FF\"Z\"')");
  }

  /**
   * Add a RETURNING statement timestamp format which may use a work-around.
   * This then requires using getTimestamp() to get a proper string.
   */
  static void addTimestampReturningFormat(StringBuilder sb) {
    OracleDatabaseImpl.addTimestampFormat(sb, JDBC_WORKAROUND);
  }

  /**
   * Add a SELECT statement timestamp format which uses colons in the time.
   */
  static void addTimestampSelectFormat(StringBuilder sb)
  {
      OracleDatabaseImpl.addTimestampFormat(sb, false);
  }

  // Use for fetching timestamp, with JDBC workaround if required
  static String getTimestamp(String tstamp)
  {
      // If necessary replace two dots with the colons we should have used
      if (JDBC_WORKAROUND)
      {
          char[] tarray = tstamp.toCharArray();
          tarray[13] = ':';
          tarray[16] = ':';
          tstamp = new String(tarray);
      }

      return tstamp;
  }

  /**
   * OracleDatabaseAdministrationImpl
   *
   * This is RDBMS-specific implementation of OracleDatabaseAdmin
   */
  private class OracleDatabaseAdministrationImpl implements OracleDatabaseAdmin
  {
    /**
     * Create a collection with the specified name.
     * This will use default options for the creation.
     * If the collection already exists, it's returned.
     */
    public OracleCollection createCollection(String collectionName)
      throws OracleException
    {
      return OracleDatabaseImpl.this.createCollection(collectionName);
    }

    public OracleCollection createCollection(String collectionName,
                                             OracleDocument collectionMetadata)
            throws OracleException
    {
      return OracleDatabaseImpl.this.createCollection(collectionName,
                                                      collectionMetadata);
    }

    /**
     * Get a list of the names of all collections in the database.
     */
    public List<String> getCollectionNames()
      throws OracleException
    {
      return OracleDatabaseImpl.this.getCollectionNames();
    }

    /**
     * Get a list of the names of collections in the database with a
     * limit on the number returned.
     *
     * This method implies that the list is ordered.
     */
    public List<String> getCollectionNames(int limit)
      throws OracleException
    {
      return OracleDatabaseImpl.this.getCollectionNames(limit);
    }

    /**
     * Get a list of the names of collections in the database with a
     * limit on the number returned, starting at a specific offset in
     * the list.
     *
     * This method implies that the list is ordered.
     */
    public List<String> getCollectionNames(int limit, int skip)
      throws OracleException
    {
      return OracleDatabaseImpl.this.getCollectionNames(limit, skip);
    }

    /**
     * Get a list of the names of collections in the database with a
     * limit on the number returned, starting at the first name greater
     * than startName. If a null or empty string is passed as startName,
     * it's equivalent to starting at the first collection in sequence.
     *
     * This method implies that the list is ordered.
     */
    public List<String> getCollectionNames(int limit, String startName)
      throws OracleException
    {
      return OracleDatabaseImpl.this.getCollectionNames(limit, startName);
    }

    public Connection getConnection()
    {
      return OracleDatabaseImpl.this.getConnection();
    }
  }

}
