#Getting started with SODA for Java

To get started, you must have a live Oracle database 12.1.0.2 instance (available from [this page](http://www.oracle.com/technetwork/database/enterprise-edition/downloads/index.html)), with patch 20885778 applied.
Please make sure you have the 12.1.0.2 release in particular (and not just any 12C release).

Obtain the patch from My Oracle Support (https://support.oracle.com). Select tab Patches & Updates. Search for patch number, 20885778 or access it directly at this URL: https://support.oracle.com/rs?type=patch&id=20885778. Make sure you follow all the installation steps specified in the README.txt file included with the patch, including the post-installation step.

SODA requires the SODA_APP role to be granted to the user (i.e. schema name) that
will be used to work with collections. The underlying tables used to store
document collections will be stored in this schema. Pick or create a schema
for this purpose, and then issue the following command (in sqlplus, for example):

    grant SODA_APP to schemaName;

The following simple SODA Java program performs several common operations:

*     Creates a new collection
*     Inserts documents into the collection
*     Retrieves the first inserted document by its auto-generated key
*     Retrieves documents matching a query-by-example, or QBE

```java
import java.sql.Connection;
import java.sql.DriverManager;

import oracle.soda.rdbms.OracleRDBMSClient;

import oracle.soda.OracleDatabase;
import oracle.soda.OracleCursor;
import oracle.soda.OracleCollection;
import oracle.soda.OracleDocument;
import oracle.soda.OracleException;

import java.util.Properties;

import oracle.jdbc.OracleConnection;

public class testSODA {
  public static void main(String[] args) {
   
    // SODA works on top of a regular JDBC connection.
    // Set up the connection string: replace hostName, port, and serviceName
    // with the info for your Oracle RDBMS instance
    String url = "jdbc:oracle:thin:@//hostName:port/serviceName";

    Properties props = new Properties();

    // Replace with your schemaName and password
    props.setProperty("user", "schemaName");
    props.setProperty("password", "password");

    OracleConnection conn = null;

    try {
        // Get a JDBC connection to an Oracle instance
        conn = (OracleConnection) DriverManager.getConnection(url, props);

        // Enable JDBC implicit statement caching
        conn.setImplicitCachingEnabled(true);
        conn.setStatementCacheSize(50);

        // Get an OracleRDBMSClient - starting point of SODA for 
        // Java application
        OracleRDBMSClient cl = new OracleRDBMSClient();

        // Get a database
        OracleDatabase db = cl.getDatabase(conn);

        // Create a collection with the name "MyFirstJSONCollection".
        // Note: Collection names are case-sensitive.
        // A table with the name "MyFirstJSONCollection" will be
        // created in the RDBMS to store the collection
        OracleCollection col = db.admin().createCollection("MyFirstJSONCollection");

        // Create a few JSON documents, representing
        // users and the number of friends they have
        OracleDocument doc1 =
          db.createDocumentFromString(
            "{ \"name\" : \"Alex\", \"friends\" : \"50\" }");

        OracleDocument doc2 =
          db.createDocumentFromString(
            "{ \"name\" : \"Mia\", \"friends\" : \"300\" }");

        OracleDocument doc3 =
          db.createDocumentFromString(
            "{ \"name\" : \"Gloria\", \"friends\" : \"399\" }");

        // Insert the documents into a collection, one-by-one.
        // The result documents contain auto-generated 
        // keys, among other documents components (version, etc).
        // Note: SODA provides the more efficient bulk insert as well
        OracleDocument resultDoc1 = col.insertAndGet(doc1);
        OracleDocument resultDoc2 = col.insertAndGet(doc2);
        OracleDocument resultDoc3 = col.insertAndGet(doc3);

        // Retrieve the first document using its auto-generated
        // unique ID (aka key)
        System.out.println ("* Retrieving the first document by its key *\n");

        OracleDocument fetchedDoc = col.find().key(resultDoc1.getKey()).getOne();

        System.out.println (fetchedDoc.getContentAsString());

        // Retrieve all documents representing users that have
        // 300 or more friends. Use the following query-by-example:
        // {friends : {$gte : 300}}.
        System.out.println ("\n* Retrieving documents representing users with" +
                            " at least 300 friends *\n");

        OracleDocument f = db.createDocumentFromString(
         "{ \"friends\" : { \"$gte\" : 300 }}");

        OracleCursor c = null;

        try {
          // Get a cursor over all documents in the collection
          // that match our query-by-example
          c = col.find().filter(f).getCursor();

          while (c.hasNext()) {
            // Get the next document
            fetchedDoc = c.next();

            System.out.println (fetchedDoc.getContentAsString());
          }
        }
        finally {
          // Important: you must close the cursor to release resources!
          if (c != null) {
            c.close();
          }
        }

        // Drop the collection, deleting the table backing
        // it and collection metadata
        if (args.length > 0 && args[0].equals("drop")) {
          col.admin().drop();
          System.out.println ("\n* Collection dropped *");
        }
    }
    catch (Exception e) {
      e.printStackTrace();
    }
    finally {
      if (conn != null) {
        try {
          conn.close();
        }
        catch (Exception e) {
        }
      }
    }
  }
}
```

Copy and paste this code into a file called testSODA.java. Then modify the "url" String at the beginning of the program with connection info for your Oracle RDBMS instance. Also "user" and "password" properties set at the beginning of the program need to be modified with the schema name which will contain the table backing the collection, and the password for that schema.

To compile and run SODA Java applications, you need the following jars:

*    ojdbc6.jar that ships with Oracle Database 12.1.0.2. Download it from [this page](http://www.oracle.com/technetwork/database/features/jdbc/default-2280470.html).

*    javax.json-1.0.4.jar. This is the JSR353 implementation, download it from [here](http://search.maven.org/remotecontent?filepath=org/glassfish/javax.json/1.0.4/javax.json-1.0.4.jar).

*    orajsoda-version.jar. The SODA jar. Download the latest version [here](https://github.com/oracle/SODA-FOR-JAVA/releases).

Compile and run testSODA.java, making sure the necessary jars are in the classpath. For example, assuming you're in the directory where the jars are located, do:

    javac -classpath "orajsoda.jar" testSODA.java
    java -classpath "orajsoda-version.jar:ojdbc6.jar:javax.json-1.0.4.jar:." testSODA

You should see the following output:

    * Retrieving the first document by its key *

    { "name" : "Alex", "friends" : "50" }

    * Retrieving documents representing users with at least 300 friends *

    { "name" : "Mia", "friends" : "300" }
    { "name" : "Gloria", "friends" : "399" }

This example illustrates two ways of retrieving documents from the collection: by using unique document keys, or by using QBEs. To find all users with at least 300 friends, the following QBE was used in the code above:

    {"friends" : {"$gte" : 300}}

As you can see, a QBE is a JSON document with a structure similar to the JSON document it's trying to match. Various operators can appear inside the QBE. In this case, $gte operator is used to find all documents where the "friends" field is set to greater than or equal to 300. See the documentation for more info on QBEs ([Using Filter Specifications (QBEs)](http://docs.oracle.com/cd/E63251_01/doc.12/e58124/soda.htm#ADSDA172)).

To check out the table backing this collection, connect to the schema associated with your JDBC connection in the example above, using SQLPlus or another similar tool, and do:

    desc "MyFirstJSONCollection"

You should see:

    SQL> desc "MyFirstJSONCollection"

     Name                                      Null?    Type
     ----------------------------------------- -------- ----------------------------
     ID                                        NOT NULL VARCHAR2(255)
     CREATED_ON                                NOT NULL TIMESTAMP(6)
     LAST_MODIFIED                             NOT NULL TIMESTAMP(6)
     VERSION                                   NOT NULL VARCHAR2(255)
     JSON_DOCUMENT                                      BLOB

As you can see a table has been created with the following columns:

    ID                   Stores the auto-generated key
    JSON_DOCUMENT        Stores the document content
    CREATED_ON           Stores the auto-generated created-on timestamp
    LAST_MODIFIED        Stores the auto-generated last-modified timestamp
    VERSION              Stores the auto-generated document version

This table schema corresponds to the default collection configuration, but SODA collections are highly configurable. For example, the timestamp and the version columns are optional, there are many possible ways of generating the IDs or versions, etc. Custom collection configuration is covered in the documentation (see  [Creating a New Document Collection](http://docs.oracle.com/cd/E63251_01/doc.12/e58124/soda.htm#ADSDA115) and [Collection Configuration Using Custom Metadata] (http://docs.oracle.com/cd/E63251_01/doc.12/e58124/soda.htm#ADSDA192)). Although most users should be fine with the defaults, custom collection configuration might be useful in some cases, such as mapping an existing table to a new collection.

To drop the collection, removing the underlying table and cleaning up the metadata persisted in the database, run the example again, but this time with the "drop" argument at the end:

    java -classpath "orajsoda.jar:ojdbc6-12.1.0.2.0.jar:javax.json-1.0.4.jar:." testSODA drop

The output will now be different from before, since the same three documents will be inserted again. But, at the end, the collection will be dropped, and the underlying table removed.

Note: do not drop the collection table from SQL.  Collections have metadata stored in the Oracle RDBMS, so must be properly dropped by using the drop() method. See the line: col.admin().drop() in the code.
