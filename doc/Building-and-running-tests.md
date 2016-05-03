#Building and running the tests

To build and execute SODA tests, you must have a live Oracle database
12.1.0.2 instance, with patch 20885778 applied.

Obtain the patch from My Oracle Support ([https://support.oracle.com](https://support.oracle.com)). 
Select tab Patches & Updates. Search for patch number, 20885778 or access it directly at this URL: [https://support.oracle.com/rs?type=patch&id=20885778](https://support.oracle.com/rs?type=patch&id=20885778).
Make sure you follow all the installation steps specified in the README.txt file included with
the patch, including the post-installation step.

The test framework is located in the /test directory under the root SODA directory (the root SODA directory is the one containing LICENSE.txt file). It's built with JUnit and driven by Ant. The actual Java test files are in /test/src/oracle/json/tests/soda. To configure the database instance and the test framework, follow these steps (note that some of them require sysdba access).

**(1)** Build the source code (which includes downloading SODA dependencies), as described
in [Building the source code](https://github.com/oracle/soda-for-java/blob/master/doc/Building-source-code.md). Make sure you perform all steps described in this link. Specifically, make sure that the JAVA6HOME environment variable is set, and that the following jars are located in the /lib directory under the top level SODA directory (the one that contains LICENSE.txt):

* ojdbc6.jar (the JDBC jar that ships with Oracle database version 12.1.0.2)
* javax.json-1.0.4.jar (JSR353 jar)
* junit-3.8.1.jar (JUnit jar)
* orajsoda.jar (SODA Java jar)
      
**(2)** Some of the tests use encrypted data, so you must open a wallet.

Set the parameter in sqlnet.ora, if not set already, to specify the location of the wallet:

    ENCRYPTION_WALLET_LOCATION= 
       (SOURCE= 
           (METHOD=FILE) 
           (METHOD_DATA= 
               (DIRECTORY= /path/to/wallet/dir)
           )         
       )       

The directory /path/to/wallet/dir should already exist; if not then you should create it. Create the wallet:

    alter system set encryption key authenticated by "mywalletpass";

This creates the wallet with the password mywalletpass and opens it.

The above steps are needed only once. After the wallet is created and open, it stays open as long as the database is up (unless it is explicitly closed). If the database is restarted, you have to open the wallet with:

    alter system set encryption wallet open identified by "mywalletpass"

**(3)** Create a new account that the test framework will use to create and destroy document collections.
For example, suppose you name the account "myaccount" and use the password "mypassword". Then create it as follows:

    create user myaccount identified by mypassword;
    grant create session to myaccount;

Note: do not use a quoted (case-sensitive) account name.

**(4)** Perform database and account initialization needed for SODA tests. The initialization file is located in /test/ant/init.sql, under the top level SODA directory. Edit it and replace all references to MYACCOUNT with the name of the account you created in step 3. Then, execute it with sysdba privileges. For example, assuming "mydbaaccount" has sysdba privileges:

    sqlplus mydbaaccount/mydbapassword as sysdba @init.sql

**(5)** Provide the database instance connection info to the SODA test framework. Edit datasource.properties
located in /test/datasource.properties, under the top level SODA directory. The file has the following contents:

    UserName=myaccount
    Password=mypassword
    Server=myoraclehost
    Port=myoracleport
    DBName=mysid

Replace myaccount and mypassword with the account name and password created in step 3. Replace myoraclehost and myoracleport with the host name and port on which the Oracle instance is running/listening. Replace mydbname with the Oracle SID.

**(6)** Supply the directory where sqlplus is located to the test framework by setting the SODA_SQLPLUS environment variable. For example, assume sqlplus is located in /oracle/software/bin. Then, assuming you're on Linux and using the C shell (csh) do:

    setenv SODA_SQLPLUS /oracle/software/bin 

If you're using an OS other then Linux, set the environment variable using the appropriate mechanism.

**(7)** Navigate to the /test directory under the root SODA directory, and run the test framework:
  
    ant -Ddatasource=datasource.properties

The test framework should run and output "BUILD SUCCESSFUL" at the end. Open up a browser, and navigate to the /test/testoutput directory under the root SODA directory. Then navigate to the directory identified by the timestamp of your run (this directory is created by the test framework, on every run). From there, nagivate to /html/index.html, for a detailed test report. In case of test failures, this report allows you to navigate to the failures, and see the stack trace of their occurrence in each failed test.
