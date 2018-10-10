# Building the source code

SODA is built using Ant with Ivy. Make sure you have Ant 1.9 or above installed and configured.
It's not required that Ivy is installed - as long as you have an internet connection, 
the build process will download Ivy and use it to download SODA dependencies. 

The only SODA dependency that must be manually downloaded is the Oracle JDBC jar that ships with 
the Oracle database. JDBC 6 or above jar is required.

For example, if you're using JDK8 or above, you could use ojdbc8.jar that ships with Oracle Database 18c, available from [this page](https://www.oracle.com/technetwork/database/application-development/jdbc/downloads/jdbc-ucp-183-5013470.html). Alternatively, you can also use ojdbc8.jar that ships with OracleDatabase 12.2.0.1, available from [this page](https://www.oracle.com/technetwork/database/features/jdbc/jdbc-ucp-122-3110062.html). If you're still on JDK7 or even JDK6, you can use ojdbc7.jar or ojdbc6.jar respectively. These ship with Oracle Database 12.1.0.2, and are downloadable from [this page](http://www.oracle.com/technetwork/database/features/jdbc/default-2280470.html).

Copy the downloaded JDBC jar to the /lib directory under the root SODA directory (the root SODA directory is the one containing LICENSE.txt file).

Other dependencies will be automatically downloaded by Ivy. You also have the
option to not use Ivy, and to download the dependencies manually (see "Building without Ivy" below).

The following instructions require setting various environment variables. They assume you're on Linux, and using the C Shell (csh). On other OSes, you would set environment variables analogously, using a mechanism appropriate for your specific operating system.
 
Set the JAVA_HOME environment variable to the JDK install directory. At a minimum, JDK 6 is required. For example, assuming you are using JDK 8 installed under /jdk8, and you're using the C Shell (csh) on Linux, do:

    setenv JAVA_HOME /jdk8 

### Building with Ivy

If you're behind a firewall, you might need to set the proxy host and port
number by setting the ANT_OPTS environment variable. For example, assuming
your proxy host is myproxy.mycompany.com and the port is 80, do:

    setenv ANT_OPTS "-Dhttp.proxyHost=myproxy.mycompany.com -Dhttp.proxyPort=80"

If you have Ivy installed already, set the IVY_HOME environment variable.
The ivy.jar file must be located under the IVY_HOME/lib. For example,
suppose your ivy.jar is in /home/myname/ivyinstall/lib. Then you would set
IVY_HOME as follows:

    setenv IVY_HOME /home/myname/invyinstall

If you don't have Ivy installed, it will be downloaded automatically to
${user.home}/.ant/lib. See [https://ant.apache.org/manual/Tasks/property.html](https://ant.apache.org/manual/Tasks/property.html) for info on what ${user.home} resolves to on different operating
systems.

Finally, navigate to the top SODA Java directory (the one that contains LICENSE.txt),
and execute the ant build file by typing:

    ant

As this command is executing, you should see messages informing you about Ivy downloading itself 
(unless it's already installed), downloading of the SODA dependencies, and code compilation 
and archiving (jar creation).

If the process gets stuck while downloading Ivy, or downloading SODA dependencies (in case you
have Ivy installed already), and you're behind a firewall, make sure your proxy host and port
settings are correct.

If the build is successful, you should see orajsoda.jar (the SODA jar) in the /lib directory
under the top SODA directory.

### Building without Ivy

Other than the ojdbc6.jar mentioned above, SODA has the following dependencies:

* javax.json-1.0.4.jar (JSR353 jar)
* junit-3.8.1.jar (JUnit jar, only used by the testsuite)

Download the javax.json-1.0.4.jar ([http://search.maven.org/remotecontent?filepath=org/glassfish/javax.json/1.0.4/javax.json-1.0.4.jar](http://search.maven.org/remotecontent?filepath=org/glassfish/javax.json/1.0.4/javax.json-1.0.4.jar)) and junit-3.8.1.jar ([http://central.maven.org/maven2/junit/junit/3.8.1/junit-3.8.1.jar](http://central.maven.org/maven2/junit/junit/3.8.1/junit-3.8.1.jar)).

Place these jars in the /lib directory under the root SODA directory. Make sure you also put the ojdbc6.jar into this directory as well, as described above.

Finally, navigate to the top SODA Java directory (the one that contains LICENSE.txt),
and execute the ant build file by typing the following command:

    ant -Ddownload.deps=false

As this command is executing, you should see messages related to code compilation and archiving 
(jar creation).

If the build is successful, you should see orajsoda.jar (the SODA jar) in the /lib directory
under the top SODA directory.
