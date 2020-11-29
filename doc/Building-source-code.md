# Building the source code

SODA is built using Ant with Ivy. Make sure you have Ant 1.9 or above installed and configured.  It's not required that Ivy is installed - as long as you have an internet connection, the build process will download Ivy and use it to download SODA dependencies. 

The following instructions require setting various environment variables. They assume you're on Linux, and using the C Shell (csh). On other OSes, you would set environment variables analogously, using a mechanism appropriate for your specific operating system.
 
Set the JAVA_HOME environment variable to the JDK install directory. At a minimum, JDK 8 is required. For example, assuming you are using JDK 8 installed under /jdk8, and you're using the C Shell (csh) on Linux, do:

    setenv JAVA_HOME /jdk8 

### Building with Ivy

If you're behind a firewall, you might need to set the proxy host and port
number by setting the ANT_OPTS environment variable. For example, assuming
your proxy host is myproxy.mycompany.com and the port is 80, do:

    setenv ANT_OPTS "-Dhttp.proxyHost=myproxy.mycompany.com -Dhttp.proxyPort=80 -Dhttps.proxyHost=myproxy.mycompany.com -Dhttps.proxyPort=80"

If you have Ivy installed already, set the IVY_HOME environment variable.
The ivy.jar file must be located under the IVY_HOME/lib. For example,
suppose your ivy.jar is in /home/myname/ivyinstall/lib. Then you would set
IVY_HOME as follows:

    setenv IVY_HOME /home/myname/invyinstall

If you don't have Ivy installed, it will be downloaded automatically to
${user.home}/.ant/lib. See [https://ant.apache.org/manual/Tasks/property.html](https://ant.apache.org/manual/Tasks/property.html) for info on what ${user.home} resolves to on different operating systems.

Finally, navigate to the top SODA Java directory (the one that contains LICENSE.txt),
and execute the ant build file by typing:

    ant

As this command is executing, you should see messages informing you about Ivy downloading itself (unless it's already installed), downloading of the SODA dependencies, and code compilation and archiving (jar creation).

If the process gets stuck while downloading Ivy, or downloading SODA dependencies (in case you
have Ivy installed already), and you're behind a firewall, make sure your proxy host and port
settings are correct.

If the build is successful, you should see orajsoda.jar (the SODA jar) in the /lib directory
under the top SODA directory.

### Building without Ivy

SODA has the following dependencies:

* ojdbc8.jar or above (JDBC jar)
* javax.json-1.1.4.jar (JSR353 jar)
* junit-3.8.1.jar (JUnit jar, only used by the testsuite)

Download the jdbc jar ([https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/19.3.0.0/ojdbc8-19.3.0.0.jar](https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/19.3.0.0/ojdbc8-19.3.0.0.jar)), javax.json-1.1.4.jar ([http://search.maven.org/remotecontent?filepath=org/glassfish/javax.json/1.1.4/javax.json-1.1.4.jar](http://search.maven.org/remotecontent?filepath=org/glassfish/javax.json/1.0.4/javax.json-1.0.4.jar)) and junit-3.8.1.jar ([http://central.maven.org/maven2/junit/junit/3.8.1/junit-3.8.1.jar](http://central.maven.org/maven2/junit/junit/3.8.1/junit-3.8.1.jar)).

Place these jars in the /lib directory under the root SODA directory. Make sure you also put the ojdbc6.jar into this directory as well, as described above.

Finally, navigate to the top SODA Java directory (the one that contains LICENSE.txt),
and execute the ant build file by typing the following command:

    ant -Ddownload.deps=false

As this command is executing, you should see messages related to code compilation and archiving (jar creation).

If the build is successful, you should see orajsoda.jar (the SODA jar) in the /lib directory
under the top SODA directory.
