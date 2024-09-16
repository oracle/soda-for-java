# Building the source code

SODA is built using Maven. Make sure you have Apache Maven 3.6.2 or above installed and configured.

The following instructions require setting various environment variables. They assume you're on Linux, and using the C Shell (csh). On other OSes, you would set environment variables analogously, using a mechanism appropriate for your specific operating system.

Set the JAVA_HOME environment variable to the JDK install directory. At a minimum, JDK 8 is required. For example, assuming you are using JDK 8 installed under /jdk8, and you're using the C Shell (csh) on Linux, do:

    setenv JAVA_HOME /jdk8 

### Building with Maven

`cd soda-for-java`\
`mvn clean install`

If you're behind a firewall, you might need to set the proxy host and port
number. For example, assuming your proxy host is myproxy.mycompany.com and the port is 80, do:

`mvn clean install -Dhttp.proxyHost=myproxy.mycompany.com -Dhttp.proxyPort=80 -Dhttps.proxyHost=myproxy.mycompany.com -Dhttps.proxyPort=80`

If the build is successful, you should see orajsoda.jar (the SODA jar) and other dependency jars in the /lib directory under the top SODA directory.
