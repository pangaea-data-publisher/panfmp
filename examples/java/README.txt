This directory contains example servlets written in Java.

Servlets may be compiled with the supplied ANT build.xml
(http://ant.apache.org) script. The script runjetty(.sh|.cmd) can be used
to start a Jetty web container listening of port 8080. After that you can test
the servlets using the following URL:
	http://localhost:8080/

Both servlets implement a different way to access search results
(as noted in JavaDocs for SearchService).

The web application is hardwired to the demo index config from the
"repository/" directory of panFMP.
