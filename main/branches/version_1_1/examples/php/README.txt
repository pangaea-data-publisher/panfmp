This directory contains example scripts written in PHP.

To run these, copy this directory onto a PHP 5.2-enabled
webserver that has support for the following PHP extensions:
 - xml
 - xslt
 - soap
Older versions of PHP (5.1) *may* work, but have some
really annoying bugs.

The scripts use the SOAP/WSDL API of panFMP and for this
to work, you have to start the jetty web application
container (from "scripts/" directory), that is bundled with
panFMP, on the same machine.

The scripts are working with the default (DIF-based)
configuration of panFMP.
