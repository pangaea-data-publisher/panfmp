@echo off

REM config file to use (relative to repository base)
SET PANFMP_CONFIG="..\repository\config.xml"

REM java options for harvesting and management tools
SET PANFMP_TOOLS_JAVA_OPTIONS=-Xms64M -Xmx512M

REM log4j configuration file for harvesting and management tools (relative to scripts dir)
SET PANFMP_TOOLS_LOG4J_CONFIG="./console.log.properties"

REM java options for jetty webserver (if installed)
rem SET PANFMP_JETTY_JAVA_OPTIONS="-Xms128M -Xmx1024"

REM log4j configuration file for jetty webserver (if installed, relative to scripts dir)
rem SET PANFMP_JETTY_LOG4J_CONFIG="./webserver.log.properties"
