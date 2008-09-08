@echo off

REM INFO: All paths are relative to the screipts/ directory

REM config file to use
SET PANFMP_CONFIG="../repository/config.xml"

REM java options for harvesting and management tools
SET PANFMP_TOOLS_JAVA_OPTIONS=-Xms64M -Xmx512M

REM log4j configuration file for harvesting and management tools
SET PANFMP_TOOLS_LOG4J_CONFIG="./console.log.properties"

REM java options for jetty webserver (if installed)
SET PANFMP_JETTY_JAVA_OPTIONS=-Xms128M -Xmx1024M -Djetty.port=8801 -Djetty.host=127.0.0.1

REM log4j configuration file for jetty webserver
SET PANFMP_JETTY_LOG4J_CONFIG="./console.log.properties"
