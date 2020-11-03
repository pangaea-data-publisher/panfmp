@echo off

REM INFO: All paths are relative to the scripts/ directory

REM config file to use
SET PANFMP_CONFIG="../conf/config.xml"

REM java options for harvesting and management tools
SET PANFMP_TOOLS_JAVA_OPTIONS=-XX:+UseG1GC -Xms64M -Xmx512M

REM log4j configuration file for harvesting and management tools
SET PANFMP_TOOLS_LOG4J_CONFIG="./console.log.properties"

REM java options for for push HTTP server (you can change port number here)
SET PANFMP_PUSH_JAVA_OPTIONS=-XX:+UseG1GC -Xms64M -Xmx512M -Dserver.port=8089

REM log4j configuration file for push HTTP server
SET PANFMP_PUSH_LOG4J_CONFIG="./push-server.log.properties"
