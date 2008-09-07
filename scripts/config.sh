#!/bin/sh

# INFO: All paths are relative to the screipts/ directory

# config file to use
PANFMP_CONFIG="../repository/config.xml"

# lock file to prevent more than one parallel harvesting/...
PANFMP_LOCK="../repository/lock"

# java options for harvesting and management tools
PANFMP_TOOLS_JAVA_OPTIONS="-Xms64M -Xmx512M"

# log4j configuration file for harvesting and management tools
PANFMP_TOOLS_LOG4J_CONFIG="./console.log.properties"

# java options for jetty webserver (if installed)
PANFMP_JETTY_JAVA_OPTIONS="-Xms128M -Xmx1024M -Djetty.port=8801 -Djetty.host=127.0.0.1"

# log4j configuration file for jetty webserver
PANFMP_JETTY_LOG4J_CONFIG="./webserver.log.properties"
