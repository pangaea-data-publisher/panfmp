#!/bin/sh

# config file to use (relative to repository base)
export PANFMP_CONFIG="../repository/config.xml"

# java options for harvesting and management tools
export PANFMP_TOOLS_JAVA_OPTIONS="-Xms64M -Xmx512M"

# log4j configuration file for harvesting and management tools (relative to scripts dir)
export PANFMP_TOOLS_LOG4J_CONFIG="./console.log.properties"

# java options for jetty webserver (if installed)
#export PANFMP_JETTY_JAVA_OPTIONS="-Xms128M -Xmx1024"

# log4j configuration file for jetty webserver (if installed, relative to scripts dir)
#export PANFMP_JETTY_LOG4J_CONFIG="./webserver.log.properties"
