#!/bin/sh

# INFO: All paths are relative to the scripts/ directory

# config file to use
PANFMP_CONFIG="../conf/config.xml"

# lock file to prevent more than one parallel harvesting/...
PANFMP_LOCK="../conf/lock"

# java options for harvesting and management tools
PANFMP_TOOLS_JAVA_OPTIONS="-XX:+UseG1GC -Xms64M -Xmx512M"

# log4j configuration file for harvesting and management tools
PANFMP_TOOLS_LOG4J_CONFIG="./console.log.properties"
