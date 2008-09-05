#!/bin/sh
cd `dirname $0`
source ./classpath.sh
java -Xms64M -Xmx512M -Dlog4j.configuration=file:./default.log.properties de.pangaea.metadataportal.harvester.Optimizer "./config.xml" "$@"