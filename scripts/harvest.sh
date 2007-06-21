#!/bin/sh
cd `dirname $0`
source ./classpath.sh
java -Xms64M -Xmx512M -Dlog4j.configuration=file:./harvest.log.properties de.pangaea.metadataportal.harvester.Harvester "./config.xml" "*"
