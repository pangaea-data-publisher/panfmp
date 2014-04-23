#!/bin/sh
cd `dirname $0`

# java options for jetty webserver (if installed)
PANFMP_JETTY_JAVA_OPTIONS="-Xms128M -Xmx1024M -Djetty.port=8080 -Djetty.host=0.0.0.0"

# log4j configuration file for jetty webserver
PANFMP_JETTY_LOG4J_CONFIG="./console.log.properties"

CLASSPATH=
for i in ../../dist/*.jar ../../libs/core/*.jar ../../libs/optional/*.jar ../../libs/jetty/*.jar; do
	CLASSPATH="$CLASSPATH:$i"
done
if [ -x /usr/bin/cygpath ]; then
	CLASSPATH=`cygpath -pw "$CLASSPATH"`
fi
export CLASSPATH

exec java ${PANFMP_JETTY_JAVA_OPTIONS} \
	-Dlog4j.configuration="file:${PANFMP_JETTY_LOG4J_CONFIG}" \
	org.mortbay.xml.XmlConfiguration jetty.xml
