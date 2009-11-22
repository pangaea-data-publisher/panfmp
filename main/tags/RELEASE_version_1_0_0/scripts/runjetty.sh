#!/bin/sh
cd `dirname $0`
. ./config.sh

CLASSPATH=
for i in ../dist/*.jar ../libs/core/*.jar ../libs/optional/*.jar ../libs/jetty/*.jar ../libs/axis/*.jar; do
	CLASSPATH="$CLASSPATH:$i"
done
if [ -x /usr/bin/cygpath ]; then
	CLASSPATH=`cygpath -pw "$CLASSPATH"`
fi
export CLASSPATH

if [ "${PANFMP_JETTY_DETACH}" = "Yes" ]; then
	java ${PANFMP_JETTY_JAVA_OPTIONS} \
		-Dlog4j.configuration="file:${PANFMP_JETTY_LOG4J_CONFIG}" \
		org.mortbay.xml.XmlConfiguration jetty.xml &
else
	exec java ${PANFMP_JETTY_JAVA_OPTIONS} \
		-Dlog4j.configuration="file:${PANFMP_JETTY_LOG4J_CONFIG}" \
		org.mortbay.xml.XmlConfiguration jetty.xml
fi