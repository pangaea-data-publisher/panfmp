#!/bin/sh
cd `dirname $0`
. ./config.sh
. ./classpath.sh
exec java ${PANFMP_TOOLS_JAVA_OPTIONS} \
	-Dlog4j.configurationFile="${PANFMP_TOOLS_LOG4J_CONFIG}" \
	de.pangaea.metadataportal.push.PushServer \
	"${PANFMP_CONFIG}" "$@"
