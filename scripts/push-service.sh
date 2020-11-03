#!/bin/sh
cd `dirname $0`
. ./config.sh
. ./classpath.sh
exec java ${PANFMP_PUSH_JAVA_OPTIONS} \
	-Dlog4j.configurationFile="${PANFMP_PUSH_LOG4J_CONFIG}" \
	de.pangaea.metadataportal.push.PushServer \
	"${PANFMP_CONFIG}" "$@"
