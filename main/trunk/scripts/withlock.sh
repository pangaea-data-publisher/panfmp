#!/bin/sh
cd `dirname $0`
. ./config.sh

if [ -f "${PANFMP_LOCK}" ]; then
	exit 1
fi

touch "${PANFMP_LOCK}"
"$@"
RET=$?
rm -f "${PANFMP_LOCK}"
exit $RET
