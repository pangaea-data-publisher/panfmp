#!/bin/sh
CWD=`pwd`
cd `dirname $0`

. ./config.sh

if [ -f "${PANFMP_LOCK}" ]; then
	exit 1
fi
touch "${PANFMP_LOCK}"

cd "$CWD"
"$@"
RET=$?

cd `dirname $0`
rm -f "${PANFMP_LOCK}"
exit $RET
