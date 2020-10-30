#!/bin/sh
CLASSPATH=
for i in ../lib/core/*.jar ../lib/addons/*.jar ../plugins/*.jar ../dist/*.jar; do
	CLASSPATH="$CLASSPATH:$i"
done
if [ -x /usr/bin/cygpath ]; then
	CLASSPATH=`cygpath -pw "$CLASSPATH"`
fi
export CLASSPATH
