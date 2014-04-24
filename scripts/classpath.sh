#!/bin/sh
CLASSPATH=
for i in ../lib/*.jar ../dist/*.jar; do
	CLASSPATH="$CLASSPATH:$i"
done
if [ -x /usr/bin/cygpath ]; then
	CLASSPATH=`cygpath -pw "$CLASSPATH"`
fi
export CLASSPATH
