#!/bin/sh
CLASSPATH=
for i in ../dist/*.jar ../libs/core/*.jar ../libs/optional/*.jar; do
	CLASSPATH="$CLASSPATH:$i"
done
if [ -x /usr/bin/cygpath ]; then
	CLASSPATH=`cygpath -pw "$CLASSPATH"`
fi
export CLASSPATH
