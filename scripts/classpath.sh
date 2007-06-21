#!/bin/sh
for i in ../lib/*.jar; do
	CLASSPATH=$CLASSPATH:$i
done
export CLASSPATH
