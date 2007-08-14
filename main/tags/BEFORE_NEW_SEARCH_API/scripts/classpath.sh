#!/bin/sh
for i in ../dist/*.jar ../libs/core/*.jar ../libs/optional/*.jar; do
	CLASSPATH=$CLASSPATH:$i
done
export CLASSPATH
