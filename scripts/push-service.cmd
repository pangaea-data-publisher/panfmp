@echo off
CALL config.cmd
CALL classpath.cmd
java %PANFMP_TOOLS_JAVA_OPTIONS% -Dlog4j.configurationFile=%PANFMP_TOOLS_LOG4J_CONFIG% de.pangaea.metadataportal.push.PushServer %PANFMP_CONFIG%
