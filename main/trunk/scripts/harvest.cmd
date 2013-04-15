@echo off
CALL config.cmd
CALL classpath.cmd
java %PANFMP_TOOLS_JAVA_OPTIONS% -Dlog4j.configuration=file:%PANFMP_TOOLS_LOG4J_CONFIG% de.pangaea.metadataportal.harvester.Harvester %PANFMP_CONFIG% %1
