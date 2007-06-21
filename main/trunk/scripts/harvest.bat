@echo off
FOR %%i IN (lib\*.jar) DO call addjar.bat %%i
echo CLASSPATH=%classpath%
java -Xms128M -Xmx512M -Dlog4j.configuration=file:./default.log.properties de.pangaea.metadataportal.harvester.Harvester "./config.xml" "*"
pause