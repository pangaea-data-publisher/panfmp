@echo off
set classpath=
FOR %%i IN (..\dist\*.jar ..\libs\core\*.jar ..\libs\optional\*.jar) DO call addjar.bat %%i
echo CLASSPATH=%classpath%
java -Xms128M -Xmx512M -Dlog4j.configuration=file:./default.log.properties de.pangaea.metadataportal.harvester.Checker "./config.xml" "*"
pause