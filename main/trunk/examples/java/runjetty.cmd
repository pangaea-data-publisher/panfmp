@echo off

REM java options for jetty webserver (if installed)
SET PANFMP_JETTY_JAVA_OPTIONS=-Xms128M -Xmx1024M -Djetty.port=8080 -Djetty.host=0.0.0.0

REM log4j configuration file for jetty webserver
SET PANFMP_JETTY_LOG4J_CONFIG="./console.log.properties"

SET CLASSPATH=
FOR %%i IN (..\..\dist\*.jar ..\..\libs\core\*.jar ..\..\libs\optional\*.jar ..\..\libs\jetty\*.jar) DO CALL :addPart %%i

java %PANFMP_JETTY_JAVA_OPTIONS% -Dlog4j.configuration=file:%PANFMP_JETTY_LOG4J_CONFIG% org.mortbay.xml.XmlConfiguration jetty.xml
GOTO :eof

:addPart
SET CLASSPATH=%CLASSPATH%;%1
GOTO :eof
