@echo off
CALL config.cmd

SET CLASSPATH=
FOR %%i IN (..\dist\*.jar ..\libs\core\*.jar ..\libs\optional\*.jar ..\libs\jetty\*.jar ..\libs\axis\*.jar) DO CALL :addPart %%i

java %PANFMP_JETTY_JAVA_OPTIONS% -Dlog4j.configuration=file:%PANFMP_JETTY_LOG4J_CONFIG% org.mortbay.xml.XmlConfiguration jetty.xml
GOTO :eof

:addPart
SET CLASSPATH=%CLASSPATH%;%1
GOTO :eof
