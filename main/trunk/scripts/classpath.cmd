@echo off
SET CLASSPATH=
FOR %%i IN (..\lib\*.jar ..\dist\*.jar) DO CALL :addPart %%i
GOTO :eof

:addPart
SET CLASSPATH=%CLASSPATH%;%1
GOTO :eof