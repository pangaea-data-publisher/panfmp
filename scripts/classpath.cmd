@echo off
SET CLASSPATH=
FOR %%i IN (..\dist\*.jar) DO CALL :addPart %%i
GOTO :eof

:addPart
SET CLASSPATH=%CLASSPATH%;%1
GOTO :eof