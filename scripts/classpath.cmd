@echo off
SET CLASSPATH=
FOR %%i IN (..\lib\core\*.jar ..\lib\addons\*.jar ..\plugins\*.jar ..\dist\*.jar) DO CALL :addPart %%i
GOTO :eof

:addPart
SET CLASSPATH=%CLASSPATH%;%1
GOTO :eof