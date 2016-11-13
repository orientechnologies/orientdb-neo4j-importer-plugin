@echo off
rem
rem Copyright (c) Orient Technologies LTD (http://www.orientechnologies.com)
rem
rem Guess ORIENTDB_HOME if not defined
set CURRENT_DIR=%cd%

if exist "%JAVA_HOME%\bin\java.exe" goto setJavaHome
set JAVA="java"
goto okJava

:setJavaHome
set JAVA="%JAVA_HOME%\bin\java"

:okJava
if not "%ORIENTDB_HOME%" == "" goto gotHome
set ORIENTDB_HOME=%CURRENT_DIR%
if exist "%ORIENTDB_HOME%\bin\orientdb-neo4j-importer.bat" goto okHome
cd ..
set ORIENTDB_HOME=%cd%
cd %CURRENT_DIR%

:gotHome
if exist "%ORIENTDB_HOME%\bin\orientdb-neo4j-importer.bat" goto okHome
echo The ORIENTDB_HOME environment variable is not defined correctly
echo This environment variable is needed to run this program
goto end

:okHome
rem Get remaining unshifted command line arguments and save them in the
set CMD_LINE_ARGS=

SET NEO4JLIB=%~2%

:setArgs
if ""%1""=="""" goto doneSetArgs
set CMD_LINE_ARGS=%CMD_LINE_ARGS% %1
shift
goto setArgs

:doneSetArgs
set LOG_FILE=%ORIENTDB_HOME%\config\orientdb-neo4j-importer-log.properties

set JAVA_MAX_DIRECT=-XX:MaxDirectMemorySize=4g
set JAVA_OPTS_SCRIPT= %JAVA_MAX_DIRECT% -Djava.util.logging.config.file=%LOG_FILE%

call %JAVA% -client -classpath "%NEO4JLIB%\*;%ORIENTDB_HOME%\lib\*" %JAVA_OPTS_SCRIPT% com.orientechnologies.orient.neo4jimporter.ONeo4jImporterMain %CMD_LINE_ARGS%

:end
