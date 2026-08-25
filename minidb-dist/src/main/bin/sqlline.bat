@echo off
rem MiniDB sqlline 命令行客户端(Windows)。linux/macOS 用 sqlline。
setlocal

set "BIN_DIR=%~dp0"
for %%I in ("%BIN_DIR%..") do set "MINIDB_HOME=%%~fI"

if defined JAVA_HOME (
  set "JAVA=%JAVA_HOME%\bin\java.exe"
) else (
  set "JAVA=java"
)
if not exist "%JAVA%" (
  echo ERROR: java not found. Set JAVA_HOME or add to PATH.
  exit /b 1
)

if not defined MINIDB_HOST set "MINIDB_HOST=localhost"
if not defined MINIDB_PORT set "MINIDB_PORT=8899"
if not defined MINIDB_URL set "MINIDB_URL=jdbc:minidb://%MINIDB_HOST%:%MINIDB_PORT%"

cd /d "%MINIDB_HOME%"
"%JAVA%" %MINIDB_JAVA_OPTS% --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -cp "tools\*;jdbc\*" sqlline.SqlLine -n "" -p "" -d com.minidb.jdbc.MiniDbDriver -u "%MINIDB_URL%" %*
endlocal