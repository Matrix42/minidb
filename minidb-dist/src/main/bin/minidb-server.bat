@echo off
rem MiniDB server launcher (Windows). Use minidb-server on linux/macOS.
rem Usage: minidb-server.bat [start|stop|status]
setlocal enabledelayedexpansion

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

if not defined MINIDB_DATA_DIR set "MINIDB_DATA_DIR=%MINIDB_HOME%\data"
if not defined MINIDB_CONF_DIR set "MINIDB_CONF_DIR=%MINIDB_HOME%\conf"

set "PID_FILE=%MINIDB_HOME%\logs\minidb.pid"
set "LOG_OUT=%MINIDB_HOME%\logs\minidb.out"

set "CMD=%1"
shift

set "JAVA_OPTS=--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -Dlog4j2.configurationFile=conf\log4j2.properties"

set "ARGS=--data "%MINIDB_DATA_DIR%" --conf "%MINIDB_CONF_DIR%""
if defined MINIDB_PORT set "ARGS=%ARGS% --port %MINIDB_PORT%"

if "%CMD%"=="start" goto :do_start
if "%CMD%"=="stop"  goto :do_stop
if "%CMD%"=="status" goto :do_status

:foreground
  rem No args = foreground (debug); use start/stop/status or env vars
  cd /d "%MINIDB_HOME%"
  "%JAVA%" %MINIDB_JAVA_OPTS% %JAVA_OPTS% -cp "libs\*" com.minidb.server.MiniDbServer %ARGS% %*
  goto :eof

:do_status
  if not exist "%PID_FILE%" (
    echo MiniDB is stopped
    exit /b 1
  )
  set /p PID=<"%PID_FILE%"
  tasklist /fi "PID eq !PID!" 2>nul | findstr /i "!PID!" >nul
  if errorlevel 1 (
    echo MiniDB is stopped (pid file exists but process !PID! is dead^)
    del "%PID_FILE%" 2>nul
    exit /b 1
  )
  echo MiniDB is running (pid !PID!^)
  goto :eof

:do_stop
  if not exist "%PID_FILE%" (
    echo MiniDB not running (no pid file^)
    exit /b 1
  )
  set /p PID=<"%PID_FILE%"
  echo Stopping MiniDB (pid !PID!^)...
  taskkill /PID !PID! 2>nul
  set WAITED=0
  :wait_stop
    if not exist "%PID_FILE%" goto :stopped
    tasklist /fi "PID eq !PID!" 2>nul | findstr /i "!PID!" >nul
    if errorlevel 1 (
      del "%PID_FILE%" 2>nul
      goto :stopped
    )
    if !WAITED! geq 30 (
      echo Graceful stop timeout, force killing
      taskkill /F /PID !PID! 2>nul
      del "%PID_FILE%" 2>nul
      goto :stopped
    )
    ping -n 2 127.0.0.1 >nul
    set /a WAITED+=1
    goto :wait_stop
  :stopped
  echo MiniDB stopped
  goto :eof

:do_start
  call :do_status >nul 2>nul
  if not errorlevel 1 (
    echo MiniDB already running
    exit /b 1
  )
  if not exist "%MINIDB_HOME%\logs" mkdir "%MINIDB_HOME%\logs"
  cd /d "%MINIDB_HOME%"
  start /b "" "%JAVA%" %MINIDB_JAVA_OPTS% %JAVA_OPTS% -cp "libs\*" com.minidb.server.MiniDbServer %ARGS% --pid-file "%PID_FILE%" >> "%LOG_OUT%" 2>&1
  set WAITED=0
  :wait_pid
    if exist "%PID_FILE%" goto :pid_ready
    ping -n 2 127.0.0.1 >nul
    set /a WAITED+=1
    if !WAITED! lss 10 goto :wait_pid
    echo MiniDB start failed (pid file not created^), check log: %LOG_OUT%
    exit /b 1
  :pid_ready
  set /p PID=<"%PID_FILE%"
  echo MiniDB started (pid !PID!^)
  goto :eof