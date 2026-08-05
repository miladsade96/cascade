@echo off
setlocal
set "SBT_VERSION=1.12.6"
set "LAUNCHER_DIR=%~dp0.tools"
set "LAUNCHER=%LAUNCHER_DIR%\sbt-launch-%SBT_VERSION%.jar"
if not exist "%LAUNCHER%" (
  if not exist "%LAUNCHER_DIR%" mkdir "%LAUNCHER_DIR%"
  echo Downloading sbt %SBT_VERSION% launcher...
  curl.exe --fail --location --silent --show-error --output "%LAUNCHER%" "https://repo.maven.apache.org/maven2/org/scala-sbt/sbt-launch/%SBT_VERSION%/sbt-launch-%SBT_VERSION%.jar"
  if errorlevel 1 exit /b %errorlevel%
)
java -Xms256m -Xmx2g -Dsbt.supershell=false -Dsbt.log.noformat=true -jar "%LAUNCHER%" %*
