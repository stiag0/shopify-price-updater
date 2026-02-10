@echo off
cd /d "%~dp0"
node log-retention.js
if errorlevel 1 (
    echo Error running log cleanup
    exit /b 1
)
exit /b 0 