@echo off
cd /d "%~dp0"
"C:\Program Files\nodejs\node.exe" server.js >> server.task.log 2>> server.task.err.log
