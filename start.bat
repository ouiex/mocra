@echo off
set /a i=0
:loop
if %i% geq 20 goto :eof
echo Iteration %i%
type prompt.md | copilot --prompt prompt.md  --allow-all --allow-all-tools --model claude-sonnet-4.5
timeout /t 1 >nul
set /a i+=1
goto loop