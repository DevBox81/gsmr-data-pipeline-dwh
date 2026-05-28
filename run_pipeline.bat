@echo off

:: ============================================================
:: GSM-R PIPELINE LAUNCHER
:: ============================================================

cd /d "C:\Users\user\Desktop\STAGE TECHNIQUE\uncef\gsmr-data-pipeline-dwh-main"

echo [%DATE% %TIME%] Scheduler started >> logs\startup.log

python scheduler.py --now

echo [%DATE% %TIME%] Scheduler exited with code %ERRORLEVEL% >> logs\startup.log

pause