@echo off
cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
    echo Cannot find .venv\Scripts\python.exe
    echo Please create the virtual environment and install requirements first.
    pause
    exit /b 1
)

echo ========================================
echo OKX log parameter optimizer
echo ========================================
echo Reading logs and replaying parameter sets.
echo This may take a few minutes depending on log size.
echo.

".venv\Scripts\python.exe" backtest\log_parameter_optimizer.py
pause
