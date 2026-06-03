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
echo Reading logs and replaying the focused live-logic parameter grid.
echo Default replay uses 3s resampling, cross-copy sizing, and add-on K-line guard.
echo This uses random search for Bollinger width, entry gap, and risk parameters,
echo then writes a balanced report with walk-forward validation.
echo.

".venv\Scripts\python.exe" backtest\log_parameter_optimizer.py --search-mode random --random-trials 300 --walk-forward-ratio 0.7 --walk-forward-top-n 10
pause
