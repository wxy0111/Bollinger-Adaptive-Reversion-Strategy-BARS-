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
echo Reading logs and replaying slim core live-logic parameters with Optuna.
echo Default replay uses 3s final replay, current sizing, and add-on guards.
echo This searches only the core risk/profit knobs:
echo TP-space width, max Bollinger width, entry gap,
echo first/second/later sizing, fixed-loss/disaster stops, and add-on gap max,
echo then writes a balanced report with walk-forward validation.
echo.

".venv\Scripts\python.exe" backtest\log_parameter_optimizer.py --search-mode optuna --optuna-trials 80 --optuna-startup-trials 16 --sample-sec 15 --two-stage --final-sample-sec 3 --refine-top-n 10 --use-cache --workers 0 --walk-forward-ratio 0.7 --walk-forward-top-n 10
pause
