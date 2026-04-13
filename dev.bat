@echo off
setlocal EnableDelayedExpansion

:: ── Script directory ─────────────────────────────────────────────────────────
set "ROOT=%~dp0"
if "%ROOT:~-1%"=="\" set "ROOT=%ROOT:~0,-1%"
set "LOCAL=%ROOT%\.local"

:: ── 1. Create local data directories ─────────────────────────────────────────
for %%D in (source target config delete-staging) do (
    if not exist "%LOCAL%\%%D" mkdir "%LOCAL%\%%D"
)

:: ── 2. Free ports 8080 / 5173 ────────────────────────────────────────────────
call :KillPort 8080
call :KillPort 5173

:: ── 3. Set environment variables (inherited by child processes) ───────────────
set "CONFIG_DIR=%LOCAL%\config"
set "SOURCE_DIR=%LOCAL%\source"
set "TARGET_DIR=%LOCAL%\target"
set "DELETE_STAGING_DIR=%LOCAL%\delete-staging"
set "PORT=8080"
set "CGO_ENABLED=0"

:: ── 4. Start backend in a new console window ─────────────────────────────────
echo Starting backend  (http://localhost:8080) ...
start "Classifier-Backend" /d "%ROOT%\backend" cmd /k go run ./cmd/server

:: ── 5. Wait for backend health check (max 20 s, 1 s interval) ────────────────
set "BACKEND_READY=0"
for /l %%i in (1,1,20) do (
    if "!BACKEND_READY!"=="0" (
        timeout /t 1 /nobreak >nul
        curl -sf http://localhost:8080/health >nul 2>&1 && set "BACKEND_READY=1"
    )
)

if "!BACKEND_READY!"=="1" (
    echo Backend ready.
) else (
    echo WARNING: Backend did not become ready within 20 s.
)

:: ── 6. Start frontend in a new console window ────────────────────────────────
echo Starting frontend (http://localhost:5173) ...
start "Classifier-Frontend" /d "%ROOT%\frontend" cmd /k npm run dev

:: ── 7. Summary ────────────────────────────────────────────────────────────────
echo.
echo   Frontend : http://localhost:5173
echo   Backend  : http://localhost:8080
echo   To stop  : close the "Classifier-Backend" and "Classifier-Frontend" windows.
echo.

goto :eof

:: ── :KillPort <port> ──────────────────────────────────────────────────────────
:KillPort
for /f "tokens=5" %%P in ('netstat -ano 2^>nul ^| findstr ":%~1 " ^| findstr "LISTENING"') do (
    if not "%%P"=="" (
        taskkill /PID %%P /F >nul 2>&1
        if not errorlevel 1 echo Killed process on port %~1 (PID %%P)
    )
)
exit /b 0
