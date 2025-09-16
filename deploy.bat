@echo off
echo ================================
echo  Media Tracker Bot - Production
echo ================================

echo [1/3] Building production image...
docker build -t media_tracker_bot .
if %errorlevel% neq 0 (
    echo ERROR: Build failed
    pause
    exit /b 1
)

echo.
echo [2/3] Starting container...
docker-compose up -d
if %errorlevel% neq 0 (
    echo ERROR: Start failed  
    pause
    exit /b 1
)

echo.
echo [3/3] Waiting for app to be ready...
timeout /t 15 /nobreak >nul

curl -s http://localhost:8000/health >nul 2>&1
if %errorlevel% equ 0 (
    echo ✅ SUCCESS! Application is running
    echo.
    echo URL: http://localhost:8000
    echo Login: admin/123456
    echo.
    echo Commands:
    echo - View logs: docker-compose logs -f
    echo - Stop app: docker-compose down
    echo.
    start http://localhost:8000
) else (
    echo ⚠️ Application starting, check logs:
    echo docker-compose logs media_tracker_bot
)

pause