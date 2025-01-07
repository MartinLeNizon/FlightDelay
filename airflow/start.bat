@echo off
REM Check if Docker is running
tasklist /FI "IMAGENAME eq docker.exe" | find /I "docker.exe" >nul

if %ERRORLEVEL% NEQ 0 (
    echo Docker failed to start. Please start Docker manually.
    exit /b 1
)

REM Run 'docker compose up airflow-init'
echo Running 'docker compose up airflow-init'...
docker compose up airflow-init
if %ERRORLEVEL% EQU 0 (
    echo airflow-init succeeded. Starting 'docker compose up'...
    docker compose up

    if %ERRORLEVEL% EQU 0 (
        echo Done.
    ) else (
        echo Docker Compose Up failed. Please check the logs.
        exit /b 1
    )
) else (
    echo airflow-init failed. Please check the logs.
    exit /b 1
)
