@echo off

cd /d "%~dp0\.."

set "VENV_DIR=%CD%\venv" REM Directorio del entorno virtual
set "ACT_DIR=%CD%"   REM Directorio de la aplicación
echo %ACT_DIR% >> "%LOG_FILE%"

set "LOG_FILE=%ACT_DIR%\log\logs_main.log" REM Ubicacion LOG_FILE

(echo %TIME% %DATE%: Starting execution) >> "%LOG_FILE%"

set "start_time=%TIME: =0%"
(echo Start time: %start_time%)>> "%LOG_FILE%"

echo Activating virtual environment... >> "%LOG_FILE%"
call "%VENV_DIR%\Scripts\activate"  REM Activar el entorno virtual

echo Running Python script... >> "%LOG_FILE%"
python "%ACT_DIR%\main.py" 

echo Deactivating virtual environment... >> "%LOG_FILE%"
call "%VENV_DIR%\Scripts\deactivate"  REM Desactivar el entorno virtual

echo Process executed >> "%LOG_FILE%"

set "end_time=%TIME: =0%"
(echo End time: %end_time%)>> "%LOG_FILE%"

for /F "tokens=1-3 delims=:." %%a in ("%start_time%") do (
    set /a "start_sec=(((1%%a*60)+1%%b)*60)+1%%c-366100"
)>> "%LOG_FILE%"

for /F "tokens=1-3 delims=:." %%a in ("%end_time%") do (
    set /a "end_sec=(((1%%a*60)+1%%b)*60)+1%%c-366100"
)>> "%LOG_FILE%"

set /a "runtime=end_sec-start_sec"

(echo Runtime: %runtime% seconds)>> "%LOG_FILE%"

echo/ >> "%LOG_FILE%"
echo/ >> "%LOG_FILE%"
