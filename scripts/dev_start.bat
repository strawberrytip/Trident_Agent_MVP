@echo off
REM ============================================================
REM Trident Agent MVP - Windows 一键开发启动
REM 开三个窗口: engine / api_server / frontend
REM ============================================================

REM 进入项目根目录（本脚本在 scripts\ 下，取上一级）
cd /d "%~dp0.."

REM 检查 backend\.env（密钥文件，不存在则提示并退出）
if not exist "backend\.env" (
    echo [错误] 未找到 backend\.env
    echo.
    echo 请先复制环境变量模板:
    echo     copy .env.example backend\.env
    echo 然后编辑 backend\.env 填入你的 API 密钥
    echo.
    pause
    exit /b 1
)

REM 检查 Python
python --version >nul 2>&1
if errorlevel 1 (
    echo [错误] 未找到 Python，请先安装 Python 3.11+
    pause
    exit /b 1
)

REM 检查 Node.js
npm --version >nul 2>&1
if errorlevel 1 (
    echo [错误] 未找到 npm，请先安装 Node.js
    pause
    exit /b 1
)

echo 启动 Trident 开发环境（三个新窗口）...
echo   engine      - 新闻引擎: WebSocket 采集 + AI worker + 前向验证
echo   api_server  - FastAPI + SSE: http://127.0.0.1:8000
echo   frontend    - Next.js:     http://localhost:3000
echo.

start "Trident Engine" cmd /k "cd /d %CD%\backend\src_python && set PYTHONIOENCODING=utf-8 && python engine.py"
start "Trident API" cmd /k "cd /d %CD%\backend\src_python && set PYTHONIOENCODING=utf-8 && uvicorn api_server:app --host 127.0.0.1 --port 8000"
start "Trident Frontend" cmd /k "cd /d %CD%\frontend && npm run dev"

echo 三个窗口已启动，关闭对应窗口即可停止该服务。
pause
