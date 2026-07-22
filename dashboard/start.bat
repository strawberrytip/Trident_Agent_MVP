@echo off
REM Trident 复盘看板 - Windows 快速启动脚本

echo 📊 Trident 量化复盘看板 - 启动中...
echo.

REM 进入脚本所在目录
cd /d "%~dp0"

REM 检查Python
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ 未找到 Python，请先安装 Python
    pause
    exit /b 1
)

REM 检查依赖
echo 📦 检查依赖...
pip install -q -r requirements.txt
if errorlevel 1 (
    echo ❌ 依赖安装失败，请手动运行: pip install -r requirements.txt
    pause
    exit /b 1
)

REM 生成示例数据（如果不存在）
if not exist "trident_signals_sample.xlsx" (
    echo 📝 生成示例数据...
    python generate_sample_data.py
)

REM 启动Streamlit
echo.
echo 🚀 启动看板...
echo 📍 访问地址: http://127.0.0.1:8501
echo.
streamlit run app.py --server.port 8501 --server.address 127.0.0.1

pause
