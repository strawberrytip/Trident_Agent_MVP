#!/bin/bash
# Trident 复盘看板 - 快速启动脚本（兼容 Windows Git Bash）

echo "📊 Trident 量化复盘看板 - 启动中..."
echo ""

# 检测Python命令（兼容 python 和 python3）
PYTHON_CMD=""
if command -v python &> /dev/null; then
    PYTHON_CMD="python"
elif command -v python3 &> /dev/null; then
    PYTHON_CMD="python3"
elif command -v py &> /dev/null; then
    PYTHON_CMD="py"
else
    echo "❌ 未找到 Python，请先安装 Python"
    echo "   检测的命令: python, python3, py"
    exit 1
fi

echo "✅ 找到 Python: $PYTHON_CMD"
$PYTHON_CMD --version

# 进入dashboard目录
cd "$(dirname "$0")"

# 检查依赖
echo "📦 检查依赖..."

# 尝试安装依赖（使用国内镜像源解决代理问题）
install_success=false

# 方法1: 直接安装
$PYTHON_CMD -m pip install -q -r requirements.txt 2>/dev/null && install_success=true

# 方法2: 使用清华镜像源
if [ "$install_success" = false ]; then
    echo "⚠️  直接安装失败，尝试使用清华镜像源..."
    $PYTHON_CMD -m pip install -q -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple 2>/dev/null && install_success=true
fi

# 方法3: 使用阿里云镜像源
if [ "$install_success" = false ]; then
    echo "⚠️  清华镜像失败，尝试使用阿里云镜像源..."
    $PYTHON_CMD -m pip install -q -r requirements.txt -i https://mirrors.aliyun.com/pypi/simple/ 2>/dev/null && install_success=true
fi

if [ "$install_success" = false ]; then
    echo "❌ 依赖安装失败，请手动运行以下命令之一："
    echo "   python -m pip install -r requirements.txt"
    echo "   python -m pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple"
    echo ""
    echo "💡 或者临时禁用代理："
    echo "   export http_proxy= && export https_proxy= && ./start.sh"
    exit 1
fi

# 生成示例数据（如果不存在）
if [ ! -f "trident_signals_sample.xlsx" ]; then
    echo "📝 生成示例数据..."
    $PYTHON_CMD generate_sample_data.py
fi

# 启动Streamlit
echo ""
echo "🚀 启动看板..."
echo "📍 访问地址: http://127.0.0.1:8501"
echo "   按 Ctrl+C 停止服务"
echo ""
$PYTHON_CMD -m streamlit run app.py --server.port 8501 --server.address 127.0.0.1
