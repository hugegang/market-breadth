@echo off
echo 正在启动市场宽度分析器...
echo.

:: 检查是否已安装所需包
python -c "import streamlit" 2>NUL
if errorlevel 1 (
    echo 正在安装必要的包...
    pip install -r requirements.txt
    echo 包安装完成！
    echo.
)

:: 启动应用
echo 启动应用...
streamlit run src/app.py

pause
