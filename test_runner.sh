#!/bin/bash
# test_runner.sh - ETL测试运行脚本

echo "开始运行ETL测试..."

# 检查是否安装了pytest
if ! command -v pytest &> /dev/null
then
    echo "pytest未安装，正在安装..."
    pip install pytest
fi

# 运行测试
echo "运行测试中..."
pytest test/test_top3_logic.py -v

echo "测试完成!"