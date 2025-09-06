#!/bin/bash

# API Test Script
# Tests all RESTful API endpoints

# 服务器地址和端口
SERVER="http://localhost:8084"

# 测试设备ID
DEVICE_ID="root.ln.dev6"

# CSV文件路径
CSV_FILE="data/sample_data.csv"

echo "Starting API tests..."

# 测试1: CSV导入功能
echo "Test 1: CSV Import Functionality"
response=$(curl -s -X POST "${SERVER}/import?csvFile=${CSV_FILE}&deviceId=${DEVICE_ID}")
echo "Response: $response"
if [[ $response == *"CSV import successful"* ]]; then
    echo "✓ CSV Import test passed"
else
    echo "✗ CSV Import test failed"
fi
echo ""

# 测试2: 统计计算功能
echo "Test 2: Statistical Calculation Functionality"
response=$(curl -s -X GET "${SERVER}/statistic?deviceId=${DEVICE_ID}")
if [[ $response == *"Cnt:"* ]] && [[ $response == *"Sum:"* ]] && [[ $response == *"Average:"* ]]; then
    echo "✓ Statistical Calculation test passed"
else
    echo "✗ Statistical Calculation test failed"
fi
echo ""

# 测试3: 相关性计算功能
echo "Test 3: Correlation Calculation Functionality"
response=$(curl -s -X GET "${SERVER}/correlation?deviceId=${DEVICE_ID}")
if [[ $response == *"1.0000"* ]]; then
    echo "✓ Correlation Calculation test passed"
else
    echo "✗ Correlation Calculation test failed"
fi
echo ""

# 测试4: 统计图表生成功能
echo "Test 4: Statistical Chart Generation Functionality"
response=$(curl -s -X GET "${SERVER}/graph?deviceId=${DEVICE_ID}")
if [[ $response == *"Generated graph"* ]]; then
    echo "✓ Statistical Chart Generation test passed"
else
    echo "✗ Statistical Chart Generation test failed"
fi
echo ""

# 测试5: 条件分析功能
echo "Test 5: Condition Analysis Functionality"
response=$(curl -s -X GET "${SERVER}/condition?deviceId=${DEVICE_ID}")
if [[ $response == *"Engine Condition Analysis"* ]] && [[ $response == *"Condition 1:"* ]] && [[ $response == *"Condition 0:"* ]]; then
    echo "✓ Condition Analysis test passed"
else
    echo "✗ Condition Analysis test failed"
fi
echo ""

echo "API tests completed!"
