# IoT数据分析平台前端

这是一个简洁的前端界面，用于与IoT数据分析后端服务进行交互。

## 功能特性

- CSV数据导入
- 统计计算
- 相关性分析
- 图表生成
- 条件分析

## 环境要求

- Python 3.6+
- Flask
- requests

## 安装步骤

1. 创建虚拟环境：
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   # 或
   venv\Scripts\activate     # Windows
   ```

2. 安装依赖：
   ```bash
   pip install -r requirements.txt
   ```

3. 运行应用：
   ```bash
   python app.py
   ```

4. 访问应用：
   打开浏览器访问 `http://localhost:5000`

## 使用说明

1. 在页面中输入设备ID（可选，默认为root.example.exampledev）
2. 对于数据导入：
   - 输入CSV文件路径
   - 点击"导入数据"按钮
3. 对于数据分析：
   - 点击相应的分析按钮（统计计算、相关性分析、生成图表、条件分析）
   - 查看返回的结果

## 注意事项

- 确保后端服务已在8084端口运行
- CSV文件路径需要是后端服务可以访问的路径