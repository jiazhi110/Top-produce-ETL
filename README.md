# Top-produce-ETL

## 项目说明
本项目是一个基于Apache Spark的数据处理ETL管道，用于分析用户行为数据，计算各地区热门商品排名。

## 技术栈
- Apache Spark 3.3.x
- Python 3.9+
- AWS S3 (数据存储)
- AWS Glue (可选运行环境)

## 环境要求
Spark 版本       Hadoop 版本     推荐 hadoop-aws         推荐 aws-java-sdk-bundle
----------------------------------------------------------------------
Spark 3.0.x      Hadoop 3.2.x    hadoop-aws-3.2.0.jar    aws-java-sdk-bundle-1.11.1026.jar
Spark 3.1.x      Hadoop 3.2.x    hadoop-aws-3.2.0.jar    aws-java-sdk-bundle-1.11.1026.jar
Spark 3.3.x      Hadoop 3.3.1    hadoop-aws-3.3.1.jar    aws-java-sdk-bundle-1.11.1026.jar

## 项目结构
```
.
├── src/                 # Source code
│   ├── main/           # Entry points
│   ├── readers/        # Data readers
│   ├── transform/      # Data transformation logic
│   ├── writers/        # Data writers
│   └── utils/          # Utility functions
├── config/             # Configuration files
├── test/               # Unit tests
└── package_for_glue.sh # Packaging script for AWS Glue
```

## 运行方式

### 本地运行
```bash
python src/main/job_runner.py --job top-produce-etl --ven dev
```

### AWS Glue运行
1. 确保已安装并配置AWS CLI
2. 修改 `package_for_glue.sh` 中的S3存储桶名称
3. 运行打包脚本:

```bash
./package_for_glue.sh
```

4. 在AWS Glue控制台创建作业，使用脚本输出的路径作为设置

### Glue作业设置
- **Script S3 path**: 打包脚本输出的路径
- **Python library path**: 同脚本路径
- **Job parameters**: 
  - `--config_path`: S3中配置文件的路径

## 测试

### 运行测试
```bash
# 运行所有测试
pytest test/

# 运行特定测试文件
pytest test/test_clean_data.py

# 使用测试运行脚本
./test_runner.sh
```

### 测试内容
- 数据清洗逻辑验证
- Top3计算准确性测试
- 数据质量检查
- 空数据处理测试

## 配置文件
- `config/config_dev.yaml` - 开发环境配置
- `config/config_prod.yaml` - 生产环境配置

## 主要功能
1. 从S3读取用户行为数据、城市信息、商品信息
2. 清洗和转换数据，关联用户行为与商品、城市信息
3. 计算各地区商品点击量排名
4. 输出每个地区点击量前3的商品及其城市分布占比