import os
import yaml
import boto3
from pyspark.sql import SparkSession

# --- Glue 相关包的惰性导入 ---
# 这样在非 Glue 环境（本地）下，即使没有安装 awsglue 库，代码也能正常运行
try:
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    IS_GLUE_AVAILABLE = True
except ImportError:
    GlueContext = None # 定义一个占位符
    IS_GLUE_AVAILABLE = False


# --- 环境检测 ---
def detect_environment():
    """
    判断当前脚本是否在 AWS Glue 环境中运行。
    这是数据工程师中最主流和标准的做法。
    """
    try:
        # 尝试导入一个只有在 AWS Glue 环境中才存在的模块
        from awsglue.utils import getResolvedOptions
        return True
    except ImportError:
        # 如果导入失败，说明不在 Glue 环境中
        return False


# --- 配置加载 ---
def load_config_from_s3(s3_path: str) -> dict:
    """从 S3 路径加载 YAML 配置文件"""
    s3 = boto3.client('s3')
    bucket, key = s3_path.replace("s3://", "").split("/", 1)
    response = s3.get_object(Bucket=bucket, Key=key)
    config_content = response['Body'].read().decode('utf-8')
    return yaml.safe_load(config_content)


def load_config_from_local_file(venv: str) -> dict:
    """从本地文件系统加载 YAML 配置文件"""
    # 使用绝对路径以避免相对路径问题
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(current_dir, f'../../config/config_{venv}.yaml')
    with open(config_path) as f:
        return yaml.safe_load(f)


# --- Spark 和 Glue 上下文创建 ---
def _get_project_root() -> str:
    """计算并返回项目的根目录路径。"""
    # __file__ -> spark_helper.py
    # .. -> utils/
    # ../.. -> src/
    # ../../.. -> project_root/
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def create_spark_session(app_name: str = "LocalETLJob") -> SparkSession:
    """
    为本地开发环境创建并配置 SparkSession，特别是 S3a 的访问。
    """
    project_root = _get_project_root()
    jars_folder = os.path.join(project_root, "jars")
    
    # 定义需要的 JAR 包
    required_jars = [
        os.path.join(jars_folder, "hadoop-aws-3.3.2.jar"),
        os.path.join(jars_folder, "aws-java-sdk-bundle-1.11.1026.jar"),
        # commons-configuration 通常作为 hadoop-aws 的传递依赖，但显式包含更可靠
    ]

    # 检查 JAR 包是否存在
    for jar_path in required_jars:
        if not os.path.exists(jar_path):
            # 使用 print 而不是 logger，因为 logger 可能还未初始化
            print(f"Warning: Required JAR not found at {jar_path}. Local S3 access may fail.")

    # 使用逗号连接所有 JAR 路径
    jars_string = ",".join(required_jars)

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        # --- Spark 性能和功能配置 ---
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        # --- S3a 连接配置 ---
        .config("spark.jars", jars_string)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # 凭证通常通过环境变量自动获取，这里显式配置作为备用
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    )
    
    spark = builder.getOrCreate()
    return spark


def create_glue_context():
    """
    在 AWS Glue 环境中创建 GlueContext 和 SparkSession。
    """
    if not IS_GLUE_AVAILABLE:
        raise ImportError("Cannot create GlueContext. The required 'awsglue' library is not available.")
    
    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    return glue_context, spark