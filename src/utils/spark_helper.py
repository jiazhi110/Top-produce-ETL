import os
from pyspark.sql import SparkSession

try:
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
except ImportError:
    # 非 Glue 环境不装 Glue 相关包
    GlueContext = None


def create_spark_session(app_name="ETL_Job", enable_hive=False, is_local=True):
    """
    创建 SparkSession，支持本地模式和 Hive 支持，并添加 S3 访问配置。
    如果在 AWS Glue 中运行，该函数自动跳过，因为会使用 create_glue_context。
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    )

    if is_local:
        builder = builder.master("local[*]")

        # --- 开始添加 S3a 相关的配置，本地环境需要这个配置。 ---
        # 1. 配置 JAR 包路径：指向你手动下载的 hadoop-aws 和 aws-java-sdk-bundle

        # 获取 spark_helper.py 文件所在的目录 (即 'utils' 目录)/mnt/e/Top-produce-ETL/src/utils
        current_file_dir = os.path.dirname(os.path.abspath(__file__))
        # 从 'utils' 目录回溯一级到 'src' 目录/mnt/e/Top-produce-ETL/src
        src_dir = os.path.abspath(os.path.join(current_file_dir, ".."))
        # 从 'src' 目录回溯一级到项目根目录/mnt/e/Top-produce-ETL
        project_root = os.path.abspath(os.path.join(src_dir, ".."))
        # 从项目根目录找到 'jars' 文件夹/mnt/e/Top-produce-ETL/jars
        jars_folder = os.path.join(project_root, "jars")

        hadoop_aws_jar = os.path.join(jars_folder, "hadoop-aws-3.3.2.jar")
        aws_sdk_bundle_jar = os.path.join(jars_folder, "aws-java-sdk-bundle-1.11.1026.jar")
        commons_configuration_jar = os.path.join(jars_folder, "commons-configuration2-2.1.1.jar")

        # 检查 JAR 包是否存在，避免路径错误导致崩溃
        if not os.path.exists(hadoop_aws_jar):
            print(f"Warning: {hadoop_aws_jar} not found. S3 access might fail. Please ensure the JAR is downloaded and path is correct.")
        if not os.path.exists(aws_sdk_bundle_jar):
            print(f"Warning: {aws_sdk_bundle_jar} not found. S3 access might fail. Please ensure the JAR is downloaded and path is correct.")
        if not os.path.exists(commons_configuration_jar):
            print(f"Warning: {commons_configuration_jar} not found. S3 access might fail. Please ensure the JAR is downloaded and path is correct.")

        # 使用 spark.jars 配置来添加本地 JAR 包
        builder = builder.config("spark.jars", f"{hadoop_aws_jar},{aws_sdk_bundle_jar},{commons_configuration_jar}")

        # 2. 配置 S3a 文件系统实现类
        builder = builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        # 3. 配置 S3 凭证 (可选，因为通常会从环境变量或 ~/.aws/credentials 中自动获取)
        builder = builder.config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", ""))
        builder = builder.config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", ""))
        builder = builder.config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        # --- 结束添加 S3a 相关的配置 ---

    if enable_hive:
        builder = builder.enableHiveSupport()

    spark = builder.getOrCreate()
    return spark


def create_glue_context():
    """
    创建 GlueContext，用于 AWS Glue Job 环境中。
    """
    if GlueContext is None:
        raise ImportError("GlueContext not available. Are you in AWS Glue?")
    
    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    return glue_context, spark


def detect_environment():
    """
    根据 AWS Glue 环境变量检测是否在 Glue 上运行。
    """
    return "AWS_EXECUTION_ENV" in os.environ and "glue" in os.environ["AWS_EXECUTION_ENV"].lower()
