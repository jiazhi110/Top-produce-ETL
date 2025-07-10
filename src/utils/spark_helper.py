# utils/spark_session.py

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
    创建 SparkSession，支持本地模式和 Hive 支持。
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
