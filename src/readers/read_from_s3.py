from pyspark.sql import SparkSession, DataFrame
import logging

logger = logging.getLogger(__name__)

def read_s3_parquet(spark: SparkSession, s3_path: str) -> DataFrame:
    logger.info(f"Reading Parquet data from S3 path: {s3_path}")
    return spark.read.parquet(s3_path)

def read_s3_csv(spark: SparkSession, s3_path: str, header=True, inferSchema=True, schema=None) -> DataFrame:
    logger.info(f"Reading CSV data from S3 path: {s3_path}")
    
    # Use explicit schema if provided (Best Practice)
    if schema:
        return spark.read.csv(s3_path, header=header, schema=schema)
    else:
        return spark.read.csv(s3_path, header=header, inferSchema=inferSchema)