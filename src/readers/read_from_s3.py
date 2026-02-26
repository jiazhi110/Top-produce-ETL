from pyspark.sql import SparkSession, DataFrame
import logging

logger = logging.getLogger(__name__)

def read_s3_parquet(spark: SparkSession, s3_path: str) -> DataFrame:
    logger.info(f"Reading Parquet data from S3 path: {s3_path}")
    return spark.read.parquet(s3_path)

def read_s3_parquet_with_bookmark(glue_context, s3_path: str, bookmark_node_name: str) -> DataFrame:
    """
    Reads Parquet data from S3 using Glue DynamicFrame to support Job Bookmarks.
    :param glue_context: GlueContext object
    :param s3_path: S3 path (base path)
    :param bookmark_node_name: Unique identifier for the bookmark state
    """
    logger.info(f"Reading Parquet data incrementally from S3: {s3_path} with ctx: {bookmark_node_name}")
    
    dynamic_frame = glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={
            "paths": [s3_path],
            "recurse": True,
            "groupFiles": "inPartition", # Optional: optimizes small files
        },
        format="parquet",
        transformation_ctx=bookmark_node_name
    )
    
    return dynamic_frame.toDF()

def read_s3_csv(spark: SparkSession, s3_path: str, header=True, inferSchema=True, schema=None) -> DataFrame:
    logger.info(f"Reading CSV data from S3 path: {s3_path}")
    
    # Use explicit schema if provided (Best Practice)
    if schema:
        return spark.read.csv(s3_path, header=header, schema=schema)
    else:
        return spark.read.csv(s3_path, header=header, inferSchema=inferSchema)