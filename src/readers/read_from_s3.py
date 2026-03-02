from pyspark.sql import SparkSession, DataFrame
import logging

logger = logging.getLogger(__name__)

def read_s3_parquet(spark: SparkSession, s3_path: str) -> DataFrame:
    logger.info(f"Reading Parquet data from S3 path: {s3_path}")
    return spark.read.parquet(s3_path)

def read_s3_parquet_with_bookmark(glue_context, s3_path: str, bookmark_node_name: str, partition_filter: str = None) -> DataFrame:
    """
    Reads Parquet data from S3 using Glue DynamicFrame to support Job Bookmarks.
    :param glue_context: GlueContext object
    :param s3_path: S3 path (base path)
    :param bookmark_node_name: Unique identifier for the bookmark state
    :param partition_filter: SQL-like string for partition pruning, e.g., "date == '2024-05-20'"
    """
    logger.info(f"Reading Parquet data incrementally from S3: {s3_path} | Filter: {partition_filter}")
    
    dynamic_frame = glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={
            "paths": [s3_path],
            "recurse": True,
            "groupFiles": "inPartition",
        },
        format="parquet",
        transformation_ctx=bookmark_node_name,
        push_down_predicate=partition_filter
    )
    
    return dynamic_frame.toDF()

def read_s3_csv(spark: SparkSession, s3_path: str, header=True, inferSchema=True, schema=None) -> DataFrame:
    logger.info(f"Reading CSV data from S3 path: {s3_path}")
    
    # Use explicit schema if provided (Best Practice)
    if schema:
        return spark.read.csv(
            s3_path, 
            header=header, 
            schema=schema,
            mode="PERMISSIVE",
            columnNameOfCorruptRecord="_corrupt_record"
        )
    else:
        return spark.read.csv(s3_path, header=header, inferSchema=inferSchema)