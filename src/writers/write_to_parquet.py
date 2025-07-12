from pyspark.sql import SparkSession,DataFrame
import logging

logging.getLogger(__name__)

def write_df_to_s3(df: DataFrame, s3_path: str, mode="overwrite", partition_cols: list = None):
    logging.info(f"write parquet to s3 path:{s3_path}, mode: {mode}, partition_cols: {partition_cols}!")

    wirte = df.write.mode(mode)
    
    if partition_cols:
        wirte.partitionBy(partition_cols)
    return wirte.parquet(s3_path)