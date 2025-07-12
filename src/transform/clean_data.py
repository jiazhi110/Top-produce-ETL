import sys
from pyspark.sql import SparkSession, DataFrame
import yaml
from readers import read_from_s3
import logging

#initialize logger
logging.getLogger(__name__)

def run(spark: SparkSession, configs: yaml):
    
    logging.info(f"clean data's configs: {configs}")

    #read data from s3
    city_df = read_from_s3.read_s3_csv(spark, configs['input']['city_path'])

    city_df.show()