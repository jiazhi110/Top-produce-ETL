import os
import sys
import yaml
import boto3
import logging
from .spark_helper import (
    detect_environment, 
    create_spark_session, 
    load_config_from_s3, 
    load_config_from_local_file,
    create_glue_context
)

# Lazy Glue Imports
try:
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    IS_GLUE_AVAILABLE = True
except ImportError:
    IS_GLUE_AVAILABLE = False

logger = logging.getLogger(__name__)

class JobContextManager:
    """
    Manager to handle environment detection, argument parsing, 
    and context initialization for both AWS Glue and local Spark.
    """
    def __init__(self):
        self.is_glue = detect_environment()
        self.spark = None
        self.glue_context = None
        self.job = None
        self.configs = None
        
        # 1. Parse Arguments
        self.args = self._resolve_args()
        
        # 2. Decision: Incremental vs Backfill
        target_date = self.args.get('target_date')
        if target_date and target_date != 'None':
            self.partition_filter = f"date == '{target_date}'"
            self.bookmark_node = f"{self.args.get('JOB_NAME', 'job')}_backfill_{target_date}"
            logger.warning(f"!!! BACKFILL MODE ENABLED for date: {target_date} !!!")
        else:
            self.partition_filter = None
            self.bookmark_node = f"{self.args.get('JOB_NAME', 'job')}_daily_v1"
            logger.info(">>> INCREMENTAL MODE: Reading new files based on bookmark.")

    def _resolve_args(self):
        if self.is_glue:
            # Expected Glue arguments
            return getResolvedOptions(sys.argv, ['JOB_NAME', 'config_path', 'target_date'])
        else:
            import argparse
            parser = argparse.ArgumentParser()
            parser.add_argument('--job', default='top-produce-etl')
            parser.add_argument('--ven', default='dev')
            parser.add_argument('--target_date', default=None)
            l_args = parser.parse_args()
            return {
                'JOB_NAME': l_args.job,
                'config_path': l_args.ven,
                'target_date': l_args.target_date
            }

    def init_env(self):
        if self.is_glue:
            self.glue_context, self.spark = create_glue_context()
            self.job = Job(self.glue_context)
            self.job.init(self.args['JOB_NAME'], self.args)
            self.configs = load_config_from_s3(self.args['config_path'])
        else:
            self.spark = create_spark_session(f"Local_{self.args['JOB_NAME']}")
            self.configs = load_config_from_local_file(self.args['config_path'])
        
        return self.spark, self.glue_context, self.configs

    def commit(self):
        if self.is_glue and self.job:
            self.job.commit()
            logger.info("Glue Job Bookmark committed.")
        else:
            logger.info("Local run finished. No bookmark to commit.")
