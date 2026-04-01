import os
import sys
import yaml
import boto3
import logging
from datetime import datetime
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
        process_date = _validate_process_date(self.args.get('process_date'))
        is_backfill = _is_truthy(self.args.get('is_backfill', 'false'))

        if process_date:
            self.partition_filter = f"dt == '{process_date}'"
            if is_backfill:
                self.bookmark_node = f"{self.args.get('JOB_NAME', 'job')}_backfill_{process_date}"
                logger.warning(f"!!! BACKFILL MODE ENABLED for date: {process_date} !!!")
            else:
                self.bookmark_node = f"{self.args.get('JOB_NAME', 'job')}_daily_v1"
                logger.info(f">>> INCREMENTAL MODE: Processing partition dt={process_date}")
        else:
            self.partition_filter = None
            self.bookmark_node = f"{self.args.get('JOB_NAME', 'job')}_daily_v1"
            logger.info(">>> INCREMENTAL MODE: Reading new files based on bookmark.")

    def _resolve_args(self):
        if self.is_glue:
            # Glue requires only the stable core arguments here; optional job
            # control arguments are parsed separately so daily runs and manual
            # backfills can share the same script entrypoint.
            args = getResolvedOptions(sys.argv, ['JOB_NAME', 'config_path'])
            args.update(self._parse_optional_args(sys.argv[1:]))
            return args
        else:
            import argparse
            parser = argparse.ArgumentParser()
            parser.add_argument('--job', default='top-produce-etl')
            parser.add_argument('--ven', default='dev')
            parser.add_argument('--process_date', default=None)
            parser.add_argument('--is_backfill', default='false')
            l_args = parser.parse_args()
            return {
                'JOB_NAME': l_args.job,
                'config_path': l_args.ven,
                'process_date': l_args.process_date,
                'is_backfill': l_args.is_backfill
            }

    def _parse_optional_args(self, argv):
        parsed = {
            'process_date': None,
            'is_backfill': 'false'
        }
        i = 0
        while i < len(argv):
            token = argv[i]
            if token.startswith('--process_date='):
                parsed['process_date'] = token.split('=', 1)[1]
            elif token == '--process_date' and i + 1 < len(argv):
                parsed['process_date'] = argv[i + 1]
                i += 1
            elif token.startswith('--is_backfill='):
                parsed['is_backfill'] = token.split('=', 1)[1]
            elif token == '--is_backfill' and i + 1 < len(argv):
                parsed['is_backfill'] = argv[i + 1]
                i += 1
            i += 1
        return parsed

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


def _validate_process_date(raw_process_date):
    """Validate process_date and return it in YYYY-MM-DD format."""
    if not raw_process_date or raw_process_date == 'None':
        return None

    process_date = raw_process_date.strip()
    if not process_date:
        return None

    try:
        return datetime.strptime(process_date, "%Y-%m-%d").date().isoformat()
    except ValueError as exc:
        raise ValueError(
            f"Invalid process_date '{raw_process_date}'. Expected YYYY-MM-DD."
        ) from exc


def _is_truthy(value):
    return str(value).lower() in {"1", "true", "yes", "y"}