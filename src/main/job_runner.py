import sys
import argparse

# --- Import helpers ---
from src.utils.spark_helper import (
    detect_environment,
    load_config_from_s3,
    load_config_from_local_file,
    create_spark_session,
    create_glue_context
)
from src.utils.logger import setup_logging

# Try to import Glue libraries; if not available, we are likely in a local environment.
try:
    from awsglue.utils import getResolvedOptions
    from awsglue.job import Job
except ImportError:
    # Define dummy placeholders or handle gracefully if strictly needed
    getResolvedOptions = None
    Job = None

# --- Import business logic ---
from src.transform import clean_data
from src.writers import write_to_parquet

def parse_local_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--job', required=True, help="need job name to run")
    parser.add_argument('--ven', default="dev", help="environment :local or dev or prod")

    return parser.parse_args()

def main():
    # Initialize logger
    logger = setup_logging()
    is_glue_env = detect_environment()

    if is_glue_env:
        # --- GLUE Environment ---
        logger.info("Running in AWS Glue environment.")
        try:
            # Since this job script is dedicated to one ETL task, we default it internally.
            args = getResolvedOptions(sys.argv, ['config_path'])
            job_param = 'top-produce-etl' # Default job name
            configs = load_config_from_s3(args['config_path'])
        except Exception as e:
            logger.error(f"Failed to parse Glue arguments: {e}")
            logger.error(f"sys.argv: {sys.argv}")
            sys.exit(1)
            
        glue_context, spark = create_glue_context()
        # Initialize Glue Job
        job = Job(glue_context)
        job.init(job_param, args)
    else:
        # --- Local Environment ---
        logger.info("Running in local environment.")
        local_args = parse_local_args()
        job_param = local_args.job
        configs = load_config_from_local_file(local_args.ven)
        spark = create_spark_session(app_name=f"Local_{job_param}")

    # --- Common ETL Logic ---
    try:
        logger.info(f"Executing logic for job: '{job_param}'")
        logger.info(f"Executing logic for configs: '{configs}'")
        if job_param == 'top-produce-etl':
            result_df = clean_data.run(spark, configs)
            write_to_parquet.write_df_to_s3(result_df, configs['output']['path'])
        else:
            raise ValueError(f"Job logic for '{job_param}' not found!")
        
        logger.info(f"Job '{job_param}' completed successfully.")
        
        # Commit Glue job if applicable
        if is_glue_env:
            job.commit()
        else:
            # Keep Spark UI alive for local debugging
            print("\n" + "="*50)
            print("Job finished! Spark UI is available at http://localhost:4040")
            print("Press ENTER to exit...")
            print("="*50 + "\n")
            input()
            
    except Exception as e:
        logger.error(f"Job '{job_param}' failed: {e}", exc_info=True)
        if is_glue_env:
            job.commit()  # Ensure logs are written even on failure
        sys.exit(1)

if __name__ == '__main__':
    main()