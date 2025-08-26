import yaml
import argparse
import sys
import os

# Add src directory to Python path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from utils.logger import setup_logging
from utils.spark_helper import create_spark_session, create_glue_context, detect_environment
from transform import clean_data
from writers import write_to_parquet

def load_config(venv:str):
    config_path = f'../../config/config_{venv}.yaml'
    with open(config_path) as f:
        return yaml.safe_load(f)
    

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--job', required=True, help="need job name to run")
    parser.add_argument('--ven', default="dev", help="environment : dev or prod")

    return parser.parse_args()
    

# print(load_config("test"))
# print(parse_args())

def main():
    #get arguments of running environment
    pars = parse_args()
    
    #initialize log
    logger = setup_logging()
    logger.info(f"Starting job {pars.job} in {pars.ven} environment")

    #get all arguments through config
    configs = load_config(pars.ven)

    try:
        # ETL逻辑
        #creating sparksession
        if detect_environment():
            glue_context, spark = create_glue_context()
        else:
            spark = create_spark_session(pars.job, is_local=True)

        if pars.job == 'top-produce-etl':
            logger.info(configs)

            result_df = clean_data.run(spark, configs)

            write_to_parquet.write_df_to_s3(result_df, configs['output']['path'])
        else:
            logger.error(f"Job name {pars.job} not found!")
            sys.exit(1)
        
        logger.info(f"Job {pars.job} completed")
    except Exception as e:
        logger.error(f"Job {pars.job} failed: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main()