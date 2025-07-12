import yaml
import argparse
import sys
import os

# 假设你项目根目录是 notebooks 的上上级目录
PROJECT_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))

print(PROJECT_ROOT)
print(sys.path)

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)
    print(sys.path)

#这个记得要删掉，Jupyter中因为没有这个python 的执行，所以只能加入sys.argv 参数去模仿。
# sys.argv = ['notebook.py', '--job', 'top-produce-etl', '--ven', 'dev']

from utils.logger import setup_logging
from utils.spark_helper import create_spark_session,create_glue_context,detect_environment
from transform import clean_data

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

    #creating sparksession
    if detect_environment():
        glue_context, spark = create_glue_context()
    else:
        spark = create_spark_session(pars.job, is_local=True)

    if pars.job == 'top-produce-etl':
        logger.info(configs['input']['city_path'])

        # spark.read.parquet(configs['input']['city_path'])

        pd = spark.read.csv(configs['input']['city_path'], header = True, inferSchema = True)

        pd.show()
        # clean_data.run(spark, configs)
    else:
        logger.error(f"Job name {pars.job} not found!")
        sys.exit(1)

    logger.info(f"Job {pars.job} completed")

if __name__ == '__main__':
    main()