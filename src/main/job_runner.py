import yaml

def load_config (venv:str):
    config_path = f'config/config_{venv}.yaml'
    with open(config_path) as f:
        return yaml.safe_load(f)
    
