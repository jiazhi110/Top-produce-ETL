# utils/config_loader.py
import yaml

def load_config(env: str):
    config_path = f'config/config_{env}.yaml'
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)
