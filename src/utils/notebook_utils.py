# notebook_utils.py
import sys
import os

def add_project_root_to_path(path:str):
    # 自动向上找“utils”文件夹所在路径并添加为根目录
    current = os.path.abspath(os.getcwd())
    while current != "/":
        if path in os.listdir(current):
            if current not in sys.path:
                sys.path.append(current)
            break
        current = os.path.dirname(current)
