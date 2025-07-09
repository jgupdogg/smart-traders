#!/usr/bin/env python3
"""
Setup script for Jupyter notebook environment
Run this in the first cell of your notebook to install required packages
"""

import subprocess
import sys

def install_packages():
    """Install required packages for database exploration"""
    packages = [
        'pandas==2.1.4',
        'numpy==1.24.3', 
        'sqlalchemy==2.0.23',
        'psycopg2-binary==2.9.9',
        'matplotlib==3.7.2',
        'seaborn==0.12.2',
        'plotly==5.17.0',
        'ipywidgets==8.1.1'
    ]
    
    for package in packages:
        print(f"Installing {package}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
    
    print("All packages installed successfully!")

if __name__ == "__main__":
    install_packages()