# file: load_csv_to_mariadb_module.py
# Purpose: Make load_csv_to_mariadb importable from other scripts
# Used by: 19_04_sqs_triggered_loader.py, Project 27, Project 28

# Re-export the core function from 19_02
# In production: this would be a proper Python package

import sys
import os

# Add current directory to path for import
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from importlib import import_module

# Import the load function
# In production: from quickcart_etl.loaders import load_csv_to_mariadb
_loader_module = import_module("19_02_load_csv_to_mariadb")
load_csv_to_mariadb = _loader_module.load_csv_to_mariadb
load_all_daily_files = _loader_module.load_all_daily_files