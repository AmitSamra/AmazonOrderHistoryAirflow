import airflow
from airflow import DAG
from airflow.operators.bash_operator import Bash_Operator 
from airflow.operators.python_operator import PythonOperator 
from datetime import timedelta, datetime
import os
import csv
import numpy as np 
import pandas as pd
from dotenv import load_dotenv

dotenv_local_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path=dotenv_local_path, verbose=True)

