import airflow
from airflow import DAG
from airflow.operators.bash_operator import Bash_Operator 
from airflow.operators.python_operator import PythonOperator 
from datetime import timedelta, datetime
import os
import csv
from sqlalchemy.sql import text
import numpy as np 
import pandas as pd
from dotenv import load_dotenv


dotenv_local_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path=dotenv_local_path, verbose=True)


default_args = {
	'owner':'amit'
	#'start_date': datetime(2020,11,4,0),
	'start_date': datetime.now(),
	'retries': 0,
	'retry_delay': timedelta(minutes=1),
}

dag = DAG(
	'amazon',
	default_args = default_args,
	description = 'amazon order history',
	#schedule_interval = timedelta(hours=1),
	catchup = False,
	max_active_runs = 1,
	)


def etl_csv(

	# Read csv
	df = pd.read_csv(os.path.abspath('amazon_purchases.csv'), parse_dates=['Order Date', 'Shipment Date'])

	# Rename columns to remove spaces.
	df.columns = df.columns.str.replace(' ', '')

	# Rename specific columns.
	df = df.rename(columns={'CarrierName&TrackingNumber':'Carrier', 'ItemSubtotalTax': 'Tax', 
		'ShipmentDate':'ShipDate'})

	# Drop Website column
	del df['Website']

	# Replace NaN
	df.Category.fillna('unknown', inplace = True)
	df.Condition.fillna('unknown', inplace = True)
	df.Carrier.fillna('unknown', inplace = True) 



	)

