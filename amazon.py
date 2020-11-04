import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime
import os
import csv
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import numpy as np 
import pandas as pd
from dotenv import load_dotenv


dotenv_local_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path=dotenv_local_path, verbose=True)


default_args = {
	'owner':'amit',
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


def etl_csv():

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

	# Remove $ and , from price columns.
	df['ListPricePerUnit'] = df['ListPricePerUnit'].str.replace('$','').str.replace(',','')
	df['PurchasePricePerUnit'] = df['PurchasePricePerUnit'].str.replace('$','').str.replace(',','')
	df['ItemSubtotal'] = df['ItemSubtotal'].str.replace('$','').str.replace(',','')
	df['Tax'] = df['Tax'].str.replace('$','').str.replace(',','')
	df['ItemTotal'] = df['ItemTotal'].str.replace('$','').str.replace(',','')

	# Convert price columns to float.
	df['ListPricePerUnit'] = df['ListPricePerUnit'].astype(float)
	df['PurchasePricePerUnit'] = df['PurchasePricePerUnit'].astype(float)
	df['ItemSubtotal'] = df['ItemSubtotal'].astype(float)
	df['Tax'] = df['Tax'].astype(float)
	df['ItemTotal'] = df['ItemTotal'].astype(float)

	# Drop rows with zero prices.
	df = df[df.ListPricePerUnit != 0]
	df = df[df.PurchasePricePerUnit != 0]
	df = df[df.ItemSubtotal != 0]
	df = df[df.ItemTotal != 0]

	# Extract year, month, & day and store them in columns in df_main
	df['OrderYear'] = df['OrderDate'].dt.year
	df['OrderMonth'] = df['OrderDate'].dt.month
	df['OrderDay'] = df['OrderDate'].dt.day
	df['OrderDayIndex'] = df['OrderDate'].dt.dayofweek
	df['OrderDayName'] = df['OrderDate'].dt.day_name()

	#df_main = df_main.drop(df_main[df_main['OrderYear'] == 2020].index)
	df = df.drop(df[df['OrderDate'].dt.year == 2020].index)

	# Combine carriers to eliminate repitition
	df['Carrier'] = df['Carrier'].replace('FEDEX', 'FedEx')
	df['Carrier'] = df['Carrier'].replace('SMARTPOST', 'FedEx SmartPost')
	df['Carrier'] = df['Carrier'].replace('Mail Innovations','UPS Mail Innovations')
	df['Carrier'] = df['Carrier'].replace('UPS MI','UPS Mail Innovations')
	df['Carrier'] = df['Carrier'].replace('US Postal Service','USPS')
	df['Carrier'] = df['Carrier'].replace('DHL Global Mail','DHL')
	df['Carrier'] = df['Carrier'].replace('US Postal Service','USPS')
	df['Carrier'] = df['Carrier'].replace('AMZN_US', 'AMZN')
	mail = ['USPS', 'UPS', 'UPS Mail Innovations', 'FedEx', 'FedEx SmartPost', 'DHL', 'AMZN']
	df.loc[~df.Carrier.isin(mail), 'Carrier'] = 'Other'

	# Reduce Sellers
	df.loc[~df.Seller.isin(['Amazon.com']), 'Seller'] = 'ThirdParty'
	df.loc[df.Seller.isin(['Amazon.com']), 'Seller'] = 'Amazon'

	# Final dataframe
	df

	# Connect to sql using sqlalchemy
	engine = create_engine('mysql+pymysql://' + os.environ.get("MYSQL_USER") + ":" + os.environ.get("MYSQL_PASSWORD") + '@localhost:3306/amazon')

	# Export df to sql using df.to_sql
	df.to_sql('purchases_airflow', con=engine, if_exists = 'replace', index=False)
	

t1 = PythonOperator(
	task_id = 'etl_amazon_purchases.csv',
	python_callable = etl_csv,
	provide_context = False,
	dag = dag
	)

t1
