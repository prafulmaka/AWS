# S3 DAG
import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
import time
from datetime import datetime
from pprint import pprint

with DAG(
	dag_id='S3_Dag',
	schedule_interval=None,
	start_date=datetime(2021,12,1),
	catchup=False
) as dag:
	
	def get_s3():
		client = boto3.client("s3")
		s3 = boto3.resource('s3')

		response = client.list_objects(
		    Bucket='emr-output-buck',
		)

		count = 0
		for item in response.get('Contents', []):
		    if not "SUCCESS" in item["Key"]:
		        print(item["Key"])
		        count += 1
		        # Download Object
		        s3.Object("emr-output-buck", item["Key"]).download_file("/Users/m_2013954/Desktop/S3_Files/Table_{}.csv".format(count))

		print("Download is Complete")

	run_this = PythonOperator(
		task_id = 's3_dag',
		python_callable = get_s3
		)