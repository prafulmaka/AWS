# EMR >> S3 Download >> Refresh Tableau >> Email Notification

from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.operators.email import EmailOperator
import time
from datetime import datetime
import sys
sys.path.append("/Users/m_2013954/airflow/dags/scripts")
import functions

with DAG(
	dag_id='SparkEMR_S3_Tableau',
	schedule_interval=None,
	start_date=datetime(2021,12,1),
	catchup=False
) as dag:
	
	step1 = PythonOperator(
		task_id = 'start_emr_cluster',
		python_callable = functions.emr_job
		)

	step2 = PythonOperator(
		task_id = 's3_download',
		python_callable = functions.s3_download
		)

	step3 = PythonOperator(
		task_id = 'tableau_refresh',
		python_callable = functions.tableau_refresh
		)

	step4 = EmailOperator(
		task_id = 'send_email',
		to = ['praful.maka@gmail.com', 'prafulmaka10@gmail.com'],
		subject = 'Tableau Dashboard',
		html_content = """ <h1> Tableau dashboard has been refreshed! <h1> """
		)

step1 >> step2 >> step3 >> step4