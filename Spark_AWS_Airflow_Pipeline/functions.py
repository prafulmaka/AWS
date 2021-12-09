# Download files from S3
import boto3
from datetime import datetime
from datetime import timedelta

def s3_download():
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


# Boto3 - EMR Job Flow

# Run Job Flow

def emr_job():
	client = boto3.client("emr", region_name = 'us-west-1')
	response = client.run_job_flow(
	    Name='EMR_Cluster',
	    LogUri='s3://aws-logs-978005213629-us-west-1/',
	    ReleaseLabel='emr-6.4.0',
	    Instances={
	        'MasterInstanceType': 'm5.xlarge',
	        'SlaveInstanceType': 'c5.4xlarge',
	        'InstanceCount': 2,
	        'Ec2KeyName': 'mykeypair',
	        'KeepJobFlowAliveWhenNoSteps': False,
	    },
	    Steps=[
	        {
	            'Name': 'Spark Job',
	            'ActionOnFailure': 'TERMINATE_CLUSTER',
	            'HadoopJarStep': {
	                'Jar': 'command-runner.jar',
	                'Args': [
	                    'spark-submit', '--deploy-mode', 'client', 's3://emr-py/cpg_spark.py'
	                ]
	            }
	        },
	    ],
	    Applications=[
	        {
	            'Name': 'Spark'
	        },
	        {
	            'Name': 'Hadoop'
	        },
	        {
	            'Name': 'Hive'
	        },
	        {
	            'Name': 'Hue'
	        },
	        {
	            'Name': 'Pig'
	        }
	    ],
	    VisibleToAllUsers=True,
	    JobFlowRole='EC2-S3+Admin',
	    ServiceRole='EMR_DefaultRole',
	    EbsRootVolumeSize=50,
	    AutoTerminationPolicy={
	        'IdleTimeout': 3600
	    }
	)


	# Wait till cluster is setup and step is complete

	# Get New Cluster ID
	client = boto3.client("emr", region_name='us-west-1')
	response = client.list_clusters(CreatedAfter=datetime.now() - timedelta(minutes=2),
	                     ClusterStates=['STARTING','RUNNING','WAITING'])

	for item in response.get('Clusters', []):
	    new_cluster_id = item["Id"]
	    print("New Cluster ID is: ", new_cluster_id)

	waiter = client.get_waiter('cluster_terminated')
	waiter.wait(
	    ClusterId=new_cluster_id,
	    WaiterConfig={
	        'Delay': 300,
	        'MaxAttempts': 100
	    }
	)

def tableau_refresh():
	# Pending!
	print("This step is still pending")