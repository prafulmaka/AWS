import boto3
from datetime import datetime

# Used to setup EMR cluster using 1 Master and 2 Slaves

# Get all Clusters
client = boto3.client("emr", region_name='us-west-1')
response = client.list_clusters(CreatedAfter=datetime(2021, 11, 1),
                     CreatedBefore=datetime(2021, 12, 1),
                     ClusterStates=['STARTING','BOOTSTRAPPING','RUNNING','WAITING','TERMINATING','TERMINATED','TERMINATED_WITH_ERRORS'])

for item in response.get('Clusters', []):
    print(item)

# Run Job Flow
client = boto3.client("emr", region_name = 'us-west-1')
response = client.run_job_flow(
    Name='emr_setup_dec1',
    LogUri='s3://aws-logs-978005213629-us-west-1/',
    ReleaseLabel='emr-6.4.0',
    Instances={
        'MasterInstanceType': 'm5.xlarge',
        'SlaveInstanceType': 'c5.4xlarge',
        'InstanceCount': 3,
        'Ec2KeyName': 'mykeypair'
    },
    Applications=[
        {
            'Name': 'JupyterEnterpriseGateway'
        },
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
    JobFlowRole='EMR_EC2_DefaultRole',
    ServiceRole='EMR_DefaultRole',
    EbsRootVolumeSize=50,
    AutoTerminationPolicy={
        'IdleTimeout': 3600
    }
)


