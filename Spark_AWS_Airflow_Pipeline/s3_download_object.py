# Tableau Public only accepts csv/json files locally
# Hence we would need to download S3 bucket objects to a local directory

import boto3

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
        s3.Object("emr-output-buck", item["Key"]).download_file("Table_{}.csv".format(count))
