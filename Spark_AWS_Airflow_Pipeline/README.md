# AWS
Tasks to schedule:
![Screen Shot 2021-12-09 at 1 26 49 AM](https://user-images.githubusercontent.com/57310445/145369721-4f7d8746-b29a-40b8-b2cb-9278d2a6504b.png)

Current limitations:
1. Although the dataset was 19gb (~89770641 rows), AWS EMR clusters are more suited for larger data processes in the 1tb range. AWS Athena would have been a better choice here
2. AWS EMR/Athena or Hive Thriftserver could have been directly connected to Tableau but since I only have access to Tableau Public (which can connect only to local files), a step had to be created to download files from S3
3. Beginning to find that Tableau Public does not allow workbook extract refreshed through API and hence the Tableau refresh step might become obsolete (I'm stuck :'( )
