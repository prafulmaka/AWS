# Used only for testing Spark and AWS on local host - must add Hadoop AWS jar and AWS Java SDK jar
# Local mode is not designed for performance



import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
import pyspark
import sys
from pyspark.sql.functions import split
from pyspark import SparkContext, SparkConf
import boto3

spark = SparkSession.builder.appName('Project').getOrCreate()

spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "<Your Access Key>")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "<Your Secret Key>")
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")

data = spark.read.csv("s3a://<S3 Path>", header=True)

data.show(10)

# Function - Split on Date
split_col = pyspark.sql.functions.split(data["purchase_time"], ' ')

# Create additional columns
data = data.withColumn('date', split_col.getItem(0))
data = data.withColumn('time', split_col.getItem(1))
data = data.withColumn('day_of_week', dayofweek('date'))
data = data.withColumn('hour_of_day', hour('time'))
data = data.withColumn('day_of_week_string', pyspark.sql.functions.when(col('day_of_week') == 1, "Sunday")
                                                                  .when(col('day_of_week') == 2, "Monday")
                                                                  .when(col('day_of_week') == 3, "Tuesday")
                                                                  .when(col('day_of_week') == 4, "Wednesday")
                                                                  .when(col('day_of_week') == 5, "Thursday")
                                                                  .when(col('day_of_week') == 6, "Friday")
                                                                  .when(col('day_of_week') == 7, "Saturday")
                                                                  .otherwise("N/A"))

# Replace values in Gender column
data = data.na.fill("not specified", ["gender"])
data = data.replace('o', 'not specified', ['gender'])

# # Column Null Values
# data.select([count(when(isnull(c), c)).alias(c) for c in data.columns]).show()

# Table 1a - Busy Times
table1 = data.groupBy("hour_of_day").count().orderBy(col("count").desc())

# Final Table - Table 1a
table1.createOrReplaceTempView("Table1")

spark.sql("CREATE TABLE table1 AS SELECT * FROM TABLE1")

# Table 2a - Popular Company (Listed)
table2 = data.groupBy("final_company").count().orderBy(col("count").desc())

# Final Table - Table 2a
table2.createOrReplaceTempView("Table2")

spark.sql("CREATE TABLE table2 AS SELECT * FROM Table2")

# Table 3a - Popular Brand Name
table3 = data.groupBy("brand_name").count().orderBy(col("count").desc())

# Final Table - Table 3a
table3.createOrReplaceTempView("Table3")

spark.sql("CREATE TABLE table3 AS SELECT * FROM Table3")

# Table 4a - Popular Retail Channel
table4 = data.groupBy("retailer_channel").count().orderBy(col("count").desc())

# Final Table - Table 4a
table4.createOrReplaceTempView("Table4")

spark.sql("CREATE TABLE table4 AS SELECT * FROM Table4")

# Table 5a - Primary Category
table5 = data.groupBy("primary_category").count().orderBy(col("count").desc())

# Final Table - Table 5a
table5.createOrReplaceTempView("Table5")

spark.sql("CREATE TABLE table5 AS SELECT * FROM Table5")

# Table 6a - Gender
table6 = data.groupBy("gender").count().orderBy(col("count").desc())

# Final Table - Table 6a
table6.createOrReplaceTempView("Table6")

spark.sql("CREATE TABLE table6 AS SELECT * FROM Table6")