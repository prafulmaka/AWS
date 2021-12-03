import pyspark
from pyspark.sql.functions import *
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext('local')
spark = SparkSession(sc)

# Read 
data = spark.read.option("delimiter", ",").csv("s3://data-apr/CPG0.csv", header=True)

# Print Schema
data.printSchema()

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


# Table 1a - Busy Times
table1 = data.groupBy("hour_of_day").count().orderBy(col("count").desc())

# Final Table - Table 1a
table1.coalesce(1).write.format('csv').option('header', 'true').mode('overwrite').save("s3://emr-output-buck/table1")

# Table 2a - Popular Company (Listed)
table2 = data.groupBy("final_company").count().orderBy(col("count").desc())

# Final Table - Table 2a
table2.coalesce(1).write.format('csv').option('header', 'true').mode('overwrite').save("s3://emr-output-buck/table2")

# Table 3a - Popular Brand Name
table3 = data.groupBy("brand_name").count().orderBy(col("count").desc())

# Final Table - Table 3a
table3.coalesce(1).write.format('csv').option('header', 'true').mode('overwrite').save("s3://emr-output-buck/table3")

# Table 4a - Popular Retail Channel
table4 = data.groupBy("retailer_channel").count().orderBy(col("count").desc())

# Final Table - Table 4a
table4.coalesce(1).write.format('csv').option('header', 'true').mode('overwrite').save("s3://emr-output-buck/table4")

# Table 5a - Primary Category
table5 = data.groupBy("primary_category").count().orderBy(col("count").desc())       

# Final Table - Table 5a
table5.coalesce(1).write.format('csv').option('header', 'true').mode('overwrite').save("s3://emr-output-buck/table5")

# Table 6a - Gender
table6 = data.groupBy("gender").count().orderBy(col("count").desc())

# Final Table - Table 6a
table6.coalesce(1).write.format('csv').option('header', 'true').mode('overwrite').save("s3://emr-output-buck/table6")

# End
