-- Glue Notebook

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract

# Initialize a Spark Session
spark = SparkSession.builder.appName('openaq_processing').getOrCreate()

# Read from S3 bucket
data_location = "s3://myrawbucket-785255668313/openaq/air_quality_data.json"
df = spark.read.json(data_location)

# Drop the original 'city' column and rename 'location' to 'city'
df = df.drop("city")
df = df.withColumnRenamed("location", "city")

# Filter out cities with names that seem coded (like MK0036A)
# This will ensure that if there's a sequence of a letter followed by a number anywhere in the city name, it's excluded.
df = df.filter(~col("city").rlike("[A-Z]+[0-9]+"))

# Show the schema and sample data
df.printSchema()
df.show(5, truncate=False)

# Filtering: Only data where 'value' is greater than 10 and the parameter is 'pm25'
filtered_df = df.filter((df.value > 10) & (df.parameter == 'pm25'))

# Show the filtered data
filtered_df.show(5, truncate=False)

# Transformation: Adding a new column "quality_label" based on 'value'
def quality_label(value):
    if value < 35:
        return 'Good'
    elif value < 75:
        return 'Moderate'
    else:
        return 'Unhealthy'

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

quality_label_udf = udf(quality_label, StringType())
transformed_df = filtered_df.withColumn("quality_label", quality_label_udf(filtered_df["value"]))

# Show transformed data
transformed_df.show(5, truncate=False)

# More data exploration: Count of data points per country
country_counts = df.groupBy("country").count().orderBy(col("count").desc())
country_counts.show()

# More data exploration: Average value per city
city_avg = df.groupBy("city").agg({"value": "avg"}).orderBy(col("avg(value)").desc())
city_avg.show()

# At the end of the exploration and transformation, you can choose to write the data back to S3 if needed.
# For demonstration purposes, I've commented out the write step below. You can uncomment it to save the data.
output_location = "s3://processed-bucket-785255668313/openaq/processed_data/"
transformed_df.write.mode("overwrite").parquet(output_location)

