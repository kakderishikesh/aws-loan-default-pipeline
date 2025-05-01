import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, abs, when, avg, count, desc
from pyspark.sql.types import IntegerType
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Application Data Processing") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()


# Load CSV from S3
file_path = "s3a://bdaminiprojectbucket/application_data.csv"
application_df = spark.read.csv(file_path, header=True, inferSchema=True).repartition(2)
print("CSV file loaded into DataFrame")

# Convert negative day columns to positive
date_cols = ['DAYS_BIRTH', 'DAYS_EMPLOYED', 'DAYS_REGISTRATION', 'DAYS_ID_PUBLISH']
for col_name in date_cols:
    application_df = application_df.withColumn(col_name, abs(col(col_name)))
print("Converted negative days to positive for date columns.")

# Bin income
application_df = application_df.withColumn('AMT_INCOME_TOTAL', col('AMT_INCOME_TOTAL') / 100000)
income_bins = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
income_slots = ['0-100K', '100K-200K', '200k-300k', '300k-400k', '400k-500k',
                '500k-600k', '600k-700k', '700k-800k', '800k-900k', '900k-1M', '1M Above']
income_binned = when(col('AMT_INCOME_TOTAL').between(0, 1), '0-100K')
for i in range(1, len(income_bins) - 1):
    income_binned = income_binned.when(
        col('AMT_INCOME_TOTAL').between(income_bins[i], income_bins[i + 1]),
        income_slots[i]
    )
income_binned = income_binned.otherwise('1M Above')
application_df = application_df.withColumn('AMT_INCOME_RANGE', income_binned)
print("Binned income ranges into a categorical column.")

# Bin age
application_df = application_df.withColumn('AGE', (col('DAYS_BIRTH') / 365).cast(IntegerType()))
age_bins = [0, 20, 30, 40, 50, 100]
age_slots = ['0-20', '20-30', '30-40', '40-50', '50 above']
age_binned = when(col('AGE').between(0, 20), '0-20')
for i in range(1, len(age_bins) - 1):
    age_binned = age_binned.when(col('AGE').between(age_bins[i], age_bins[i + 1]), age_slots[i])
age_binned = age_binned.otherwise('50 above')
application_df = application_df.withColumn('AGE_GROUP', age_binned)
print("Binned age groups into a categorical column.")

# Bin employment years
application_df = application_df.withColumn('YEARS_EMPLOYED', (col('DAYS_EMPLOYED') / 365).cast(IntegerType()))
employment_bins = [0, 5, 10, 20, 30, 40, 50, 60, 150]
employment_slots = ['0-5', '5-10', '10-20', '20-30', '30-40', '40-50', '50-60', '60 above']
employment_binned = when(col('YEARS_EMPLOYED').between(0, 5), '0-5')
for i in range(1, len(employment_bins) - 1):
    employment_binned = employment_binned.when(
        col('YEARS_EMPLOYED').between(employment_bins[i], employment_bins[i + 1]),
        employment_slots[i]
    )
employment_binned = employment_binned.otherwise('60 above')
application_df = application_df.withColumn('EMPLOYMENT_YEAR', employment_binned)
print("Binned employment duration into a categorical column.")

# Handle missing values
application_df = application_df.fillna({'NAME_TYPE_SUITE': 'Unknown'})
application_df = application_df.fillna({'OCCUPATION_TYPE': 'Unknown'})
print("Imputed missing values in categorical columns.")

# Impute numeric columns with median
amount_cols = [
    'AMT_REQ_CREDIT_BUREAU_HOUR', 'AMT_REQ_CREDIT_BUREAU_DAY',
    'AMT_REQ_CREDIT_BUREAU_WEEK', 'AMT_REQ_CREDIT_BUREAU_MON',
    'AMT_REQ_CREDIT_BUREAU_QRT', 'AMT_REQ_CREDIT_BUREAU_YEAR'
]
for col_name in amount_cols:
    median_value = application_df.approxQuantile(col_name, [0.5], 0)[0]
    application_df = application_df.fillna({col_name: median_value})
print("Imputed null values with median for specific numeric columns.")

# Save cleaned file
cleaned_file_path = "./cleaned_application_data.csv"
application_df.write.csv(cleaned_file_path, header=True)
print("Cleaned DataFrame saved locally.")

# Aggregated metrics
metric1 = application_df.select("DAYS_EMPLOYED").orderBy(desc("DAYS_EMPLOYED")).limit(3)
metric1.write.csv("./AggregatedMetric-1.csv", header=True)

metric2 = application_df.groupBy("NAME_EDUCATION_TYPE").agg(avg("AMT_INCOME_TOTAL").alias("Average_Income"))
metric2.write.csv("./AggregatedMetric-2.csv", header=True)

metric3 = application_df.groupBy("NAME_FAMILY_STATUS").agg(count("*").alias("Applicant_Count"))
metric3.write.csv("./AggregatedMetric-3.csv", header=True)

metric4 = application_df.groupBy("NAME_HOUSING_TYPE").agg(avg("AMT_CREDIT").alias("Average_Credit"))
metric4.write.csv("./AggregatedMetric-4.csv", header=True)

metric5 = application_df.groupBy("OCCUPATION_TYPE") \
    .agg(count("*").alias("Applicant_Count")) \
    .orderBy(desc("Applicant_Count")).limit(3)
metric5.write.csv("./AggregatedMetric-5.csv", header=True)

print("All metrics calculated and saved.")
spark.stop()
print("Spark session stopped.")