import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Initialize Spark Session with AWS S3 config
spark = SparkSession.builder \
    .appName("Application Data Processing") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

# Load the cleaned data from S3 into a DataFrame
cleaned_file_path = "s3a://bdaminiprojectbucket/cleaned_application_data.csv"
print("Loading the cleaned data from S3...")
application_df = spark.read.csv(cleaned_file_path, header=True, inferSchema=True)
print("Cleaned data loaded successfully!")

# Register DataFrame as SQL table
application_df.createOrReplaceTempView("application_data")

# Query 1: Top Regions by Client Ratings
print("Running Query 1: Top Regions by Client Ratings...")
query_1_result = spark.sql("""
    SELECT REGION_RATING_CLIENT, COUNT(*) AS applicant_count
    FROM application_data
    GROUP BY REGION_RATING_CLIENT
    ORDER BY applicant_count DESC
""")
query_1_result.write.csv("TopRegionsByClientRatings.csv", header=True)
print("Query 1 completed. Results saved to 'TopRegionsByClientRatings.csv'.")

# Query 2: Income Trends Across Education Types
print("Running Query 2: Income Trends Across Education Types...")
query_2_result = spark.sql("""
    SELECT NAME_EDUCATION_TYPE, AVG(AMT_INCOME_TOTAL) AS avg_income
    FROM application_data
    GROUP BY NAME_EDUCATION_TYPE
    ORDER BY avg_income DESC
""")
query_2_result.write.csv("IncomeTrendsByEducation.csv", header=True)
print("Query 2 completed. Results saved to 'IncomeTrendsByEducation.csv'.")

# Query 3: Popular Occupation Types
print("Running Query 3: Popular Occupation Types...")
query_3_result = spark.sql("""
    SELECT OCCUPATION_TYPE, COUNT(*) AS applicant_count
    FROM application_data
    GROUP BY OCCUPATION_TYPE
    ORDER BY applicant_count DESC
    LIMIT 5
""")
query_3_result.write.csv("PopularOccupationTypes.csv", header=True)
print("Query 3 completed. Results saved to 'PopularOccupationTypes.csv'.")

# Query 4: Credit Amount Patterns by Housing Type
print("Running Query 4: Credit Amount Patterns by Housing Type...")
query_4_result = spark.sql("""
    SELECT NAME_HOUSING_TYPE, AVG(AMT_CREDIT) AS avg_credit
    FROM application_data
    GROUP BY NAME_HOUSING_TYPE
    ORDER BY avg_credit DESC
""")
query_4_result.write.csv("CreditPatternsByHousing.csv", header=True)
print("Query 4 completed. Results saved to 'CreditPatternsByHousing.csv'.")

# Query 5: Loan Trends Across Age Groups
print("Running Query 5: Loan Trends Across Age Groups...")
query_5_result = spark.sql("""
    SELECT AGE_GROUP, AVG(AMT_CREDIT) AS avg_credit, COUNT(*) AS applicant_count
    FROM application_data
    GROUP BY AGE_GROUP
    ORDER BY avg_credit DESC
""")
query_5_result.write.csv("LoanTrendsByAgeGroup.csv", header=True)
print("Query 5 completed. Results saved to 'LoanTrendsByAgeGroup.csv'.")

# Close Spark session
spark.stop()
print("Data analysis completed and Spark session stopped.")