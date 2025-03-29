from hdfs import InsecureClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pandas as pd

# HDFS Client Setup
HDFS_NAMENODE = "hdfs://localhost:9000" 
HDFS_WEBHDFS = "http://localhost:9870" 
HDFS_BASE_PATH = "/bigdata_project"
client = InsecureClient(HDFS_WEBHDFS, user="kishan")

#Create Folder in HDFS
try:
    client.makedirs(HDFS_BASE_PATH)
    print("HDFS Folder Created:", HDFS_BASE_PATH)
except:
    print("HDFS Folder Already Exists:", HDFS_BASE_PATH)

# CSV Files Ka Local Path
local_files = {
    "transactions": "transactions.csv",
    "products": "products.csv",
    "users": "users.csv"
}

#Upload CSV File in HDFS
for name, file_path in local_files.items():
    hdfs_path = f"{HDFS_BASE_PATH}/{file_path}"
    with open(file_path, "rb") as local_file:
        client.write(hdfs_path, local_file, overwrite=True)
    print(f"Uploaded {file_path} to HDFS: {hdfs_path}")

#Spark Session Start
spark = SparkSession.builder \
    .appName("BigDataPipeline") \
    .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE) \
    .getOrCreate()

#Read Data from HDFS using Spark
dfs = {}
for name, file_path in local_files.items():
    hdfs_path = f"{HDFS_NAMENODE}{HDFS_BASE_PATH}/{file_path}"  # Correct Path
    dfs[name] = spark.read.option("header", "true").option("inferSchema", "true").csv(hdfs_path)
    print(f"Loaded {name} DataFrame from HDFS")

#Data Cleaning & Preprocessing
for name in dfs:
    dfs[name] = dfs[name].dropna().dropDuplicates()

# Convert Data Types
if "transactions" in dfs:
    dfs["transactions"] = dfs["transactions"].withColumn("Amount", col("Amount").cast("double"))

#Spark SQL Register
dfs["transactions"].createOrReplaceTempView("transactions")
dfs["products"].createOrReplaceTempView("products")
dfs["users"].createOrReplaceTempView("users")

#Exploratory Data Analysis (EDA)
# Top-selling Products
query1 = """
SELECT p.Name, COUNT(*) as Sales
FROM transactions t
JOIN products p ON t.ProductID = p.ProductID
GROUP BY p.Name
ORDER BY Sales DESC
LIMIT 10
"""
spark.sql(query1).show()

# Total Revenue per User
query2 = """
SELECT u.UserID, u.Name, SUM(t.Amount) as TotalSpent
FROM transactions t
JOIN users u ON t.UserID = u.UserID
GROUP BY u.UserID, u.Name
ORDER BY TotalSpent DESC
LIMIT 10
"""
spark.sql(query2).show()

#Write Processed Data in HDFS
dfs["transactions"].write.mode("overwrite").parquet("/bigdata_project/cleaned_transactions.parquet")
dfs["users"].write.mode("overwrite").parquet("/bigdata_project/cleaned_users.parquet")
dfs["products"].write.mode("overwrite").parquet("/bigdata_project/cleaned_products.parquet")

# Data Export for Pandas (Optional)
pandas_df = dfs["transactions"].toPandas()
pandas_df.to_csv("cleaned_transactions.csv", index=False)

#Spark Session Stop
spark.stop()
print("Big Data Pipeline Execution Completed")
