from hdfs import InsecureClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
import pandas as pd

# HDFS Client Setup
HDFS_NAMENODE = "hdfs://localhost:9000" 
HDFS_WEBHDFS = "http://localhost:9870" 
HDFS_BASE_PATH = "/bigdata_project"
client = InsecureClient(HDFS_WEBHDFS, user="kishan")

#HDFS Folder Create
try:
    client.makedirs(HDFS_BASE_PATH)
    print("HDFS Folder Created:", HDFS_BASE_PATH)
except:
    print("HDFS Folder Already Exists:", HDFS_BASE_PATH)

#CSV Files Local Paths
local_files = {
    "transactions": "transactions.csv",
    "products": "products.csv",
    "users": "users.csv"
}

#Upload CSVs to HDFS
for name, file_path in local_files.items():
    hdfs_path = f"{HDFS_BASE_PATH}/{file_path}"
    with open(file_path, "rb") as local_file:
        client.write(hdfs_path, local_file, overwrite=True)
    print(f"Uploaded {file_path} to HDFS: {hdfs_path}")

#Start Spark Session
spark = SparkSession.builder \
    .appName("BigDataPipeline") \
    .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE) \
    .getOrCreate()

# Read Data from HDFS
dfs = {}
for name, file_path in local_files.items():
    hdfs_path = f"{HDFS_NAMENODE}{HDFS_BASE_PATH}/{file_path}"
    dfs[name] = spark.read.option("header", "true").option("inferSchema", "true").csv(hdfs_path)
    print(f"Loaded {name} DataFrame from HDFS")

#Data Cleaning & Preprocessing
for name in dfs:
    dfs[name] = dfs[name].dropna().dropDuplicates()

#Convert Data Types
dfs["transactions"] = dfs["transactions"].withColumn("Amount", col("Amount").cast("double"))

#Register DataFrames as SQL Tables
dfs["transactions"].createOrReplaceTempView("transactions")
dfs["products"].createOrReplaceTempView("products")
dfs["users"].createOrReplaceTempView("users")

#Useful Queries for Power BI
queries = {
    "top_selling_products": """
        SELECT p.Name AS ProductName, COUNT(*) AS Sales, 'top_selling_products' AS QueryType
        FROM transactions t
        JOIN products p ON t.ProductID = p.ProductID
        GROUP BY p.Name
        ORDER BY Sales DESC
        LIMIT 10
    """,
    "total_revenue_per_user": """
        SELECT u.UserID, u.Name AS UserName, SUM(t.Amount) AS TotalSpent, 'total_revenue_per_user' AS QueryType
        FROM transactions t
        JOIN users u ON t.UserID = u.UserID
        GROUP BY u.UserID, u.Name
        ORDER BY TotalSpent DESC
        LIMIT 10
    """,
    "most_active_users": """
        SELECT UserID, COUNT(TransactionID) AS TotalTransactions, 'most_active_users' AS QueryType
        FROM transactions
        GROUP BY UserID
        ORDER BY TotalTransactions DESC
        LIMIT 10
    """,
    "highest_revenue_products": """
        SELECT p.Name AS ProductName, SUM(t.Amount) AS TotalRevenue, 'highest_revenue_products' AS QueryType
        FROM transactions t
        JOIN products p ON t.ProductID = p.ProductID
        GROUP BY p.Name
        ORDER BY TotalRevenue DESC
        LIMIT 10
    """
}

# Merge All Query Results into One DataFrame
final_df = None
for name, query in queries.items():
    df = spark.sql(query)
    common_columns = ["QueryType"]
    for column in final_df.columns if final_df else []:
        if column not in df.columns:
            df = df.withColumn(column, lit(None))
    for column in df.columns:
        if final_df and column not in final_df.columns:
            final_df = final_df.withColumn(column, lit(None))
    final_df = df if final_df is None else final_df.unionByName(df)

# Export Merged DataFrame to CSV
final_df.toPandas().to_csv("powerbi_combined_data.csv", index=False)
print("Exported powerbi_combined_data.csv for Power BI")

#Stop Spark Session
spark.stop()
print("Big Data Pipeline Execution Completed")
