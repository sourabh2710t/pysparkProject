from enum import unique
from tabnanny import check
from typing import final

from Tools.scripts.fixcid import Reverse
from Tools.scripts.generate_opcode_h import header
from pyspark.sql import SparkSession
import os

from pyspark.sql.types import StructType, StructField, IntegerType

os.environ["PYSPARK_PYTHON"] = "C:\\Python311\\python.exe"
import requests
import pandas as pd
# spark = SparkSession.builder \
#     .appName("PySparkTest") \
#     .config("spark.driver.host", "127.0.0.1") \
#     .getOrCreate()

spark = SparkSession.builder \
    .appName("PySparkExample") \
    .config("spark.pyspark.python", "C:\\Python311\\python.exe") \
    .getOrCreate()

# Step 2: Define Student Marks Data
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dense_rank
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("SecondHighestSalary").getOrCreate()

# Sample data


data = {
    "name": ["anand", "anand", "anand", "mahesh", "mahesh", "mahesh"],
    "subject": ["eng", "math", "science", "eng", "math", "science"],
    "mark":[50,70,30,90,92,78]
}

df = pd.DataFrame(data)
# print(df)
from pyspark.sql.functions import *

df = df.groupby("name").size()
print(df)

