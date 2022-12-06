from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, IntegerType, FloatType
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("TotalSpent").getOrCreate()

schema = StructType([
    StructField("ID",IntegerType(),True),
    StructField("item_id",IntegerType(),True),
    StructField("spent",FloatType(),True)
])

df = spark.read.schema(schema).csv("data/customer-orders.csv")
totalByCustomer = df.groupBy("ID").agg(func.round(func.sum("spent"),2).alias("total_spent")).show()

spark.stop()

