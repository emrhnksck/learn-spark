from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

spark = SparkSession.builder.appName("sql-excersize").getOrCreate()

lines = spark.read.option("header","true").option("inferSchema","true").csv("data/fakefriends-header.csv")

friendsByAge = lines.select("age","friends")

friendsByAge.groupBy("age").avg("friends").show()

spark.stop()

