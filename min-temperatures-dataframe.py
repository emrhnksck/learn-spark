from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("MinTemperatures").getOrCreate()

schema = StructType([
    StructField("stationID",StringType(),True),
    StructField("date",IntegerType(),True),
    StructField("measure_type",StringType(),True),
    StructField("temperature",FloatType(),True)
])

df = spark.read.schema(schema).csv("data/1800.csv")
df.printSchema()

minTemps = df.filter(df.measure_type == "TMIN")

stationTemps = minTemps.select("stationID","temperature")

minTempsByStation = stationTemps.groupBy("stationID").min("temperature")
minTempsByStation.show()