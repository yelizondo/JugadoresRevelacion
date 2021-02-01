from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (DateType, FloatType, StructField, StructType, StringType)

spark = SparkSession.builder.appName("Calculate score").getOrCreate()

mapr_schema = StructType([StructField('name2', StringType()), StructField('future', FloatType())])
hive_schema = StructType([StructField('name', StringType()), StructField('team', StringType()), StructField('current', FloatType())])

hive_dataframe = spark.read.csv("hive.csv", schema=hive_schema, header=False)

mapr_dataframe = spark.read.csv("mapr.csv", schema=mapr_schema, header=False)

shared = hive_dataframe.join(mapr_dataframe,hive_dataframe["name"]==mapr_dataframe["name2"],"inner")
shared = shared.drop("name2")
shared.show()
