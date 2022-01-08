from _ctypes_test import func

from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("friendsByAgeAvg").getOrCreate()
people =spark.read.option("header", "true").option("inferschema", "true")\
    .csv("file:///home/vagrant/PycharmProjects/pySpark/ml-100k/fakefriends-header.csv")

print("Here is our inferred schema:")
people.printSchema()

people.select("age", "friends").groupBy("age").agg(func.round(func.avg("friends")).alias("avg_friends"))\
    .sort("age").show()

spark.stop()