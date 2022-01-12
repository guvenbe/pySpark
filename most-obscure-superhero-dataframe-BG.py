from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([ \
    StructField("id", IntegerType(), True), \
    StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv(
    "file:///home/vagrant/PycharmProjects/pySpark/ml-100k/Marvel-Names")

lines = spark.read.text("file:///home/vagrant/PycharmProjects/pySpark/ml-100k/Marvel-Graph")

# Small tweak vs. what's shown in the video: we trim each line of whitespace as that could
# throw off the counts.
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))

minConnectionCount = connections.agg(func.min("connections")).first()[0]
leastPopular = connections.filter(func.column("connections") == minConnectionCount).alias("id")
leastPopularName = names.join(leastPopular, "id").select("name").orderBy("name")
leastPopularName.show()


