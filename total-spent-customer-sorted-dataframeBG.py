from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, IntegerType, StructField, FloatType

# 44,8602,37.19
# 35,5368,65.89
# 2,3391,40.64
spark = SparkSession.builder.appName("TotalSpentCustomerSortedDataFrame").getOrCreate()

schema = StructType([StructField("cust_id", IntegerType(), True), \
                     StructField("item_id", IntegerType(), True), \
                     StructField("amount_spent", FloatType(), True)])
df = spark.read.schema(schema).csv("file:///home/vagrant/PycharmProjects/pySpark/ml-100k/customer-orders.csv")
df.printSchema()
amount_spent_customer = df.select("cust_id", "amount_spent").groupBy("cust_id")\
    .agg(func.round(func.sum("amount_spent"), 2).alias("total_spent")).sort("total_spent")
amount_spent_customer.show(amount_spent_customer.count())

spark.stop()