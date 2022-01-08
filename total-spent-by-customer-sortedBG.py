from pyspark import SparkConf, SparkContext


def parseline(line):
    fields = line.split(',')
    customerId = fields[0]
    amount = fields[2]
    return (int(customerId), float(amount))


conf = SparkConf().setMaster("local").setAppName("totalSpentByCustomer")
sc = SparkContext(conf=conf)

lines = sc.textFile("file:///home/vagrant/PycharmProjects/SparkCourse/spark/ml-100k/customer-orders.csv")
rdd = lines.map(parseline)
totalsByCustomer =rdd.reduceByKey(lambda x, y: x+ y)
results = totalsByCustomer.collect()

for result in results:
    print(str(result[0]) + str("\t{:.2f}".format(result[1])))
