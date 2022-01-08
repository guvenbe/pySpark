from pyspark import SparkConf, SparkContext


def parseline(line):
    fields = line.split(',')
    customerId = fields[0]
    amount = fields[2]
    return (int(customerId), float(amount))


conf = SparkConf().setMaster("local").setAppName("totalSpentByCustomer")
sc = SparkContext(conf=conf)

lines = sc.textFile("file:///home/vagrant/PycharmProjects/pySpark/ml-100k/customer-orders.csv")
rdd = lines.map(parseline)
totalsByCustomer =rdd.reduceByKey(lambda x, y: x + y)
flipped = totalsByCustomer.map(lambda x: (x[1], x[0]))
totalByCustomerSorted = flipped.sortByKey()
sortedResults = totalByCustomerSorted.collect()

for result in sortedResults:
    print(str("{:.2f}".format(result[0]) +"\t\t"+ str(result[1])))
    # print(result)
