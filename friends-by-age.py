from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)


# 0,Will,33,385
# 1,Jean-Luc,33,2
# 2,Hugh,55,221
# 3,Deanna,40,465
# 4,Quark,68,21
# 5,Weyoun,59,318
# Parseline get last 2 values lines.map(parseLine) i.e (33,385) tupple
# (lambda x: (x, 1)    transforms (33,385) -> (33, (385, 1))  , (26, (2,1))
# reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) => (33, (387,2))   <== First Action Waits Lazy until here
# averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1]) (33, 193.5)

lines = sc.textFile("file:///home/vagrant/PycharmProjects/pySpark/ml-100k/fakefriends.csv")
rdd = lines.map(parseLine)
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
results = averagesByAge.collect()
for result in results:
    print(result)
