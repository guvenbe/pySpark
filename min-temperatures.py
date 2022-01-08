from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

# ITE00100554,18000101,TMAX,-75,,,E,
# ITE00100554,18000101,TMIN,-148,,,E,
# GM000010962,18000101,PRCP,0,,,E,
# EZE00100082,18000101,TMAX,-86,,,E,
# EZE00100082,18000101,TMIN,-135,,,E,
# ITE00100554,18000102,TMAX,-60,,I,E,
# ITE00100554,18000102,TMIN,-125,,,E,
# GM000010962,18000102,PRCP,0,,,E,
# EZE00100082,18000102,TMAX,-44,,,E,
# parsedLines = lines.map(parseLine) => ITE00100554,18000101, -75 (converted to F)
# minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])  Get the TMIN in Filed 1
# stationTemps = minTemps.map(lambda x: (x[0], x[2])) (StationId, minTemp) get rid of entryType
# minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y)) Find min temp for stationId for entire year
lines = sc.textFile("file:///home/vagrant/PycharmProjects/SparkCourse/spark/ml-100k/1800.csv")
parsedLines = lines.map(parseLine)
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))
results = minTemps.collect();

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
