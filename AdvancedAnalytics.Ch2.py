__author__ = 'Andrey Mironoff'
execfile('Init.py')
from Chapter2Helpers import Chapter2Helpers, MatchData, NAStatCounter

import os
import os.path

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.statcounter import StatCounter

os.chdir("/home/vagrant/")

def quiet_logs( sc ):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

# Setup the context
conf = SparkConf().setMaster("local").setAppName("AdvancedAnalytics_Ch2")

# External modules are imported via a separate array. This can also be done
# on a SparkContext that has already been constructed.
sc = SparkContext(conf=conf, pyFiles=['pycharm/Chapter2Helpers.py'])
quiet_logs(sc)

# Actual code goes here.
basePath = 'data/advanced_analytics'
datasetFolder = 'linkage'
datasetPath = os.path.join(basePath, datasetFolder)
numPartitions = 2
rawDataRDD = sc.textFile(datasetPath, numPartitions)
rawDataRDD.cache()
print "Loaded linkage dataset with {0} points".format(rawDataRDD.count())
head = rawDataRDD.take(10)


noHeader = rawDataRDD.filter(lambda x: Chapter2Helpers.isNotHeader(x))
noHeader.cache()
print "# Points excluding header: {0}".format(noHeader.count())

line = head[5]
pieces = line.split(",")
print pieces
id1 = int(pieces[0])
id2 = int(pieces[1])
matched = pieces[11] == 'TRUE'

print "ID 1: {0}, ID 2: {1}, Is Match: {2}".format(id1, id2, matched)

rawScores = pieces[2:11]
# account for unknown values.
rawScores = map(lambda x: None if x == "?" else float(x), rawScores)

print rawScores

print line
print Chapter2Helpers.parse(line)

# now let's parse the entire dataset
parsed = noHeader.map(lambda x: Chapter2Helpers.parse(x))
parsed.cache()

print "Parsed data points: {0}".format(parsed.count())

weights = [.01, 0.99]
seed = 42
parsedSample, someOtherJunk = parsed.randomSplit(weights, seed)
parsedSample.cache()

# Count how many of the MatchData records in parsed have a value of true or false for the matched field
matchCounts = parsedSample.map(lambda item: item.matched).countByValue()

# Sort statistics by value
from collections import OrderedDict
matchCounts = OrderedDict(sorted(matchCounts.items(), key=lambda entry: entry[1]))

print matchCounts

# Get summary statistics for a variable.
print parsedSample.map(lambda item: item.scores[2]).stats()

# Now let's use the NASTatCounter class that takes care of missing values!!!
# Here we start by mapping each score to a NAStatCounter instance (we'll merge (i.e. ZIP) them later)
nasRDD = parsedSample.map(lambda item: map(lambda score: NAStatCounter().add(score), item.scores))

# Okay. So we zip statcounters pairwise for each point, then sum them up.
merged = nasRDD.reduce(lambda x, y: map(lambda a: a[0].mergeStats(a[1]), zip(x, y)))

print merged

# Now let's encapsulate this logic into a function. That'll make stuff easier for partition-based stats!!


def printStatistics(onThisRdd):
    print onThisRdd.map(lambda item: map(lambda score: NAStatCounter().add(score), item.scores)).reduce(lambda x, y: map(lambda a: a[0].mergeStats(a[1]), zip(x, y)))


printStatistics(parsedSample.filter(lambda item: item.matched))
printStatistics(parsedSample.filter(lambda item: item.matched is not True))
