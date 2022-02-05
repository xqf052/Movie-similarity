import sys
from pyspark import SparkContext, SparkConf
from math import sqrt
import codecs

#To run on EMR successfully + output results for Star Wars:
#aws s3 cp s3://spark-qifan/movie-similarity-1k.py ./
#aws s3 cp s3://spark-qifan/u.item ./
#spark-submit --executor-memory 1g movie-similarity-1k.py 50

def loadMovieNames():
    movieNames = {}
    with codecs.open('u.item', 'r', encoding = 'ISO-8859-1', errors = 'ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

def removeDuplication(line):
    (movie1, rating1) = line[1][0]
    (movie2, rating2) = line[1][1]
    return movie1 < movie2

def makePairs(line):
    (movie1, rating1) = line[1][0]
    (movie2, rating2) = line[1][1]
    return ((movie1, movie2), (rating1, rating2))

def calculateSimilarity(ratings):
    numPairs = 0
    sum_xx = sum_yy = sum_xy = 0
    for rating1, rating2 in ratings:
        sum_xy += rating1 * rating2
        sum_xx += rating1 * rating1
        sum_yy += rating2 * rating2
        numPairs += 1

    numerator = sum_xy
    denominator = sqrt(sum_xx)*sqrt(sum_yy)
    score = 0
    if(denominator):
        score = (numerator/(float(denominator)))
    return (score, numPairs)



conf = SparkConf()
sc = SparkContext(conf = conf)

print("\nLoading movie names...")
movieDict = loadMovieNames()

data = sc.textFile('s3n://spark-qifan/u.data')
ratings = data.map(lambda x: x.split('\t')).map(lambda x: (int(x[0]), (int(x[1]), float(x[2]))))
partitionedRatings = ratings.partitionBy(100)
joinedRatings = partitionedRatings.join(partitionedRatings)
uniqueRatings = joinedRatings.filter(removeDuplication)
ratingPairs = uniqueRatings.map(makePairs).partitionBy(100)
groupRatings = ratingPairs.groupByKey()
similarityScore = groupRatings.mapValues(calculateSimilarity).persist()

similarityScore.sortByKey()
# similarityScore.saveAsTextFile("movie-sims")

if(len(sys.argv) > 1):
    movieId = int(sys.argv[1])
    scoreThreshold = 0.97
    occurenceThreshold = 50
    filterScore = similarityScore.filter(lambda line: (line[0][0] == movieId or line[0][1] == movieId)\
                                         and line[1][0] > scoreThreshold and line[1][1] > occurenceThreshold)
    results = filterScore.map(lambda line:(line[1], line[0])).sortByKey(ascending = False).take(10)
    print('Top 10 related movie for ' + movieDict[movieId])

    for result in results:
        (sim, pair) = result
        similarMovieId = pair[0]
        if (similarMovieId == movieId):
            similarMovieId = pair[1]
        print(movieDict[similarMovieId] + ' score: ' + str(sim[0]) + ' strength: ' + str(sim[1]))