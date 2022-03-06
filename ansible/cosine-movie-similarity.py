from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType
import codecs
import sys
import json

#To run on EMR successfully + output results for Star Wars:
#aws s3 cp s3://spark-qifan/cosine-movie-similarity.py ./
#aws s3 cp s3://spark-qifan/movie_titles.csv ./
#spark-submit --executor-memory 1g cosine-movie-similarity.py 50


def load_movie_names():
    movieNames = {}
    with codecs.open('movie_titles.csv', 'r', encoding = 'ISO-8859-1', errors = 'ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

def calculate_cos_similarity(spark, rating_pairs_DF):
    pair_score = rating_pairs_DF.withColumn('xx', func.col('rating1')*func.col('rating1'))\
                                .withColumn('xy', func.col('rating1')*func.col('rating2'))\
                                .withColumn('yy', func.col('rating2')*func.col('rating2'))
    calculate_cos = pair_score.groupBy('movie1', 'movie2').agg(func.sum('xy').alias('numerator'), \
                                      (func.sqrt(func.sum('xx'))*func.sqrt(func.sum('yy'))).alias('denominator'), \
                                       func.count(func.col('xy')).alias('pairs_number'))
    result = calculate_cos.withColumn('score', func.when(func.col('denominator') != 0, func.col('numerator')/func.col('denominator'))\
                                      .otherwise(0)).select('movie1', 'movie2', 'score', 'pairs_number')
    return result

# Create a spark session
spark = SparkSession.builder.appName('cosine movie similarity').getOrCreate()

# Create schema for the dataframes
rating_schema = StructType([
    StructField('userID', IntegerType(), True),
    StructField('movieID', IntegerType(), True),
    StructField('rating', IntegerType(), True),
    StructField('timeStamp', LongType(), True)
])

# Create dataframes for the raw data
rating_DF = spark.read.schema(rating_schema).option('sep', '\t').\
    csv('s3n://spark-qifan/movie_ratings.csv')
rating_DF = rating_DF.select('userID', 'movieID', 'rating')

movieDict = load_movie_names()

# Create rating pairs dataframe
rating_pairs_DF = rating_DF.alias('rating_DF1').join(rating_DF.alias('rating_DF2'), \
                                (func.col('rating_DF1.userID') == func.col('rating_DF2.userID')) & \
                                (func.col('rating_DF1.movieID') < func.col('rating_DF2.movieID')))\
                                .select(func.col('rating_DF1.movieID').alias('movie1'), func.col('rating_DF2.movieID').alias('movie2'),\
                                        func.col('rating_DF1.rating').alias('rating1'), func.col('rating_DF2.rating').alias('rating2'))

# calculate cosine similarities for rating pairs
cos_results = calculate_cos_similarity(spark, rating_pairs_DF).cache()

if (len(sys.argv) > 1):
    score_threshold = 0.97
    pairs_number_threshold = 50
    movieID = int(sys.argv[1])

    related_cos_results = cos_results.filter(((func.col('movie1') == movieID) | (func.col('movie2') == movieID)) & \
                                             (func.col('score') > score_threshold) & (func.col('pairs_number') > pairs_number_threshold))

    results = related_cos_results.sort(func.col('score').desc()).take(10)

    movie_name = movieDict[movieID]
    print('Top 10 similar movie for: ' + movie_name)

    similar_movies_list = []

    for result in results:
        similar_movie_dict = {}
        similar_movieID = result.movie1
        if(similar_movieID == movieID):
            similar_movieID = result.movie2
            similar_movie_name = movieDict[similar_movieID]
            score = str(result.score)
            strength = str(result.pairs_number)
            print(similar_movie_name + '\tscore: ' + score + \
              '\tstrength: ' + strength)
            similar_movie_dict['movie_id'] = movieID
            similar_movie_dict['similar_movie_id'] = similar_movieID
            similar_movie_dict['movie_name'] = movie_name
            similar_movie_dict['similar_movie_name'] = similar_movie_name
            similar_movie_dict['score'] = score
            similar_movie_dict['strength'] = strength
            similar_movies_list.append(similar_movie_dict)

    with open('movie.json', 'w') as f:
        json.dump(similar_movies_list, f)

spark.stop()