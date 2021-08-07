from pyspark.sql import SQLContext
from pyspark import SparkContext
# other required imports here
import re
from pyspark.sql.functions import *


def process(line):
	line = line.lower()
	line = re.sub('[^0-9a-zA-Z]+', ' ', line)
	wordsList = line.split(' ')
	return wordsList

# fill
#convert all characters into lower case
#replace all non-alphanumerics with whitespace
#split on whitespaces
#return list of words

if __name__ == "__main__":
	# create Spark context with necessary configuration
	spark = SparkContext("local", "Word Count")
	# read json data from the newdata directory
	df = SQLContext(spark).read.option("multiLine", True) \
	.option("mode", "PERMISSIVE").json("./newsdata")
	# split each line into words
	lines = df.select("date_published", "article_body").rdd
	words = lines.flatMap(process)
	# count the occurrence of each word
	wordCounts = words.map(lambda w: (w, 1)).reduceByKey(lambda a,b: a+b)
	# save the counts to output
	wordCounts.saveAsTextFile("./wordcount/")