from pyspark.sql import SQLContext
from pyspark import SparkContext
# other required imports here
import re
 
def process(line):
    # fill
    year_month = line[0][:7]
    # convert all characters into lower case
    lower = line[1].lower()
    # replace all non-alphanumerics with whitespace
    lower = re.sub('[^0-9a-zA-Z]+', ' ', lower)
    # split on whitespaces
    splitted = lower.split(' ')
    # return list of words
    return [year_month+' '+x for x in splitted]

if __name__ == "__main__":
    # create Spark context with necessary configuration
    spark = SparkContext("local", "Word Count")

    # read json data from the newdata directory
    df = SQLContext(spark).read.option("multiLine", True).option("mode", "PERMISSIVE").json("./newsdata")

    # split each line into words
    lines = df.select("date_published", "article_body").rdd
    words = lines.flatMap(process)

    # count the occurrence of each word
    wordCounts = words.map(lambda x: (x,1)).reduceByKey(int.__add__)

    # save the counts to output
    wordCounts.saveAsTextFile("./wordcount/")
