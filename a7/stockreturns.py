from pyspark.sql import SQLContext
from pyspark import SparkContext
import pyspark.sql.functions as sf

if __name__ == "__main__":
    # create Spark context with necessary configuration
    spark = SparkContext("local", "Stock Returns")

    # read json data from the newdata directory
    df = SQLContext(spark).read.csv('./stock_prices.csv', header=True)

    # select date, sum((cl-op)/op*100) as avg_return
    # from r
    # group by date
    returns = df.selectExpr("date", "(close-open)/open*100 as return")\
        .groupBy("date").agg(sf.avg("return").alias("avg_return"))

    # save the counts to output
    returns.write.csv("./stockreturns/")
