from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql.functions import *

spark = SparkContext("local", "stock prices")

df = SQLContext(spark).read.option("header", "true").csv("stock_prices.csv")

# df.select(df['date'], \
# 	((df['close']-df['open'])*100.00/df['open']).alias("return"))\
# 	.groupBy('date').agg(avg('return').alias('avg_return')).show()

df1 = df.select(df['date'], \
	((df['close']-df['open'])*100.00/df['open']).alias("return"))\
	.groupBy('date').agg(avg('return').alias('avg_return'))

df1.select('date', 'avg_return').coalesce(1).write.format('csv').save('stockreturns')
