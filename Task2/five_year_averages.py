import sys
from pyspark.sql import SparkSession , SQLContext
from pyspark import SparkContext as sc, SparkConf
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DataType,IntegerType
from pyspark.sql import Window

spk_session=SparkSession.builder.appName('Fit_PSSQLApp01').getOrCreate()
df = spk_session.read.format('csv').option('header',True).load('new_cleaned.csv')
df = df.withColumn('id',F.monotonically_increasing_id())
df.show()
df2 = df.withColumn('year',F.year(df['date']))
df2.show()
df3 = df2.groupBy("year").agg(F.avg("SP500").alias("avg_sp5000"),\
         F.avg("Dividend").alias("avg_dividend"), \
         F.avg("Earnings").alias("avg_earnings"), \
         F.avg("Consumer Price Index").alias("max_consumer_price_index"), \
         F.avg("Long Interest Rate").alias("avg_long_interest_rate"), \
         F.avg("Real Price").alias("avg_real_price"), \
         F.avg("Real Dividend").alias("avg_real_dividend"), \
         F.avg("Real Earnings").alias("avg_real_earning"), \
         F.avg("PE10").alias("avg_pe10")).orderBy('Year')
df3.show(148)
df3 = df3.withColumn('id',F.monotonically_increasing_id())
group_size = 5
w = Window.orderBy('id')
df3 = df3.withColumn('group', F.floor((F.row_number().over(w) - 1) / group_size))\
    .select('year','group','avg_sp5000','avg_earnings','avg_dividend','max_consumer_price_index','avg_long_interest_rate','avg_real_price','avg_real_dividend','avg_real_earning','avg_pe10')
df3.show(148)
df4=df3.groupBy('group').agg(F.avg('avg_sp5000').alias('avg_sp'),\
                         F.avg('avg_dividend'),\
                         F.avg('avg_earnings'),\
                         F.avg('max_consumer_price_index'),\
                         F.avg('avg_long_interest_rate'),\
                         F.avg('avg_real_price'),\
                         F.avg('avg_real_dividend'),\
                         F.avg('avg_real_earning'),\
                         F.avg('avg_pe10'))
df4.show(29)
df4.write.csv("five_year_avg.csv",header=True)
