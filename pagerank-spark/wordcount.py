import pyspark

spark = pyspark.SparkContext("local", "hello world")

a = spark.parallelize(["a", "b", "c", "a"])

b = a.map(lambda w: (w, 1)).reduceByKey(lambda x, y: x + y)

print(b.collect())