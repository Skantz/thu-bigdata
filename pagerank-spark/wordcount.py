import sys
import pyspark
import re
import time

first = time.time()
config = pyspark.SparkConf()
context = pyspark.SparkContext(conf=config)

lines = context.textFile(sys.argv[1])
lines = lines.flatMap(lambda x: re.split('[^\w]+', x))
res = lines.map(lambda w: (w, 1)).reduceByKey(lambda x, y: x + y)
print(res.takeOrdered(10, lambda x: -x[1]))
last = time.time()
print("time taken", last - first)