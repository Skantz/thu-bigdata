import sys
import pyspark
import re
import time

if __name__ == '__main__':

    config = pyspark.SparkConf()
    context = pyspark.SparkContext(conf=config)

    first = time.time()

    lines = context.textFile(sys.argv[1])
    lines = lines.map(lambda x: (x.split()[0], x.split()[1]))
    lines = lines.groupByKey().map(lambda x : (x[0], list(x[1])))

    n = lines.count()
    nodes = lines.map(lambda x: x[0])
    rank  = nodes.map(lambda x: (x, 1. / n))

    kv_ = None
    for iter in range(10):
        kv_ = lines.join(rank)
        delta = kv_.map(lambda x: x[1][1] / len(x[0]) * 0.85 + 0.15)
        rank = delta
        print(rank)

    print(rank.collect())

    last = time.time()

    print("Total time: %.2f s" % (last - first))
    context.stop()
