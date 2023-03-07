import sys
from csv import reader
from pyspark import SparkContext

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark-submit task1.py <open>", file=sys.stderr)
        exit(-1)

    sc = SparkContext()

    open_lines = sc.textFile(sys.argv[1], 1)
    open_lines = open_lines.mapPartitions(lambda x: reader(x))

    violated_lines = open_lines.map(lambda x: (x[7], 1)) \
        .reduceByKey(lambda x, y: x + y) \
        .map(lambda x: "%s\t %d" % (x[0], x[1]))

    violated_lines.saveAsTextFile("yfw215_task2.out")

    sc.stop()
