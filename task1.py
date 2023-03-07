import sys
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: spark-submit task1.py <isssued> <open>", file=sys.stderr)
        exit(-1)

    sc = SparkContext()
    issued_lines = sc.textFile(sys.argv[1], 1)
    issued_lines = issued_lines.mapPartitions(lambda x: reader(x))

    open_lines = sc.textFile(sys.argv[2], 1)
    open_lines = open_lines.mapPartitions(lambda x: reader(x))
    open_SN = open_lines.map(lambda x: x[3]).collect()[1:]

    paid_lines = issued_lines.filter(lambda x: x[0] not in open_SN) \
        .map(lambda x: "%s\t%s, %s, %s, %s" % (x[0], x[3], x[16], x[2], x[1]))

    paid_lines.saveAsTextFile("yfw215_task1.out")

    sc.stop()
