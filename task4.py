import sys
from csv import reader
from pyspark import SparkContext

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark-submit task4.py <issued>", file=sys.stderr)
        exit(-1)

    sc = SparkContext()
    issued_lines = sc.textFile(sys.argv[1], 1)
    issued_lines = issued_lines.mapPartitions(lambda x: reader(x))

    to_be_strip = issued_lines.first()
    issued_lines = issued_lines.filter(lambda x: x != to_be_strip)

    count = issued_lines.map(lambda x: ("NY" if x[16]=="NY" else "Other", 1)) \
        .reduceByKey(lambda x, y: x + y) \
        .map(lambda x: "%s\t %d" % (x[0], x[1]))

    count.saveAsTextFile("task4.out")

    sc.stop()