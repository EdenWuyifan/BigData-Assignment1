import sys
from csv import reader
from pyspark import SparkContext

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark-submit task3.py <open>", file=sys.stderr)
        exit(-1)

    sc = SparkContext()

    open_lines = sc.textFile(sys.argv[1], 1)
    open_lines = open_lines.mapPartitions(lambda x: reader(x))
    to_be_strip = open_lines.first()
    open_lines = open_lines.filter(lambda x: x != to_be_strip)
    
    total = open_lines.map(lambda x: (x[5], float(x[12]))) \
        .reduceByKey(lambda x, y: x + y)
    
    count = open_lines.map(lambda x: (x[5], 1)) \
        .reduceByKey(lambda x, y: x + y)

    average = total.join(count) \
        .map(lambda x: "%s\t %.2f, %.2f" % (x[0], x[1][0], x[1][0]/x[1][1]))

    average.saveAsTextFile("task3.out")

    sc.stop()