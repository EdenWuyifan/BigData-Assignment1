import sys
from csv import reader
from pyspark import SparkContext

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark-submit task7.py <issued>", file=sys.stderr)
        exit(-1)

    sc = SparkContext()
    issued_lines = sc.textFile(sys.argv[1], 1)
    issued_lines = issued_lines.mapPartitions(lambda x: reader(x))

    to_be_strip = issued_lines.first()
    issued_lines = issued_lines.filter(lambda x: x != to_be_strip)

    count = issued_lines.map(lambda x: (x[3], (1 if x[1] in ('2016-03-05', '2016-03-06', '2016-03-12', '2016-03-13', '2016-03-19', '2016-03-20', '2016-03-26', '2016-03-27') else 0, 1 if x[1] not in ('2016-03-05', '2016-03-06', '2016-03-12', '2016-03-13', '2016-03-19', '2016-03-20', '2016-03-26', '2016-03-27') and "2016-03-" in x[1] else 0))).reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])).map(lambda x: "%s\t %.2f, %.2f" % (x[0], x[1][0]/8, x[1][1]/23))
    count.saveAsTextFile("yfw215_task7.out")

    sc.stop()