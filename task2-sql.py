import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, format_string

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark-submit task2-sql.py <open>", file=sys.stderr)
        exit(-1)

    sc = SparkContext()
    spark = SparkSession.builder.appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    opened = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[1])
    opened.createOrReplaceTempView("opened")

    result=spark.sql("SELECT violation, COUNT(*) AS counts FROM opened GROUP BY violation")
    result.select(format_string('%s\t %d', result.violation, result.counts)).write.save("task2-sql.out", format="text")

    sc.stop()