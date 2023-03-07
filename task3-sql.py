import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, format_string

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark-submit task3-sql.py <open>", file=sys.stderr)
        exit(-1)

    sc = SparkContext()
    spark = SparkSession.builder.appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    opened = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[1])
    opened.createOrReplaceTempView("opened")

    result=spark.sql("SELECT precinct, SUM(amount_due) AS total, AVG(amount_due) AS average FROM opened GROUP BY precinct")
    result.select(format_string('%s\t %.2f, %.2f', result.precinct, result.total, result.average)).write.save("task3-sql.out", format="text")

    sc.stop()