import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark-submit task4-sql.py <issued>", file=sys.stderr)
        exit(-1)

    sc = SparkContext()
    spark = SparkSession.builder.appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    issued = spark.read.format('csv').options(header='true',inferschema='true').load("/shared/CS-GY-6513/parking-violations/parking-violations-header.csv")
    issued.createOrReplaceTempView("issued")

    result = spark.sql("SELECT registration_state, COUNT(registration_state) AS count FROM (SELECT CASE WHEN issued.registration_state LIKE 'NY' THEN 'NY' ELSE 'Other' END AS registration_state FROM issued) GROUP BY registration_state")
    result.select(format_string('%s\t %d', result.registration_state, result.count)).write.save("sql.out", format="text")

    sc.stop()