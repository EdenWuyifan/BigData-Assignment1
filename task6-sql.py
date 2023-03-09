import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark-submit task6-sql.py <issued>", file=sys.stderr)
        exit(-1)

    sc = SparkContext()
    spark = SparkSession.builder.appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    issued = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
    issued.createOrReplaceTempView("issued")

    result = spark.sql("SELECT vehicle_make, COUNT(*) AS counts FROM issued GROUP BY vehicle_make ORDER BY counts DESC LIMIT 10")
    result.select(format_string('%s\t %d', result.vehicle_make, result.counts)).write.save("task6-sql.out", format="text")

    sc.stop()