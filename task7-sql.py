import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark-submit task7-sql.py <issued>", file=sys.stderr)
        exit(-1)

    sc = SparkContext()
    spark = SparkSession.builder.appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    issued = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
    issued.createOrReplaceTempView("issued")

    result = spark.sql("SELECT violation_county, CAST(SUM(weekend) AS float)/8 as weekend_avg, CAST(SUM(week) AS float)/23 as week_avg FROM (SELECT violation_county, CASE WHEN issue_date IN ('2016-03-05', '2016-03-06', '2016-03-12', '2016-03-13', '2016-03-19', '2016-03-20', '2016-03-26', '2016-03-27') THEN 1 ELSE 0 END AS weekend, CASE WHEN issue_date NOT IN ('2016-03-05', '2016-03-06', '2016-03-12', '2016-03-13', '2016-03-19', '2016-03-20', '2016-03-26', '2016-03-27') AND issue_date LIKE '2016-03-%' THEN 1 ELSE 0 END AS week FROM issued) GROUP BY violation_county")
    result.select(format_string('%s\t %.2f, %.2f', result.violation_county, result.weekend_avg, result.week_avg)).write.save("task7-sql.out", format="text")

    sc.stop()

