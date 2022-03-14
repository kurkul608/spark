from pyspark.sql import SparkSession
import pyspark.sql.functions as f
spark = SparkSession.builder.getOrCreate()

df = spark.read.format("csv").option("header", "true").load("./archive")


output = (df.groupBy("author").agg(f.count("followers")))

print("Count of uniq authors")
print(df.select("author").distinct().count())
uniqAuthor = df.select("author").distinct().count()

print("Count of uniq regions")
print(df.select("region").distinct().count())
uniqRegion = df.select("region").distinct().count()

print("top 10 authors by followers count")
output.orderBy(-f.col("count(followers)")).show(10)

output.coalesce(1).write.mode("overwrite").format("csv").save("result.csv")


