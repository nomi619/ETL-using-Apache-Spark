from pyspark.sql import SparkSession

spark=SparkSession.builder.getOrCreate()
spark.sql("select * from reduced_table limit 100").show()

spark.stop()