from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet('/opt/pipeline/data/silver/trips_cleaned')
# On utilise les vrais noms de colonnes : Borough, Zone et payment_method
df.select('Borough', 'Zone', 'payment_method', 'trip_distance', 'total_amount').show(10)