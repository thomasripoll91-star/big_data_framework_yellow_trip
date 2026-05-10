import argparse
import logging
import os
import sys # Ajout de sys
from pyspark.sql import SparkSession, functions as F

# 1. Arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_bronze", required=True)
parser.add_argument("--output_silver", required=True)
args = parser.parse_args()

# 2. Logs (On force l'ecriture avec delay=False)
log_dir = "/opt/pipeline/logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

logger = logging.getLogger("Preprocessor")
logger.setLevel(logging.INFO)

# Handler Fichier (On ajoute delay=False pour forcer l'ouverture du fichier)
fh = logging.FileHandler(os.path.join(log_dir, "preprocessor_logs.txt"), mode='a')
fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(fh)

# Handler Console
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(ch)

# 3. Spark
spark = (SparkSession.builder
    .appName("preprocessor_silver_taxi")
    .config("spark.sql.parquet.enableVectorizedReader", "false")
    .getOrCreate())

logger.info("--- DEBUT TRAITEMENT SILVER ---")

try:
    # 4. Lecture
    df_trips = spark.read.parquet(args.input_bronze + "/trips")
    df_zones = spark.read.parquet(args.input_bronze + "/zones")
    df_dict = spark.read.parquet(args.input_bronze + "/data_dictionary")

    # 5. Nettoyage
    df_cleaned = df_trips.filter((F.col("trip_distance") > 0) & (F.col("total_amount") > 0))

    # 6. Jointure Zones
    df_with_zones = df_cleaned.join(df_zones, df_cleaned.PULocationID == df_zones.LocationID, "left").drop("LocationID")

    # 7. Jointure Dictionnaire (Correctif doublons)
    df_pay_type = df_dict.filter(F.col("category") == "payment_type").drop("year", "month", "day")
    df_final = df_with_zones.join(df_pay_type, df_with_zones.payment_type == df_pay_type.code, "left") \
                            .withColumnRenamed("description", "payment_method") \
                            .drop("code", "category")

    # 8. Action et Cache
    df_final.cache()
    count = df_final.count()
    logger.info("Lignes traitees : " + str(count))

    # 9. Ecriture
    df_final.write.mode("overwrite").parquet(args.output_silver + "/trips_cleaned")
    
    logger.info("--- FIN TRAITEMENT SILVER SUCCES ---")

except Exception as e:
    logger.error("Erreur durant le traitement : " + str(e))
finally:
    # On force la fermeture des logs proprement
    fh.close()
    ch.close()
    spark.stop()