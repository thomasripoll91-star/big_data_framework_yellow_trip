
# -*- coding: utf-8 -*-

import argparse
import logging
import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def setup_logger():
    if not os.path.exists("logs"):
        os.makedirs("logs")

    logger = logging.getLogger("FeederLogger")
    logger.setLevel(logging.INFO)

    # Evite les doublons si Spark recharge le script
    if logger.handlers:
        return logger

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    file_handler = logging.FileHandler("logs/feeder_logs.txt", mode="a", encoding="utf-8")
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


def create_spark_session():
    spark = SparkSession.builder \
        .appName("NYC_Taxi_Feeder_Bronze") \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .config("spark.sql.parquet.rebaseModeInRead", "LEGACY") \
        .config("spark.sql.parquet.int96RebaseModeInRead", "LEGACY") \
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()

    return spark


def write_trips_to_bronze(spark, logger, input_trips, output_dir, year_value, month_value, day_value):
    logger.info("Lecture du fichier trips : %s", input_trips)

    df_trips = spark.read.parquet(input_trips)

    logger.info("Schema trips :")
    df_trips.printSchema()

    # Exigence prof : repartition + cache
    df_trips = df_trips.repartition(4)
    df_trips.cache()

    try:
        logger.info("Comptage des lignes trips...")
        row_count = df_trips.count()
        logger.info("Nombre de lignes trips : %s", row_count)

        df_trips = df_trips \
            .withColumn("year", lit(year_value)) \
            .withColumn("month", lit(month_value)) \
            .withColumn("day", lit(day_value))

        output_trips = output_dir + "/trips"

        logger.info("Ecriture trips vers : %s", output_trips)

        df_trips.write \
            .mode("overwrite") \
            .partitionBy("year", "month", "day") \
            .parquet(output_trips)

        logger.info("SUCCES : trips ecrits en Bronze.")

    finally:
        df_trips.unpersist()
        logger.info("Cache trips libere.")



def write_zones_to_bronze(spark, logger, input_zones, output_dir):
    logger.info("Lecture du fichier zones : %s", input_zones)

    df_zones = spark.read.csv(input_zones, header=True, inferSchema=True)

    logger.info("Schema zones :")
    df_zones.printSchema()

    output_zones = output_dir + "/zones"

    logger.info("Ecriture zones vers : %s", output_zones)

    df_zones.write \
        .mode("overwrite") \
        .parquet(output_zones)

    logger.info("SUCCES : zones ecrites en Bronze.")


def write_dictionary_to_bronze(spark, logger, output_dir, year_value, month_value, day_value):
    dictionary_data = [
        ("payment_type", 1, "Credit card"),
        ("payment_type", 2, "Cash"),
        ("payment_type", 3, "No charge"),
        ("payment_type", 4, "Dispute"),
        ("payment_type", 5, "Unknown"),
        ("payment_type", 6, "Voided trip"),

        ("RatecodeID", 1, "Standard rate"),
        ("RatecodeID", 2, "JFK"),
        ("RatecodeID", 3, "Newark"),
        ("RatecodeID", 4, "Nassau or Westchester"),
        ("RatecodeID", 5, "Negotiated fare"),
        ("RatecodeID", 6, "Group ride"),

        ("VendorID", 1, "Creative Mobile Technologies, LLC"),
        ("VendorID", 2, "VeriFone Inc.")
    ]

    schema = StructType([
        StructField("category", StringType(), False),
        StructField("code", IntegerType(), False),
        StructField("description", StringType(), False)
    ])

    df_dict = spark.createDataFrame(dictionary_data, schema)

    df_dict = df_dict \
        .withColumn("year", lit(year_value)) \
        .withColumn("month", lit(month_value)) \
        .withColumn("day", lit(day_value))

    output_dictionary = output_dir + "/data_dictionary"

    logger.info("Ecriture dictionnaire vers : %s", output_dictionary)

    df_dict.write \
        .mode("overwrite") \
        .partitionBy("year", "month", "day") \
        .parquet(output_dictionary)

    logger.info("SUCCES : dictionnaire ecrit en Bronze.")


def run_ingestion():
    logger = setup_logger()

    logger.info("====================================================")
    logger.info(" DEMARRAGE DU JOB FEEDER - COUCHE BRONZE")
    logger.info("====================================================")

    parser = argparse.ArgumentParser(description="Ingestion NYC Taxi vers Bronze")

    parser.add_argument("--input_trips", required=True, help="Chemin du fichier trips Parquet")
    parser.add_argument("--input_zones", required=True, help="Chemin du fichier zones CSV")
    parser.add_argument("--output_dir", required=True, help="Chemin de sortie Bronze")

    args = parser.parse_args()

    spark = create_spark_session()

    now = datetime.now()
    year_value = now.strftime("%Y")
    month_value = now.strftime("%m")
    day_value = now.strftime("%d")

    logger.info("Date de partition ingestion : year=%s, month=%s, day=%s",
                year_value, month_value, day_value)

    try:
        write_trips_to_bronze(
            spark=spark,
            logger=logger,
            input_trips=args.input_trips,
            output_dir=args.output_dir,
            year_value=year_value,
            month_value=month_value,
            day_value=day_value
        )

        write_zones_to_bronze(
            spark=spark,
            logger=logger,
            input_zones=args.input_zones,
            output_dir=args.output_dir
        )

        write_dictionary_to_bronze(
            spark=spark,
            logger=logger,
            output_dir=args.output_dir,
            year_value=year_value,
            month_value=month_value,
            day_value=day_value
        )

        logger.info("====================================================")
        logger.info(" FIN DU JOB FEEDER - SUCCES")
        logger.info("====================================================")

    except Exception as e:
        logger.error("====================================================")
        logger.error(" ERREUR CRITIQUE DANS LE FEEDER")
        logger.error("====================================================")
        logger.error(str(e))

        logger.error("")
        logger.error("Si l'erreur mentionne zStandard ou zstd,")
        logger.error("cela signifie que tu as probablement donne le fichier original")
        logger.error("yellow_tripdata_2025-02.parquet au lieu du fichier converti")
        logger.error("yellow_tripdata_2025-02-snappy.parquet.")
        logger.error("")

        raise e

    finally:
        spark.stop()


if __name__ == "__main__":
    run_ingestion()