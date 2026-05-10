
# -*- coding: utf-8 -*-
import argparse
import logging
import sys
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def setup_logger():
    """Configuration du logger : ecrit dans la console ET dans logs/feeder_logs.txt"""
    # Pour s'assurer que le dossier logs existe
    if not os.path.exists('logs'):
        os.makedirs('logs')

    logger = logging.getLogger("FeederLogger")
    logger.setLevel(logging.INFO)
    
    # Formatteur de log (sans accents dans le format pour plus de stabilite)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # Sortie fichier: fichier .txt (Exigence prof)
    file_handler = logging.FileHandler('logs/feeder_logs.txt', mode='a')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Sortie console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger

def run_ingestion():
    logger = setup_logger()
    logger.info("--- DEMARRAGE DU JOB FEEDER (COUCHE BRONZE) ---")

    # =====================================================================
    # 1. PARAMETRES DYNAMIQUES (Exigence : pas de chemin en dur)
    # =====================================================================
    parser = argparse.ArgumentParser(description="Ingestion NYC Taxi vers Bronze")
    parser.add_argument('--input_trips', required=True, help="Source Parquet des trajets")
    parser.add_argument('--input_zones', required=True, help="Source CSV des zones")
    parser.add_argument('--output_dir', required=True, help="Dossier destination (data/bronze)")
    args = parser.parse_args()

    # Creation de la session Spark
    spark = SparkSession.builder \
        .appName("NYC_Taxi_Feeder_Bronze") \
        .getOrCreate()

    # Date actuelle pour le partitionnement (Exigence : year=YYYY/month=MM/day=DD)
    now = datetime.now()
    y, m, d = now.year, now.month, now.day

    # =====================================================================
    # 2. TRAITEMENT DES TRIPS (TRAJETS)
    # =====================================================================
    try:
        logger.info("Lecture des donnees sources : " + str(args.input_trips))
        
        # Lecture + Repartition (Exigence : faciliter le parallelisme)
        df_trips = spark.read.parquet(args.input_trips).repartition(4)
        
        # Optimisation (Exigence : cache/persist avant une action comme count)
        df_trips.cache()
        nb_lignes = df_trips.count() 
        logger.info("Donnees Trips mises en cache. Nombre de lignes : " + str(nb_lignes))

        # Ajout des colonnes de partitionnement
        df_trips = df_trips.withColumn("year", lit(y)) \
                           .withColumn("month", lit(m)) \
                           .withColumn("day", lit(d))

        # Ecriture en format Parquet (Exigence : format binaire performant)
        path_bronze_trips = args.output_dir + "/trips"
        df_trips.write.mode("overwrite").partitionBy("year", "month", "day").parquet(path_bronze_trips)
        
        logger.info("SUCCES : Trips ecrits dans " + str(path_bronze_trips))
        df_trips.unpersist() # On libere la memoire apres usage (Bonne pratique)

    except Exception as e:
        logger.error("ERREUR Ingestion Trips : " + str(e))
        sys.exit(1)

    # =====================================================================
    # 3. TRAITEMENT DES ZONES
    # =====================================================================
    try:
        logger.info("Ingestion de la table de reference des zones...")
        df_zones = spark.read.csv(args.input_zones, header=True, inferSchema=True)
        
        # On sauvegarde aussi en parquet dans bronze pour homogeneite
        path_bronze_zones = args.output_dir + "/zones"
        df_zones.write.mode("overwrite").parquet(path_bronze_zones)
        logger.info("SUCCES : Zones ecrites dans " + str(path_bronze_zones))
    except Exception as e:
        logger.error("ERREUR Ingestion Zones : " + str(e))

    # =====================================================================
    # 4. CREATION DU DICTIONNAIRE METIER (Complet)
    # =====================================================================
    try:
        logger.info("Generation du dictionnaire metier complet...")
        dictionary_data = [
            # Types de paiement
            ("payment_type", 1, "Credit card"),
            ("payment_type", 2, "Cash"),
            ("payment_type", 3, "No charge"),
            ("payment_type", 4, "Dispute"),
            ("payment_type", 5, "Unknown"),
            ("payment_type", 6, "Voided trip"),
            # Codes tarifaires
            ("RatecodeID", 1, "Standard rate"),
            ("RatecodeID", 2, "JFK"),
            ("RatecodeID", 3, "Newark"),
            ("RatecodeID", 4, "Nassau or Westchester"),
            ("RatecodeID", 5, "Negotiated fare"),
            ("RatecodeID", 6, "Group ride"),
            # Fournisseurs
            ("VendorID", 1, "Creative Mobile Technologies, LLC"),
            ("VendorID", 2, "VeriFone Inc.")
        ]
        
        schema = StructType([
            StructField("category", StringType(), False),
            StructField("code", IntegerType(), False),
            StructField("description", StringType(), False)
        ])
        
        df_dict = spark.createDataFrame(dictionary_data, schema)
        
        # Partitionnement du dictionnaire pour la coherence de la couche Bronze
        df_dict = df_dict.withColumn("year", lit(y)).withColumn("month", lit(m)).withColumn("day", lit(d))
        
        path_bronze_dict = args.output_dir + "/data_dictionary"
        df_dict.write.mode("overwrite").partitionBy("year", "month", "day").parquet(path_bronze_dict)
        logger.info("SUCCES : Dictionnaire metier cree dans la couche Bronze.")
        
    except Exception as e:
        logger.error("ERREUR Dictionnaire : " + str(e))

    logger.info("--- FIN DU JOB FEEDER AVEC SUCCES ---")

if __name__ == "__main__":
    run_ingestion()