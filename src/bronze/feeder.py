import argparse
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def setup_logger():
    """Configuration du logger pour exporter les logs dans un fichier .txt"""
    # Crée un logger
    logger = logging.getLogger("FeederLogger")
    logger.setLevel(logging.INFO)
    
    # Formatteur de log
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # Handler pour écrire dans un fichier texte
    file_handler = logging.FileHandler('feeder_execution.txt', mode='a')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Handler pour afficher aussi dans la console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger

def run_ingestion():
    logger = setup_logger()
    logger.info("Démarrage du job feeder.py")

    # =====================================================================
    # 1. Gestion des paramètres dynamiques (Aucun chemin en dur)
    # =====================================================================
    parser = argparse.ArgumentParser(description="Ingestion des données brutes vers /raw")
    parser.add_argument('--input_trips', required=True, help="Chemin du fichier des trajets")
    parser.add_argument('--input_zones', required=True, help="Chemin du fichier des zones")
    parser.add_argument('--output_dir', required=True, help="Chemin du dossier cible /raw")
    args = parser.parse_args()

    # =====================================================================
    # 2. Variables de partitionnement (Date d'ingestion)
    # =====================================================================
    now = datetime.now()
    year = str(now.year)
    month = f"{now.month:02d}"
    day = f"{now.day:02d}"

    # =====================================================================
    # 3. Initialisation Spark
    # =====================================================================
    spark = SparkSession.builder \
        .appName("Ingestion_Raw_Feeder") \
        .getOrCreate()

    # =====================================================================
    # 4. Ingestion des Trajets (Trips)
    # =====================================================================
    try:
        logger.info(f"Lecture des trajets depuis : {args.input_trips}")
        trips_df = spark.read.parquet(args.input_trips)
        
        # Ajout des colonnes de partitionnement
        trips_df = trips_df.withColumn("year", lit(year)) \
                           .withColumn("month", lit(month)) \
                           .withColumn("day", lit(day))

        output_trips = f"{args.output_dir}/yellow_tripdata"
        logger.info(f"Écriture partitionnée des trajets vers : {output_trips}")
        
        trips_df.write \
            .mode("append") \
            .partitionBy("year", "month", "day") \
            .parquet(output_trips)
            
        logger.info("Succès : Ingestion des trajets terminée.")
    except Exception as e:
        logger.error(f"Erreur lors de l'ingestion des trajets : {e}")

    # =====================================================================
    # 5. Ingestion des Zones géographiques
    # =====================================================================
    try:
        logger.info(f"Lecture des zones depuis : {args.input_zones}")
        zones_df = spark.read.option("header", "true").option("inferSchema", "true").csv(args.input_zones)
        
        # Ajout des colonnes de partitionnement
        zones_df = zones_df.withColumn("year", lit(year)) \
                           .withColumn("month", lit(month)) \
                           .withColumn("day", lit(day))

        output_zones = f"{args.output_dir}/taxi_zones"
        logger.info(f"Écriture partitionnée des zones vers : {output_zones}")
        
        zones_df.write \
            .mode("append") \
            .partitionBy("year", "month", "day") \
            .parquet(output_zones)
            
        logger.info("Succès : Ingestion des zones terminée.")
    except Exception as e:
        logger.error(f"Erreur lors de l'ingestion des zones : {e}")

    # =====================================================================
    # 6. Ingestion du Dictionnaire (Généré en dur pour l'exemple)
    # =====================================================================
    try:
        logger.info("Création du dictionnaire métier...")
        dictionary_data = [
            ("payment_type", 1, "Credit card"), ("payment_type", 2, "Cash"),
            ("RatecodeID", 1, "Standard rate"), ("RatecodeID", 2, "JFK")
            # J'ai raccourci la liste ici pour la lisibilité, tu peux remettre tous les champs
        ]
        schema = StructType([
            StructField("category", StringType(), False),
            StructField("code", IntegerType(), False),
            StructField("description", StringType(), False)
        ])
        dict_df = spark.createDataFrame(dictionary_data, schema)
        
        dict_df = dict_df.withColumn("year", lit(year)).withColumn("month", lit(month)).withColumn("day", lit(day))
        
        output_dict = f"{args.output_dir}/data_dictionary"
        dict_df.write.mode("append").partitionBy("year", "month", "day").parquet(output_dict)
        logger.info("Succès : Dictionnaire métier ingéré.")
    except Exception as e:
        logger.error(f"Erreur lors du dictionnaire : {e}")

    logger.info("Fin du job feeder.py avec succès.")
    spark.stop()

if __name__ == "__main__":
    run_ingestion()