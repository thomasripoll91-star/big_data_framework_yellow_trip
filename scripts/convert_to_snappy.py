# -*- coding: utf-8 -*-

import argparse
from pathlib import Path
import sys

import pyarrow.parquet as pq


def convert_parquet_to_snappy(input_file: Path, output_file: Path, batch_size: int = 100_000):
    if not input_file.exists():
        print("ERREUR : le fichier d'entree n'existe pas.")
        print(f"Chemin cherche : {input_file}")
        sys.exit(1)

    output_file.parent.mkdir(parents=True, exist_ok=True)

    print("============================================")
    print(" CONVERSION PARQUET ZSTD -> PARQUET SNAPPY")
    print("============================================")
    print(f"Fichier source      : {input_file}")
    print(f"Fichier destination : {output_file}")
    print("")

    parquet_file = pq.ParquetFile(input_file)
    schema_original = parquet_file.schema_arrow

    print("Schema original detecte :")
    print(schema_original)
    print("")

    total_rows = 0

    writer = pq.ParquetWriter(
        where=str(output_file),
        schema=schema_original,
        compression="snappy",
        use_dictionary=True,
        write_statistics=True
    )

    try:
        for batch in parquet_file.iter_batches(batch_size=batch_size):
            writer.write_batch(batch)
            total_rows += batch.num_rows
            print(f"Lignes converties : {total_rows}")
    finally:
        writer.close()

    print("")
    print("Conversion terminee.")
    print(f"Nombre total de lignes converties : {total_rows}")
    print("")

    print("Verification du fichier converti...")
    converted_file = pq.ParquetFile(output_file)
    schema_converti = converted_file.schema_arrow

    if schema_original.equals(schema_converti):
        print("OK : le schema est identique.")
    else:
        print("ATTENTION : le schema semble different.")
        print("Schema original :")
        print(schema_original)
        print("Schema converti :")
        print(schema_converti)

    print("")
    print("Verification de la compression :")

    if converted_file.metadata.num_row_groups > 0:
        row_group = converted_file.metadata.row_group(0)
        for i in range(row_group.num_columns):
            column = row_group.column(i)
            print(f"- {column.path_in_schema} : {column.compression}")

    print("")
    print("FICHIER PRET POUR SPARK :")
    print(output_file)


def main():
    parser = argparse.ArgumentParser(
        description="Convertit yellow_tripdata_2025-02.parquet en Parquet Snappy lisible par Spark 3.0 Hadoop 3.2."
    )

    parser.add_argument(
        "--input",
        default=r"C:\Users\binto\Desktop\test_projet_taxi\data\raw_input\yellow_tripdata_2025-02.parquet",
        help="Chemin du fichier Parquet original compresse en ZSTD."
    )

    parser.add_argument(
        "--output",
        default=r"C:\Users\binto\Desktop\test_projet_taxi\data\raw_input\yellow_tripdata_2025-02-snappy.parquet",
        help="Chemin du fichier Parquet de sortie compresse en Snappy."
    )

    args = parser.parse_args()

    input_file = Path(args.input)
    output_file = Path(args.output)

    convert_parquet_to_snappy(input_file, output_file)


if __name__ == "__main__":
    main()