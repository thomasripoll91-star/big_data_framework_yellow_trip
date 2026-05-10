# Projet Data Engineering : Plateforme NYC Taxi

Ce dépôt contient l’infrastructure et le code pour une plateforme de données basée sur l’architecture **médaillon (Bronze / Silver / Gold)**, traitant les données open-source des taxis new-yorkais (TLC Trip Record Data).


## 1. Installation de l’environnement (Ubuntu / WSL)

Pour exécuter ce projet, vous avez besoin de **Java** (requis par Spark) et **Python**.

### A. Dépendances système

```bash
sudo apt update
sudo apt install default-jdk python3-pip -y
```

### B. Installation d’Apache Spark

```bash
wget https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
tar -xvf spark-3.5.1-bin-hadoop3.tgz
sudo mv spark-3.5.1-bin-hadoop3 /opt/spark
```

### C. Variables d’environnement

```bash
echo 'export SPARK_HOME=/opt/spark' >> ~/.bashrc
echo 'export PATH=$PATH:$SPARK_HOME/bin' >> ~/.bashrc
source ~/.bashrc
```

### D. Librairies Python

```bash
pip3 install pyspark jupyterlab pandas pyarrow --break-system-packages
```

## 2. Lancement de Jupyter Lab

### 2.1. Lancer le serveur

```bash
jupyter lab
```

### 2.2 Accéder à l’interface

Un lien apparaît dans le terminal, par exemple :

```
http://localhost:8888/lab?token=...
```

👉 Copier-coller ce lien dans votre navigateur.

### 3. Utilisation

* Créer un notebook `.ipynb`
* Sélectionner le kernel Python
* Exemple :

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Test").getOrCreate()
df = spark.read.parquet("dataset/yellow_tripdata_2025-01.parquet")
df.show()
```

###  Astuce

Si le navigateur ne s’ouvre pas automatiquement :

```bash
jupyter lab --no-browser
```


### 🔍 Phase d’Exploration (Jupyter Notebooks)

```python
df = spark.read.parquet("dataset/yellow_tripdata_2025-01.parquet")
df.show()
```

---

### ⚡ Phase de Production (Scripts paramétrables)

Les scripts :

* Ne contiennent **aucun chemin en dur**
* S’exécutent via le terminal

#### Exemple : ingestion des données

```bash
spark-submit scripts/feeder.py dataset/yellow_tripdata_2025-01.parquet raw/
```


##  Bonnes pratiques respectées

* Architecture **médaillon (Bronze / Silver / Gold)**
* Séparation **exploration / production**
* Scripts **paramétrables**
* Utilisation de **Spark pour le traitement distribué**
* Format **Parquet optimisé**



##  Objectif

Construire une plateforme de données scalable permettant :

* L’ingestion de données massives
* Leur transformation fiable
* La création de datamarts analytiques exploitables


