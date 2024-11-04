from os import path, environ
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
)
from pyspark.sql import functions as F
from dotenv import load_dotenv

# ENVIRONMENT VARIABLES
load_dotenv()
AZURE_ACCOUNT_NAME = environ.get("AZURE_ACCOUNT_NAME")
AZURE_ACCOUNT_KEY = environ.get("AZURE_ACCOUNT_KEY")
AZURE_CONTAINER_NAME = environ.get("AZURE_CONTAINER_NAME")
AWS_ACCESS_KEY_ID = environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = environ.get("AWS_SECRET_ACCESS_KEY")
S3_BUCKET_NAME = environ.get("S3_BUCKET_NAME")

# DIRECTORIES
DIR = path.dirname(__file__)
DATA_DIRECTORY = path.abspath(path.join(DIR, "../data"))

# INPUT FILES
GEO_CATALOG_FILE = path.join(DATA_DIRECTORY, "Catálogo de relación geográfica.csv")
PRIMERA_INGESTA_FILE = path.join(DATA_DIRECTORY, "Primera ingesta.csv")
SEGUNDA_INGESTA_FILE = path.join(DATA_DIRECTORY, "Segunda ingesta.csv")

# OUTPUT FILES
EJERCICIO1_FILE = path.join(DATA_DIRECTORY, "Ejercicio_1.csv")
EJERCICIO2_FILE = path.join(DATA_DIRECTORY, "Ejercicio_2.csv")
VENTAS_POR_PAIS_FILE = path.join(DATA_DIRECTORY, "ventas_por_pais.csv")

spark = SparkSession.builder.appName("PruebaTecnica").getOrCreate()

## Ejercicio 1 #####################################################################################################
schema = StructType(
    [
        StructField("Venta", DoubleType(), True),
        StructField("Timestamp", TimestampType(), True),
        StructField("Ticket", IntegerType(), True),
        StructField("Estado/Provincia", StringType(), True),
    ]
)

df_geo_rel = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv(GEO_CATALOG_FILE)
    .withColumnRenamed("Estado/Provincia", "Estado_Provincia")
)

# Clean Primera Ingesta data
df_ingesta = (
    spark.read.option("header", True)
    .schema(schema)
    .option("timestampFormat", "dd/MM/yyyy HH:mm")
    .csv(PRIMERA_INGESTA_FILE)
    .withColumnRenamed("Estado/Provincia", "Estado_Provincia")
    .withColumn(
        "Timestamp", F.from_utc_timestamp("Timestamp", "UTC-6")
    )  # Transform UTC -> UTC-6
    # Define Primary Key using tickeit ID, local time stamp and Estado provicia
    .withColumn(
        "primary_key",
        F.concat_ws(
            "_", F.col("Ticket"), F.col("Timestamp"), F.col("Estado_Provincia")
        ),
    )
)

# Remove Duplicates
df_agg = df_ingesta.groupBy(
    "primary_key", "Ticket", "Timestamp", "Estado_Provincia"
).agg(F.sum("Venta").alias("Total_Venta"))

# Left join of data frames
df_complete = df_agg.join(
    df_geo_rel, df_agg.Estado_Provincia == df_geo_rel.Estado_Provincia, "left"
).select(df_agg["*"], df_geo_rel["País"], df_geo_rel["Continente"])

# Summarize sales per country
df_vts_pais = (
    df_complete.withColumn("fecha", F.to_date("Timestamp"))
    .groupBy("fecha", "País")
    .agg(F.sum("Total_Venta").alias("Total_Venta"))
)

# Load Population
df_pop = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("country_population.csv")
)

ventas_por_pais = df_vts_pais.join(
    df_pop, df_vts_pais.País == df_pop.País, "left"
).select(df_vts_pais["*"], df_pop["Población"])

ventas_por_pais = ventas_por_pais.withColumn(
    "Venta_por_hab", ventas_por_pais.Total_Venta / ventas_por_pais.Población
).withColumn("Total_Venta", ventas_por_pais.Total_Venta * 1000000)

df_pandas = ventas_por_pais.toPandas()
df_pandas.to_csv(EJERCICIO1_FILE, index=False)

# Ejercicio 2############################################################################################################
df_segunda = (
    spark.read.option("header", True)
    .schema(schema)
    .option("timestampFormat", "dd/MM/yyyy HH:mm")
    .csv(SEGUNDA_INGESTA_FILE)
    .withColumnRenamed("Estado/Provincia", "Estado_Provincia")
    .withColumn("Timestamp", F.from_utc_timestamp("Timestamp", "UTC-6"))
    .withColumn(
        "primary_key",
        F.concat_ws(
            "_", F.col("Ticket"), F.col("Timestamp"), F.col("Estado_Provincia")
        ),
    )
)

df_segunda_filter = df_segunda.join(df_ingesta, on="primary_key", how="left_anti")

df_segunda_agg = df_segunda_filter.groupBy(
    "primary_key", "Ticket", "Timestamp", "Estado_Provincia"
).agg(F.sum("Venta").alias("Total_Venta"))

df_ventas_segunda = df_segunda_agg.join(
    df_geo_rel, df_segunda_agg.Estado_Provincia == df_geo_rel.Estado_Provincia, "left"
).select(df_segunda_agg["*"], df_geo_rel["País"], df_geo_rel["Continente"])

df_segunda_pais = (
    df_ventas_segunda.withColumn("fecha", F.to_date("Timestamp"))
    .groupBy("fecha", "País")
    .agg(F.sum("Total_Venta").alias("Total_Venta"))
)


df_ventas_all = df_segunda_pais.union(df_vts_pais)

ventas_pais_todas_ingestas = df_ventas_all.join(
    df_pop, df_ventas_all.País == df_pop.País, "left"
).select(df_ventas_all["*"], df_pop["Población"])

ventas_pais_todas_ingestas = ventas_pais_todas_ingestas.groupBy(
    "fecha", "País", "Población"
).agg(F.sum("Total_Venta").alias("Total_Venta"))

ventas_pais_todas_ingestas = ventas_pais_todas_ingestas.withColumn(
    "Venta_por_hab",
    ventas_pais_todas_ingestas.Total_Venta / ventas_pais_todas_ingestas.Población,
).withColumn("Total_Venta", ventas_pais_todas_ingestas.Total_Venta * 1000000)

df_pd_segunda = ventas_pais_todas_ingestas.toPandas()
df_pd_segunda.to_csv(EJERCICIO2_FILE, index=False)

# Seccion 4 - Subir a la nube: AWS ####################################################################################
import boto3

s3 = boto3.client("s3")

try:
    s3.create_bucket(
        Bucket=S3_BUCKET_NAME,
        CreateBucketConfiguration={"LocationConstraint": "us-west-1"},
    )
except Exception as e:
    print(f"Bucket creation failed: {e}")

ventas_pais_todas_ingestas.write.csv(VENTAS_POR_PAIS_FILE, header=True)

s3.upload_file(
    VENTAS_POR_PAIS_FILE, S3_BUCKET_NAME, path.basename(VENTAS_POR_PAIS_FILE)
)

# CON AZURE #########################################################################################################
from azure.storage.blob import BlobServiceClient


blob_service_client = BlobServiceClient.from_connection_string(
    f"DefaultEndpointsProtocol=https;AccountName={AZURE_ACCOUNT_NAME};AccountKey={AZURE_ACCOUNT_KEY};EndpointSuffix=core.windows.net"
)

try:
    blob_service_client.create_container(AZURE_CONTAINER_NAME)
except Exception as e:
    print(f"Container creation failed: {e}")


ventas_pais_todas_ingestas.write.csv(VENTAS_POR_PAIS_FILE, header=True)

blob_client = blob_service_client.get_blob_client(
    container=AZURE_CONTAINER_NAME, blob=path.basename(VENTAS_POR_PAIS_FILE)
)

with open(VENTAS_POR_PAIS_FILE, "rb") as data:
    blob_client.upload_blob(data)
