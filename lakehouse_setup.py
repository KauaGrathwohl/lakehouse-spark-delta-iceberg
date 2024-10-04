from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Configurar Spark com Delta Lake

builder = SparkSession.builder \
    .appName("Lakehouse") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Criar dados de exemplo
data = [
    (1, "Alice", 29),
    (2, "Bob", 31),
    (3, "Cathy", 25)
]

columns = ["id", "name", "age"]

df = spark.createDataFrame(data, columns)

# Salvar dados no Delta Lake
df.write.format("delta").mode("overwrite").save("/tmp/delta-table")

# Salvar dados no Apache Iceberg
df.write.format("iceberg").mode("overwrite").save("/tmp/iceberg-table")