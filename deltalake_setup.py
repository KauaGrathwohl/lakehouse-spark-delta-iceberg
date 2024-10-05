# configuracoes delta

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from delta import *

# cria uma sessao spark
spark = (
    SparkSession
    .builder
    .appName("MovieLensDelta")
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# define o esquema da tabela de filmes
movie_schema = StructType([
    StructField("movieId", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("genres", StringType(), True)
])

movies_path = "movies.csv" 

# cria a tabela delta
(
    spark.read
    .csv(movies_path, header=True, schema=movie_schema)
    .write
    .format("delta")
    .mode('overwrite')
    .save("./delta/movies")
)

# consulta a tabela delta criada
deltaTable = DeltaTable.forPath(spark, "./delta/movies")
deltaTable.toDF().show()

# inserir novo filme
new_movie = [(9999, "Novo Filme", "Sci-Fi")]
new_movie_df = spark.createDataFrame(new_movie, movie_schema)
deltaTable.alias("movies").merge(
    new_movie_df.alias("new_movies"),
    "movies.movieId = new_movies.movieId"
).whenNotMatchedInsertAll().execute()

# atualizar genero de filme
deltaTable.update(
    set={"genres": "Drama"},
    condition="title = 'Old Movie'"
)

# deletando um filme
deltaTable.delete("movieId = 9999")

# consulta na tabela
deltaTable.toDF().show()
