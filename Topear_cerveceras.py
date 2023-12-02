from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, collect_list, desc

# Crear una sesión de Spark
sc = SparkSession.builder.appName("ListaCervezas").getOrCreate()

# Cargar el archivo JSON en un DataFrame
path = "beeradvocate_note.json"
df = sc.read.json(path)

df = df.withColumn("review/note", col("review/note").cast("float"))

# Calcular la nota media por cerveza
df_notas_medias = df.groupBy("beer/name", "beer/brewerId").agg(avg("review/note").alias("beers_average"))

# Agrupar por "beer/brewerId" y obtener una lista de "beer/name" ordenadas por nota media
df_agrupado = df_notas_medias.groupBy("beer/brewerId") \
                .agg(collect_list("beer/name").alias("beer_names"), avg("beers_average").alias("brewer_average")) \
                .orderBy(desc("brewer_average"))
                

df_agrupado.show(truncate=False)


sc.stop()
