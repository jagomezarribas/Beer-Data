from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, collect_list, desc,  round as spark_round
import sys

# Crear una sesión de Spark
sc = SparkSession.builder.appName("ListaCervezas").getOrCreate()

cervecera = sys.argv[1]

# Cargar el archivo JSON en un DataFrame
path = "beeradvocate_note.json"
df = sc.read.json(path)

df = df.withColumn("review/note", col("review/note").cast("float"))

#Seleccionamos solos las cervezas con el id de la cervecera que nos interesa
df_filtrado = df.filter(df["beer/brewerId"] == cervecera)

# Calcular la nota media por cerveza
df_notas_medias = df_filtrado.groupBy("beer/name").agg(spark_round(avg("review/note"), 2).alias("beers_average"))

#Ordenar por la nota media
df_ordenado = df_notas_medias.orderBy(desc("beers_average"))

df_ordenado.show(truncate=False)

sc.stop()
