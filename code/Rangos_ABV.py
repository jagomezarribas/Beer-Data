"""
El usuario introduce un Rango de % de alcohol y se le topean las cervezas dentro de ese rango
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, floor, avg, desc, round as spark_round
import sys

#Creamos una sesión de Spark
sc = SparkSession.builder.appName("Rangos_ABV").getOrCreate()

#Leemos el ABV en el que estamos interesados
ABV = sys.argv[1]

#Cargamos el archivo JSON en un DataFrame
path = "beeradvocate_note.json"
df = sc.read.json(path)

#Creamos columna que es el ABV de la cerveza pero redondeado a la unidad con floor (x,2->x, x'7->x)
df_rango = df.withColumn("rango_ABV", (floor(col("beer/ABV"))).cast("float"))

#Seleccionamos solos las cervezas con ABV en el que estamos interesados
df_filtrado = df_rango.filter(df_rango["rango_ABV"] == ABV)

#Calculamos la nota media por cerveza
df_notas_medias = df_filtrado.groupBy("beer/name").agg(spark_round(avg("review/note"), 2).alias("beers_average"))

#Unimos df_notas_medias con el DataFrame original para obtener la columna "beer/ABV" de cada cerveza
df_join = df_notas_medias.join(df_rango.select("beer/name", "beer/ABV"), "beer/name").orderBy(desc("beers_average"))

#Eliminamos duplicados basados en la columna "beer/name"
df_no_duplicados = df_join.dropDuplicates(["beer/name"])

#Ordenamos por la nota media de las cervezas en orden descendente
df_final = df_no_duplicados.orderBy(desc("beers_average"))

df_final.show(truncate=False)

sc.stop()
