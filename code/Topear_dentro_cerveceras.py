"""
El usuario mete el id de la cervecera que le interesa y se le topean las cervezas de esa cervecera
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, collect_list, desc,  round as spark_round
import sys

#Creamos una sesión de Spark
sc = SparkSession.builder.appName("Topear_dentro_cerveceras").getOrCreate()

#Leemos el id de la cervecera en la que estamos interesados
cervecera = sys.argv[1]

#Cargamos el archivo JSON en un DataFrame
path = "beeradvocate_note.json"
df = sc.read.json(path)

#Añadimos una columna en la que pasamos la "review/note" a float para trabajar con sus datos como unidades
df = df.withColumn("review/note", col("review/note").cast("float"))

#Seleccionamos solos las cervezas con el id de la cervecera que nos interesa
df_filtrado = df.filter(df["beer/brewerId"] == cervecera)

#Calculamos la nota media por cerveza
df_notas_medias = df_filtrado.groupBy("beer/name").agg(spark_round(avg("review/note"), 2).alias("beers_average"))

#Ordenamos por la nota media
df_ordenado = df_notas_medias.orderBy(desc("beers_average"))

df_ordenado.show(truncate=False)

sc.stop()
