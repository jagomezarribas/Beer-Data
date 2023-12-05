from pyspark.sql import SparkSession
from pyspark.sql.functions import col, floor, avg, desc, round as spark_round
import sys

#Creamos una sesión de Spark
sc = SparkSession.builder.appName("Rangos_ABV").getOrCreate()

#Cargamos el archivo JSON en un DataFrame
path = "beeradvocate_note.json"
df = sc.read.json(path)

#Creamos columna que es el ABV de la cerveza pero redondeado a la unidad con floor (x,2->x, x'7->x)
#df_rango = df.withColumn("rango_note", (floor(col("review/note"))).cast("float"))

#Calculamos la nota media por cerveza
df_notas_medias = df.groupBy("beer/name").agg(spark_round(avg("review/note"), 2).alias("beers_average"))


#Ordenamos por la nota media de las cervezas en orden descendente
df_final = df_notas_medias.orderBy(desc("beers_average"))

for i in range(5):
    df_fx = df_final.filter((df_final['beers_average'] >= i) & (df_final['beers_average'] <= i+1))
    df_fx.show(df_fx.count(), truncate=False)

sc.stop()