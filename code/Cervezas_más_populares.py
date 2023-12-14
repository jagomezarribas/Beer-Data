from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, desc, max, min, first, round as spark_round
import sys

#Creamos una sesión de Spark
sc = SparkSession.builder.appName("Popularidad").getOrCreate()

#Cargamos el archivo JSON en un DataFrame
df = sc.read.json(sys.argv[1])

#Agrupamos por nombre y sacamos nota media con todas las reviews
df_con_medias = df.groupBy("beer/name").agg(spark_round(avg("review/note"), 2).alias("beer_average"))

#Contamos las veces que aparece cada cerveza
df_conteo = df.groupBy("beer/name").agg(count("*").alias("beer/count"))

#Sacamos la nota maxima y minima que cada cerveza ha obtenido
df_max_note = df.groupBy("beer/name").agg(max(col("review/note")).alias("beer/max_note"))
df_min_note = df.groupBy("beer/name").agg(min(col("review/note")).alias("beer/min_note"))

#Renombramos los atributos para evitar ambiguedades en el join
df_max_note = df_max_note.withColumnRenamed("beer/name", "beer/max_name")
df_min_note = df_min_note.withColumnRenamed("beer/name", "beer/min_name")

# Unir los DataFrames que contienen las notas máximas y mínimas con el DataFrame original
df_max_review = df.join(df_max_note, (df["beer/name"] == df_max_note["beer/max_name"]) & (df["review/note"] == df_max_note["beer/max_note"])) \
                    .groupBy("beer/name").agg(first("review/text").alias("beer/best_text"), first("beer/max_note").alias("beer/max_note"))
                    
df_min_review = df.join(df_min_note, (df["beer/name"] == df_min_note["beer/min_name"]) & (df["review/note"] == df_min_note["beer/min_note"])) \
                    .groupBy("beer/name").agg(first("review/text").alias("beer/worst_text"), first("beer/min_note").alias("beer/min_note"))


#Unimos para juntar la nota media con las veces que se ha hecho una review de la cerveza (count)
df_join = df_conteo.join(df_con_medias.select("beer/name", "beer_average"), "beer/name")


#VERSIÓN PARA MOSTRAR EL RESULTADO EN UNA TABLA
#Hacemos join para juntar las mejor y la peor review con sus datos correspondientes
df_max_min_review = df_max_review.join(df_min_review.select("beer/name", "beer/min_note", "beer/worst_text"), "beer/name")

#Hacemos join para juntar los datos de las reviews con la nota media y el count y ordenamos por count
df_final = df_join.join(df_max_min_review.select("beer/name", "beer/max_note", "beer/best_text", "beer/min_note", "beer/worst_text"), "beer/name").orderBy(desc("beer/count"))
df_final.coalesce(1).write.options(header = 'True', delimiter = ',').mode("overwrite").csv(sys.argv[2] + "Tabla_única")
df_final.show(truncate=False)

"""
#VERSIÓN PARA MOSTRAR EL RESULTADO EN DOS TABLAS(UNA PARA MEJOR REVIEW Y OTRA PARA LA PEOR)

#Hacemos join para juntar con las mejores reviews con la nota media y el count y ordenado por count
df_final_max = df_join.join(df_max_review.select("beer/name", "beer/max_note", "beer/best_text"), "beer/name").orderBy(desc("beer/count"))

#Hacemos join para juntar con las peores reviews con la nota media y el count y ordenado por count
df_final_min = df_join.join(df_min_review.select("beer/name", "beer/min_note", "beer/worst_text"), "beer/name").orderBy(desc("beer/count"))

df_final_max.coalesce(1).write.options(header = 'True', delimiter = ',').mode("overwrite").csv(sys.argv[2] + "Tabla_max")
df_final_min.coalesce(1).write.options(header = 'True', delimiter = ',').mode("overwrite").csv(sys.argv[2] + "Tabla_min")

df_final_max.show(truncate=False)
df_final_min.show(truncate=False)
"""

sc.stop()
