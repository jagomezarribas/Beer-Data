from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, desc, round as spark_round

sc = SparkSession.builder.appName("Topear").getOrCreate()

path = "beeradvocate_note.json"

df = sc.read.json(path)

#Agrupar por nombre y sacar nota media con todas las reviews
df_con_medias = df.groupBy("beer/name").agg(spark_round(avg("review/note"), 2).alias("note_average"))

#Ordenar por la nota media
df_final = df_con_medias.orderBy(desc("note_average"))

df_final.show(truncate=False)

sc.stop()