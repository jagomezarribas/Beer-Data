from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, desc, round as spark_round

#Creamos una sesión de Spark
sc = SparkSession.builder.appName("Topear").getOrCreate()

#Cargamos el archivo JSON en un DataFrame
path = "beeradvocate_note.json"
df = sc.read.json(path)

#Agrupamos por nombre y sacamos nota media con todas las reviews
df_con_medias = df.groupBy("beer/name").agg(spark_round(avg("review/note"), 2).alias("note_average"))

#Ordenamos por la nota media
df_final = df_con_medias.orderBy(desc("note_average"))

df_final.show(df_final.count(), truncate=False)

sc.stop()
