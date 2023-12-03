from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_set

sc = SparkSession.builder.appName("Cerveceras").getOrCreate()

path = "beeradvocate_note.json"

df = sc.read.json(path)

#Agrupar por cerveceras y hacer un listado sin repeticiones de las cervezas creadas por esa cervecera
df_final = df.groupBy("beer/brewerId").agg(collect_set("beer/name").alias("beers/brewerId"))

df_final.show(truncate=False)

sc.stop()