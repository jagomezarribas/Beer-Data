from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_set

#Creamos una sesión de Spark
sc = SparkSession.builder.appName("Cerveceras").getOrCreate()

#Cargamos el archivo JSON en un DataFrame
path = "beeradvocate_note.json"
df = sc.read.json(path)

#Agrupamos por cerveceras y hacemos un listado sin repeticiones de las cervezas creadas por esa cervecera
df_final = df.groupBy("beer/brewerId").agg(collect_set("beer/name").alias("beers/brewerId"))

df_final.show(truncate=False)

sc.stop()