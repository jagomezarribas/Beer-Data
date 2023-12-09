from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as spark_round

#Creamos una sesión de Spark
sc = SparkSession.builder.appName("NoteJSON").getOrCreate()

#Cargamos el archivo JSON en un DataFrame
path = "beeradvocate.json"
df_originial = sc.read.json(path)

#Sacamos nota aplicando porcentajes
def nota_cerveza():
    return spark_round((col("review/appearance")*0.06  + col("review/aroma")*0.24 + col("review/palate")*0.10 + col("review/taste")*0.40 + col("review/overall")*0.20), 2)

#Introducimos nuevo atributo al json con la nota
df_con_nota = df_originial.withColumn("review/note", nota_cerveza().cast("string"))

atributos_eliminar = ["review/appearance", "review/aroma", "review/palate", "review/taste", "review/overall"]

#Eliminamos los atributos que no queremos de la review
df_sin_atributos = df_con_nota.drop(*atributos_eliminar)

#Ordenamos los atributos en el orden que queremos
atributos_ordenados = ["beer/name", "beer/beerId", "beer/brewerId", "beer/ABV", "beer/style", "review/note", "review/time", "review/profileName", "review/text"]

#Creamos json final
df_final = df_sin_atributos.select(atributos_ordenados)

new_path = "beeradvocate_note.json"

#Escribimos y guardamos
df_final.write.json(new_path, mode='overwrite')

sc.stop()
