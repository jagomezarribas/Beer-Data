from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, collect_list, desc, round as spark_round
import sys

spark = SparkSession.builder.appName("ConsultaCerveza").getOrCreate()

if len(sys.argv) < 2:
    print("Introduzca el nombre de la cerveza")
    sys.exit(1)

nombre_cerveza = sys.argv[1]
df = spark.read.json("beeradvocate.json")

df_filtrado = df.filter(col("beer/name") == nombre_cerveza)

df_notas_medias = df_filtrado.select(spark_round(avg("review/overall"), 2).alias("beers_average"),
                                     col("beer/ABV").alias("volumen"),
                                     col("beer/brewerId").alias("cervecera"))

df_mejor_nota = df_filtrado.select(spark_round(max("review/overall"), 2).alias("best_beer_grade"),
                                     col("review/text").alias("Critica"),
                                     col("review/profileName").alias("Usuario"))

df_peor_nota = df_filtrado.select(spark_round(min("review/overall"), 2).alias("worst_beer_grade"),
                                     col("review/text").alias("Critica"),
                                     col("review/profileName").alias("Usuario"))
#.collect?

if df_notas_medias:
    print(f"Media de la nota para {nombre_cerveza}: {df_notas_medias[0]['beers_averege']}")
    print(f"Volumen de la cerveza: {df_notas_medias[0]['volumen']}")
    print(f"Cervecera: {df_notas_medias[0]['cervecera']}")
    print(f"Mejor Crítica: {df_mejor_nota[0]['Critica']}")
    print(f"Autor de la mejor Crítica: {df_mejor_nota[0]['Usuario']}")
    print(f"Peor Crítica: {df_mejor_nota[0]['Critica']}")
    print(f"Autor de la peor Crítica: {df_peor_nota[0]['Usuario']}")

else:
    print(f"No se encontraron datos para la cerveza {nombre_cerveza}")

spark.stop()