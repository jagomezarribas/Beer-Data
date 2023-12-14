from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, year, col, desc, avg, collect_list, round as spark_round, struct, slice, concat_ws, transform
import sys

#Configuración de la sesión de Spark
sc = SparkSession.builder.appName("FavoriteBeerByYear").getOrCreate()

#Cargamos el archivo JSON en un DataFrame
df = sc.read.json(sys.argv[1])

#Convertimos a float la columna reivew/note para luego poder hacer la nota media
df = df.withColumn("review/note", col("review/note").cast("float"))

df_timestamp = df.select("review/time", "beer/style", "review/note")

#Función para convertir el review/time en formato Unix a una fecha legible
def convert_unix_timestamp(timestamp):
    if timestamp is not None:
        return datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    else:
        return None

convert_timestamp_udf = udf(convert_unix_timestamp)

#Convertimos toda la columna de "review/time"
df_converted = df_timestamp.withColumn("review/readable_time", convert_timestamp_udf(df_timestamp["review/time"].cast("bigint")))

#Filtramos las fechas por si alguna es null
df_filter = df_converted.filter(col("review/readable_time").isNotNull())

#Convertimos la columna "review/time" a formato de fecha cogiendo el año
df_converted_year = df_filter.withColumn("review/year", year("review/readable_time"))

#Agrupamos por año y tipo de cerveza, y calculamos la media de las notas
df_beer = df_converted_year.groupBy("review/year", "beer/style").agg(spark_round(avg("review/note"), 2).alias("style_average"))

#Creamos una columna stuct (style_average, beer/style) y la ordenamos de mayor a menor nota media
df_struct = df_beer.withColumn("ordered_styles", struct(col("style_average"), col("beer/style"))).orderBy(desc("style_average"))

#Agrupamos por año, recogemos todas las cervezas en una lista y seleccionamos solo las 5 mejores (que serán las primeras)
df_top_beers = df_struct.groupBy("review/year").agg(slice(collect_list("ordered_styles"), 1, 5).alias("top_beers"))

#Convertimos cada estructura en la columna "top_beers" a una cadena
df_top_beers = df_top_beers.withColumn(
    "top_beers", 
    transform(
        col("top_beers"), 
        lambda s: concat_ws(": ", s["style_average"].cast("string"), s["beer/style"])
    )
)

#Convertimos la columna "top_beers" a una cadena
df_top_beers = df_top_beers.withColumn("top_beers", concat_ws(", ", col("top_beers")))

#Ordenamos por año
df_final = df_top_beers.orderBy(desc("review/year"))

#Guardamos el Dataframe en un csv
df_final.coalesce(1).write.options(header = 'True', delimiter = ',').mode("overwrite").csv("sys.argv[2]/")

df_final.show(df_final.count(), truncate=False)

sc.stop()
