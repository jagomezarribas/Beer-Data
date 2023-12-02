from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("BeerAnalysis").getOrCreate()

path = "beeradvocate.json"
df = spark.read.json(path)

selected_columns = ["beer/beerId", "beer/name", "review/overall", "review/text"]
df_selected = df.select(selected_columns)

df_selected = df_selected.withColumn("review/overall", col("review/overall").cast("float"))

worst_beer = df_selected.orderBy("review/overall").limit(1).collect()[0]

# analiza palabras clave para determinar si es buena o mala
keywords_good = ["good", "excellent", "pleasant", "delicious"]
keywords_bad = ["bad", "poor", "unpleasant", "disappointing"]

review_text = worst_beer['review/text'].lower()

if any(keyword in review_text for keyword in keywords_good):
    print("La cerveza podría ser considerada buena.")
elif any(keyword in review_text for keyword in keywords_bad):
    print("La cerveza podría ser considerada mala.")
else:
    print("No se encontraron palabras clave para determinar si es buena o mala.")
