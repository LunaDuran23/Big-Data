#Librerias
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType


spark = SparkSession.builder.getOrCreate()

"""Por Genero ¿Cual es el proemdio dela puntuacion de los videojuegos  y el promedio de numero de 
caracteres  de las opiniones de los usuarios """

#Para cargar el dataset utilizamos los siguientes comandos para que no tomara los elementos dentro d euna lista que estaban 
# divididos por una coma, y los saltos de lines.
df_games = spark.read.option("quote", "\"") \
                     .option("escape", "\"") \
                     .option("multiline", True) \
                    .csv('input/games.csv', sep = ",", header='true')

#Con este comando podemos ver el esquema deldataset
#df_games.printSchema()

# Inicialmente volvemos la columna genero en una lista de strings, ya que inicialmente es un string.
# Aca se genera una (TRANSFORMACIÓN)
df_games = df_games.withColumn("Genres", F.split(F.regexp_replace("Genres", r"[\[\]']", ""), ", "))


#Inicialmente volvemos la columna reviews en una lista de strings, ya que inicialmente es un string
# generando una  (TRANSFORMACIÓN)
df_games = df_games.withColumn("Reviews", F.split(F.regexp_replace("Reviews", r"[\[\]']", ""), ", "))


#con explode dividimos la lista para cada uno de los otros datos del dataset (TRANSFORMACIÓN)
df_games = df_games.withColumn("Genres",F.explode(df_games.Genres))
df_games = df_games.withColumn("Reviews",F.explode(df_games.Reviews))

#Generamos una nueva columna con la longitud de los reviews (TRANSFORMACIÓN)
df_games = df_games.withColumn("length",F.length(F.col("Reviews")))
# Con show() podemos ver el data set y esta es una ACCION
#df_games.show(5)

#Agrupamos por el genero y sacamos el promedio del reiting y el promedio de la longitud de las opiniones d elos usuarios.
# GroupBy (TRANSFORMACIÓN)
# agg y avg (ACCIONES)
result = df_games.groupBy("Genres").agg(F.avg("Rating").cast(StringType()).alias("avg_rating"),F.avg("length").cast(StringType()).alias("avg_character")).orderBy("Genres")
# Con show() podemos ver el data set y esta es una ACCION
#result.show(5)

# Convertimos nuestro dataframe en un rdd y realizamos un map para poder tomar cada una de las filas
# Realizamos una partición de 2 con coalesce 
# Map es una (TRANSFORMACIÓN)
# Coalesce es una (ACCIÓN)
result = result.rdd.map(lambda x: (x[0], x[1], x[2])).coalesce(2)

# Por ultimo lo guardamos y esta es una ACCION
result.saveAsTextFile("5_out")