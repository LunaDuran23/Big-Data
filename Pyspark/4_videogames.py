#Librerias
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, IntegerType, ArrayType, StringType

spark = SparkSession.builder.getOrCreate()

"""¿Cual es el video juego  más popular por genero y desarrollador team"""
# Datos: base de datos que son necesarios para nuestro analisis
# Team: Grupo de desarrolladores de videojuegos
# Genres : Genero del videjuego (Ej: Aventura, terror)
# Plays : Numero de personas que han jugado este video juego

#leemos el archivo y creamos nuestro dataframe
df_games = spark.read.option("quote", "\"") \
                     .option("escape", "\"") \
                     .option("multiline", True) \
                     .csv('input/games.csv', sep = ",", header='true')

# withColumn (TRANSFORMACION)
#Los datos de la columna Plays estan en la forma "34K", por lo que
# quitamos la K, multiplicamos por 1000 y convertimos a doble para
#poder hacer operaciones mas adelante. 
df_games = df_games.withColumn("Plays", F.substring_index("Plays", "K", 1).cast(DoubleType())*1000)
#Convertimos la columna en arreglo de string, primero realizamos un 
# split que serpare cada uno de los valores y luego se convierta en
# un arreglo de strings. 
df_games_T = df_games.withColumn("Team", F.split(F.regexp_replace("Team", r"[\[\]']", ""), ", "))
df_games_g = df_games.withColumn("Genres", F.split(F.regexp_replace("Genres", r"[\[\]']",""), ", "))

# Dividimos cada genero con el uso de la funcion explode
df_games_g = df_games_g.withColumn("Genres",F.explode(df_games_g.Genres))
#Dividimos cada compañia con el uso del explode
df_games_T = df_games_T.withColumn("Team",F.explode(df_games_T.Team))

'''Para genero'''
# Agrupamos por genero y obtenemos el maximo del numero de gente la
#  que ha jugado un respectivo juego. Ademas lo organizamos por numero de jugadas
w = Window.partitionBy('Genres')
result_g =df_games_g.withColumn('maxPlays', F.max('Plays').over(w))\
    #creamos una nueva columna con los valores maximos de plays con respecto a los generos 
    .where(F.col('Plays') == F.col('maxPlays'))\ 
    .repartition(2)\
    #hacemos dos particiones
    .drop('maxPlays')\
    #borramos la columna que habiamos creado anteriormente
    .dropDuplicates(['Genres'])\
    #quitamos duplicados en caso que haya dos videojuegos con la misma polularidad
    #para el mismo genero
    .select(['Genres', 'Title'])

#concatenamos para tener el resultado de titulo y grupo de desarrolladores
result_g = result_g.withColumn('res', F.concat_ws(",", result_g.Genres, result_g.Title))
result_g.select("res").write.text("4_genreout")


'''Para grupo de desarrolladores'''
#Agrupo por el team y busco el maximo de la personas que han  jugado un respactivo juego, tomando el titulo del juego
w2 = Window.partitionBy('Team')
result_T = df_games_T.withColumn('maxPlays', F.max('Plays').over(w))\
    #creamos una nueva columna con los valores maximos de plays con respecto a los desarroladores
    .where(F.col('Plays') == F.col('maxPlays'))\
    .repartition(2)\
    #hacemos dos particiones
    .drop('maxPlays')\
    #borramos la columna que habiamos creado anteriormente
    .dropDuplicates(['Team'])\
    #quitamos duplicados en caso que haya dos videojuegos con la misma polularidad
    #para el mismo desarrollador
    .select(['Team', 'Title'])

#concatenamos para tener el resultado de titulo y grupo de desarrolladores
result_T = result_T.withColumn('res', F.concat_ws(",", result_T.Team, result_T.Title))
result_T.select("res").write.text("4_teamout")




