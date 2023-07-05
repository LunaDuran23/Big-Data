#importamos librerias
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

#inicializamos la SparkSession
spark = SparkSession.builder.appName('movies25').getOrCreate()

#Para cada género, encuentre la calificación promedio del género 
# y la cantidad de películas que pertenecen a este género


#leemos nuestras bases de datos
movies = spark.read.option("header", "true").csv("ml-25m/movies.csv")
ratings = spark.read.option("header", "true").csv("ml-25m/ratings.csv")


#agrupamos por pelicula para sacar su rating promedio y lo agregamos
ratings_group = ratings\
.groupby("movieId")\
.agg(F.avg(F.col("rating")))\
.withColumnRenamed("avg(rating)", "avg_rating_m")
#GROUPBY: TRANSFORMATION, WITHCOLUMN: , COUNT: ACTION, AVERAGE:ACTION, ADD: TRANSFORMATION

#hacemos explote para separar cada pelicula por generos
movies = movies.withColumn("genres", F.split(F.col("genres"),"\|")) #TRANSFORMATION
movies_explode = movies.select(movies.movieId,F.explode(movies.genres)) #TRANSFORMATION

#unimos los dos anteriores por movieId
movie_ratings = ratings_group.join(movies_explode, "movieId") #TRANSFORMATION


#agrupamos por genero para sacar su rating promedio y lo agregamos
#ademas de contar cuantas pelis hay por cada genero
genres_group = movie_ratings\
.groupby("col")\
.agg(F.count("movieId"),F.avg(F.col("avg_rating_m")))\
.withColumnRenamed("avg(avg_rating_m)", "avg_rating")\
.withColumnRenamed("count(movieId))", "num_movies")\
.withColumnRenamed("col", "genres")
#GROUPBY: TRANSFORMATION, WITHCOLUMN: , COUNT: ACTION, AVERAGE:ACTION

#para exportar el archivo como texto necesitamos primero convertirlo en rdd
final = genres_group.rdd.map(lambda x: (x[0], x[1], x[2])).coalesce(1) #TRANSFORMATION
final.saveAsTextFile("3_out")
