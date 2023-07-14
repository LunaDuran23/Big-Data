#importamos las librerias
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, sum

#configuramos las caracteristicas
host = '0.tcp.ngrok.io'
port = 12424

#creamos la sesion de Spark
spark = SparkSession \
    .builder \
    .appName("Tweet_2020") \
    .getOrCreate()


# dataFrame con el STREAM del host y puerto
Tweets_FIFA = spark \
    .readStream \
    .format("socket") \
    .option("host", host) \
    .option("port", port) \
    .option("includeTimestamp", "true") \
    .load()


#arreglamos el dataFrame por columnas y agregamos un watermark cada segundo del STREAM 
Tweets_FIFA = Tweets_FIFA.withWatermark('timestamp', '1 seconds')
Tweets_FIFA = Tweets_FIFA.select(split(Tweets_FIFA.value, ',').alias("columns"))

Tweets_Stream = Tweets_FIFA.selectExpr("columns[0] as id",\
                                        "columns[1] as hour",\
                                        "columns[2] as likes",\
                                        "columns[3] as source",\
                                        "columns[4] as text",\
                                        "columns[5] as sentiment" )


# Configurar la consulta de Spark Streaming
query = (
    Tweets_Stream
    .groupBy("source")
    .agg(sum("likes").alias("total_likes"))
)

#funcion para guardar cada archivo de resultados como bach_id.txt (extraido internet)
def txt_files(batch_df, batch_id):
    txt_name = f"bach_{batch_id}.txt"
    result = [str(row) for row in batch_df.collect()]

    with open(txt_name, "w") as f:
        f.write("\n".join(result))

# Ejecutar la consulta de Spark Streaming
streaming_query = (
    query
    .writeStream
    .foreachBatch(txt_files)
    .outputMode("update")  # opción "update" para resultados incrementales
    .option("checkpointLocation", "out") 
    .option("path", "out")
    .start()
)

# Esperar a que la consulta finalice
streaming_query.awaitTermination(1440) #24 minutos en segundos
streaming_query.stop() #detiene el proceso y guarda los resultados


#correr en cmd con el comando
#     pscp -i keyTest.ppk usuarioSSH_declusterERM:bach_???.txt .\out