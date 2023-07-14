"""
Calcule, para cada año del DataSet dados para el punto 1, qué sector tuvo el mayor
número de operaciones. La salida debe mencionar el año, el nombre del sector y el valor global de operaciones.
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StructType, StructField, StringType

# Cración de un esquema para el dataset NASDAQ
schema = StructType([
    StructField("exchange", StringType(), False),
    StructField("Symbol", StringType(), False),
    StructField("date", StringType(), False),
    StructField("openPrice", DoubleType(), False),
    StructField("maxPrice", DoubleType(), False),
    StructField("minPrice", DoubleType(), False),
    StructField("closePrice", DoubleType(), False),
    StructField("volume", IntegerType(), False),
    StructField("closePriceAdj", DoubleType(), False)])

spark = SparkSession.builder.getOrCreate()

df_nasdaq = spark.read.csv('input/NASDAQsample.csv', header=False, schema=schema)
# Creamos una nueva columna que contiene el valor del año de la columna "date"
df_nasdaq = df_nasdaq.withColumn("year", df_nasdaq.date[0:4])

df_company = spark.read.csv('input/companylist.tsv', sep=r'\t', header='true')

# Inner join entre los dataframes de NASDAQ y companylist mediante la llave "Symbol"
df = df_company.join(df_nasdaq, "Symbol", "inner")

# Convertir el dataframe en rdd
df = df.rdd

# Map (TRANSFORMACIÓN): Tomamos como key una tupla: (year, sector) y valores: volume
# ReduceByKey (TRANSFORMACIÓN): Suma de los values (volumes) de cada key (year, sector)
# Map (TRANSFORMACIÓN): Tomamos como key el año y como valores una tupla (TotalVolume, sector)
# ReduceByKey (TRANSFORMACIÓN): Tomamos el máximo de los valores de cada año
# Map (TRANSFORMACIÓN): Re-estructuramos el resultado en una tripla (sector, year, TotalVolume)
# sortBy (TRANSFORMACIÓN): Ordenamos por la columa year
# coalesce (TRANSFORMACIÓN): Reducimos el número de particiones a 1 para retornar el resultado en una parte
result = df.map(lambda x: ((x[-1], x[3]), x[-3])) \
           .reduceByKey(lambda x,y: x+y) \
           .map(lambda x: (x[0][0], (x[1], x[0][1]))) \
           .reduceByKey(lambda x, y: max(x,y)) \
           .map(lambda x: (x[1][1], x[0], x[1][0])) \
           .sortBy(lambda x: x[1]) \
           .coalesce(1)

# Guardamos el resultado (ACCIÓN)
result.saveAsTextFile("1_out")