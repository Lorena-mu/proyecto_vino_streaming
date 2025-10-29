from pyspark.sql import SparkSession

# Inicializa Spark
spark = SparkSession.builder.appName("WineBatchAnalysis").getOrCreate()

# Carga el dataset
df = spark.read.csv("winequality.csv", header=True, inferSchema=True)

# Limpieza básica
df = df.dropna()

# Análisis exploratorio: promedio de alcohol por calidad
df.groupBy("quality").avg("alcohol").show()

# Guarda resultados
df.write.csv("resultados/wine_batch", header=True)

print("Análisis batch completado y resultados guardados.")
