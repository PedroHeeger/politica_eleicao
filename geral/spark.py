from pyspark.sql import SparkSession

# Criando a sessão do Spark informando suas configurações
spark_geral = SparkSession.builder \
    .config("spark.jars", "/home/python/app/eleicao_18/conector/mysql-connector-java-8.0.30.jar") \
    .config("spark.driver.memory", "6g") \
    .config("spark.executor.memory", "6g") \
    .config("spark.driver.maxResultSize", "0") \
    .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC") \
    .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .master("local").appName("PySpark_MySQL_test2").getOrCreate()
