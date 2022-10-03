
# Importações sistema
import sys
import os
from dotenv import load_dotenv

# Carregando as variáveis de ambiente
load_dotenv()

# Buscando cada variável
path_eleicao_18 = os.environ['path_eleicao_18']
# print(path_eleicao_18)

# Outras Importações
from cassandra.cluster import Cluster
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext

# Conexão com o banco criado no Cassandra
cluster = Cluster(['cassandra-app'], port=9042)
session = cluster.connect('politica')

# Configuração do Contexto do Spark
conf_spark = SparkConf().setAppName("etl_politica") \
    .set("spark.jars", "/home/pyspark/app/eleicao_18/conector/spark-cassandra-connector-assembly_2.12-3.2.0.jar, /home/pyspark/app/eleicao_18/conector/mysql-connector-java-8.0.30.jar, /home/pyspark/app/eleicao_18/conector/spark-excel_2.12-3.2.0_0.16.0.jar") \
    .set("spark.cassandra.connection.host" , "cassandra-app") \
    .set("spark.driver.memory", "6g") \
    .set("spark.executor.memory", "6g") \
    .set("spark.driver.maxResultSize", "0") \
    .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC") \
    .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC") \
    .set("spark.sql.autoBroadcastJoinThreshold", "-1")
sc = SparkContext(conf=conf_spark)
sqlContext = SQLContext(sc)

# sqlContext.sql("set spark.sql.caseSensitive=false")


session_spark = SparkSession.builder \
    .config("spark.jars", "/home/pyspark/app/eleicao_18/conector/spark-cassandra-connector-assembly_2.12-3.2.0.jar, /home/pyspark/app/eleicao_18/conector/mysql-connector-java-8.0.30.jar, /home/pyspark/app/eleicao_18/conector/spark-excel_2.12-3.2.2_0.18.0.jar, /home/pyspark/app/eleicao_18/conector/commons-collections4-4.1.jar, /home/pyspark/app/eleicao_18/conector/xmlbeans-5.1.1.jar") \
    .config("spark.driver.memory", "6g") \
    .config("spark.executor.memory", "6g") \
    .config("spark.driver.maxResultSize", "0") \
    .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC") \
    .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .master("local").appName("PySpark_MySQL_test2").getOrCreate()



# session_spark = session_spark.sql('set spark.sql.caseSensitive=true')






# conf = SparkConf().setAppName("etl_politica").set( "spark.jars" , "/home/python/app/cql/spark-cassandra-connector-assembly_2.12-3.2.0.jar").set("spark.driver.extraClassPath", "home/python/app/cql/spark-cassandra-connector-driver_2.12-3.2.0.jar" ).set("spark.cassandra.connection.host" , "cassandra-app") 
# sc = SparkContext(conf=conf)
# sqlContext = SQLContext(sc)