from cassandra.cluster import Cluster

# Conexão com o banco criado no Cassandra
cluster = Cluster(['cassandra-app'], port=9042)
session = cluster.connect('politica')

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext

# Configuração do Contexto do Spark
conf = SparkConf().setAppName("etl_politica") \
    .set("spark.jars", "/home/pyspark/app/eleicao_18/conector/spark-cassandra-connector-assembly_2.12-3.2.0.jar, /home/pyspark/app/eleicao_18/conector/mysql-connector-java-8.0.30.jar") \
    .set("spark.cassandra.connection.host" , "cassandra-app") \
    .set("spark.driver.memory", "4g") \
    .set("spark.executor.memory", "4g") \
    .set("spark.driver.maxResultSize", "0")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)



# conf = SparkConf().setAppName("etl_politica").set( "spark.jars" , "/home/python/app/cql/spark-cassandra-connector-assembly_2.12-3.2.0.jar").set("spark.driver.extraClassPath", "home/python/app/cql/spark-cassandra-connector-driver_2.12-3.2.0.jar" ).set("spark.cassandra.connection.host" , "cassandra-app") 
# sc = SparkContext(conf=conf)
# sqlContext = SQLContext(sc)