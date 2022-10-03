# Insert Partido

# Importações sistema
import sys
import os
from dotenv import load_dotenv

# Carregando as variáveis de ambiente
load_dotenv()

# Buscando cada variável
path_eleicao_18 = os.environ['path_eleicao_18']

# Importando o Dataframe Spark com os dado do banco Cassandra
# sys.path.insert(0, f'{end}cql')
# from selection import df_geral

# Importando as conexões
sys.path.insert(0, f'{path_eleicao_18}')
from con_spark import session_spark
from con_sqlalch import *

# Outras Importações
import pandas as pd
from pyspark.sql.types import *


# SPARK
# Criando um Dataframe Spark para tabela de partido a partir de um Excel
# df_partido_geral = sqlContext.read.format("com.crealytics.spark.excel")\
#     .option("location", f'{path_eleicao_18}dataset/histo_partidos.xlsx') \
#     .option("useHeader", "true") \
#     .option("inferSchema", "false") \
#     .option("addColorColumns", "false") \
#     .option("treatEmptyValuesAsNulls", "false") \
#     .option("caseSensitive", "true") \
#     .option("sheetName", "sheet1") \
#     .load()  

# Criando um Dataframe Pandas para tabela de partido a partir de um Excel
df_partido_geral = pd.read_excel(f'{path_eleicao_18}dataset/histo_partidos.xlsx', sheet_name='Sheet1', dtype=str)

# print(df_partido_geral['ano'].head(50))

# Criando um Schema para o Dataframe Spark
myschema = StructType([ StructField("nr_partido", StringType(), False)\
                       ,StructField("num", StringType(), True)\
                       ,StructField("sg_part_now", StringType(), True)\
                       ,StructField("nm_part_now", StringType(), True)\
                       ,StructField("ano", StringType(), True)\
                       ,StructField("sg_part_old", StringType(), True)\
                       ,StructField("nm_part_old", StringType(), True)\
                       ,StructField("pos_final", StringType(), True)\
                       ,StructField("pos_inicial", StringType(), True)\
                       ,StructField("sit", StringType(), True)\
                       ,StructField("consid", StringType(), True)
])

# Convertendo o Dataframe Pandas em Spark
df_partido_geral = session_spark.createDataFrame(df_partido_geral, schema=myschema)

# Filtrando apenas os partidos que estão no Dataset da Eleicao 2018
df_partido_geral = df_partido_geral.filter(df_partido_geral.consid == "sim")

# Substituindo os valores NaN por null
df_partido_geral = df_partido_geral.replace('NaN', 'null')

# Removendo a coluna da consideração do partido na eleição 2018
df_partido_geral = df_partido_geral.drop(df_partido_geral.consid)

# df_partido_geral.show(102, truncate=False)

# Inserindo no banco MySQL com JDBC
df_partido_geral.write.format("jdbc") \
    .option("url", f"jdbc:mysql://mysql-app/{db}?user=root&password=root") \
    .mode("append") \
	.option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "partido").save()




# # PANDAS
# # Transformando o Dataframe Spark em Pandas
# df_partido = df_partido.toPandas()
# print(df_partido.head(5))

# # Inserindo o Dataframe na tabela criada.
# df_partido.to_sql(name='partido', con=conection, if_exists="append", index=False)












# Selecionando as colunas necessárias
# df_partido = df_geral.select("nr_partido", "sg_partido", "nm_partido").distinct()
# df_partido.show(5)

# Remoção de partido duplicado
# df_partido = df_partido.select("nr_partido", "sg_partido", "nm_partido").drop(df_partido.sg_partido == "PATRIOTA")

# df_partido = df_partido.orderBy('nr_partido')

# df_partido = df_partido.filter(df_partido.nr_partido == "51").show(10)

# df_partido.show(110, truncate=False)