
# Importações sistema
import sys
import os
from dotenv import load_dotenv

# Carregando as variáveis de ambiente
load_dotenv()

# Buscando cada variável
path_eleicao_18 = os.environ['path_eleicao_18']

# Importando as conexões
sys.path.insert(0, f'{path_eleicao_18}')
from con_spark import sqlContext
from con_sqlalch import *

# Outras Importações
import pandas as pd

#  Extraindo os dados da tabela quociente do banco MySQL.
df_quociente = sqlContext.read \
    .format("jdbc") \
    .option("driver", "com.mysql.jdbc.Driver") \
    .option("url", f"jdbc:mysql://{host}/{db}?user={user}&password={passw}") \
    .option("dbtable", "quociente") \
    .load()


# Selecionando as colunas e ordenando o Dataframe
df_quociente = df_quociente.select("cd_cargo", "sg_uf", "sg_part_now", "sum_partido_vv", "sum_total_vv","porc", "cad_uf", "qe", "qp").orderBy(['cd_cargo', 'sg_uf', 'sum_partido_vv'], ascending=[True, True, False])


df_quociente.show(20)


# SPARK
# Exportando para um arquivo Excel via Spark
# df_quociente.write.format("com.crealytics.spark.excel")\
#   .option("header", "true")\
#   .mode("overwrite")\
#   .save("/home/pyspark/app/teste/view_cand_total.xlsx")

# PANDAS
# Transformando o Dataframe Spark em Pandas
df_quociente = df_quociente.toPandas()

# df_quociente = df_quociente.select("cd_cargo", "sg_uf", "sg_part_now", "sum_partido_vv", "sum_total_vv","porc", "cad_uf", "qe", "qp").sort_values(df_quociente.cd_cargo.asc() & df_quociente.sg_uf.desc())

# print(df_quociente.head(30))

# Exportando o Dataframe Pandas para um arquivo Excel
df_quociente.to_excel('/home/pyspark/app/teste/quociente.xlsx', index=False)
