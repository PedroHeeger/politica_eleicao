
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
from con_spark import session_spark
from con_sqlalch import *

# Outras Importações
import pandas as pd


#  Extraindo os dados da view_cand_total do banco MySQL.
df_view_cand_total = session_spark.read \
    .format("jdbc") \
    .option("driver", "com.mysql.jdbc.Driver") \
    .option("url", f"jdbc:mysql://{host}/{db}?user={user}&password={passw}") \
    .option("dbtable", "view_cand_total") \
    .load()


# df_view_cand_total.show(2)

# Selecionando as colunas e ordenando o Dataframe
df_view_cand_total = df_view_cand_total.select('nr_turno', 'sg_uf', 'sg_ue','nm_candidato', 'nm_urna_candidato', 'cd_cargo', 'ds_cargo', 'nr_partido', "sg_part_now",  'sum_votos_total', 'ds_sit_tot_turno', 'ds_composicao_coligacao', 'ds_eleicao').orderBy(['nr_turno', 'cd_cargo', 'sg_uf', 'sg_ue', 'sum_votos_total'], ascending=[True, True, True, True, False])


# df_view_cand_total.show(50)

# SPARK
# Exportando para um arquivo Excel via Spark
# df_view_cand_total.write.format("com.crealytics.spark.excel")\
#   .option("header", "true")\
#   .mode("overwrite")\
#   .save("/home/pyspark/app/teste/view_cand_total.xlsx")



# PANDAS
# Transformando o Dataframe Spark em Pandas
df_view_cand_total = df_view_cand_total.toPandas()

# print(df_view_cand_total.head(30))

# Exportando o Dataframe Pandas para um arquivo Excel
df_view_cand_total.to_excel('/home/pyspark/app/teste/view_cand_total.xlsx', index=False)



