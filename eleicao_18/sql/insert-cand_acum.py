# Insert cand_acum

# Importações sistema
import sys
import os
from dotenv import load_dotenv

# Carregando as variáveis de ambiente
load_dotenv()

# Buscando cada variável
path_eleicao_18 = os.environ['path_eleicao_18']

# Importando o Dataframe Spark com os dado do banco Cassandra
# sys.path.insert(0, f'{path_eleicao_18}cql')
# from selection import df_geral

# Importando as conexões
sys.path.insert(0, f'{path_eleicao_18}')
from lixo.con_spark import session_spark
from con_sqlalch import *

# Outras Importações
import pandas as pd

# Lendo os dados da tabela cand_geral do banco MySQL
df_cand_total = session_spark.read \
    .format("jdbc") \
    .option("driver", "com.mysql.jdbc.Driver") \
    .option("url", f"jdbc:mysql://{host}/{db}?user={user}&password={passw}") \
    .option("dbtable", "cand_geral") \
    .load()

# Somando a quantidades de votos de cada candidato por município (SEM ZONA)
df_cand_total = df_cand_total.groupBy("cd_tipo_eleicao", "cd_eleicao", "cd_municipio", "sg_uf", "sg_ue", "cd_cargo", "nr_turno", "sq_candidato", "nr_candidato","nm_candidato", "nm_urna_candidato", "cd_situacao_candidatura", "cd_detalhe_situacao_cand", "nr_partido", "sq_coligacao", "ds_composicao_coligacao","cd_sit_tot_turno").sum("qt_votos_nominais")

# Renomeando a coluna do somatório de votos
df_cand_total = df_cand_total.withColumnRenamed("sum(qt_votos_nominais)", "sum_votos_municipio")

# Inserindo no banco MySQL com JDBC
df_cand_total.write.format("jdbc") \
    .option("url", f"jdbc:mysql://mysql-app/{db}?user=root&password=root") \
    .mode("append") \
	.option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "cand_total").save()


