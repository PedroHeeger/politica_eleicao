# Extrai por Spark do banco Cassandra via connector (arquivo JAR) e insere com Spark via JDBC para o banco MySQL também utilizando o connector (arquivo JAR)

# Importações sistema
import sys
import os
from dotenv import load_dotenv

# Carregando as variáveis de ambiente
load_dotenv()

# Buscando cada variável
path_eleicao_18 = os.environ['path_eleicao_18']

# Importando o Dataframe Spark com os dado do banco Cassandra
sys.path.insert(0, f'{path_eleicao_18}cql')
from selection import df_geral

# Importando as conexões
sys.path.insert(0, f'{path_eleicao_18}')
# from con_spark import session_spark
from con_sqlalch import *

# Outras Importações
import pandas as pd

# SPARK
# Selecionando as colunas necessárias desse Dataframe
df_cand_geral = df_geral.select("cd_tipo_eleicao", "cd_eleicao", "cd_municipio", "sg_uf", "sg_ue", "nr_zona", "cd_cargo", "nr_turno", "sq_candidato", "nr_candidato", "nm_candidato", "nm_urna_candidato", "cd_situacao_candidatura", "cd_detalhe_situacao_cand", "nr_partido", "sq_coligacao", "ds_composicao_coligacao", "cd_sit_tot_turno", "qt_votos_nominais").distinct()
df_cand_geral.show(20)

# Inserindo no banco MySQL com JDBC
df_cand_geral.write.format("jdbc") \
    .option("url", f"jdbc:mysql://mysql-app/{db}?user=root&password=root") \
    .mode("append") \
	.option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "cand_geral").save()








# PANDAS - Não consegui por causa da memória