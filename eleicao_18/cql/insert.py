# from pyspark import SparkContext, SparkConf
# from pyspark.sql import SQLContext
from pyspark.sql.types import StringType 
from pyspark.sql.functions import udf
import uuid

# Importações sistema
import sys
import os
from dotenv import load_dotenv

# Carregando as variáveis de ambiente
load_dotenv()

# Buscando variável de ambiente
path_eleicao_18 = os.environ['path_eleicao_18']

# Importando a configuração do spark
sys.path.insert(0, f'{path_eleicao_18}')
from con_spark import *

# Lendo o arquivo em parquet
df_insert = sqlContext.read.parquet("dataset/parquet/tabela.parquet")

# Criando uma nova coluna do tipo UUID 
uuidUdf= udf(lambda : str(uuid.uuid4()),StringType())
df_insert = df_insert.withColumn("id",uuidUdf())

# Renomeando as colunas de maíscula para minúscula
df_insert = df_insert.withColumnRenamed("DT_GERACAO", "dt_geracao")
df_insert = df_insert.withColumnRenamed("HH_GERACAO", "hh_geracao")
df_insert = df_insert.withColumnRenamed("ANO_ELEICAO", "ano_eleicao")
df_insert = df_insert.withColumnRenamed("CD_TIPO_ELEICAO", "cd_tipo_eleicao")
df_insert = df_insert.withColumnRenamed("NM_TIPO_ELEICAO", "nm_tipo_eleicao")
df_insert = df_insert.withColumnRenamed("NR_TURNO", "nr_turno")
df_insert = df_insert.withColumnRenamed("CD_ELEICAO", "cd_eleicao")
df_insert = df_insert.withColumnRenamed("DS_ELEICAO", "ds_eleicao")
df_insert = df_insert.withColumnRenamed("DT_ELEICAO", "dt_eleicao")
df_insert = df_insert.withColumnRenamed("TP_ABRANGENCIA", "tp_abrangencia")
df_insert = df_insert.withColumnRenamed("SG_UF", "sg_uf")
df_insert = df_insert.withColumnRenamed("SG_UE", "sg_ue")
df_insert = df_insert.withColumnRenamed("NM_UE", "nm_ue")
df_insert = df_insert.withColumnRenamed("CD_MUNICIPIO", "cd_municipio")
df_insert = df_insert.withColumnRenamed("NM_MUNICIPIO", "nm_municipio")
df_insert = df_insert.withColumnRenamed("NR_ZONA", "nr_zona")
df_insert = df_insert.withColumnRenamed("CD_CARGO", "cd_cargo")
df_insert = df_insert.withColumnRenamed("DS_CARGO", "ds_cargo")
df_insert = df_insert.withColumnRenamed("SQ_CANDIDATO", "sq_candidato")
df_insert = df_insert.withColumnRenamed("NR_CANDIDATO", "nr_candidato")
df_insert = df_insert.withColumnRenamed("NM_CANDIDATO", "nm_candidato")
df_insert = df_insert.withColumnRenamed("NM_URNA_CANDIDATO", "nm_urna_candidato")
df_insert = df_insert.withColumnRenamed("NM_SOCIAL_CANDIDATO", "nm_social_candidato")
df_insert = df_insert.withColumnRenamed("CD_SITUACAO_CANDIDATURA", "cd_situacao_candidatura")
df_insert = df_insert.withColumnRenamed("DS_SITUACAO_CANDIDATURA", "ds_situacao_candidatura")
df_insert = df_insert.withColumnRenamed("CD_DETALHE_SITUACAO_CAND", "cd_detalhe_situacao_cand")
df_insert = df_insert.withColumnRenamed("DS_DETALHE_SITUACAO_CAND", "ds_detalhe_situacao_cand")
df_insert = df_insert.withColumnRenamed("TP_AGREMIACAO", "tp_agremiacao")
df_insert = df_insert.withColumnRenamed("NR_PARTIDO", "nr_partido")
df_insert = df_insert.withColumnRenamed("SG_PARTIDO", "sg_partido")
df_insert = df_insert.withColumnRenamed("NM_PARTIDO", "nm_partido")
df_insert = df_insert.withColumnRenamed("SQ_COLIGACAO", "sq_coligacao")
df_insert = df_insert.withColumnRenamed("NM_COLIGACAO", "nm_coligacao")
df_insert = df_insert.withColumnRenamed("DS_COMPOSICAO_COLIGACAO", "ds_composicao_coligacao")
df_insert = df_insert.withColumnRenamed("CD_SIT_TOT_TURNO", "cd_sit_tot_turno")
df_insert = df_insert.withColumnRenamed("DS_SIT_TOT_TURNO", "ds_sit_tot_turno")
df_insert = df_insert.withColumnRenamed("ST_VOTO_EM_TRANSITO", "st_voto_em_transito")
df_insert = df_insert.withColumnRenamed("QT_VOTOS_NOMINAIS", "qt_votos_nominais")

# Verificando a quantidade de linhas
print(df_insert.count())

# Verificando os dados
df_insert.show(2)

# Inserção na tabela criada do banco no Cassandra
df_insert.write.format("org.apache.spark.sql.cassandra").mode('append').options(table="resultado_eleicao_18", keyspace="politica").save()