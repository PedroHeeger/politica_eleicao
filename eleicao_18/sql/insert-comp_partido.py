

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
from pyspark.sql.functions import lit, round

#  Extraindo os dados da view_cand_total do banco MySQL.
df_comp = session_spark.read \
    .format("jdbc") \
    .option("driver", "com.mysql.jdbc.Driver") \
    .option("url", f"jdbc:mysql://{host}/{db}?user={user}&password={passw}") \
    .option("dbtable", "view_cand_total") \
    .load()

# DATAFRAME DE TODOS CANDIDATOS DO PLEITO

# Selecionando as colunas que iremos utilizar
df_part_total = df_comp.select('nm_candidato', 'nm_urna_candidato', 'cd_cargo',  'ds_cargo', 'nr_partido', 'sg_part_now', 'sg_uf', 'sg_ue', "sum_votos_total")

# Contando a quantidade de candidato no pleito de cada partido por estado e cargo
df_part_total = df_part_total.groupBy('cd_cargo', 'ds_cargo', 'sg_uf', 'sg_ue',"sg_part_now").count()

# Renomeando a coluna da contagem
df_part_total = df_part_total.withColumnRenamed("count", "qt_cand_pleito")

# Ordenando o Dataframe
df_part_total = df_part_total.orderBy(['cd_cargo', 'sg_uf', 'sg_ue', 'qt_cand_pleito'], ascending=['True', 'True', 'True', 'False'])

# df_part_total.show(40)

# DATAFRAME DOS CANDIDATOS ELEITOS

# Selecionando as colunas que iremos utilizar e filtrando só os candidatos eleitos
df_part_eleito = df_comp.select('nm_candidato', 'nm_urna_candidato', 'cd_cargo',  'ds_cargo', 'nr_partido', 'sg_part_now', 'sg_uf', 'sg_ue', "sum_votos_total", "ds_sit_tot_turno").filter((df_comp.ds_sit_tot_turno == "ELEITO") | (df_comp.ds_sit_tot_turno == "ELEITO POR QP") | (df_comp.ds_sit_tot_turno == "ELEITO POR MÉDIA"))

# Contando a quantidade de candidatos eleitos de cada partido por estado e cargo
df_part_eleito  = df_part_eleito .groupBy('cd_cargo', 'ds_cargo', 'sg_uf', 'sg_ue',"sg_part_now").count()

# Renomeando a coluna da contagem
df_part_eleito = df_part_eleito.withColumnRenamed("count", "qt_cand_eleito")

# Ordenando o Dataframe
df_part_eleito = df_part_eleito.orderBy(['cd_cargo', 'sg_uf', 'sg_ue', 'qt_cand_eleito'], ascending=['True', 'True', 'True', 'False'])

# df_part_eleito.show(40)

# UNINDO OS DATAFRAMES

# Trazendo a quantidade de candidatos eleitos para o Dataframe com a quantidade de candidatos no pleito
df_part_total = df_part_total.join(df_part_eleito, on=['cd_cargo', 'ds_cargo', 'sg_uf', 'sg_ue', 'sg_part_now'], how='left')

# Substituindo os valores null por zero
df_part_total = df_part_total.replace('', '0')

# Criando uma coluna para calcular o quociente partidário (qp), ou seja, o número de cadeiras de cada partido para cada estado e cargo
df_part_total  = df_part_total.withColumn("porc", lit((df_part_total.qt_cand_eleito/df_part_total.qt_cand_pleito)))

# df_part_total.printSchema()

df_part_total.show(50)