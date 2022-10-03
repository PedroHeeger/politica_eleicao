# APENAS PARA ELEIÇÕES PROPORCIONAIS
# Extrai os dados da view final, soma o número de votos validos (vv) de cada partido por estado para os cargo de deputado que irá compor uma nova tabela chamada quociente. Será realizado um merge de um Dataframe auxiliar para trazer dele, o número de cadeiras na câmara e assembléia de cada estado. Com esses dados, serão criados mais três colunas para os quocientes eleitoral (qe) e partidário (qp), além da porcentagem (porc) de votos do partido para cada estado e cargo.
# Neste exemplo, será criado dois Datafrmaes, um Dataframe com os votos validos (vv) de cada partido por estado, e outro com os votos validos do estado, ou seja, somando todos os partidos, que também será enviado para o primeiro Dataframe para auxiliar nos cálculos.

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
from pyspark.sql import Row
from pyspark.sql.types import IntegerType, FloatType, DoubleType
from pyspark.sql.functions import when
from pyspark.sql.functions import regexp_replace

# Criação o Dataframe auxiliar com número de cadeiras dos cargos por estado
df_cad = session_spark.createDataFrame([
    Row(cd_cargo="6", sg_uf="AC", cad_uf=8),
    Row(cd_cargo="6", sg_uf="AL", cad_uf=9),
    Row(cd_cargo="6", sg_uf="AM", cad_uf=8),
    Row(cd_cargo="6", sg_uf="AP", cad_uf=8),
    Row(cd_cargo="6", sg_uf="BA", cad_uf=39),
    Row(cd_cargo="6", sg_uf="CE", cad_uf=22),
    Row(cd_cargo="6", sg_uf="DF", cad_uf=8),
    Row(cd_cargo="6", sg_uf="ES", cad_uf=10),
    Row(cd_cargo="6", sg_uf="GO", cad_uf=17),
    Row(cd_cargo="6", sg_uf="MA", cad_uf=18),
    Row(cd_cargo="6", sg_uf="MG", cad_uf=53),
    Row(cd_cargo="6", sg_uf="MS", cad_uf=8),
    Row(cd_cargo="6", sg_uf="MT", cad_uf=8),
    Row(cd_cargo="6", sg_uf="PA", cad_uf=17),
    Row(cd_cargo="6", sg_uf="PB", cad_uf=12),
    Row(cd_cargo="6", sg_uf="PE", cad_uf=25),
    Row(cd_cargo="6", sg_uf="PI", cad_uf=10),
    Row(cd_cargo="6", sg_uf="PR", cad_uf=30),
    Row(cd_cargo="6", sg_uf="RJ", cad_uf=46),
    Row(cd_cargo="6", sg_uf="RN", cad_uf=8),
    Row(cd_cargo="6", sg_uf="RO", cad_uf=8),
    Row(cd_cargo="6", sg_uf="RR", cad_uf=8),
    Row(cd_cargo="6", sg_uf="RS", cad_uf=31),
    Row(cd_cargo="6", sg_uf="SC", cad_uf=16),
    Row(cd_cargo="6", sg_uf="SE", cad_uf=8),
    Row(cd_cargo="6", sg_uf="SP", cad_uf=70),
    Row(cd_cargo="6", sg_uf="TO", cad_uf=8),
    Row(cd_cargo="7", sg_uf="AC", cad_uf=24),
    Row(cd_cargo="7", sg_uf="AL", cad_uf=27),
    Row(cd_cargo="7", sg_uf="AM", cad_uf=24),
    Row(cd_cargo="7", sg_uf="AP", cad_uf=24),
    Row(cd_cargo="7", sg_uf="BA", cad_uf=63),
    Row(cd_cargo="7", sg_uf="CE", cad_uf=46),
    Row(cd_cargo="7", sg_uf="DF", cad_uf=24),
    Row(cd_cargo="7", sg_uf="ES", cad_uf=30),
    Row(cd_cargo="7", sg_uf="GO", cad_uf=41),
    Row(cd_cargo="7", sg_uf="MA", cad_uf=42),
    Row(cd_cargo="7", sg_uf="MG", cad_uf=77),
    Row(cd_cargo="7", sg_uf="MS", cad_uf=24),
    Row(cd_cargo="7", sg_uf="MT", cad_uf=24),
    Row(cd_cargo="7", sg_uf="PA", cad_uf=41),
    Row(cd_cargo="7", sg_uf="PB", cad_uf=36),
    Row(cd_cargo="7", sg_uf="PE", cad_uf=49),
    Row(cd_cargo="7", sg_uf="PI", cad_uf=30),
    Row(cd_cargo="7", sg_uf="PR", cad_uf=54),
    Row(cd_cargo="7", sg_uf="RJ", cad_uf=70),
    Row(cd_cargo="7", sg_uf="RN", cad_uf=24),
    Row(cd_cargo="7", sg_uf="RO", cad_uf=24),
    Row(cd_cargo="7", sg_uf="RR", cad_uf=24),
    Row(cd_cargo="7", sg_uf="RS", cad_uf=55),
    Row(cd_cargo="7", sg_uf="SC", cad_uf=40),
    Row(cd_cargo="7", sg_uf="SE", cad_uf=24),
    Row(cd_cargo="7", sg_uf="SP", cad_uf=94),
    Row(cd_cargo="7", sg_uf="TO", cad_uf=24),
])

#  Extraindo os dados da view_cand_total do banco MySQL.
df_quociente = session_spark.read \
    .format("jdbc") \
    .option("driver", "com.mysql.jdbc.Driver") \
    .option("url", f"jdbc:mysql://{host}/{db}?user={user}&password={passw}") \
    .option("dbtable", "view_cand_total") \
    .load()

# df_qe_qp.show(10)    

# Filtrando apenas os Deputados (Estaduais e Federais) e selecionando as colunas que iremos utilizar
df_quociente = df_quociente.select('nm_candidato', 'nm_urna_candidato', 'cd_cargo', 'ds_cargo', 'nr_partido', 'sg_part_now', 'sg_uf', 'sg_ue', "sum_votos_total").filter((df_quociente.cd_cargo == "6") | (df_quociente.cd_cargo == "7"))


# DATAFRAME PARA VOTOS VÁLIDOS TOTAIS:

# Somando a quantidade de votos totais por estado e cargo
df_total_vv = df_quociente.groupBy("cd_cargo", "sg_uf").sum("sum_votos_total")

# Renomeando a coluna do somatório de votos
df_total_vv = df_total_vv.withColumnRenamed("sum(sum_votos_total)", "sum_total_vv")

# Informando o valor total para a coluna de partido
df_total_vv = df_total_vv.withColumn('sg_part_now', lit("total"))

# Trazendo a quantidade de cadeiras por estado para o Dataframe geral
df_total_vv = df_total_vv.join(df_cad, on=['cd_cargo', "sg_uf"], how='left')

# Criando a coluna de Quociente Eleitoral e calculando
df_total_vv = df_total_vv.withColumn('qe', lit(df_total_vv.sum_total_vv/df_total_vv.cad_uf))

# Formatando a coluna do Quociente Eleitoral 
df_total_vv = df_total_vv.withColumn("qe", df_total_vv["qe"].cast(IntegerType()))

# df_total_vv.show(33)  


## DATAFRAME PARA VOTOS VÁLIDOS POR PARTIDO:

# Somando a quantidade de votos totais por estado, cargo e agora por partido
df_partido_vv = df_quociente.groupBy("cd_cargo", "sg_uf", "sg_part_now").sum("sum_votos_total")

# Renomeando a coluna do somatório de votos
df_partido_vv = df_partido_vv.withColumnRenamed("sum(sum_votos_total)", "sum_partido_vv")

## JUNTANDO OS DOIS DATAFRAMES

# Trazendo a quantidade de cadeiras por estado, a soma de vv total por estado e o quociente eleitoral (qe) para o Dataframe com partido
df_partido_vv = df_partido_vv.join(df_total_vv, on=['cd_cargo', "sg_uf"], how='left').drop(df_total_vv.sg_part_now)

# Criando uma coluna para calcular a porcentagem de votos em cada partido por estado e cargo
df_partido_vv  = df_partido_vv.withColumn('porc', lit((df_partido_vv.sum_partido_vv/df_partido_vv.sum_total_vv)*100))

# Formatando a coluna de porcentagem para 2 casas decimais
df_partido_vv  = df_partido_vv.withColumn("porc", round("porc",2))

# Criando uma coluna para calcular o quociente partidário (qp), ou seja, o número de cadeiras de cada partido para cada estado e cargo
df_partido_vv  = df_partido_vv.withColumn("qp", lit((df_partido_vv.sum_partido_vv/df_partido_vv.qe)))

# Formatando a coluna qp para 1 casa decimal
df_partido_vv  = df_partido_vv.withColumn("qp", round("qp",1))

df_partido_vv = df_partido_vv.select("cd_cargo", "sg_uf", "sg_part_now", "sum_partido_vv", "sum_total_vv","porc", "cad_uf", "qe", "qp")

df_partido_vv.show(33) 


# Inserindo na tabela cand_acum no banco MySQL via JDBC
df_partido_vv.write.format("jdbc") \
    .option("url", f"jdbc:mysql://mysql-app/{db}?user=root&password=root") \
    .mode("append") \
	.option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "quociente").save()