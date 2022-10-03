
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

tabela = """create table if not exists resultado_eleicao_18 (
    DT_GERACAO text,
    HH_GERACAO text,
    ANO_ELEICAO int,
    CD_TIPO_ELEICAO	text,
    NM_TIPO_ELEICAO text,
    NR_TURNO text,
    CD_ELEICAO text,
    DS_ELEICAO text,
    DT_ELEICAO text,
    TP_ABRANGENCIA text,
    SG_UF text,
    SG_UE text,
    NM_UE text,
    CD_MUNICIPIO text,
    NM_MUNICIPIO text,
    NR_ZONA text,
    CD_CARGO text,
    DS_CARGO text,
    SQ_CANDIDATO text,
    NR_CANDIDATO text,
    NM_CANDIDATO text,
    NM_URNA_CANDIDATO text,
    NM_SOCIAL_CANDIDATO text,
    CD_SITUACAO_CANDIDATURA text,
    DS_SITUACAO_CANDIDATURA text,
    CD_DETALHE_SITUACAO_CAND text,
    DS_DETALHE_SITUACAO_CAND text,
    TP_AGREMIACAO text,
    NR_PARTIDO text,
    SG_PARTIDO text,
    NM_PARTIDO text,
    SQ_COLIGACAO text,
    NM_COLIGACAO text,
    DS_COMPOSICAO_COLIGACAO text,
    CD_SIT_TOT_TURNO text,
    DS_SIT_TOT_TURNO text,
    ST_VOTO_EM_TRANSITO text,
    QT_VOTOS_NOMINAIS int,
    ID text primary key
)"""

session.execute(tabela)