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
# from con_spark import session_spark
from con_sqlalch import *


# Criação das Tabelas do banco MySQL
cand_geral = """CREATE TABLE IF NOT EXISTS cand_geral(
    id INT AUTO_INCREMENT PRIMARY KEY,
    cd_tipo_eleicao VARCHAR(10),
    cd_eleicao VARCHAR(10), 
    cd_municipio VARCHAR(10), 
    sg_uf VARCHAR(10), 
    sg_ue VARCHAR(10), 
    nr_zona VARCHAR(10), 
    cd_cargo VARCHAR(10),
    nr_turno TEXT, 
    sq_candidato TEXT, 
    nr_candidato TEXT,
    nm_candidato TEXT, 
    nm_urna_candidato TEXT, 
    cd_situacao_candidatura VARCHAR(10),
    cd_detalhe_situacao_cand VARCHAR(10), 
    nr_partido VARCHAR(10), 
    sq_coligacao VARCHAR(30), 
    ds_composicao_coligacao VARCHAR(200), 
    cd_sit_tot_turno VARCHAR(10), 
    qt_votos_nominais INT,
    foreign key (cd_tipo_eleicao) references tp_eleicao(cd_tipo_eleicao),
    foreign key (cd_eleicao) references ds_eleicao(cd_eleicao),
    foreign key (cd_municipio, nr_zona) references municipio(cd_municipio, nr_zona),
    foreign key (sg_uf, sg_ue) references estado(sg_uf, sg_ue),
    foreign key (cd_cargo) references cargo(cd_cargo),
    foreign key (cd_situacao_candidatura) references sit_cand(cd_situacao_candidatura),
    foreign key (cd_detalhe_situacao_cand) references det_cand(cd_detalhe_situacao_cand),
    foreign key (nr_partido) references partido(nr_partido),
    foreign key (sq_coligacao, ds_composicao_coligacao) references coligacao(sq_coligacao, ds_composicao_coligacao),
    foreign key (cd_sit_tot_turno) references turno(cd_sit_tot_turno)
)
    """


cargo = """CREATE TABLE IF NOT EXISTS cargo(
    cd_cargo VARCHAR(10) PRIMARY KEY,
    ds_cargo TEXT
)
    """

coligacao = """CREATE TABLE IF NOT EXISTS coligacao(
    sq_coligacao VARCHAR(30),
    tp_agremiacao TEXT,
    nm_coligacao TEXT,
    ds_composicao_coligacao VARCHAR(200),
    PRIMARY KEY (sq_coligacao, ds_composicao_coligacao)
)
    """

det_cand = """CREATE TABLE IF NOT EXISTS det_cand(
    cd_detalhe_situacao_cand VARCHAR(10) PRIMARY KEY,
    ds_detalhe_situacao_cand TEXT
)
    """

ds_eleicao = """CREATE TABLE IF NOT EXISTS ds_eleicao(
    cd_eleicao VARCHAR(10) PRIMARY KEY,
    ds_eleicao TEXT,
    dt_eleicao TEXT
)
    """

estado = """CREATE TABLE IF NOT EXISTS estado(
    sg_uf VARCHAR(10),
    sg_ue VARCHAR(10),
    nm_ue TEXT,
    PRIMARY KEY (sg_uf, sg_ue)
)
    """    

municipio = """CREATE TABLE IF NOT EXISTS municipio(
    cd_municipio VARCHAR(10),
    nm_municipio TEXT,
    sg_uf VARCHAR(10),
    nr_zona VARCHAR(10),
    PRIMARY KEY (cd_municipio, nr_zona)
)
    """

partido = """CREATE TABLE IF NOT EXISTS partido(
    nr_partido VARCHAR(10) PRIMARY KEY,
    num VARCHAR(10),
    sg_part_now TEXT,
    nm_part_now TEXT,
    ano TEXT,
    sg_part_old TEXT,
    nm_part_old TEXT,
    pos_final TEXT,
    pos_inicial TEXT,
    sit TEXT
)
    """

sit_cand = """CREATE TABLE IF NOT EXISTS sit_cand(
    cd_situacao_candidatura VARCHAR(10) PRIMARY KEY,
    ds_situacao_candidatura TEXT
)
    """

tp_eleicao = """CREATE TABLE IF NOT EXISTS tp_eleicao(
    cd_tipo_eleicao VARCHAR(10) PRIMARY KEY,
    nm_tipo_eleicao TEXT
)
    """

turno = """CREATE TABLE IF NOT EXISTS turno(
    cd_sit_tot_turno VARCHAR(10) PRIMARY KEY,
    ds_sit_tot_turno TEXT
)
    """


conection.execute(cargo)
conection.execute(coligacao)
conection.execute(det_cand)
conection.execute(ds_eleicao)
conection.execute(estado)
conection.execute(municipio)
conection.execute(partido)
conection.execute(sit_cand)
conection.execute(tp_eleicao)
conection.execute(turno)
conection.execute(cand_geral)





cand_acum = """CREATE TABLE IF NOT EXISTS cand_acum(
    id INT AUTO_INCREMENT PRIMARY KEY,
    cd_tipo_eleicao VARCHAR(10),
    cd_eleicao VARCHAR(10), 
    cd_municipio VARCHAR(10), 
    sg_uf VARCHAR(10), 
    sg_ue VARCHAR(10), 
    cd_cargo VARCHAR(10),
    nr_turno TEXT, 
    sq_candidato TEXT, 
    nr_candidato TEXT,
    nm_candidato TEXT, 
    nm_urna_candidato TEXT, 
    cd_situacao_candidatura VARCHAR(10),
    cd_detalhe_situacao_cand VARCHAR(10), 
    nr_partido VARCHAR(10), 
    sq_coligacao VARCHAR(30), 
    ds_composicao_coligacao VARCHAR(200), 
    cd_sit_tot_turno VARCHAR(10), 
    sum_votos_municipio INT,
    foreign key (cd_tipo_eleicao) references tp_eleicao(cd_tipo_eleicao),
    foreign key (cd_eleicao) references ds_eleicao(cd_eleicao),
    foreign key (cd_municipio) references municipio(cd_municipio),
    foreign key (sg_uf, sg_ue) references estado(sg_uf, sg_ue),
    foreign key (cd_cargo) references cargo(cd_cargo),
    foreign key (cd_situacao_candidatura) references sit_cand(cd_situacao_candidatura),
    foreign key (cd_detalhe_situacao_cand) references det_cand(cd_detalhe_situacao_cand),
    foreign key (nr_partido) references partido(nr_partido),
    foreign key (sq_coligacao, ds_composicao_coligacao) references coligacao(sq_coligacao, ds_composicao_coligacao),
    foreign key (cd_sit_tot_turno) references turno(cd_sit_tot_turno)
)
    """


cand_total = """CREATE TABLE IF NOT EXISTS cand_total(
    id INT AUTO_INCREMENT PRIMARY KEY,
    cd_tipo_eleicao VARCHAR(10),
    cd_eleicao VARCHAR(10), 
    sg_uf VARCHAR(10), 
    sg_ue VARCHAR(10), 
    cd_cargo VARCHAR(10),
    nr_turno TEXT, 
    sq_candidato TEXT, 
    nr_candidato TEXT,
    nm_candidato TEXT, 
    nm_urna_candidato TEXT, 
    cd_situacao_candidatura VARCHAR(10),
    cd_detalhe_situacao_cand VARCHAR(10), 
    nr_partido VARCHAR(10), 
    sq_coligacao VARCHAR(30), 
    ds_composicao_coligacao VARCHAR(200), 
    cd_sit_tot_turno VARCHAR(10), 
    sum_votos_total INT,
    foreign key (cd_tipo_eleicao) references tp_eleicao(cd_tipo_eleicao),
    foreign key (cd_eleicao) references ds_eleicao(cd_eleicao),
    foreign key (sg_uf, sg_ue) references estado(sg_uf, sg_ue),
    foreign key (cd_cargo) references cargo(cd_cargo),
    foreign key (cd_situacao_candidatura) references sit_cand(cd_situacao_candidatura),
    foreign key (cd_detalhe_situacao_cand) references det_cand(cd_detalhe_situacao_cand),
    foreign key (nr_partido) references partido(nr_partido),
    foreign key (sq_coligacao, ds_composicao_coligacao) references coligacao(sq_coligacao, ds_composicao_coligacao),
    foreign key (cd_sit_tot_turno) references turno(cd_sit_tot_turno)
)
    """

conection.execute(cand_acum)
conection.execute(cand_total)
