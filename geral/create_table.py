from conexao import *

composicao = """
CREATE TABLE IF NOT EXISTS composicao(
    id int auto_increment not null primary key,
    sq_candidato VARCHAR(30),
    nm_candidato TEXT,
    nm_urna_candidato TEXT,
    cd_cargo VARCHAR(10), 
    ds_cargo TEXT, 
    nr_partido VARCHAR(10), 
    sg_partido TEXT, 
    sg_uf VARCHAR(10), 
    sg_ue VARCHAR(10), 
    dt_entrada DATE,
    dt_saida DATE
);
"""

conection.execute(composicao)