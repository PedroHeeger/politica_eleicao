from conexao import *

# Criando o Banco geral no MySQL para receber os dados finais das eleições
create_db = """
CREATE DATABASE IF NOT EXISTS geral;
"""

conection_inicial.execute(create_db)