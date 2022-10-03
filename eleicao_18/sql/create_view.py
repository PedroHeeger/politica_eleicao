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
view_cand_geral = """CREATE VIEW view_cand_total as
    SELECT a.nr_turno, a.sg_uf, a.sg_ue, a.nm_candidato, a.nm_urna_candidato, a.cd_cargo, b.ds_cargo, a.nr_partido, c.sg_part_now, a.sum_votos_total, d.ds_sit_tot_turno, a.cd_sit_tot_turno, a.ds_composicao_coligacao, e.ds_situacao_candidatura, f.ds_detalhe_situacao_cand, g.ds_eleicao, g.dt_eleicao, h.nm_tipo_eleicao
    FROM cand_total a inner join cargo b on a.cd_cargo = b.cd_cargo
    inner join partido c on a.nr_partido = c.nr_partido
    inner join turno d on a.cd_sit_tot_turno = d.cd_sit_tot_turno
    inner join sit_cand e on a.cd_situacao_candidatura = e.cd_situacao_candidatura
    inner join det_cand f on a.cd_detalhe_situacao_cand = f.cd_detalhe_situacao_cand
    inner join ds_eleicao g on a.cd_eleicao = g.cd_eleicao
    inner join tp_eleicao h on a.cd_tipo_eleicao = h.cd_tipo_eleicao
    ORDER BY sum_votos_total desc
"""

conection.execute(view_cand_geral)