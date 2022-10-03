# PROJETO PESSOAL POLITICA_ELEICAO

Esse projeto tem como objetivo fazer comparações entre a composição dos políticos eleitos em 2018, com os de 2022 que será feito futuramente (Aguardar o termíno das eleiçõs, apuração e disponibilização da base de dados pelo TSE).
Iremos verificar quantos candidatos a cada cargo, quanto cada partido elegeu em cada estado, iremos calcular os quocientes eleitoral e partidário, a somatória de votos válidos total e de cada partido para os cargos que sejam de eleições proporcionais.

Esse projeto é dividido em:
- eleicao_18: Toda a construção dos bancos para eleição de 2018.
- eleicao_22: Toda a construção dos bancos para eleição de 2022.
- geral (EM CONSTRUÇÃO): Essa pasta será para construção de um outro banco no MySQL que vai receber dos bancos das eleições 18 e 22, apenas os candidatos eleitos, assim montaremos a composição dos políticos eleitos nos cargos com a inserção de duas colunas de data para monitorar a saída e entrada de políticos aos cargos durante o mandato. Sendo possível adicionar os suplentes que entraram/entrarão ao longo dos mandatos.

A pasta __eleição_18__ contém todos os arquivos para construção do banco tanto no Cassandra como no MySQL, além dos códigos para inserção e manipulação dos dados. É dividida em:
- Pasta conector: Contém os arquivos JAR que serão utilizado para fazer as conexões na configuração da sessão do Spark.
- Pasta cql
- Pasta cql-sql
- Pasta sql
- con_spark: Arquivo de configuração da sessão do Spark para ser usado nos demais arquivos. Essa configuração contém arquivos JAR (conectores) que permitem a conexão com o banco Cassandra e MySQL e também com o formato de arquivo em Excel.
- con_sqlalch: Arquivo de configuração da conexão do SQL Alchemy com o banco MySQL

### Pasta __cql__
Essa é a primeira pasta a ser utilizada que irá construir o banco Cassandra e apenas uma tabela geral, esse será nosso Data Lake. Então iremos pegar os dados no arquivo excel dentro da pasta __dataset/csv__, transformará em arquivo parquet que será salvo dentro dessa mesma pasta em __dataset/parquet__. Com os dados mais leve em parquet, será extraído esses dados e inseridos na tabela criada no Cassandra.
 
1) create_keyspace: Cria o banco no Cassandra.
2) main: Arquivo principal que cria a conexão com o banco criado.
3) create_table: Cria a tabela geral para receber os dados no banco criado.
4) parquet: Extrai os dados do arquivo csv e transforma em parquet.
5) insert: Extrai os dados do arquivo parquet, trata as colunas, transformando os nomes de maíscula para minúscula, cria uma nova coluna do tipo UUID para uso do banco Cassandra, e por fim, insere na tabela criada no banco.
6) zdelete: Deleta a tabela criada no banco.
7) selection: Extrai os dados na tabela criada no banco Cassandra e esse Dataframe será utilizado por arquivos da pasta __cql-sql__.

### Pasta __cql-sql__
Essa pasta é a terceira a ser utilizada que irá utilizar os dados do Dataframe criado no arquivo __selection__ da pasta __cql__. A partir desse Dataframe será selecionado as colunas para cada tabela, dividido a tabela original do Cassandra em várias tabelas menores (auxiliares) e a tabela principal (candidato). Essas tabelas utilizarão o conceito de chaves primárias e estrangeiras para que seja possível conectar as informações entre eleas.

Essa pasta é referente ao processo de ETL que será realizado. Extração dos dados da tabela do banco Cassandra, manipulação dos dados para construção de tabelas separadas e utilização do conceito de chaves e inserção no banco MySQL. Todos os inserts podem ser feitos em dois modelos, inserção em Spark via JDBC (Recomendado), e inserção em Pandas via SQLAlchemy. As colunas serão selecionadas conforme seja a tabela a ser criada.


### Pasta __sql__
Essa pasta é a segunda a ser utilizada onde além de criar o banco, as tabelas e as views no banco MySQL, vai extrair os dados da tabela principal candidato, somar o numéro de votos de cada candidato e inserir em duas novas tabelas que serão criadas para informar os resultados acumulados por estado e município.

1) create_db: Cria o banco de dados no MySQL.
2) create_table: Cria todas as 11 tabelas informando as colunas e seus tipos. Porém as tabelas podem ser criadas diretamente nos Inserts, sendo criada automaticamente sem determinar os tipos.
3) zdelete_table: Deleta qualquer tabela que seja especificada.
4) insert-cand_acum: Inserção dos dados na tabela cand_acum, onde será somado a quantidade de votos de cada candidato por município, estado e cargo. (Soma as zonas de cada município).
5) insert-cand_total: Inserção dos dados na tabela cand_total, onde será somado a quantidade de votos de cada candidato por estado e cargo. (Soma os municípios de cada estado).
6) insert-comp_partido: Inserção dos dados na tabela comp_partido, onde será informado quantos candidatos disputarão o pleito e quantos foram eleito por estado e cargo.
7) insert-quociente: Inserção dos dados na tabela quociente, onde será calculado a somatória dos votos válidos por partido, dos votos válidos totais, o quociente eleitoral (qe), o quociente partidário (qp) e a porcentagem dos votos (porc) por partido, estado e cargo.
8) export-"nome_tabela": São dois arquivos que irão exportar nossos resultados finais para uma tabela de excel.






## LINKS DE CONSULTA

    . Como funciona o sistema proporcional?: https://www.tse.jus.br/o-tse/escola-judiciaria-eleitoral/publicacoes/revistas-da-eje/artigos/revista-eletronica-eje-n.-5-ano-3/como-funciona-o-sistema-proporcional

    . RESULTADO ELEIÇÃO 2018: placar.eleicoes.uol.com.br/2018/1turno/rs/apuracao-no-estado/

    . NÚMERO DE DEPUTADOS POR ESTADO: www2.camara.leg.br/a-camara/conheca/numero-de-deputados-por-estado

    . NÚMERO DE DEPUTADOS ESTADUAIS POR ESTADO: https://noticias.uol.com.br/politica/eleicoes/2018/raio-x/assembleias/numero-de-deputados-estaduais-eleitos-por-partido/?uf=sp

    . COMPOSICAO CAMARA 2018: camara.leg.br/internet/infdoc/novoconteudo/Acervo/CELEG/Carometro/carometro_legislatura56.pdf

    . SUPLENTES EM EXERCÍCIO: camara.leg.br/deputados/suplentes-em-exercicio

    . COMPOSICAO GOVERNO 2018: pt.wikipedia.org/wiki/Lista_de_governadores_das_unidades_federativas_do_Brasil_(2019–2023)

    . LISTA COM TODOS PARTIDOS ANTIGOS: https://www.tse.jus.br/eleitor/glossario/termos/partido-politico

    . LISTA COM OS PARTIDOS ATUAIS: pt.wikipedia.org/wiki/Lista_de_partidos_políticos_do_Brasil#:~:text=Desde%20fevereiro%20de%202022%2C%20o,Tribunal%20Superior%20Eleitoral%20(TSE).
