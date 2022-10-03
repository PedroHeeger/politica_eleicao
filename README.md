Esse projeto tem como objetivo fazer comparações entre a composição dos políticos eleitos em 2018, com os de 2022 que será feito futuramente.
Iremos verificar quantos candidatos a cada cargo, quanto cada partido elegeu em cada estado, iremos calcular os quocientes eleitoral e partidário, a somatória de votos válidos total e de cada partido para os cargos que sejam de eleições proporcionais.


A pasta __eleição_18__ contém todos os arquivos para construção do banco tanto no Cassandra como no MySQL, além dos códigos para inserção e manipulação dos dados.

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
Essa pasta é a segunda a ser utilizada que irá utilizar os dados no Dataframe criado no arquivo __selection__ da pasta __cql__. A partir desse Dataframe será selecionado as colunas para cada tabela, dividido a tabela original do Cassandra em várias tabelas menores (auxiliares) e a tabela principal (candidato). Essas tabelas utilizarão o conceito de chaves primárias e estrangeiras para que seja possível conectar as informações entre eleas.

    . create_table: Cria todas as 11 tabelas, porém as tabelas podem ser criadas diretamente nos Inserts, sendo criada automaticamente sem determinar os tipos.
    . delete_table: Deleta qualquer tabela que seja especificada.
    . insert-"nome_table": Todos os inserts serão feitos em dois modelos, inserção em Spark via JDBC, e inserção em Pandas via SQLAlchemy. As colunas serão selecionadas conforme seja a tabela.


### Pasta __sql__
Essa pasta é a terceira a ser utilizada que extrair os dados da tabela principal candidato, e somar o numéro de votos de cada candidato e inserir em duas novas tabelas que serão criadas no arquivo __create_table__ na pasta __cql-sql__.

    . conexão: Conexão criada para manipular os dados no MySQL via SQLAlchemy, utilizando as variáveis de ambiente do arquivo __.env__. Essas variáveis é utilizada também na configuração da URL do Spark pelo JDBC.
    . insert-cand_acum: Extrai os dados da tabela principal (candidato) e soma o número de votos por candidato e município e insere em uma nova tabela, utilizando o Spark via JDBC.
    . insert-cand_total: Extrai os dados da tabela principal (candidato) e soma o número de votos por candidato e ESTADO e insere em uma nova tabela, utilizando o Spark via JDBC.





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
