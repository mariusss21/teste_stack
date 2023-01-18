# Teste Técnico para engenharia de dados

## Descrição do teste:

O objetivo do teste é fazer ingestão, transformação e carga de dados de um dataset público que contém informações sobre a vacinação contra a COVID-19.
https://opendatasus.saude.gov.br/dataset/covid-19-vacinacao

## Tarefas:

* Escreva uma aplicação em Python que seja capaz de extrair dados da APi e realizar os seguintes requisitos necessários:
* Extraia 10 colunas (as colunas que você julgar necessárias).
* Persistir o mesmo em uma base não relacional (NoSql) em alguma Cloud (AWS, GCP ou Azure)
* Persistir o mesmo em uma base relacional (SQL). Sugestão: MySQL ou PostgreSQL em alguma Cloud (AWS, GCP ou Azure)
* Persistir o mesmo no formato parquet com o devido particionamento que julgar necessário em seu file system local.

## O que será avaliado:

* O padrão de codificação e o conhecimento de bibliotecas em Python; 
* Conhecimento básico de modelagem de dados; 
* Conhecimento no tratamento dos dados a partir de um dataset; 
* Processo de ETL bem executado, levando em consideração: encoding, tratamento de campos vazios, dados duplicados e demais boas práticas;
* Código organizado no Github;
* Item opcional: Criar algum processo integrado com o Airflow ou outro orquestrador será um diferencial.

## Solução do problema:

### Ferramentas utilizadas:

A solução foi desenvolvida em Python e SQL, fazendo uso da AWS como provedor Cloud.

### Bancos de dados utilizados

* SQL: Amazon RDS (Managed Relational database service) com engine PostgreSQL
* NoSQL: Amazon DynamoDB

### Arquivos

* aws_dynamodb.py: Contém todas as funções do DynamoDB
* aws_dynamodb_write_data.py: Efetua escrita e leitura de dados do DynamoDB
* aws_rds.py: Contém todas as funções do RDS
* aws_rds_queries.py: Cria a tabela, efetua escrita e leitura de dados do RDS
* aws_IaC_create.py: Cria instância RDS na AWS e uma tabela no DynamoDB
* config.cfg*: Contém as informções estáticas necessárias para rodar a aplicação (usuários, senhas...)
* read_from_api.py: Efetua a leitura e tratamento dos dados da API e os salva em arquivos

### Dicionário de dados

![](/images/dicionario_dados.png)

Com a limitação de 10 colunas para os dados, foram selecionadas as colunas que pudessem minimamente prover informações  sobre o paciente, a vacina e o estabelecimento. Destaco entre elas a idade (a vacinação é agrupada pela idade), dados de grupos de paciente (sexo, raça/cor, estado) e informações sobre a vacina (data de aplicação, lote, nome e descrição da dose) e o estabelecimento onde ela foi aplicada (razão social).

Colunas selecionadas:

* paciente_id
* paciente_enumSexoBiologico
* paciente_idade 
* paciente_racaCor_valor 
* paciente_endereco_uf 
* vacina_descricao_dose 
* vacina_dataAplicacao 
* vacina_nome 
* vacina_lote 
* estabelecimento_razaoSocial 

### Análise exploratória/tratamento de dados


## Próximos passos/melhorias

* Criar script Python para deletar a estrutura montada na AWS
* Utilizar o Airflow para orquestrar o ETL