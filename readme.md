# Teste Técnico para engenharia de dados

## Descrição do teste:
O objetivo do teste é fazer ingestão, transformação e carga de dados de um dataset público que contém informações sobre a vacinação contra a COVID-19.
https://opendatasus.saude.gov.br/dataset/covid-19-vacinacao

Tarefas:

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
A solução foi desenvolvida em Python e fez uso da AWS como provedor Cloud

### Configurações da AWS

### Bancos de dados utilizados
* SQL: Amazon RDS (Managed Relational database service) com engine PostgreSQL
* NoSQL: Amazon DynamoDB

### Arquivos

### Dicionário de dados

** Colocar imagem

Colunas selecionadas:

* paciente_id: 
* paciente_enumSexoBiologico: 
* paciente_idade: 
* paciente_racaCor_valor: 
* paciente_endereco_uf: 
* vacina_descricao_dose: 
* vacina_dataAplicacao: 
* vacina_nome: 
* vacina_lote: 
* estabelecimento_razaoSocial: 

### Análise exploratória/tratamento de dados
