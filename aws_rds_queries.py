import boto3
import configparser
import pandas as pd

from aws_rds import vacinacao_rds

if __name__ == '__main__':
    
    print('Getting config...')
    config = configparser.ConfigParser()
    config.read_file(open('config.cfg'))

    AWS_REGION = config.get('AWS', 'AWS_REGION')
    AWS_ID = config.get('AWS', 'AWS_ID')
    AWS_SECRET = config.get('AWS', 'AWS_SECRET')

    RDS_DB_NAME = config.get('RDS', 'RDS_DB_NAME')
    RDS_INSTANCE_ID = config.get('RDS', 'RDS_INSTANCE_ID')
    RDS_USER_NAME = config.get('RDS', 'RDS_USER_NAME')
    RDS_USER_PASS = config.get('RDS', 'RDS_USER_PASS')
    RDS_PORT = config.get('RDS', 'RDS_PORT') 
    RDS_TABLE_NAME = config.get('RDS', 'RDS_TABLE_NAME')

    print('Instantiating RDS client...')
    rds_client = boto3.client(service_name = 'rds',region_name = AWS_REGION,
              aws_access_key_id = AWS_ID,
              aws_secret_access_key = AWS_SECRET)

    vac_rds = vacinacao_rds(rds_client)

    print('Getting endpoint URL')
    instances = rds_client.describe_db_instances(DBInstanceIdentifier=RDS_INSTANCE_ID)
    ENDPOINT = instances.get('DBInstances')[0].get('Endpoint').get('Address')

    # queries
    drop_table = """
    DROP TABLE IF EXISTS vacinacao"""

    create_table = """
    CREATE TABLE IF NOT EXISTS vacinacao(
        paciente_id VARCHAR,
        paciente_enumSexoBiologico VARCHAR,
        paciente_idade INT,
        paciente_racaCor_valor VARCHAR,
        paciente_endereco_uf VARCHAR,
        vacina_descricao_dose VARCHAR,
        vacina_dataAplicacao VARCHAR,
        vacina_nome VARCHAR,
        vacina_lote VARCHAR,
        estabelecimento_razaoSocial VARCHAR,
        PRIMARY KEY (paciente_id, vacina_dataAplicacao))
    """ 

    select_from = """ SELECT * FROM vacinacao """

    print('Connecting to database...')
    cur, conn = vac_rds.connect_database(
        endpoint=ENDPOINT,
        dbname=RDS_DB_NAME,
        user=RDS_USER_NAME,
        password=RDS_USER_PASS,
        port=RDS_PORT
    )

    print('Executing drop table query...')
    vac_rds.execute_query(cur, drop_table)

    print('Executing create table query...')
    vac_rds.execute_query(cur, create_table)

    print('Reading data from csv file...')
    df = pd.read_csv('vacinacao.csv')
    df = df.head(1500)

    print('Executing insert query...')
    vac_rds.execute_query_write_dataframe(conn, cur, df, RDS_TABLE_NAME)

    print('Executing select query...')
    df_select = vac_rds.execute_query_select_df(cur, select_from)
    print(df_select)

