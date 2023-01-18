import boto3
import configparser
import pandas as pd

from aws_dynamodb import vacinacao_dynamodb


if __name__ == '__main__':
    print('Getting config...')
    config = configparser.ConfigParser()
    config.read_file(open('config.cfg'))

    AWS_REGION = config.get('AWS', 'AWS_REGION')
    AWS_ID = config.get('AWS', 'AWS_ID')
    AWS_SECRET = config.get('AWS', 'AWS_SECRET')
    TABLE_NAME = config.get('DYNAMO', 'TABLE_NAME')
    
    print('Instantiating DynamoDB resource...')
    dyn_resource = boto3.resource(service_name = 'dynamodb',region_name = AWS_REGION,
              aws_access_key_id = AWS_ID,
              aws_secret_access_key = AWS_SECRET)

    vacinacao = vacinacao_dynamodb(dyn_resource)

    print('Reading data from csv file...')
    df = pd.read_csv('vacinacao.csv')
    df = df.head(1500)

    print('Verifying table status...')
    if vacinacao.table_status(TABLE_NAME) == 'ACTIVE':
        print('The table is ACTIVE, writing data...')
        vacinacao.batch_write(df, TABLE_NAME)
    else:
        print('Table not active!')

    print('Reading data from table...')
    data = vacinacao.read_data(TABLE_NAME)
    print(pd.DataFrame(data))