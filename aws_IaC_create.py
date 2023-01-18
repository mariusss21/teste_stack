import configparser
import boto3

from aws_rds import vacinacao_rds
from aws_dynamodb import vacinacao_dynamodb

if __name__ == "__main__":
    """
    DynamoDB:
    * Instantiate DynamoDB resource
    * Check if Table exists
    * Create table is doesn't exists


    RDS (PostgrSQL):
    * Instantiate RDS client
    * Create DB instance
    """
    
    print('Getting config...')
    config = configparser.ConfigParser()
    config.read_file(open('config.cfg'))

    AWS_REGION = config.get('AWS', 'AWS_REGION')
    AWS_ID = config.get('AWS', 'AWS_ID')
    AWS_SECRET = config.get('AWS', 'AWS_SECRET')
    TABLE_NAME = config.get('DYNAMO', 'TABLE_NAME')
    
    # DynamoDB
    print('Instantiating DynamoDB resource...')
    dyn_resource = boto3.resource(service_name = 'dynamodb',region_name = AWS_REGION,
              aws_access_key_id = AWS_ID,
              aws_secret_access_key = AWS_SECRET)

    vac_dynamo = vacinacao_dynamodb(dyn_resource)
    dynamo_table_exists = vac_dynamo.exists(TABLE_NAME)

    print(f'Check if {TABLE_NAME} exists: {dynamo_table_exists}')

    if not dynamo_table_exists:
        print(f"\nCreating table {TABLE_NAME}...")
        vac_dynamo.create_table(TABLE_NAME)
        print(f"\nCreated table {vac_dynamo.table.name}.")

    print(f'DynamoDB status: {vac_dynamo.table_status(TABLE_NAME)}')


    # RDS
    RDS_DB_NAME = config.get('RDS', 'RDS_DB_NAME')
    RDS_INSTANCE_ID = config.get('RDS', 'RDS_INSTANCE_ID')
    RDS_USER_NAME = config.get('RDS', 'RDS_USER_NAME')
    RDS_USER_PASS = config.get('RDS', 'RDS_USER_PASS')
    RDS_PORT = config.get('RDS', 'RDS_PORT') 

    print('Instantiating RDS client...')
    rds_client = boto3.client(service_name = 'rds',region_name = AWS_REGION,
              aws_access_key_id = AWS_ID,
              aws_secret_access_key = AWS_SECRET)

    vac_rds = vacinacao_rds(rds_client)

    print(f'Creating RDS instance...')
    vac_rds.create_rds_instance(
        rds_db_name = RDS_DB_NAME,
        rds_instance_id = RDS_INSTANCE_ID,
        rds_user_pass = RDS_USER_NAME,
        rds_user_name = RDS_USER_PASS,
        rds_port = RDS_PORT
    )


