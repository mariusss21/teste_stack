import json
import logging
from decimal import Decimal
from botocore.exceptions import ClientError

logging.basicConfig(filename='app.log', filemode='w')


class vacinacao_dynamodb:
    def __init__(self, dyn_resource):
        self.dyn_resource = dyn_resource
        self.table = None

    def exists(self, table_name):
        """
        Check if the given table exists

        Args:
            table_name (string): Table name

        Returns:
            bool: True if table exists and False otherwise
        """
        try:
            table = self.dyn_resource.Table(table_name)
            table.load()
            exists = True
        except ClientError as err:
            if err.response['Error']['Code'] == 'ResourceNotFoundException':
                exists = False
            else:
                logging.error(
                    "Couldn't check for existence of %s. Here's why: %s: %s",
                    table_name,
                    err.response['Error']['Code'], err.response['Error']['Message'])
                raise
        else:
            self.table = table
        return exists

    def create_table(self, table_name):
        """
        Create table

        Args:
            table_name (string): table name

        """
        try:
            self.table = self.dyn_resource.create_table(
                TableName=table_name,
                KeySchema=[
                    {'AttributeName': 'paciente_id', 'KeyType': 'HASH'},  # Partition key
                    {'AttributeName': 'vacina_dataAplicacao', 'KeyType': 'RANGE'}  # Sort key
                ],
                AttributeDefinitions=[
                    {'AttributeName':'paciente_id', 'AttributeType': 'S'}, 
                    {'AttributeName':'vacina_dataAplicacao', 'AttributeType': 'S'}
                ],
                ProvisionedThroughput={'ReadCapacityUnits': 10, 'WriteCapacityUnits': 10})
            
            self.table.wait_until_exists()
        except ClientError as err:
            logging.error(
                "Couldn't create table %s. Here's why: %s: %s", table_name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise


    def batch_write(self, data, table_name):
        """
        Batch write data from dataframe

        Args:
            data (pd.DataFrame): Dataframe with the data
            table_name (string): table name
        """
        table = self.dyn_resource.Table(table_name)

        try:
            with table.batch_writer() as writer:
                for index, row in data.iterrows():
                    writer.put_item(json.loads(row.to_json(), parse_float=Decimal))
        except ClientError as err:
            logging.error(
                "Couldn't load data into table %s. Here's why: %s: %s", self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise 

    def table_status(self, table_name):
        """
        Gets table status

        Args:
            table_name (string): table name

        Returns:
            string: table status
        """
        table = self.dyn_resource.Table(table_name)
        return table.table_status


    def read_data(self, table_name):
        """
        Reads data from DynamoDB

        Args:
            table_name (string): table name

        Returns:
            string: received data
        """
        table = self.dyn_resource.Table(table_name)
        response = table.scan() #FilterExpression=Attr("paciente_enumSexoBiologico").eq('M')
        data = response['Items']
        return data