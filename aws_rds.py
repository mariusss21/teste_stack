import logging
import pandas as pd

import psycopg2
import psycopg2.extras as extras

from botocore.exceptions import ClientError

logging.basicConfig(filename='app.log', filemode='a')


class vacinacao_rds:
    def __init__(self, rds_cliente):
        self.rds_client = rds_cliente


    def create_rds_instance(self, rds_db_name, rds_instance_id, rds_user_pass, rds_user_name, rds_port):
        """
        Creates AWS RDS PostgreSQL instance

        Args:
            rds_db_name (string): Database name
            rds_instance_id (string): Database instance name
            rds_user_pass (string): PostgreSQL password
            rds_user_name (string): PostgreSQL username
            rds_port (string): PostgreSQL port
        """
        try:
            response = self.rds_client.create_db_instance(
                AllocatedStorage=5,
                DBName=rds_db_name,
                DBInstanceClass='db.t3.micro',
                DBInstanceIdentifier=rds_instance_id,
                Engine='postgres',
                MasterUserPassword=rds_user_pass,
                MasterUsername=rds_user_name,
                Port=int(rds_port),
            )
        except ClientError as err:
            logging.error(
                "Couldn't create DB instance %s. Here's why: %s: %s", rds_instance_id,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            print(f'RDS instance ({rds_instance_id}) was created')


    def connect_database(self, endpoint, port, dbname, user, password):
        """
        Connect to PostgreSQL database

        Args:
            endpoint (string): AWS PostgreSQL endpoint
            port (string): PostgreSQL port
            dbname (string): Database name
            user (string): PostgreSQL username
            password (string): PostgreSQL password

        Returns:
            cur: Connection cursor
            conn: Connection
        """
        try:
            conn = psycopg2.connect(host=endpoint, port=port, database=dbname, user=user, password=password, sslrootcert="SSLCERTIFICATE")
            conn.set_session(autocommit=True)
            cur = conn.cursor()
        except Exception as e:
            print("Database connection failed due to {}".format(e)) 
        else:
            return cur, conn


    def execute_query_select_df(self, cur, query):
        """
        Execute select query and returns dataframe with data

        Args:
            cur: Connection cursor
            query (String): Select query

        Returns:
            pd.DataFrame: Dataframe with query result
        """
        cur.execute(query)
        query_results = cur.fetchall()
        return pd.DataFrame(query_results)

    
    def execute_query(self, cur, query):
        """
        Exectute received query and returns nothing

        Args:
            cur: Connection cursor
            query (String): Select query
        """
        cur.execute(query)
        

    def execute_query_write_dataframe(self, conn, cur, df, table):
        """
        Receives dataframe and writes data in PostgreSQL

        Args:
            conn: Connection
            cur: Connection cursor
            df (pd.DataFrame): Dataframe with data to be write
            table (string): Table name

        Returns:
            int: returns 1 in case of error and 0 otherwise
        """

        tuples = [tuple(x) for x in df.to_numpy()]
        cols = ','.join(list(df.columns))

        # SQL query to execute
        query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
        try:
            extras.execute_values(cur, query, tuples)
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            return 1
        print("the dataframe is inserted")
        return 0



