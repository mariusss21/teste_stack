import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
import logging

logging.basicConfig(filename='app.log', filemode='a')

def read_from_api(url, parameter, auth_id, auth_pass):
    """
    Request data from server and returns a JSON object

    Args:
        url (string): API URL
        parameter (string): JSON parameters
        auth_id (string): Auth login
        auth_pass (string): Auth password

    Returns:
        JSON: response from request
    """
    try:
        r = requests.post(url, json=parameter, auth = HTTPBasicAuth(auth_id, auth_pass))
        r.raise_for_status()
    except requests.exceptions.HTTPError as errh:
        logging.error("Http Error:",errh)
    except requests.exceptions.ConnectionError as errc:
        logging.error("Error Connecting:",errc)
    except requests.exceptions.Timeout as errt:
        logging.error("Timeout Error:",errt)
    except requests.exceptions.RequestException as err:
        logging.error("OOps: Something Else",err)
    return r.json()


def pandas_json_normalize(data, record_path_, separator, level):
    """
    Normalize data returned from API

    Args:
        data (JSON): Data
        record_path_ (list): Path inside JSON
        separator (string): Separator
        level (int): Amount of levels inside JSON
    """

    df = pd.json_normalize(data, record_path=record_path_, sep=separator, max_level=level)
    return df


def data_transformation(df_api, filename):
    """
    Transform received data. Transformations:
    * Remove columns that aren't in API manual
    * Filter more relevant columns
    * Remove duplicates
    * Rename columns 
    * Remove time from vacina_dataAplicacao
    * Save dataframe to CSV and parquet


    Args:
        df_api (pd.DataFrame): Dataframe generated after json_normalize
        filename (string): 
    """

    df = df_api.copy()

    # API manual has a list of variables that starts with the words of words_list (Useful for EDA)
    columns_list_from_manual = []
    words_list = ['paciente', 'vacina', 'estabelecimento', 'document', 'sistema']

    for c in df.columns:
        for w in words_list:
            if w in c:
                columns_list_from_manual.append(c)

    df_from_manual = df[columns_list_from_manual]

    print('Filtering columns...')
    df_filtered = df_from_manual[[
        '_source_paciente_id',
        '_source_paciente_enumSexoBiologico',
        '_source_paciente_idade',
        '_source_paciente_racaCor_valor',
        '_source_paciente_endereco_uf',
        '_source_vacina_descricao_dose',
        '_source_vacina_dataAplicacao',
        '_source_vacina_nome',
        '_source_vacina_lote',
        '_source_estabelecimento_razaoSocial'
        ]]

    print('Droping duplicates...')
    df_filtered = df_filtered.drop_duplicates()

    print('Renaming columns...')
    df_filtered.columns = df_filtered.columns.str.replace('_source_', '')

    print('Filling missing values')
    df_filtered['paciente_endereco_uf'].fillna('BR', inplace=True)
    df_filtered['vacina_descricao_dose'].fillna('-', inplace=True)

    df_filtered.loc[df_filtered.duplicated(subset=['paciente_id']), 'vacina_dataAplicacao'] = df_filtered.loc[df_filtered.duplicated(subset=['paciente_id']), 'vacina_dataAplicacao'].str.replace('000Z', '001Z')

    print('Saving data in CSV file')
    df_filtered.to_csv(path_or_buf= filename + '.csv', index=False)

    print('Adding year and month columns to partition parquet file...')
    df_filtered.loc[:, 'vacina_dataAplicacao'] = pd.to_datetime(df_filtered['vacina_dataAplicacao'])
    df_filtered['ano'] = df_filtered['vacina_dataAplicacao'].dt.year
    df_filtered['mes'] = df_filtered['vacina_dataAplicacao'].dt.month

    print('Saving parquet file')
    df_filtered.to_parquet('./vacinacao_parquet', partition_cols=['ano', 'mes'])


if __name__ == '__main__':
    
    URL = 'https://imunizacao-es.saude.gov.br/_search'
    PARAMETER = {'size': 10000}
    AUTH_ID = 'imunizacao_public'
    AUTH_PASS = 'qlto5t&7r_@+#Tlstigi'
    FILENAME = 'vacinacao'

    print('Reading data from API...')
    Data = read_from_api(url=URL,
                         parameter=PARAMETER,
                         auth_id=AUTH_ID,   
                         auth_pass=AUTH_PASS)

    if Data is not None:
        print('Normalizing json data...')
        df = pandas_json_normalize(data=Data,
                                record_path_=['hits', 'hits'],
                                separator='_',
                                level=1)

        print('Transforming data...')
        data_transformation(df_api=df, filename=FILENAME)

        print('Data collected')
        print(pd.read_csv('vacinacao.csv'))