'''
This module will contain the ETL pipeline for the project.
'''

import os
from dotenv import load_dotenv
import psycopg2
from sqlalchemy import create_engine, text
from sqlalchemy.types import String, Integer, Date
from sqlalchemy.dialects.postgresql import insert
import pandas as pd
import tempfile
import logging

# Load environment variables
load_dotenv()


def upsert_method(table, conn, keys, data_iter):
    """
    Custom method for pandas.to_sql() to perform upsert (ON CONFLICT DO NOTHING).
    """
    # Convert the iterator of rows into a list of dictionaries
    data = [dict(zip(keys, row)) for row in data_iter]
    if not data:
        return

    from sqlalchemy import MetaData, Table
    # Create a fresh MetaData instance
    metadata = MetaData()
    # Reflect the table using its name and the fresh metadata
    reflected_table = Table(table.name, metadata, autoload_with=conn)

    # Build the insert statement with ON CONFLICT DO NOTHING on 'numero_documento'
    stmt = insert(reflected_table).values(data)
    stmt = stmt.on_conflict_do_nothing(index_elements=['numero_documento'])
    conn.execute(stmt)


def extract(**kwargs):
    '''
    This function will extract the data from the different CSV files
    and store the data in a staging (PostgreSQL) database.
    '''
    logging.info('Starting extraction process...\n')

    ##################################################
    # Read the data from different sources (CSV files)
    ##################################################
    db_staging_user = os.getenv('DB_STAGING_USER')
    db_staging_password = os.getenv('DB_STAGING_PASSWORD')
    db_staging_host = os.getenv('DB_STAGING_HOST')
    db_staging_port = int(os.getenv('DB_STAGING_PORT', 5432))
    db_staging_name = os.getenv('DB_STAGING_NAME')

    resources_path = os.getenv('RESOURCES_PATH')
    mascotas_propietarios_filename = 'Mascotas_Propietarios_despensaAnimal_Generated.csv'
    propietarios_transacciones_filename = 'Propietarios_Transacciones_despensaAnimal_Generated.csv'

    if db_staging_user is None:
        raise ValueError('DB_STAGING_USER is not set')
    if db_staging_password is None:
        raise ValueError('DB_STAGING_PASSWORD is not set')
    if db_staging_host is None:
        raise ValueError('DB_STAGING_HOST is not set')
    if db_staging_port is None:
        raise ValueError('DB_STAGING_PORT is not set')
    if db_staging_name is None:
        raise ValueError('DB_STAGING_NAME is not set')
    if resources_path is None:
        raise ValueError('RESOURCES_PATH is not set')

    connStaging = psycopg2.connect(
        dbname=db_staging_name,
        user=db_staging_user,
        password=db_staging_password,
        host=db_staging_host,
        port=db_staging_port
    )
    connStaging.autocommit = True

    #################################################
    # Create the database engine for the staging data
    #################################################
    #db_staging_engine = create_engine(f'postgresql://{db_staging_user}:{db_staging_password}@{db_staging_host}:{db_staging_port}/{db_staging_name}')
    db_staging_engine = create_engine(f'postgresql+psycopg2://{db_staging_user}:{db_staging_password}@{db_staging_host}:{db_staging_port}/{db_staging_name}')

    #####################################################################################################
    # Read the file 'Mascotas_Propietarios_despensaAnimal_Generated.csv' and store it in a database table
    #####################################################################################################
    df_mascotas_propietarios = pd.read_csv(f'{resources_path}/{mascotas_propietarios_filename}', delimiter=',', skiprows=1, low_memory=False)

    # Rename columns after loading the data
    df_mascotas_propietarios.columns = [
        'nombre_mascota',
        'raza',
        'peso',
        'fecha_nacimiento',
        'sexo',
        'temperamento',
        'numero_carnet',
        'estado_reproductivo',
        'numero_partos',
        'color',
        'fecha_fallecimiento',
        'motivo_fallecimiento',
        'comentarios_fallecimiento',
        'nombre_propietario',
        'ciudad',
        'direccion',
        'telefono',
        'whatsapp',
        'email',
        'tipo_documento',
        'numero_documento',
        'profesion',
        'estado',
        'notificaciones_whatsapp'
        ]

    # Just print the first 20 rows to test
    logging.info(df_mascotas_propietarios.head(10))
    logging.info(df_mascotas_propietarios.dtypes)

    # Store the CSV file into a database
    with db_staging_engine.begin() as conn:
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS mascotas_propietarios_staging (
                nombre_mascota CHARACTER VARYING,
                raza CHARACTER VARYING,
                peso CHARACTER VARYING,
                fecha_nacimiento DATE,
                sexo CHARACTER VARYING,
                temperamento CHARACTER VARYING,
                numero_carnet CHARACTER VARYING,
                estado_reproductivo CHARACTER VARYING,
                numero_partos CHARACTER VARYING,
                color CHARACTER VARYING,
                fecha_fallecimiento DATE,
                motivo_fallecimiento CHARACTER VARYING,
                comentarios_fallecimiento CHARACTER VARYING,
                nombre_propietario CHARACTER VARYING,
                ciudad CHARACTER VARYING,
                direccion CHARACTER VARYING,
                telefono CHARACTER VARYING,
                whatsapp CHARACTER VARYING,
                email CHARACTER VARYING,
                tipo_documento CHARACTER VARYING,
                numero_documento CHARACTER VARYING,
                profesion CHARACTER VARYING,
                estado CHARACTER VARYING,
                notificaciones_whatsapp CHARACTER VARYING
            );
        '''))
        logging.info('Table "mascotas_propietarios_staging" created successfully.')

    # Store DataFrame into PostgreSQL table named 'mascotas_propietarios_staging'
    with db_staging_engine.connect().execution_options(autocommit=True) as connection:
        df_mascotas_propietarios.to_sql(
            'mascotas_propietarios_staging',
            con=connection,
            if_exists='append',
            index=False
        )

    logging.info('The CSV file data was inserted successfully!')

    #################################################################
    # Get data from the 'mascotas_propietarios_staging' table to test
    #################################################################
    df_mascotas_propietarios_verification = pd.read_sql('SELECT * FROM mascotas_propietarios_staging LIMIT 20;', db_staging_engine)
    df_mascotas_propietarios_verification

    ##########################################################################################################
    # Read the file 'Propietarios_Transacciones_despensaAnimal_Generated.csv' and store it in a database table
    ##########################################################################################################
    df_propietarios_transacciones = pd.read_csv(f'{resources_path}/{propietarios_transacciones_filename}', delimiter=',', skiprows=1, dtype={'numero_documento': str}, low_memory=False)

    # Rename columns after loading the data
    df_propietarios_transacciones.columns = [
        'nombre_propietario',
        'tipo_documento',
        'numero_documento',
        'nombre_mascota',
        'servicio_prestado',
        'valor_servicio',
        'fecha_servicio'
        ]

    # Just print the first 20 rows to test
    df_propietarios_transacciones.head(20)

    # Store the CSV file into a database 
    with db_staging_engine.begin() as conn:
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS propietarios_transacciones_staging (
                nombre_propietario CHARACTER VARYING,
                tipo_documento CHARACTER VARYING,
                numero_documento CHARACTER VARYING,
                nombre_mascota CHARACTER VARYING,
                servicio_prestado CHARACTER VARYING,
                valor_servicio NUMERIC,
                fecha_servicio DATE
            );
        '''))
        logging.info('Table "propietarios_transacciones_staging" created successfully.')

    # Store DataFrame into PostgreSQL table named 'propietarios_transacciones_staging'
    with db_staging_engine.connect().execution_options(autocommit=True) as connection:
        df_propietarios_transacciones.to_sql(
            'propietarios_transacciones_staging',
            con=connection,
            if_exists='append',
            index=False,
            method='multi',
            dtype={
                'nombre_propietario': String,
                'tipo_documento': String,
                'numero_documento': String,
                'nombre_mascota': String,
                'servicio_prestado': String,
                'valor_servicio': Integer,
                'fecha_servicio': Date
            }
        )

    logging.info('The CSV file data was inserted successfully!')

    ######################################################################
    # Get data from the 'propietarios_transacciones_staging' table to test
    ######################################################################
    df_propietarios_transacciones = pd.read_sql('SELECT * FROM propietarios_transacciones_staging LIMIT 20;', db_staging_engine)
    df_propietarios_transacciones

    logging.info('Ending extraction process...\n')


def transform(**kwargs):
    '''
    This function will transform the data from the staging database
    '''
    logging.info('Starting transformation process...\n')

    ######################################################################################
    # Read the data from the staging area and find outliers, null or empty data, and more.
    ######################################################################################
    db_staging_user = os.getenv('DB_STAGING_USER')
    db_staging_password = os.getenv('DB_STAGING_PASSWORD')
    db_staging_host = os.getenv('DB_STAGING_HOST')
    db_staging_port = int(os.getenv('DB_STAGING_PORT', 5432))
    db_staging_name = os.getenv('DB_STAGING_NAME')

    resources_path = os.getenv('RESOURCES_PATH')
    mascotas_propietarios_filename = 'Mascotas_Propietarios_despensaAnimal_Generated.csv'
    propietarios_transacciones_filename = 'Propietarios_Transacciones_despensaAnimal_Generated.csv'

    if db_staging_user is None:
        raise ValueError('DB_STAGING_USER is not set')
    if db_staging_password is None:
        raise ValueError('DB_STAGING_PASSWORD is not set')
    if db_staging_host is None:
        raise ValueError('DB_STAGING_HOST is not set')
    if db_staging_port is None:
        raise ValueError('DB_STAGING_PORT is not set')
    if db_staging_name is None:
        raise ValueError('DB_STAGING_NAME is not set')
    if resources_path is None:
        raise ValueError('RESOURCES_PATH is not set')

    connStaging = psycopg2.connect(
        dbname=db_staging_name,
        user=db_staging_user,
        password=db_staging_password,
        host=db_staging_host,
        port=db_staging_port
    )
    connStaging.autocommit = True 

    #################################################
    # Create the database engine for the staging data
    #################################################
    db_staging_engine = create_engine(f'postgresql://{db_staging_user}:{db_staging_password}@{db_staging_host}:{db_staging_port}/{db_staging_name}')

    df_mascotas_propietarios_staging = pd.read_sql('SELECT * FROM mascotas_propietarios_staging;', db_staging_engine)
    logging.info(df_mascotas_propietarios_staging)

    ##############################################################
    # Remove data that not add value to the transacctions scenario
    ##############################################################
    def drop_unnecessary_columns(df: pd.DataFrame) -> pd.DataFrame:
        columns_to_drop = [
            'comentarios_fallecimiento',
            'motivo_fallecimiento',
            'fecha_fallecimiento',
            'numero_carnet',
            'estado_reproductivo',
            'numero_partos',
            'profesion',
            'notificaciones_whatsapp'
        ]

        for col in columns_to_drop:
            if col in df.columns:
                logging.info(f'Removing column: {col}')
                df.drop(columns=[col], inplace=True)
            else:
                logging.info(f'Warning: Column {col} not found in the dataset.')
        logging.info(f'Columns removed. New shape: {df.shape}')
        return df

    # Apply the function
    df_mascotas_propietarios_cleaned = drop_unnecessary_columns(df_mascotas_propietarios_staging)
    logging.info(df_mascotas_propietarios_cleaned)

    ####################################################
    # Analyze the field: ciudad and its different values
    ####################################################
    df_ciudad = df_mascotas_propietarios_cleaned.groupby('ciudad').size().reset_index(name='total_registros')
    logging.info(df_ciudad)

    # Update all values in 'ciudad' column to 'Cali'
    df_mascotas_propietarios_cleaned['ciudad'] = 'Cali'

    df_ciudad = df_mascotas_propietarios_cleaned.groupby('ciudad').size().reset_index(name='total_registros')
    logging.info(df_ciudad)

    ##################################################
    # Analyze the field: peso and its different values
    ##################################################
    logging.info(df_mascotas_propietarios_cleaned)

    df_mascotas_propietarios_cleaned.groupby('peso').size().reset_index(name='total_registros')

    # Convert all values in 'peso' column to float, if possible (otherwise, set to NaN)
    df_mascotas_propietarios_cleaned['peso'] = pd.to_numeric(df_mascotas_propietarios_cleaned['peso'], errors='coerce')
    df_mascotas_propietarios_cleaned.groupby('peso').size().reset_index(name='total_registros')

    ####################################################
    # Analyze the field: 'date' and its different values
    ####################################################
    df_mascotas_propietarios_cleaned.groupby('fecha_nacimiento').size().reset_index(name='total_registros')

    # Convert all values in 'fecha_nacimiento' column to a datetime greater than 2016-01-01
    df_mascotas_propietarios_cleaned['fecha_nacimiento'] = pd.to_datetime(df_mascotas_propietarios_cleaned['fecha_nacimiento'], errors='coerce')
    df_mascotas_propietarios_cleaned = df_mascotas_propietarios_cleaned[df_mascotas_propietarios_cleaned['fecha_nacimiento'] >= '2016-01-01']
    df_mascotas_propietarios_cleaned.groupby('fecha_nacimiento').size().reset_index(name='total_registros')

    ##############################################
    # Analyze the field: 'telefono' and 'whatsapp'
    ##############################################
    # Ensure 'telefono' column only contains numeric values (otherwise, set to None)
    df_mascotas_propietarios_cleaned.loc[:, 'telefono'] = df_mascotas_propietarios_cleaned['telefono'].apply(
        lambda x: x if str(x).isnumeric() else None
    )

    # Fill missing 'telefono' values with 'whatsapp' numbers
    df_mascotas_propietarios_cleaned.loc[:, 'telefono'] = df_mascotas_propietarios_cleaned['telefono'].fillna(
        df_mascotas_propietarios_cleaned['whatsapp']
    )

    # Display the updated DataFrame
    df_mascotas_propietarios_cleaned[['telefono', 'whatsapp']]

    # Drop the 'whatsapp' column
    df_mascotas_propietarios_cleaned = df_mascotas_propietarios_cleaned.drop(columns=['whatsapp'])
    logging.info(df_mascotas_propietarios_cleaned)

    ############################################################
    # Analyze the field: 'tipo_documento' and 'numero_documento'
    ############################################################
    # Remove every row where 'numero_documento' is NaN or empty
    df_mascotas_propietarios_cleaned = df_mascotas_propietarios_cleaned.dropna(subset=['numero_documento'])
    df_mascotas_propietarios_cleaned = df_mascotas_propietarios_cleaned[df_mascotas_propietarios_cleaned['numero_documento'] != '']
    logging.info(df_mascotas_propietarios_cleaned)

    # Show all the different values in 'tipo_documento'
    df_mascotas_propietarios_cleaned['tipo_documento'].unique()

    # Remove every row where 'tipo_documento' is not 'CC' and overage people so, fecha_nacimiento is less than 18 years
    df_mascotas_propietarios_cleaned = df_mascotas_propietarios_cleaned[df_mascotas_propietarios_cleaned['tipo_documento'] == 'CC']
    logging.info(df_mascotas_propietarios_cleaned)

    ###################################################################################################################
    # Create a merge between the new df_mascotas_propietarios_cleaned and the table: propietarios_transacciones_staging
    ###################################################################################################################
    # Create a merge between the new df_mascotas_propietarios_cleaned and the table: propietarios_transacciones_staging
    df_propietarios_transacciones_staging = pd.read_sql('SELECT * FROM propietarios_transacciones_staging;', db_staging_engine)
    logging.info(df_propietarios_transacciones_staging)

    # Merge the DataFrames with explicit suffixes
    df_merged = pd.merge(
        df_mascotas_propietarios_cleaned,
        df_propietarios_transacciones_staging,
        how='inner',
        on='numero_documento',
        suffixes=('_staging', '_transacciones')
    )

    # Rename the staging version to the canonical name
    if 'nombre_propietario_staging' in df_merged.columns:
        df_merged.rename(columns={'nombre_propietario_staging': 'nombre_propietario'}, inplace=True)

    # Save DataFrame as Parquet (efficient for large data)
    temp_dir = tempfile.gettempdir()
    file_path = os.path.join(temp_dir, 'df_merged.parquet')
    df_merged.to_parquet(file_path)

    logging.info(f'Transformed data saved to {file_path}')

    # Push only file path to XCom
    ti = kwargs['ti']
    ti.xcom_push(key='df_merged_path', value=file_path)


def load(**kwargs):
    '''
    Load transformed data into a data warehouse (Fact & Dimension tables).
    '''
    logging.info('Starting load process...\n')

    # Retrieve the file path from XCom (from transform task)
    ti = kwargs['ti']
    file_path = ti.xcom_pull(task_ids='transform_task', key='df_merged_path')

    if file_path is None or not os.path.exists(file_path):
        logging.error('No valid file found for transformed data!')
        return

    # Read back DataFrame from Parquet file
    df_merged = pd.read_parquet(file_path)
    logging.info(f'Transformed data loaded from {file_path}, shape: {df_merged.shape}')

    ##################################################
    # Database connection details (Data Warehouse)
    ##################################################
    db_dwh_user = os.getenv('DB_DWH_USER')
    db_dwh_password = os.getenv('DB_DWH_PASSWORD')
    db_dwh_host = os.getenv('DB_DWH_HOST')
    db_dwh_port = int(os.getenv('DB_DWH_PORT', 5432))
    db_dwh_name = os.getenv('DB_DWH_NAME')

    if not all([db_dwh_user, db_dwh_password, db_dwh_host, db_dwh_name]):
        raise ValueError('One or more Data Warehouse environment variables are missing.')

    db_dwh_engine = create_engine(f'postgresql+psycopg2://{db_dwh_user}:{db_dwh_password}@{db_dwh_host}:{db_dwh_port}/{db_dwh_name}')

    ##################################################
    # Creating Tables (if they don't exist)
    ##################################################
    with db_dwh_engine.begin() as conn:
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS dim_clients (
                numero_documento VARCHAR PRIMARY KEY,
                nombre_propietario VARCHAR,
                ciudad VARCHAR,
                direccion VARCHAR
            );
        '''))

        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS fact_transactions (
                id SERIAL PRIMARY KEY,
                numero_documento VARCHAR REFERENCES dim_clients(numero_documento),
                servicio_prestado VARCHAR,
                valor_servicio NUMERIC,
                fecha_servicio DATE
            );
        '''))

        logging.info('Fact and Dimension tables created successfully (if not existed).')

    ##################################################
    # Insert Data into Dimension Table using upsert
    ##################################################
    df_dim_clients = df_merged[['numero_documento', 'nombre_propietario', 'ciudad', 'direccion']].drop_duplicates()

    with db_dwh_engine.connect().execution_options(autocommit=True) as connection:
        df_dim_clients.to_sql(
            'dim_clients',
            con=connection,
            if_exists='append',
            index=False,
            method=upsert_method  # Use our custom upsert method here
        )

    logging.info(f'{len(df_dim_clients)} records processed for dim_clients.')

    ##################################################
    # Insert Data into Fact Table
    ##################################################
    df_fact_transactions = df_merged[['numero_documento', 'servicio_prestado', 'valor_servicio', 'fecha_servicio']]

    with db_dwh_engine.connect().execution_options(autocommit=True) as connection:
        df_fact_transactions.to_sql(
            'fact_transactions',
            con=connection,
            if_exists='append',
            index=False,
            method='multi'
        )

    logging.info(f'{len(df_fact_transactions)} records inserted into fact_transactions.')
    logging.info('Load process completed successfully!\n')