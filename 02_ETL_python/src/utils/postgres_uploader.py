import os
import logging
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime
import pandas as pd
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class PostgresUploader:
    def __init__(self):
        load_dotenv()
        db_name = os.getenv("DB_NAME")
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        host = os.getenv("DB_HOST")
        port = os.getenv("DB_PORT", 5432)

        conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}"
        self.engine = create_engine(conn_str)
        logging.info(f"Conectado ao Postgres RDS em {host}:{port}/{db_name}")

    def upload_df(self, df, schema, table_name, if_exists="replace", index=False):
        try:
            df['data_insercao'] = datetime.now()
            df.to_sql(
                table_name,
                self.engine,
                schema=schema,
                if_exists=if_exists,
                index=index,
            )
            logging.info(f"DataFrame salvo na tabela '{schema}.{table_name}'")
        except Exception as e:
            logging.error(f"Erro ao salvar DataFrame no Postgres: {e}")
            raise
    def read_table(self, schema, table_name):
        """Lê uma tabela do PostgreSQL e retorna como DataFrame."""
        try:
            query = text(f'SELECT * FROM "{schema}"."{table_name}"')
            df = pd.read_sql(query, self.engine)
            logging.info(f"Tabela '{schema}.{table_name}' lida com sucesso.")
            return df
        except Exception as e:
            logging.error(f"Erro ao ler a tabela '{schema}.{table_name}': {e}")
            raise