import logging
import pandas as pd
from utils.postgres_uploader import PostgresUploader

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

class AgregacoesDelivery:
    def __init__(self):
        self.db = PostgresUploader()
        self.trusted_schema = "trusted"
        self.delivery_schema = "delivery"
        logging.info("AgregacoesDelivery inicializada.")

    def executar(self):
        logging.info("Iniciando agregação para camada DELIVERY...")

        # Leitura das tabelas
        df_bancos = self.db.read_table(self.trusted_schema, "bancos")
        df_empregados = self.db.read_table(self.trusted_schema, "empregados")
        df_reclamacoes = self.db.read_table(self.trusted_schema, "reclamacoes")

        logging.info("Tabela 'trusted.bancos' lida com sucesso.")
        logging.info("Tabela 'trusted.empregados' lida com sucesso.")
        logging.info("Tabela 'trusted.reclamacoes' lida com sucesso.")
        logging.info(f"Linhas lidas de 'bancos': {len(df_bancos)}")
        logging.info(f"Linhas lidas de 'empregados': {len(df_empregados)}")
        logging.info(f"Linhas lidas de 'reclamacoes': {len(df_reclamacoes)}")

        # JOIN 1 - Bancos x Reclamações pelo CNPJ
        df_bancos = df_bancos[df_bancos["CNPJ"].notnull() & (df_bancos["CNPJ"] != "0")]
        df_reclamacoes = df_reclamacoes[df_reclamacoes["CNPJ IF"].notnull() & (df_reclamacoes["CNPJ IF"] != "0")]

        df_join_br = pd.merge(
            df_bancos,
            df_reclamacoes,
            how="inner",
            left_on="CNPJ",
            right_on="CNPJ IF",
            suffixes=("_banco", "_reclamacao"),
        )

        logging.info(f"Join bancos x reclamações resultou em {len(df_join_br)} registros.")
        logging.info(f"CNPJs únicos pós join: {df_join_br['CNPJ'].nunique()}")

        if df_join_br.empty:
            logging.warning("Join bancos x reclamações resultou em 0 registros. Encerrando execução.")
            return



        df_final = pd.merge(
            df_join_br,
            df_empregados,
            how="inner",
            left_on="Nome_processed",
            right_on="Nome_processed",
            suffixes=("", "_empregado")
        )

        logging.info(f"Join com empregados resultou em {len(df_final)} registros finais.")
        logging.info(f"CNPJs únicos no DataFrame final: {df_final['CNPJ'].nunique()}")

        if df_final.empty:
            logging.warning("Join final com empregados resultou em 0 registros. Encerrando execução.")
            return

        # Upload final
        self.db.upload_df(df_final, schema=self.delivery_schema, table_name="bancos_unificados")
        logging.info("Tabela 'bancos_unificados' carregada com sucesso na camada DELIVERY.")
