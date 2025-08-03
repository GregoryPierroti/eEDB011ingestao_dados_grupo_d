import logging
import unicodedata
import re
import pandas as pd
from utils.postgres_uploader import PostgresUploader

# Configuração de logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class TransformacoesTrusted:
    """
    Classe responsável por ler dados da camada RAW, aplicar as regras de
    negócio e limpeza, e preparar os dados para a camada TRUSTED.
    """

    def __init__(self):
        self.db = PostgresUploader()
        self.raw_schema = "raw"
        self.trusted_schema = "trusted"
        self.tabelas = ["bancos", "empregados", "reclamacoes"]
        logging.info("TransformacoesTrusted inicializada.")

    def executar(self):
        logging.info("Iniciando pipeline: RAW -> TRUSTED.")
        for nome in self.tabelas:
            # try:
            logging.info(f"Lendo tabela '{self.raw_schema}.{nome}'...")
            df = self.db.read_table(self.raw_schema, nome)
            if df.empty:
                logging.warning(f"Tabela '{nome}' vazia. Pulando transformação.")
                continue
            df_transformado = self._aplicar_transformacoes(df, nome)
            if df_transformado is not None:
                logging.info(f"Fazendo upload para '{self.trusted_schema}.{nome}'...")
                self.db.upload_df(
                    df_transformado, schema=self.trusted_schema, table_name=nome
                )
                logging.info(f"Upload de '{nome}' concluído com sucesso.")
            else:
                logging.warning(f"Transformação retornou None para '{nome}'.")

            # except Exception as e:
            #    logging.error(f"Erro ao processar tabela '{nome}': {e}")
        logging.info("Pipeline RAW -> TRUSTED finalizado.")

    def _aplicar_transformacoes(self, df: pd.DataFrame, nome: str) -> pd.DataFrame:
        if nome == "bancos":
            return self._transformar_bancos(df)
        elif nome == "empregados":
            return self._transformar_empregados(df)
        else:
            logging.info(f"Nenhuma transformação aplicada para '{nome}'.")
            return df

    def _remover_caracteres_invalidos(self, texto):
        """Remove acentos e caracteres não ASCII."""
        if isinstance(texto, str):
            texto = unicodedata.normalize("NFKD", texto)
            texto = "".join(c for c in texto if not unicodedata.combining(c))
            texto = texto.encode("ascii", errors="ignore").decode("ascii")
        return texto

    def _criar_chave_nome(self, nome):
        """Gera uma chave de junção limpa a partir do nome."""
        if not isinstance(nome, str):
            return ""

        nome_limpo = self._remover_caracteres_invalidos(nome).upper()

        padroes_remocao = [
            r"\bS\.A\b",
            r"\bS/A\b",
            r"\bLTDA\b",
            r" - PRUDENCIAL",
            r"\bBANCO\b",
            r"\(BRASIL\)",
        ]

        for pattern in padroes_remocao:
            nome_limpo = re.sub(pattern, "", nome_limpo)

        nome_limpo = re.sub(r"[^\w\s]", "", nome_limpo)  # Remove pontuação
        nome_limpo = re.sub(r"\s+", " ", nome_limpo).strip()  # Remove espaços extras

        return nome_limpo

    def _transformar_bancos(self, df: pd.DataFrame) -> pd.DataFrame:
        if "Nome" in df.columns:
            df["Nome_processed"] = df["Nome"].apply(self._criar_chave_nome)
        if "CNPJ" in df.columns:
            df["CNPJ"] = df["CNPJ"].astype(str)
        return df

    def _transformar_empregados(self, df: pd.DataFrame) -> pd.DataFrame:
        if "Nome" in df.columns:
            df["Nome_processed"] = df["Nome"].apply(self._criar_chave_nome)
        if "CNPJ" in df.columns:
            df["CNPJ"] = df["CNPJ"].fillna(0).astype(int).astype(str)
        return df
