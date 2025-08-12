import logging
import os
from pyspark.sql import SparkSession, functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class TransformacoesTrustedSpark:
    def __init__(self, spark: SparkSession, base_path: str):
        self.spark = spark
        base_camadas_dir = os.path.join(base_path, "pipeline/Camadas")
        self.raw_dir = os.path.join(base_camadas_dir, "RAW")
        self.trusted_dir = os.path.join(base_camadas_dir, "Trusted")
        os.makedirs(self.trusted_dir, exist_ok=True)

    def _criar_chave_nome(self, df, nome_coluna):
        coluna = F.upper(F.col(nome_coluna))
        coluna = F.translate(coluna, "ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ", "AAAAAEEEEIIIIOOOOOUUUUC")
        coluna = F.regexp_replace(coluna, "[^A-Z0-9 ]", "")
        padroes = ["S A", "S/A", "LTDA", " - PRUDENCIAL", "BANCO", "BRASIL"]
        for p in padroes:
            coluna = F.regexp_replace(coluna, p, "")
        coluna = F.regexp_replace(coluna, "\\s+", " ")
        return df.withColumn("Nome_processed", F.trim(coluna))

    def transformar_e_salvar(self, nome_tabela, nome_coluna_chave):
        caminho_raw = os.path.join(self.raw_dir, f"{nome_tabela}_parquet")
        caminho_trusted = os.path.join(self.trusted_dir, f"{nome_tabela}_parquet")
        
        try:
            df = self.spark.read.parquet(caminho_raw)
            df_transformado = self._criar_chave_nome(df, nome_coluna_chave)
            df_transformado.write.mode("overwrite").parquet(caminho_trusted)
            logging.info(f"Trusted salva: {caminho_trusted}")
        except Exception as e:
            if "Path does not exist" in str(e):
                logging.warning(f"Camada RAW não encontrada para {nome_tabela}: {caminho_raw}")
            else:
                logging.error(f"Erro ao transformar {nome_tabela}: {e}")

    def executar(self):
        logging.info("🧹 Etapa 2: RAW -> TRUSTED")
        logging.info("Iniciando RAW -> TRUSTED")
        self.transformar_e_salvar("bancos", "Nome")
        self.transformar_e_salvar("empregados", "Nome")
        self.transformar_e_salvar("reclamacoes", "Instituicao financeira")
        logging.info("Transformações TRUSTED finalizadas.")