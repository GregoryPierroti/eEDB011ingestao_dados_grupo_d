import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class IngestaoRawSpark:
    def __init__(self, spark: SparkSession, base_path: str):
        self.spark = spark
        self.base_dir = os.path.join(base_path, "pipeline/Dados")
        self.output_dir = os.path.join(base_path, "pipeline/Camadas/RAW")
        os.makedirs(self.output_dir, exist_ok=True)

    def ler_e_salvar_parquet(self, reader_func, formato, caminho, destino):
        try:
            logging.info(f"Lendo dados {formato} de: {caminho}")
            df = reader_func(caminho)
            destino_completo = os.path.join(self.output_dir, destino)
            df.write.mode("overwrite").parquet(destino_completo)
            logging.info(f"RAW salva: {destino_completo}")
        except Exception as e:
            if "Path does not exist" in str(e):
                logging.error(f"ERRO CRÍTICO: O caminho de origem não foi encontrado: {caminho}")
            logging.error(f"Erro ao processar {caminho}: {e}")
            raise

    def executar(self):
        logging.info("📥 Etapa 1: Popular RAW")

        caminho_bancos = os.path.join(self.base_dir, "Bancos", "EnquadramentoInicia_v2.tsv")
        schema_bancos = StructType([
            StructField("Segmento", StringType(), True),
            StructField("CNPJ", StringType(), True),
            StructField("Nome", StringType(), True)
        ])
        bancos_reader = lambda path: self.spark.read.format("csv") \
            .schema(schema_bancos).option("header", True).option("delimiter", "\t").load(path)
        self.ler_e_salvar_parquet(bancos_reader, "TSV", caminho_bancos, "bancos_parquet")

        caminho_reclamacoes = os.path.join(self.base_dir, "Reclamacoes")
        schema_reclamacoes = StructType([
            StructField("Ano", IntegerType(), True), StructField("Trimestre", StringType(), True),
            StructField("Categoria", StringType(), True), StructField("Tipo", StringType(), True),
            StructField("CNPJ IF", StringType(), True), StructField("Instituicao financeira", StringType(), True),
            StructField("Indice", StringType(), True), StructField("Quantidade de reclamacoes reguladas procedentes", IntegerType(), True),
            StructField("Quantidade de reclamacoes reguladas - outras", IntegerType(), True), StructField("Quantidade de reclamacoes nao reguladas", IntegerType(), True),
            StructField("Quantidade total de reclamacoes", IntegerType(), True), StructField("Quantidade total de clientes CCS e SCR", LongType(), True),
            StructField("Quantidade de clientes CCS", LongType(), True), StructField("Quantidade de clientes SCR", LongType(), True)
        ])
        reclamacoes_reader = lambda path: self.spark.read.format("csv") \
            .schema(schema_reclamacoes).option("header", True).option("delimiter", ";").option("encoding", "UTF-8").load(path)
        self.ler_e_salvar_parquet(reclamacoes_reader, "CSV", caminho_reclamacoes, "reclamacoes_parquet")

        caminho_empregados = os.path.join(self.base_dir, "Empregados", "glassdoor_consolidado_join_match_less_v2.csv")
        schema_empregados = StructType([
            StructField("employer_name", StringType(), True), StructField("reviews_count", IntegerType(), True),
            StructField("culture_count", IntegerType(), True), StructField("salaries_count", IntegerType(), True),
            StructField("benefits_count", IntegerType(), True), StructField("employer-website", StringType(), True),
            StructField("employer-headquarters", StringType(), True), StructField("employer-founded", IntegerType(), True),
            StructField("employer-industry", StringType(), True), StructField("employer-revenue", StringType(), True),
            StructField("url", StringType(), True), StructField("Geral", DoubleType(), True),
            StructField("Cultura e valores", DoubleType(), True), StructField("Diversidade e inclusão", DoubleType(), True),
            StructField("Qualidade de vida", DoubleType(), True), StructField("Alta liderança", DoubleType(), True),
            StructField("Remuneração e benefícios", DoubleType(), True), StructField("Oportunidades de carreira", DoubleType(), True),
            StructField("Recomendam para outras pessoas(%)", StringType(), True), StructField("Perspectiva positiva da empresa(%)", StringType(), True),
            StructField("CNPJ", StringType(), True),
            StructField("Nome", StringType(), True), StructField("match_percent", DoubleType(), True)
        ])
        empregados_reader = lambda path: self.spark.read.format("csv") \
            .schema(schema_empregados).option("header", True).option("delimiter", "|").load(path)
        self.ler_e_salvar_parquet(empregados_reader, "CSV", caminho_empregados, "empregados_parquet")