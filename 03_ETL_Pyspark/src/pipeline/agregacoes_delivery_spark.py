import logging
import os
from pyspark.sql import SparkSession, functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class AgregacoesDeliverySpark:
    def __init__(self, spark: SparkSession, base_path: str):
        self.spark = spark
        base_camadas_dir = os.path.join(base_path, "pipeline/Camadas")
        self.trusted_dir = os.path.join(base_camadas_dir, "Trusted")
        self.delivery_dir = os.path.join(base_camadas_dir, "Delivery")
        os.makedirs(self.delivery_dir, exist_ok=True)
        self.jdbc_url = os.getenv("POSTGRES_JDBC_URL", "jdbc:postgresql://postgres:5432/spark_db")
        self.tabela_destino = "reclamacoes_consolidadas"
        self.properties = {
            "user": os.getenv("POSTGRES_USER", "spark_user"),
            "password": os.getenv("POSTGRES_PASSWORD", "spark_password"),
            "driver": "org.postgresql.Driver"
        }

    def executar(self):
        logging.info("📦 Etapa 3: TRUSTED -> DELIVERY + Postgres")
        logging.info("Iniciando agregações DELIVERY...")
        
        bancos = self.spark.read.parquet(os.path.join(self.trusted_dir, "bancos_parquet"))
        empregados = self.spark.read.parquet(os.path.join(self.trusted_dir, "empregados_parquet"))
        reclamacoes = self.spark.read.parquet(os.path.join(self.trusted_dir, "reclamacoes_parquet"))

        logging.info(f"Bancos: {bancos.count()} linhas")
        logging.info(f"Empregados: {empregados.count()} linhas")
        logging.info(f"Reclamações: {reclamacoes.count()} linhas")

        bancos_join = bancos.withColumn("CNPJ_join", F.regexp_replace(F.col("CNPJ"), r'[^0-9]', ''))
        reclamacoes_join = reclamacoes.withColumn("CNPJ_join", F.regexp_replace(F.col("CNPJ IF"), r'[^0-9]', ''))
        empregados_join = empregados.withColumn("CNPJ_join", F.regexp_replace(F.col("CNPJ"), r'[^0-9]', ''))

        bancos_join = bancos_join.filter(F.col("CNPJ_join") != "")
        reclamacoes_join = reclamacoes_join.filter(F.col("CNPJ_join") != "")
    
        bancos_renomeado = bancos_join.withColumnRenamed("Segmento", "segmento_banco")
        join_br = bancos_renomeado.join(reclamacoes_join.drop("Nome_processed"), "CNPJ_join", "inner")

        logging.info(f"Join bancos x reclamações: {join_br.count()} linhas")

        if join_br.rdd.isEmpty():
            logging.warning("Join entre Bancos e Reclamações resultou em 0 linhas.")
            return

        empregados_renomeado = empregados_join.withColumnRenamed("Nome", "nome_glassdoor") \
                                              .withColumnRenamed("CNPJ", "cnpj_glassdoor")
        
        colunas_empregados = [
            "CNPJ_join", "nome_glassdoor", "Geral", "Cultura e valores", "Qualidade de vida",
            "Alta liderança", "Remuneração e benefícios", "Oportunidades de carreira"
        ]
        
        df_final = join_br.join(empregados_renomeado.select(colunas_empregados), "CNPJ_join", "left")
        
        logging.info(f"Join final com empregados: {df_final.count()} linhas")
        
        df_final = df_final.drop("CNPJ_join", "CNPJ IF", "cnpj_glassdoor")

        destino_parquet = os.path.join(self.delivery_dir, "reclamacoes_unificadas_parquet")
        df_final.write.mode("overwrite").parquet(destino_parquet)
        logging.info(f"Delivery salvo em Parquet: {destino_parquet}")
        
        logging.info("Salvando no Postgres...")
        df_final.write.mode("overwrite").jdbc(url=self.jdbc_url, table=self.tabela_destino, properties=self.properties)
        logging.info(f"Tabela '{self.tabela_destino}' salva com sucesso no Postgres.")