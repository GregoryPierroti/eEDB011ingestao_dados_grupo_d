# popular_local_spark.py
import os
import logging
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class PopularLocalSpark:
    def __init__(self, spark: SparkSession, base_dir="camadas", fonte_dir="Dados"):
        self.spark = spark
        self.base_dir = base_dir
        self.raw_dir = os.path.join(base_dir, "RAW")
        self.categorias = ["Bancos", "Reclamacoes", "Empregados"]
        self.fonte_dir = fonte_dir
        os.makedirs(self.raw_dir, exist_ok=True)

    def executar(self):
        for categoria in self.categorias:
            pasta_fonte = os.path.join(self.fonte_dir, categoria)
            if not os.path.isdir(pasta_fonte):
                logging.warning(f"Pasta não encontrada: {pasta_fonte}")
                continue

            # Define separador
            if categoria == "Bancos":
                sep, encoding = "\t", "utf-8"
            elif categoria == "Empregados":
                sep, encoding = "|", "utf-8"
            elif categoria == "Reclamacoes":
                sep, encoding = ";", "latin1"
            else:
                sep, encoding = ",", "utf-8"

            df = self.spark.read.option("header", True).option("sep", sep).option("encoding", encoding).csv(pasta_fonte)
            destino = os.path.join(self.raw_dir, f"{categoria.lower()}_parquet")
            df.write.mode("overwrite").parquet(destino)
            logging.info(f"RAW salva: {destino}")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("PopularLocalSpark").getOrCreate()
    PopularLocalSpark(spark).executar()
    spark.stop()
