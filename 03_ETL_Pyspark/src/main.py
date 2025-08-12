import sys
import os
import logging
from pyspark.sql import SparkSession

# 🔹 Garante que a raiz do projeto está no sys.path
# Isso permite importar "pipeline" de qualquer lugar
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = current_dir  # pois main.py já está dentro de src/
if project_root not in sys.path:
    sys.path.insert(0, project_root)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Imports da pipeline - CORRIGIDOS com os nomes corretos das classes
from pipeline.popular_spark import PopularLocalSpark
from pipeline.transformacoes_trusted_spark import TransformacoesTrustedSpark
from pipeline.agregacoes_delivery_spark import AgregacoesDeliverySpark

if __name__ == "__main__":
    logging.info("🚀 Iniciando pipeline completa (RAW -> TRUSTED -> DELIVERY)")

    spark = (
        SparkSession.builder
        .appName("PipelineETL")
        .config("spark.jars", "/opt/spark/jars/postgresql-42.7.3.jar")  # driver Postgres
        .getOrCreate()
    )

    base_dir = "pipeline/camadas"   # ajusta para o caminho correto dentro de src
    dados_dir = "pipeline/Dados"    # ajusta para o caminho correto dentro de src

    # 1️⃣ Popular RAW
    logging.info("📥 Etapa 1: Popular RAW")
    popular = PopularLocalSpark(spark, base_dir=base_dir, fonte_dir=dados_dir)
    popular.executar()

    # 2️⃣ Transformar para Trusted
    logging.info("🧹 Etapa 2: RAW -> TRUSTED")
    trusted = TransformacoesTrustedSpark(spark, base_dir=base_dir)
    trusted.executar()

    # 3️⃣ Gerar Delivery e enviar para Postgres
    logging.info("📦 Etapa 3: TRUSTED -> DELIVERY + Postgres")
    delivery = AgregacoesDeliverySpark(spark, base_dir=base_dir)
    delivery.executar()

    logging.info("✅ Pipeline completa finalizada com sucesso!")
    spark.stop()