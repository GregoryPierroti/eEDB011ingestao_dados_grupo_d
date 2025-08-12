import logging
from pyspark.sql import SparkSession
from pipeline.ingestao_raw_spark import IngestaoRawSpark
from pipeline.transformacoes_trusted_spark import TransformacoesTrustedSpark
from pipeline.agregacoes_delivery_spark import AgregacoesDeliverySpark

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    spark = None
    try:
        logging.info("🚀 Iniciando pipeline completa (RAW -> TRUSTED -> DELIVERY)")

        spark = SparkSession.builder \
            .appName("PipelineCompletaBancos") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
            .getOrCreate()
        
        # Define o caminho base absoluto para ser usado por todas as etapas
        base_dir = "/app/src"

        # Etapa 1: Ingestão para a camada RAW
        raw = IngestaoRawSpark(spark, base_path=base_dir)
        raw.executar()

        # Etapa 2: Transformação para a camada TRUSTED
        trusted = TransformacoesTrustedSpark(spark, base_path=base_dir)
        trusted.executar()
        
        # Etapa 3: Agregação para a camada DELIVERY e ingestão no Postgres
        delivery = AgregacoesDeliverySpark(spark, base_path=base_dir)
        delivery.executar()

    except Exception as e:
        logging.error(f"Ocorreu um erro na execução do pipeline: {e}", exc_info=True)
    finally:
        if spark:
            spark.stop()
            logging.info("Sessão Spark finalizada.")

if __name__ == "__main__":
    main()