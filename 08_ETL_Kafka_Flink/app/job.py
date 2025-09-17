# app/job.py
from pyflink.table import EnvironmentSettings, TableEnvironment

def main():
    # ----- Table Env em streaming -----
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(settings)

    # ----- Configurações globais -----
    conf = t_env.get_config().get_configuration()

    # Conectores necessários para o job (presentes em /opt/flink/extra-connectors)
    conf.set_string(
        "pipeline.jars",
        ";".join([
            "file:///opt/flink/extra-connectors/flink-sql-connector-kafka-3.3.0-1.19.jar",
            "file:///opt/flink/extra-connectors/flink-json-1.19.1.jar",
            "file:///opt/flink/extra-connectors/flink-connector-jdbc-3.2.0-1.19.jar",
            "file:///opt/flink/extra-connectors/postgresql-42.7.3.jar",
        ])
    )

    # Nome/parallelism
    conf.set_string("pipeline.name", "reclamacoes-enriquecidas")
    conf.set_string("parallelism.default", "2")

    # ===== S3A (Hadoop FS) -> MinIO =====
    # Requer: flink-s3-fs-hadoop no /opt/flink/plugins/s3-fs-hadoop
    #         hadoop-aws + aws-java-sdk-bundle em /opt/flink/lib
    conf.set_string("fs.s3a.endpoint", "http://minio:9000")
    conf.set_string("fs.s3a.path.style.access", "true")
    conf.set_string("fs.s3a.access.key", "minio")
    conf.set_string("fs.s3a.secret.key", "minio12345")
    conf.set_string("fs.s3a.connection.ssl.enabled", "false")

    # ===== Checkpointing (obrigatório para o filesystem sink em streaming) =====
    conf.set_string("execution.checkpointing.interval", "10 s")
    conf.set_string("execution.checkpointing.mode", "EXACTLY_ONCE")

    # (Opcional) ver arquivos menores mais cedo
    conf.set_string("table.exec.sink.rolling-policy.rollover-interval", "1 min")
    conf.set_string("table.exec.sink.rolling-policy.file-size", "64 MB")

    # ----- Tabela Kafka: reclamacoes -----
    # Flink 1.19: prefira value.format / value.json.*
    t_env.execute_sql("""
        CREATE TABLE reclamacoes (
            id STRING,
            cliente_id STRING,
            produto_id STRING,
            descricao STRING,
            event_ts BIGINT,
            `ingest_time` AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'reclamacoes',
            'properties.bootstrap.servers' = 'kafka:9092',
            'scan.startup.mode' = 'earliest-offset',
            'value.format' = 'json',
            'value.json.ignore-parse-errors' = 'true'
        )
    """)

    # ----- Dimensões via JDBC (Postgres) como lookup tables -----
    t_env.execute_sql("""
        CREATE TABLE dim_clientes (
            cliente_id STRING,
            segmento STRING,
            uf STRING,
            PRIMARY KEY (cliente_id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/dw',
            'table-name' = 'dim.clientes',
            'username' = 'dwuser',
            'password' = 'dwpass',
            'driver' = 'org.postgresql.Driver',
            'lookup.cache.max-rows' = '10000',
            'lookup.cache.ttl' = '10 min'
        )
    """)

    t_env.execute_sql("""
        CREATE TABLE dim_produtos (
            produto_id STRING,
            categoria STRING,
            linha STRING,
            PRIMARY KEY (produto_id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/dw',
            'table-name' = 'dim.produtos',
            'username' = 'dwuser',
            'password' = 'dwpass',
            'driver' = 'org.postgresql.Driver',
            'lookup.cache.max-rows' = '10000',
            'lookup.cache.ttl' = '10 min'
        )
    """)

    # ----- Sink no MinIO (S3A) -----
    t_env.execute_sql("""
        CREATE TABLE silver_reclamacoes (
            id STRING,
            cliente_id STRING,
            produto_id STRING,
            descricao STRING,
            event_ts BIGINT,
            segmento STRING,
            uf STRING,
            categoria STRING,
            linha STRING
        ) WITH (
            'connector' = 'filesystem',
            'path' = 's3a://datalake/silver/reclamacoes_enriquecidas/',
            'format' = 'json'
        )
    """)

    # ----- INSERT com lookup temporal nas dimensões -----
    t_env.execute_sql("""
        INSERT INTO silver_reclamacoes
        SELECT
            r.id,
            r.cliente_id,
            r.produto_id,
            r.descricao,
            r.event_ts,
            c.segmento,
            c.uf,
            p.categoria,
            p.linha
        FROM reclamacoes AS r
        JOIN dim_clientes FOR SYSTEM_TIME AS OF r.ingest_time AS c
          ON r.cliente_id = c.cliente_id
        JOIN dim_produtos FOR SYSTEM_TIME AS OF r.ingest_time AS p
          ON r.produto_id = p.produto_id
    """)

    # Em streaming, o INSERT já dispara a execução.

if __name__ == "__main__":
    main()
