from pyflink.table import EnvironmentSettings, TableEnvironment

def main():
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(settings)

    conf = t_env.get_config().get_configuration()
    conf.set_string(
        "pipeline.jars",
        ";".join(
            [
                "file:///opt/flink/extra-connectors/flink-sql-connector-kafka-3.3.0-1.19.jar",
            "file:///opt/flink/extra-connectors/flink-json-1.19.1.jar",
            "file:///opt/flink/extra-connectors/flink-connector-jdbc-3.2.0-1.19.jar",
            "file:///opt/flink/extra-connectors/postgresql-42.7.3.jar",
            "file:///opt/flink/extra-connectors/flink-parquet-1.19.1.jar",
            "file:///opt/flink/extra-connectors/hadoop-common-3.3.6.jar",
            "file:///opt/flink/extra-connectors/hadoop-client-api-3.3.6.jar",
            "file:///opt/flink/extra-connectors/hadoop-client-runtime-3.3.6.jar",
            "file:///opt/flink/extra-connectors/hadoop-hdfs-3.3.6.jar",
            "file:///opt/flink/extra-connectors/hadoop-mapreduce-client-core-3.3.6.jar",
            "file:///opt/flink/extra-connectors/parquet-hadoop-1.12.3.jar",
            "file:///opt/flink/extra-connectors/parquet-common-1.12.3.jar",
            "file:///opt/flink/extra-connectors/parquet-column-1.12.3.jar",
            "file:///opt/flink/extra-connectors/parquet-encoding-1.12.3.jar",
            "file:///opt/flink/extra-connectors/parquet-avro-1.12.3.jar",
            "file:///opt/flink/extra-connectors/parquet-format-2.9.0.jar",
            "file:///opt/flink/extra-connectors/woodstox-core-6.5.1.jar",
            "file:///opt/flink/extra-connectors/stax2-api-4.2.jar",
            "file:///opt/flink/extra-connectors/hadoop-shaded-guava-1.1.1.jar",
        ]
        ),
    )
    conf.set_string("pipeline.name", "mod-final-reclamacoes")
    conf.set_string("parallelism.default", "2")
    conf.set_string("fs.s3a.endpoint", "http://minio:9000")
    conf.set_string("fs.s3a.path.style.access", "true")
    conf.set_string("fs.s3a.access.key", "minio")
    conf.set_string("fs.s3a.secret.key", "minio12345")
    conf.set_string("fs.s3a.connection.ssl.enabled", "false")
    conf.set_string("execution.checkpointing.interval", "10 s")
    conf.set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
    conf.set_string("table.exec.sink.rolling-policy.rollover-interval", "1 min")
    conf.set_string("table.exec.sink.rolling-policy.file-size", "64 MB")

    t_env.execute_sql(
        """
        CREATE OR REPLACE TABLE reclamacoes (
            ano INT,
            trimestre STRING,
            categoria STRING,
            tipo STRING,
            cnpj_if STRING,
            instituicao_financeira STRING,
            indice DOUBLE,
            quantidade_de_reclamacoes_reguladas_procedentes INT,
            quantidade_de_reclamacoes_reguladas_outras INT,
            quantidade_de_reclamacoes_nao_reguladas INT,
            quantidade_total_de_reclamacoes INT,
            quantidade_total_de_clientes_ccs_e_scr INT,
            quantidade_de_clientes_ccs INT,
            quantidade_de_clientes_scr INT,
            fonte_tabela STRING,
            event_ts TIMESTAMP(3),
            ingest_time AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'reclamacoes',
            'properties.bootstrap.servers' = 'kafka:9092',
            'scan.startup.mode' = 'earliest-offset',
            'value.format' = 'json',
            'value.json.fail-on-missing-field' = 'false',
            'value.json.ignore-parse-errors' = 'true'
        )
        """
    )

    t_env.execute_sql(
        """
        CREATE OR REPLACE TABLE bancos (
            segmento STRING,
            cnpj STRING,
            nome STRING,
            nome_processed STRING,
            data_atualizacao TIMESTAMP(3),
            PRIMARY KEY (cnpj, segmento) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/dw',
            'table-name' = 'bancos',
            'username' = 'dwuser',
            'password' = 'dwpass',
            'driver' = 'org.postgresql.Driver',
            'lookup.cache.max-rows' = '5000',
            'lookup.cache.ttl' = '10 min'
        )
        """
    )

    t_env.execute_sql(
        """
        CREATE OR REPLACE TABLE empregados (
            employer_sk STRING,
            nome_processed_empregado STRING,
            employer_name STRING,
            reviews_count INT,
            culture_count INT,
            salaries_count INT,
            benefits_count INT,
            employer_website STRING,
            employer_headquarters STRING,
            employer_founded INT,
            employer_industry STRING,
            employer_revenue STRING,
            url STRING,
            geral DOUBLE,
            cultura_e_valores DOUBLE,
            diversidade_e_inclusao DOUBLE,
            qualidade_de_vida DOUBLE,
            alta_lideranca DOUBLE,
            remuneracao_e_beneficios DOUBLE,
            oportunidades_de_carreira DOUBLE,
            recomendam_para_outras_pessoas INT,
            perspectiva_positiva_da_empresa INT,
            segmento_empregado STRING,
            match_percent INT,
            nome_empregado STRING,
            _source_table STRING,
            data_atualizacao TIMESTAMP(3),
            PRIMARY KEY (employer_sk) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/dw',
            'table-name' = 'empregados',
            'username' = 'dwuser',
            'password' = 'dwpass',
            'driver' = 'org.postgresql.Driver',
            'lookup.cache.max-rows' = '5000',
            'lookup.cache.ttl' = '10 min'
        )
        """
    )

    t_env.execute_sql(
        """
        CREATE TABLE mod_final_stream (
            segmento STRING,
            cnpj STRING,
            nome STRING,
            nome_processed STRING,
            ano INT,
            trimestre STRING,
            categoria STRING,
            tipo STRING,
            cnpj_if STRING,
            instituicao_financeira STRING,
            indice DOUBLE,
            quantidade_de_reclamacoes_reguladas_procedentes INT,
            quantidade_de_reclamacoes_reguladas_outras INT,
            quantidade_de_reclamacoes_nao_reguladas INT,
            quantidade_total_de_reclamacoes INT,
            quantidade_total_de_clientes_ccs_e_scr INT,
            quantidade_de_clientes_ccs INT,
            quantidade_de_clientes_scr INT,
            fonte_tabela STRING,
            nome_processed_empregado STRING,
            employer_sk STRING,
            employer_name STRING,
            reviews_count INT,
            culture_count INT,
            salaries_count INT,
            benefits_count INT,
            employer_website STRING,
            employer_headquarters STRING,
            employer_founded INT,
            employer_industry STRING,
            employer_revenue STRING,
            url STRING,
            geral DOUBLE,
            cultura_e_valores DOUBLE,
            diversidade_e_inclusao DOUBLE,
            qualidade_de_vida DOUBLE,
            alta_lideranca DOUBLE,
            remuneracao_e_beneficios DOUBLE,
            oportunidades_de_carreira DOUBLE,
            recomendam_para_outras_pessoas INT,
            perspectiva_positiva_da_empresa INT,
            segmento_empregado STRING,
            match_percent INT,
            nome_empregado STRING,
            _source_table STRING,
            data_atualizacao TIMESTAMP(3)
        ) WITH (
            'connector' = 'filesystem',
            'path' = 's3a://datalake/gold/mod_final_parquet/',
            'format' = 'parquet',
            'sink.rolling-policy.file-size' = '64 MB',
            'sink.rolling-policy.rollover-interval' = '15 min'
        )
        """
    )

    t_env.execute_sql(
        """
        INSERT INTO mod_final_stream
        SELECT
            b.segmento,
            b.cnpj,
            b.nome,
            b.nome_processed,
            r.ano,
            r.trimestre,
            r.categoria,
            r.tipo,
            r.cnpj_if,
            r.instituicao_financeira,
            r.indice,
            r.quantidade_de_reclamacoes_reguladas_procedentes,
            r.quantidade_de_reclamacoes_reguladas_outras,
            r.quantidade_de_reclamacoes_nao_reguladas,
            r.quantidade_total_de_reclamacoes,
            r.quantidade_total_de_clientes_ccs_e_scr,
            r.quantidade_de_clientes_ccs,
            r.quantidade_de_clientes_scr,
            r.fonte_tabela,
            e.nome_processed_empregado,
            e.employer_sk,
            e.employer_name,
            e.reviews_count,
            e.culture_count,
            e.salaries_count,
            e.benefits_count,
            e.employer_website,
            e.employer_headquarters,
            e.employer_founded,
            e.employer_industry,
            e.employer_revenue,
            e.url,
            e.geral,
            e.cultura_e_valores,
            e.diversidade_e_inclusao,
            e.qualidade_de_vida,
            e.alta_lideranca,
            e.remuneracao_e_beneficios,
            e.oportunidades_de_carreira,
            e.recomendam_para_outras_pessoas,
            e.perspectiva_positiva_da_empresa,
            e.segmento_empregado,
            e.match_percent,
            e.nome_empregado,
            e._source_table,
            e.data_atualizacao
        FROM reclamacoes AS r
        JOIN bancos FOR SYSTEM_TIME AS OF r.ingest_time AS b
          ON b.cnpj = r.cnpj_if
        JOIN empregados FOR SYSTEM_TIME AS OF r.ingest_time AS e
          ON b.nome_processed = e.nome_processed_empregado
        WHERE r.cnpj_if IS NOT NULL AND r.cnpj_if <> '0'
        """
    )

if __name__ == "__main__":
    main()
