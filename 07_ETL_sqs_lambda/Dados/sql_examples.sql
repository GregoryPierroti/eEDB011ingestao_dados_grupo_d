-- =========================================================================
-- EXEMPLOS DE CONSULTAS SQL PARA ENRIQUECIMENTO DE DADOS
-- Pipeline ETL: RAW → TRUSTED → DELIVERY
-- =========================================================================

-- -------------------------------------------------------------------------
-- 1. SETUP DO AMBIENTE ATHENA
-- -------------------------------------------------------------------------

-- Criar database para o pipeline
CREATE DATABASE IF NOT EXISTS etl_pipeline
COMMENT 'Database para pipeline ETL com camadas RAW, TRUSTED e DELIVERY';

-- -------------------------------------------------------------------------
-- 2. DEFINIÇÃO DE TABELAS EXTERNAS
-- -------------------------------------------------------------------------

-- Tabela RAW (dados brutos)
CREATE EXTERNAL TABLE IF NOT EXISTS etl_pipeline.raw_data (
    -- Estrutura flexível para dados brutos
    raw_content string
)
PARTITIONED BY (
    year string,
    month string,
    day string
)
STORED AS TEXTFILE
LOCATION 's3://SEU_RAW_BUCKET/'
TBLPROPERTIES ('has_encrypted_data'='false');

-- Tabela TRUSTED (dados validados e limpos)
CREATE EXTERNAL TABLE IF NOT EXISTS etl_pipeline.trusted_data (
    id bigint,
    name string,
    amount double,
    transaction_date date,
    category string,
    subcategory string,
    customer_id string,
    product_id string,
    quantity int,
    unit_price double,
    -- Campos de qualidade e metadados
    processed_timestamp timestamp,
    source_file string,
    processing_layer string,
    data_quality_score double,
    validation_status string,
    error_flags array<string>
)
PARTITIONED BY (
    year string,
    month string,
    day string
)
STORED AS PARQUET
LOCATION 's3://SEU_TRUSTED_BUCKET/'
TBLPROPERTIES ('has_encrypted_data'='false');

-- Tabela DELIVERY (dados prontos para análise)
CREATE EXTERNAL TABLE IF NOT EXISTS etl_pipeline.delivery_data (
    -- Campos principais
    id bigint,
    name string,
    amount double,
    amount_category string,
    transaction_date date,
    category string,
    subcategory string,
    customer_id string,
    product_id string,
    quantity int,
    unit_price double,
    -- Campos calculados e enriquecidos
    total_value double,
    profit_margin double,
    customer_segment string,
    product_popularity_rank int,
    seasonal_indicator string,
    -- Metadados de processamento
    record_type string,
    delivery_timestamp timestamp,
    processing_layer string,
    pipeline_version string,
    data_lineage string
)
PARTITIONED BY (
    record_type string,
    year string,
    month string,
    day string
)
STORED AS PARQUET
LOCATION 's3://SEU_DELIVERY_BUCKET/'
TBLPROPERTIES ('has_encrypted_data'='false');

-- -------------------------------------------------------------------------
-- 3. CONSULTAS DE ENRIQUECIMENTO (Simulando transformações ETL)
-- -------------------------------------------------------------------------

-- 3.1. Enriquecimento com Categorização de Valores
WITH enriched_amounts AS (
    SELECT *,
        CASE 
            WHEN amount < 50 THEN 'Micro'
            WHEN amount >= 50 AND amount < 200 THEN 'Pequeno'
            WHEN amount >= 200 AND amount < 500 THEN 'Médio'
            WHEN amount >= 500 AND amount < 1000 THEN 'Grande'
            ELSE 'Premium'
        END as amount_category,
        
        CASE 
            WHEN amount < 0 THEN 'NEGATIVE_AMOUNT'
            WHEN amount = 0 THEN 'ZERO_AMOUNT'
            ELSE 'VALID_AMOUNT'
        END as amount_validation
    FROM etl_pipeline.trusted_data
    WHERE year = '2024' AND month = '01'
)
SELECT * FROM enriched_amounts;

-- 3.2. Enriquecimento Temporal e Sazonal
WITH temporal_enrichment AS (
    SELECT *,
        -- Extrair componentes temporais
        EXTRACT(DOW FROM transaction_date) as day_of_week,
        EXTRACT(WEEK FROM transaction_date) as week_of_year,
        EXTRACT(QUARTER FROM transaction_date) as quarter,
        
        -- Indicadores sazonais
        CASE 
            WHEN EXTRACT(MONTH FROM transaction_date) IN (12, 1, 2) THEN 'Verão'
            WHEN EXTRACT(MONTH FROM transaction_date) IN (3, 4, 5) THEN 'Outono'
            WHEN EXTRACT(MONTH FROM transaction_date) IN (6, 7, 8) THEN 'Inverno'
            ELSE 'Primavera'
        END as season,
        
        -- Indicador de fim de semana
        CASE 
            WHEN EXTRACT(DOW FROM transaction_date) IN (6, 0) THEN 'Weekend'
            ELSE 'Weekday'
        END as weekend_indicator,
        
        -- Período do dia (assumindo timestamp completo)
        CASE 
            WHEN EXTRACT(HOUR FROM processed_timestamp) BETWEEN 6 AND 11 THEN 'Manhã'
            WHEN EXTRACT(HOUR FROM processed_timestamp) BETWEEN 12 AND 17 THEN 'Tarde'
            WHEN EXTRACT(HOUR FROM processed_timestamp) BETWEEN 18 AND 23 THEN 'Noite'
            ELSE 'Madrugada'
        END as time_period
    FROM etl_pipeline.trusted_data
    WHERE year = '2024'
)
SELECT * FROM temporal_enrichment;

-- 3.3. Enriquecimento com Métricas de Performance
WITH performance_metrics AS (
    SELECT *,
        -- Ranking por categoria
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount DESC) as category_rank,
        
        -- Percentil dentro da categoria
        PERCENT_RANK() OVER (PARTITION BY category ORDER BY amount) as category_percentile,
        
        -- Média móvel de 7 dias
        AVG(amount) OVER (
            PARTITION BY customer_id 
            ORDER BY transaction_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as rolling_avg_7d,
        
        -- Diferença em relação à média da categoria
        amount - AVG(amount) OVER (PARTITION BY category) as amount_vs_category_avg,
        
        -- Contador de transações por cliente
        COUNT(*) OVER (PARTITION BY customer_id) as customer_transaction_count,
        
        -- Valor acumulado por cliente
        SUM(amount) OVER (
            PARTITION BY customer_id 
            ORDER BY transaction_date 
            ROWS UNBOUNDED PRECEDING
        ) as customer_cumulative_amount
    FROM etl_pipeline.trusted_data
    WHERE year = '2024'
)
SELECT * FROM performance_metrics;

-- 3.4. Enriquecimento com Segmentação de Clientes
WITH customer_segments AS (
    SELECT 
        customer_id,
        COUNT(*) as transaction_frequency,
        SUM(amount) as total_spent,
        AVG(amount) as avg_transaction_value,
        MAX(transaction_date) as last_transaction_date,
        MIN(transaction_date) as first_transaction_date,
        
        -- Segmentação RFM simplificada
        CASE 
            WHEN COUNT(*) >= 10 AND SUM(amount) >= 1000 THEN 'Champions'
            WHEN COUNT(*) >= 5 AND SUM(amount) >= 500 THEN 'Loyal Customers'
            WHEN COUNT(*) >= 3 AND SUM(amount) >= 200 THEN 'Potential Loyalists'
            WHEN COUNT(*) >= 2 THEN 'New Customers'
            ELSE 'At Risk'
        END as customer_segment,
        
        -- Indicador de valor do cliente
        NTILE(5) OVER (ORDER BY SUM(amount) DESC) as value_quintile
        
    FROM etl_pipeline.trusted_data
    WHERE year = '2024'
    GROUP BY customer_id
),
enriched_with_segments AS (
    SELECT 
        td.*,
        cs.customer_segment,
        cs.value_quintile,
        cs.transaction_frequency,
        cs.total_spent,
        cs.avg_transaction_value
    FROM etl_pipeline.trusted_data td
    LEFT JOIN customer_segments cs ON td.customer_id = cs.customer_id
    WHERE td.year = '2024'
)
SELECT * FROM enriched_with_segments;

-- -------------------------------------------------------------------------
-- 4. AGREGAÇÕES PARA CAMADA DELIVERY
-- -------------------------------------------------------------------------

-- 4.1. Resumo Diário por Categoria
CREATE TABLE etl_pipeline.daily_category_summary AS
SELECT 
    transaction_date,
    category,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount,
    MIN(amount) as min_amount,
    MAX(amount) as max_amount,
    STDDEV(amount) as amount_stddev,
    COUNT(DISTINCT customer_id) as unique_customers,
    
    -- Métricas de qualidade
    AVG(data_quality_score) as avg_quality_score,
    COUNT(CASE WHEN data_quality_score < 80 THEN 1 END) as low_quality_records,
    
    -- Metadados
    'DAILY_CATEGORY_SUMMARY' as record_type,
    CURRENT_TIMESTAMP as delivery_timestamp
FROM etl_pipeline.trusted_data
WHERE year = '2024'
GROUP BY transaction_date, category;

-- 4.2. Análise de Tendências Semanais
CREATE TABLE etl_pipeline.weekly_trends AS
WITH weekly_data AS (
    SELECT 
        DATE_TRUNC('week', transaction_date) as week_start,
        category,
        SUM(amount) as weekly_amount,
        COUNT(*) as weekly_transactions
    FROM etl_pipeline.trusted_data
    WHERE year = '2024'
    GROUP BY DATE_TRUNC('week', transaction_date), category
),
trends AS (
    SELECT *,
        LAG(weekly_amount, 1) OVER (PARTITION BY category ORDER BY week_start) as prev_week_amount,
        LAG(weekly_transactions, 1) OVER (PARTITION BY category ORDER BY week_start) as prev_week_transactions,
        
        -- Cálculo de crescimento
        CASE 
            WHEN LAG(weekly_amount, 1) OVER (PARTITION BY category ORDER BY week_start) > 0 
            THEN ((weekly_amount - LAG(weekly_amount, 1) OVER (PARTITION BY category ORDER BY week_start)) / 
                  LAG(weekly_amount, 1) OVER (PARTITION BY category ORDER BY week_start)) * 100
            ELSE NULL
        END as amount_growth_pct,
        
        'WEEKLY_TRENDS' as record_type,
        CURRENT_TIMESTAMP as delivery_timestamp
    FROM weekly_data
)
SELECT * FROM trends;

-- 4.3. Top Performers por Período
CREATE TABLE etl_pipeline.top_performers AS
SELECT 
    'MONTHLY' as period_type,
    DATE_TRUNC('month', transaction_date) as period_start,
    category,
    customer_id,
    product_id,
    SUM(amount) as total_amount,
    COUNT(*) as transaction_count,
    ROW_NUMBER() OVER (
        PARTITION BY DATE_TRUNC('month', transaction_date), category 
        ORDER BY SUM(amount) DESC
    ) as ranking,
    'TOP_PERFORMERS' as record_type,
    CURRENT_TIMESTAMP as delivery_timestamp
FROM etl_pipeline.trusted_data
WHERE year = '2024'
GROUP BY 
    DATE_TRUNC('month', transaction_date), 
    category, 
    customer_id, 
    product_id
HAVING ROW_NUMBER() OVER (
    PARTITION BY DATE_TRUNC('month', transaction_date), category 
    ORDER BY SUM(amount) DESC
) <= 10;

-- -------------------------------------------------------------------------
-- 5. CONSULTAS DE QUALIDADE DE DADOS
-- -------------------------------------------------------------------------

-- 5.1. Relatório de Qualidade de Dados
WITH quality_metrics AS (
    SELECT 
        processing_layer,
        source_file,
        COUNT(*) as total_records,
        
        -- Métricas de completude
        COUNT(CASE WHEN id IS NOT NULL THEN 1 END) as id_complete,
        COUNT(CASE WHEN name IS NOT NULL THEN 1 END) as name_complete,
        COUNT(CASE WHEN amount IS NOT NULL THEN 1 END) as amount_complete,
        COUNT(CASE WHEN transaction_date IS NOT NULL THEN 1 END) as date_complete,
        
        -- Métricas de validade
        COUNT(CASE WHEN amount >= 0 THEN 1 END) as valid_amounts,
        COUNT(CASE WHEN transaction_date <= CURRENT_DATE THEN 1 END) as valid_dates,
        
        -- Score médio de qualidade
        AVG(data_quality_score) as avg_quality_score,
        MIN(data_quality_score) as min_quality_score,
        MAX(data_quality_score) as max_quality_score,
        
        -- Timestamp de processamento
        MIN(processed_timestamp) as first_processed,
        MAX(processed_timestamp) as last_processed
    FROM etl_pipeline.trusted_data
    WHERE year = '2024' AND month = '01'
    GROUP BY processing_layer, source_file
)
SELECT *,
    ROUND((id_complete * 100.0 / total_records), 2) as id_completeness_pct,
    ROUND((name_complete * 100.0 / total_records), 2) as name_completeness_pct,
    ROUND((amount_complete * 100.0 / total_records), 2) as amount_completeness_pct,
    ROUND((date_complete * 100.0 / total_records), 2) as date_completeness_pct,
    ROUND((valid_amounts * 100.0 / total_records), 2) as amount_validity_pct,
    ROUND((valid_dates * 100.0 / total_records), 2) as date_validity_pct
FROM quality_metrics;

-- 5.2. Detecção de Anomalias
WITH anomaly_detection AS (
    SELECT *,
        -- Z-score para detecção de outliers
        (amount - AVG(amount) OVER (PARTITION BY category)) / 
        NULLIF(STDDEV(amount) OVER (PARTITION BY category), 0) as amount_z_score,
        
        -- Detecção baseada em IQR
        PERCENTILE_CONT(0.25) OVER (PARTITION BY category ORDER BY amount) as q1,
        PERCENTILE_CONT(0.75) OVER (PARTITION BY category ORDER BY amount) as q3
    FROM etl_pipeline.trusted_data
    WHERE year = '2024'
),
anomalies AS (
    SELECT *,
        q3 - q1 as iqr,
        CASE 
            WHEN amount < (q1 - 1.5 * (q3 - q1)) OR amount > (q3 + 1.5 * (q3 - q1)) THEN 'IQR_OUTLIER'
            WHEN ABS(amount_z_score) > 3 THEN 'Z_SCORE_OUTLIER'
            WHEN ABS(amount_z_score) > 2 THEN 'POTENTIAL_OUTLIER'
            ELSE 'NORMAL'
        END as anomaly_flag
    FROM anomaly_detection
)
SELECT 
    category,
    anomaly_flag,
    COUNT(*) as record_count,
    ROUND(AVG(amount), 2) as avg_amount,
    ROUND(AVG(ABS(amount_z_score)), 2) as avg_z_score
FROM anomalies
GROUP BY category, anomaly_flag
ORDER BY category, anomaly_flag;

-- -------------------------------------------------------------------------
-- 6. CONSULTAS DE MONITORAMENTO DA PIPELINE
-- -------------------------------------------------------------------------

-- 6.1. Status de Processamento por Dia
SELECT 
    DATE(processed_timestamp) as processing_date,
    processing_layer,
    COUNT(*) as records_processed,
    COUNT(DISTINCT source_file) as files_processed,
    AVG(data_quality_score) as avg_quality,
    MIN(processed_timestamp) as first_processing,
    MAX(processed_timestamp) as last_processing
FROM etl_pipeline.trusted_data
WHERE year = '2024' AND month = '01'
GROUP BY DATE(processed_timestamp), processing_layer
ORDER BY processing_date DESC, processing_layer;

-- 6.2. Performance da Pipeline
WITH processing_stats AS (
    SELECT 
        source_file,
        COUNT(*) as record_count,
        MIN(processed_timestamp) as start_time,
        MAX(processed_timestamp) as end_time,
        MAX(processed_timestamp) - MIN(processed_timestamp) as processing_duration
    FROM etl_pipeline.trusted_data
    WHERE year = '2024' AND month = '01'
    GROUP BY source_file
)
SELECT 
    source_file,
    record_count,
    start_time,
    end_time,
    processing_duration,
    ROUND(record_count / EXTRACT(SECOND FROM processing_duration), 2) as records_per_second
FROM processing_stats
WHERE processing_duration > INTERVAL '0' SECOND
ORDER BY processing_duration DESC;