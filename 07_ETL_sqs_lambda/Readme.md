# Pipeline Cloud Streaming ETL
## AWS Lambda + S3 + SQS (RAW → Trusted → Delivery)

Uma pipeline de processamento de dados em tempo real implementada com Terraform, seguindo as melhores práticas de arquitetura de dados moderna.

## 🏗️ Arquitetura

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   RAW       │    │    SQS      │    │   LAMBDA    │    │  TRUSTED    │
│   Bucket    ├───►│   Queue     ├───►│    ETL      ├───►│   Bucket    │
│             │    │             │    │             │    │             │
└─────────────┘    └─────────────┘    └─────────────┘    └─────┬───────┘
                                                                │
                                                                ▼
                                                      ┌─────────────┐
                                                      │  DELIVERY   │
                                                      │   Bucket    │
                                                      │             │
                                                      └─────────────┘
```

### Fluxo de Dados

1. **RAW Layer**: Dados brutos são carregados no bucket S3 RAW
2. **Event Trigger**: Upload dispara notificação para SQS
3. **Lambda Processing**: Função Lambda processa dados através das camadas
4. **TRUSTED Layer**: Dados validados, limpos e enriquecidos
5. **DELIVERY Layer**: Dados agregados e prontos para consumo

## 🚀 Deploy Rápido

```bash
# 1. Clone e configure
git clone <repository>
cd 07_ETL_Lambda_SQS

# 2. Configure AWS credentials
aws configure

# 3. Execute o deployment
chmod +x deploy.sh
./deploy.sh
```

## 📋 Pré-requisitos

- **Terraform** >= 1.0
- **AWS CLI** configurado
- **Python 3.11+**
- **pip3**
- Permissões AWS apropriadas

## 🔧 Configuração Manual

### 1. Inicialização do Terraform

```bash
terraform init
```

### 2. Criação do Pacote Lambda

```bash
# Instalar dependências
pip3 install -r requirements.txt -t ./lambda_package/
cp lambda_function.py ./lambda_package/

# Criar ZIP
cd lambda_package && zip -r ../lambda_function.zip . && cd ..
```

### 3. Deployment da Infraestrutura

```bash
# Planejar
terraform plan

# Aplicar
terraform apply
```

## 📁 Estrutura do Projeto

```
07_ETL_Lambda_SQS/
├── main.tf                     # Configuração principal
├── variables.tf                # Variáveis globais
├── outputs.tf                  # Outputs do Terraform
├── terraform.tfvars            # Valores das variáveis
├── lambda_function.py          # Código ETL Python
├── requirements.txt            # Dependências Python
├── deploy.sh                   # Script de deployment
├── README.md                   # Esta documentação
└── modules/
    ├── s3/                     # Módulo S3
    │   ├── main.tf
    │   ├── variables.tf
    │   └── outputs.tf
    ├── sqs/                    # Módulo SQS
    │   ├── main.tf
    │   ├── variables.tf
    │   └── outputs.tf
    └── lambda/                 # Módulo Lambda
        ├── main.tf
        ├── variables.tf
        └── outputs.tf
```

## 🗄️ Camadas de Dados

### RAW Layer
- **Propósito**: Armazenamento de dados brutos
- **Formato**: Qualquer (CSV, JSON, Excel)
- **Retenção**: 7 anos (com lifecycle policy)
- **Particionamento**: Por data de upload

### TRUSTED Layer
- **Propósito**: Dados validados e limpos
- **Formato**: Parquet (otimizado)
- **Transformações**:
  - Remoção de duplicatas
  - Tratamento de valores nulos
  - Validação de tipos de dados
  - Aplicação de regras de negócio
  - Cálculo de score de qualidade
- **Particionamento**: `year=YYYY/month=MM/day=DD/`

### DELIVERY Layer
- **Propósito**: Dados prontos para análise
- **Formato**: Parquet particionado
- **Conteúdo**:
  - Registros detalhados
  - Agregações diárias
  - Métricas de qualidade
  - Metadados de processamento
- **Particionamento**: `record_type={DETAIL|DAILY_SUMMARY}/year=YYYY/month=MM/day=DD/`

## ⚡ Processamento ETL

### Funcionalidades Implementadas

#### Data Quality
- ✅ Detecção e remoção de duplicatas
- ✅ Tratamento inteligente de valores nulos
- ✅ Validação de tipos de dados
- ✅ Detecção de outliers
- ✅ Score de qualidade por registro

#### Enriquecimento
- ✅ Categorização automática de valores
- ✅ Cálculos derivados
- ✅ Metadados de processamento
- ✅ Timestamp de processamento

#### Agregações
- ✅ Sumarização diária
- ✅ Métricas estatísticas (count, sum, mean)
- ✅ Particionamento por tipo de registro

### Regras de Negócio

```python
# Exemplo de regras implementadas
- Validação de valores negativos em campos de valor
- Detecção de datas futuras
- Categorização automática por faixas de valor
- Cálculo de score de qualidade baseado em completude
```

## 🔍 Monitoramento

### CloudWatch Logs
```bash
# Acompanhar logs em tempo real
aws logs tail /aws/lambda/etl-processor --follow

# Buscar logs por período
aws logs filter-log-events \
  --log-group-name /aws/lambda/etl-processor \
  --start-time $(date -d "1 hour ago" +%s)000
```

### Métricas Importantes
- **Invocações**: Número de execuções da Lambda
- **Duração**: Tempo de processamento
- **Erros**: Falhas na execução
- **SQS Messages**: Mensagens na fila
- **S3 Objects**: Objetos processados

### Alertas Recomendados
```bash
# Criar alerta para erros Lambda
aws cloudwatch put-metric-alarm \
  --alarm-name "ETL-Lambda-Errors" \
  --alarm-description "ETL Lambda function errors" \
  --metric-name Errors \
  --namespace AWS/Lambda \
  --statistic Sum \
  --period 300 \
  --evaluation-periods 1 \
  --threshold 1 \
  --comparison-operator GreaterThanOrEqualToThreshold
```

## 🧪 Testes

### Teste com Dados de Exemplo

```bash
# Criar arquivo de teste
cat > sample_data.csv << EOF
id,name,amount,date,category
1,Transaction A,150.50,2024-01-15,Sales
2,Transaction B,75.25,2024-01-16,Marketing
3,Transaction C,300.00,2024-01-17,Sales
EOF

# Upload para bucket RAW
aws s3 cp sample_data.csv s3://SEU_RAW_BUCKET/
```

### Validação do Processamento

```bash
# Verificar buckets
aws s3 ls s3://SEU_TRUSTED_BUCKET/ --recursive
aws s3 ls s3://SEU_DELIVERY_BUCKET/ --recursive

# Baixar arquivo processado
aws s3 cp s3://SEU_DELIVERY_BUCKET/record_type=DETAIL/year=2024/month=01/day=15/delivery_detail_sample_data_20240115_120000.parquet ./
```

## 📊 Consultas de Exemplo

### AWS Athena Setup

```sql
-- Criar database
CREATE DATABASE etl_pipeline;

-- Tabela TRUSTED
CREATE EXTERNAL TABLE etl_pipeline.trusted_data (
    id bigint,
    name string,
    amount double,
    date string,
    category string,
    processed_timestamp timestamp,
    source_file string,
    processing_layer string,
    data_quality_score double
)
PARTITIONED BY (
    year string,
    month string,
    day string
)
STORED AS PARQUET
LOCATION 's3://SEU_TRUSTED_BUCKET/';

-- Tabela DELIVERY
CREATE EXTERNAL TABLE etl_pipeline.delivery_data (
    id bigint,
    name string,
    amount double,
    amount_category string,
    record_type string,
    delivery_timestamp timestamp,
    processing_layer string,
    pipeline_version string
)
PARTITIONED BY (
    record_type string,
    year string,
    month string,
    day string
)
STORED AS PARQUET
LOCATION 's3://SEU_DELIVERY_BUCKET/';
```

### Consultas Úteis

```sql
-- Registros processados hoje
SELECT record_type, COUNT(*) as total_records
FROM etl_pipeline.delivery_data
WHERE year = '2024' AND month = '01' AND day = '15'
GROUP BY record_type;

-- Qualidade dos dados
SELECT 
    AVG(data_quality_score) as avg_quality,
    MIN(data_quality_score) as min_quality,
    MAX(data_quality_score) as max_quality
FROM etl_pipeline.trusted_data
WHERE year = '2024' AND month = '01';

-- Top categorias por valor
SELECT 
    category,
    SUM(amount) as total_amount,
    COUNT(*) as transaction_count
FROM etl_pipeline.trusted_data
WHERE year = '2024'
GROUP BY category
ORDER BY total_amount DESC;
```

## 🔒 Segurança

### IAM Permissions
- Lambda tem acesso mínimo necessário aos buckets S3
- SQS com encryption habilitada
- S3 buckets com public access bloqueado
- Versionamento habilitado em todos os buckets

### Encryption
- S3: Server-side encryption (AES256)
- SQS: KMS encryption
- Lambda: Encryption at rest

## 💰 Otimização de Custos

### Lifecycle Policies
```hcl
# RAW bucket
- 30 dias → Standard-IA
- 90 dias → Glacier
- 2555 dias → Deletion (7 anos)

# TRUSTED bucket
- 60 dias → Standard-IA
- 180 dias → Glacier
```

### Configurações Lambda
- **Memory**: 512MB (ajustável conforme necessidade)
- **Timeout**: 5 minutos
- **Reserved Concurrency**: Não configurada (uso sob demanda)

## 🚨 Troubleshooting

### Problemas Comuns

#### Lambda Timeout
```bash
# Aumentar timeout
aws lambda update-function-configuration \
  --function-name etl-processor \
  --timeout 600  # 10 minutos
```

#### SQS Messages em DLQ
```bash
# Verificar DLQ
aws sqs get-queue-attributes \
  --queue-url DEAD_LETTER_QUEUE_URL \
  --attribute-names ApproximateNumberOfMessages

# Reprocessar mensagens da DLQ
aws sqs receive-message --queue-url DEAD_LETTER_QUEUE_URL
```

#### Erro de Permissão S3
```bash
# Verificar IAM role da Lambda
aws iam get-role --role-name LAMBDA_ROLE_NAME
aws iam list-attached-role-policies --role-name LAMBDA_ROLE_NAME
```

### Logs de Debug

```python
# Adicionar debug no lambda_function.py
import logging
logging.getLogger().setLevel(logging.DEBUG)

# Logs detalhados aparecerão no CloudWatch
```

## 📈 Escalabilidade

### Configurações para Alto Volume

```bash
# Aumentar batch size SQS→Lambda
aws lambda update-event-source-mapping \
  --uuid EVENT_SOURCE_MAPPING_UUID \
  --batch-size 50

# Configurar reserved concurrency
aws lambda put-reserved-concurrency \
  --function-name etl-processor \
  --reserved-concurrency-configuration 100
```

### Particionamento Avançado

```python
# Exemplo de particionamento por categoria
partition_path = f"category={row['category']}/year={timestamp.year}/month={timestamp.month:02d}/day={timestamp.day:02d}"
```

## 🔄 Backup e Recovery

### Versionamento S3
- Todos os buckets têm versionamento habilitado
- Permite recovery de dados sobrescritos
- Cross-region replication pode ser configurada

### Backup de Configurações
```bash
# Export Terraform state
terraform show -json > infrastructure-backup.json

# Backup de código Lambda
aws lambda get-function \
  --function-name etl-processor \
  --query 'Code.Location' \
  --output text | xargs curl -o lambda-backup.zip
```

## 🌐 Ambientes

### Configuração Multi-Ambiente

```bash
# Desenvolvimento
terraform workspace new dev
terraform apply -var-file="dev.tfvars"

# Produção
terraform workspace new prod
terraform apply -var-file="prod.tfvars"
```

### Variáveis por Ambiente

```hcl
# dev.tfvars
aws_region = "us-east-1"
bucket_prefix = "etl-pipeline-dev"
lambda_function_name = "etl-processor-dev"

# prod.tfvars
aws_region = "us-east-1"
bucket_prefix = "etl-pipeline-prod"
lambda_function_name = "etl-processor-prod"
```

## 📚 Recursos Adicionais

### Documentação AWS
- [Lambda Best Practices](https://docs.aws.amazon.com/lambda/latest/dg/best-practices.html)
- [S3 Performance](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html)
- [SQS Best Practices](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-best-practices.html)

### Terraform Resources
- [AWS Provider](https://registry.terraform.io/providers/hashicorp/aws/latest/docs)
- [Terraform Best Practices](https://www.terraform-best-practices.com/)

## 🤝 Contribuição

1. Fork o projeto
2. Crie uma branch para sua feature (`git checkout -b feature/AmazingFeature`)
3. Commit suas mudanças (`git commit -m 'Add some AmazingFeature'`)
4. Push para a branch (`git push origin feature/AmazingFeature`)
5. Abra um Pull Request

## 📄 Licença

Este projeto está licenciado sob a MIT License - veja o arquivo [LICENSE](LICENSE) para detalhes.

## ✨ Próximos Passos

- [ ] Implementar CDC (Change Data Capture)
- [ ] Adicionar suporte a streaming com Kinesis
- [ ] Integração com Apache Airflow
- [ ] Dashboard de monitoramento com QuickSight
- [ ] Implementar data lineage tracking
- [ ] Adicionar testes automatizados
- [ ] CI/CD pipeline com GitHub Actions

---

**Desenvolvido com ❤️ para processamento de dados em escala na AWS**
