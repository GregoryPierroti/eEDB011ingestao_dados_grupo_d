# Ingestão de Dados - Entregas

Repositório com os trabalhos feitos para a disciplina de **Ingestão de Dados**.

## Integrantes

- Gregory Pierroti Macario Toledo  
- Gabriel Siqueira  
- Guilherme Monte  
- Guilherme Mafra  

---

Disciplina do curso de graduação. Todos os trabalhos têm fins acadêmicos.

---

## Estrutura do Projeto

```
.
├── 01_ETL_apache_hop/
│   ├── docker-compose.yml
│   ├── README.md
│   ├── Dados/
│   │   ├── Bancos/
│   │   ├── Empregados/
│   │   └── Reclamacoes/
│   ├── export_postgres/
│   └── workflow_apache_hop/
├── 02_ETL_python/
│   ├── docker-compose.yaml
│   ├── dockerfile
│   ├── requirements.txt
│   └── src/
│       ├── main.py
│       ├── pipeline/
│       └── utils/
├── .gitignore
└── README.md
```

## Descrição Geral

Este repositório contém dois pipelines de ETL distintos para integração, tratamento e carga de dados em um banco de dados PostgreSQL, utilizando diferentes tecnologias:

- **[01_ETL_apache_hop](01_ETL_apache_hop/README.md):** Pipeline desenvolvido com Apache Hop, focado em integração visual e manipulação de dados via interface gráfica.
- **02_ETL_python:** Pipeline desenvolvido em Python, com scripts customizados para processamento e ingestão dos dados.

## 01_ETL_apache_hop

Pipeline de ETL criado com **Apache Hop** para integração de dados e carga em uma instância **PostgreSQL RDS (AWS)**.

### Funcionalidades

- Leitura de múltiplos arquivos CSV (Reclamações, Bancos, Funcionários)
- Remoção de nulos e duplicatas
- Junções entre as fontes (por nome da instituição)
- Limpeza e padronização de strings
- Escrita no PostgreSQL e exportação em CSV

### Tabela Destino

Os dados tratados são gravados na tabela `bancos_unificados` no PostgreSQL RDS e exportados como CSV.

### Tecnologias

- Apache Hop
- PostgreSQL RDS (AWS)
- DBeaver (acesso ao banco)
- CSV

### Conexão com o Banco

- **Host:** `database-1.cjmy2emmosmf.us-east-2.rds.amazonaws.com`
- **Porta:** `5432`
- **Banco:** `postgres`

> As credenciais são armazenadas localmente no Apache Hop via conexão segura — não incluídas neste repositório.

Mais detalhes em [01_ETL_apache_hop/README.md](01_ETL_apache_hop/README.md).

---

## 02_ETL_python

Pipeline de ETL implementado em Python para processamento e ingestão dos mesmos conjuntos de dados.

### Estrutura

- `src/main.py`: Script principal de execução do pipeline.
- `src/pipeline/`: Módulos do pipeline ETL.
- `src/utils/`: Utilitários auxiliares.
- `requirements.txt`: Dependências do projeto.
- `docker-compose.yaml` e `dockerfile`: Configuração para execução em ambiente Docker.

### Como Executar

1. Instale as dependências:
    ```sh
    pip install -r requirements.txt
    ```
2. Execute o pipeline:
    ```sh
    python src/main.py
    ```
3. Ou utilize Docker:
    ```sh
    docker-compose up --build
    ```

---

## Licença

Uso acadêmico apenas.

---

## Contato

Dúvidas ou sugestões: abra uma issue ou entre em contato com um dos integrantes do grupo.


## 03_ETL_PySpark

Projeto de Pipeline de ETL com PySpark e Docker
1. Visão Geral
Este projeto implementa um pipeline de Extração, Transformação e Carga (ETL) utilizando PySpark para processamento de dados distribuído e Docker/Docker Compose para criar um ambiente conteinerizado e reproduzível.
O objetivo do pipeline é consolidar dados de três fontes distintas sobre instituições financeiras no Brasil:
Dados Cadastrais de Bancos: Informações básicas como CNPJ, nome e segmento.
Reclamações de Clientes: Dados trimestrais de reclamações registadas no Banco Central.
Avaliações de Funcionários: Dados de avaliações de empresas extraídos do Glassdoor.
O pipeline processa esses dados através de uma arquitetura de camadas (RAW, TRUSTED, DELIVERY) e, ao final, salva o resultado consolidado num banco de dados PostgreSQL para análise.
2. Tecnologias Utilizadas
Linguagem: Python 3.11
Processamento de Dados: Apache Spark (PySpark)
Banco de Dados: PostgreSQL
Ambiente: Docker e Docker Compose
Bibliotecas Python Principais:
pyspark
psycopg2-binary
python-dotenv
3. Estrutura do Projeto
03_ETL_spark/
|-- docker-compose.yaml      # Orquestra os containers do Spark e Postgres
|-- Dockerfile               # Define a imagem do container Spark
|-- .env                     # Armazena as variáveis de ambiente (credenciais)
|-- requirements.txt         # Lista as dependências Python
└── src/
    ├── main.py              # Ponto de entrada que executa o pipeline completo
    └── pipeline/
        ├── Dados/             # Contém os ficheiros de dados brutos
        │   ├── Bancos/
        │   ├── Empregados/
        │   └── Reclamacoes/
        ├── Camadas/           # (Criado em tempo de execução)
        │   ├── RAW/
        │   ├── Trusted/
        │   └── Delivery/
        ├── ingestao_raw_spark.py          # Lógica da Etapa 1
        ├── transformacoes_trusted_spark.py # Lógica da Etapa 2
        └── agregacoes_delivery_spark.py   # Lógica da Etapa 3


4. Configuração do Ambiente
Pré-requisitos
Docker
Docker Compose
Arquivo de Ambiente (.env)
Antes de iniciar, crie um arquivo chamado .env na raiz do projeto (03_ETL_spark/) com o seguinte conteúdo, que define as credenciais para o banco de dados PostgreSQL:
# Credenciais do PostgreSQL
POSTGRES_USER=etl_user
POSTGRES_PASSWORD=etl_pass
POSTGRES_DB=spark_db

# Credenciais do PgAdmin (opcional)
PGADMIN_EMAIL=admin@admin.com
PGADMIN_PASSWORD=admin


5. Como Executar o Pipeline
Abra um terminal na pasta raiz do projeto (03_ETL_spark/) e execute os seguintes comandos na ordem especificada.
1. Limpar o Ambiente Anterior (Opcional, recomendado se houver erros)
Este comando para e remove containers, redes e volumes de execuções anteriores para garantir um início limpo.
docker-compose down
docker volume rm 03_ETL_spark_postgres_data


2. Construir e Iniciar os containers
Este comando irá construir a imagem do Spark (instalando todas as dependências) e iniciar os containers do Spark e do Postgres em segundo plano.
docker-compose up -d --build


3. Aguardar a Inicialização do Banco de Dados
Espere cerca de 15 a 20 segundos para que o serviço do Postgres esteja totalmente pronto para aceitar conexões.

4. Executar o Pipeline de ETL
Este comando entra no container do Spark e executa o script main.py, que orquestra todo o processo de ETL.
docker-compose exec spark python /app/src/main.py


Ao final da execução, os dados consolidados estarão disponíveis na tabela reclamacoes_consolidadas dentro do banco de dados spark_db no Postgres.
6. Verificando a camada de Delivery
Após a execução, pode-se verificar os dados na tabela final de duas maneiras:
a - Usando uma Ferramenta Gráfica (ex: DBeaver, pgAdmin)
Conecte-se ao banco de dados com as seguintes credenciais:
Host: localhost
Porta: 5432
Banco de Dados: spark_db
Utilizador: etl_user
Senha: etl_pass
Execute a seguinte consulta SQL para ver uma amostra dos dados:
SELECT * FROM reclamacoes_consolidadas LIMIT 10;


b - Usando o Terminal
Execute no terminal:
docker-compose exec postgres psql -U etl_user -d spark_db


Quando for solicitada a senha, digite etl_pass e pressione Enter.
Execute a mesma consulta SQL:
SELECT * FROM reclamacoes_consolidadas LIMIT 10;


