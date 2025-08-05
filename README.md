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
