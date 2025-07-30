

# ETL com Apache Hop para PostgreSQL RDS

Este diretório contém o pipeline de ETL desenvolvido com **Apache Hop** para integração de dados e carga em uma instância **PostgreSQL RDS (AWS)**.

## ⚙️ Descrição

O processo realiza a leitura de três conjuntos de arquivos CSV:

* Reclamações
* Bancos
* Funcionários

Esses dados são tratados, padronizados e unificados em um único fluxo no Apache Hop, e o resultado final é:

* Gravado na tabela `bancos_unificados` no PostgreSQL RDS
* Exportado como um CSV, disponível neste diretório

## 🧪 Tecnologias utilizadas

* Apache Hop (ETL)
* PostgreSQL RDS (AWS)
* DBeaver (acesso ao banco)
* CSV (como input intermediário)

## 🛠️ Pipeline ETL

O fluxo inclui:

* Leitura de múltiplos arquivos por categoria
* Remoção de nulos e duplicatas
* Junções entre as fontes (por nome da instituição)
* Limpeza de strings e padronização de nomes (`Trim`, `Upper`, remoção de termos como "Banco", "PRUDENCIAL")
* Escrita no PostgreSQL e exportação em CSV
<img width="1280" height="552" alt="image" src="https://github.com/user-attachments/assets/1af32d30-c267-498c-bb47-200b8d4d8a42" />

## 🧾 Tabela destino: `bancos_unificados`

A tabela contém colunas integradas dos três conjuntos de dados.

## 🔗 Conexão com RDS
<img width="1061" height="399" alt="image" src="https://github.com/user-attachments/assets/3b3a5c92-c4f1-4e44-acd2-00fc7a026e8e" />

* **Host:** `database-1.cjmy2emmosmf.us-east-2.rds.amazonaws.com`
* **Porta:** `5432`
* **Banco:** `postgres`
<img width="761" height="597" alt="image" src="https://github.com/user-attachments/assets/af3ef13b-e1f2-4b7c-8dbb-bc0e65ca4b28" />

(As credenciais são armazenadas localmente no Apache Hop via conexão segura — não incluídas neste repositório.)


