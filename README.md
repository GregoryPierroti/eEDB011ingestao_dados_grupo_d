# Ingestão de Dados - Entregas

Repositório com os trabalhos feitos para a disciplina de **Ingestão de Dados**.

## Integrantes

- Gregory Pierroti Macario Toledo  
- Gabriel Siqueira  
- Guilherme Monte  
- Guilherme Mafra  

Cada trabalho está em sua respectiva pasta, com os arquivos e instruções necessários.
---

Disciplina do curso de graduação. Todos os trabalhos têm fins acadêmicos.


# eEDB011 - Ingestão de Dados: Fase 02 - ETL com Python

Este repositório contém a segunda fase do projeto de ingestão de dados `eEDB011`, focada na implementação de um pipeline ETL (Extract, Transform, Load) utilizando a linguagem de programação Python. O objetivo principal desta etapa é demonstrar o processo de extração de dados de diversas fontes, sua transformação para um formato padronizado e a carga em um destino específico.

## Descrição do Projeto

O módulo `02_ETL_python` é responsável por automatizar o fluxo de processamento de dados. Ele aborda as seguintes etapas cruciais:

*   **Extração (Extract):** Coleta de dados de fontes brutas, que podem incluir arquivos CSV, JSON, bancos de dados, APIs, entre outros.
*   **Transformação (Transform):** Limpeza, enriquecimento, normalização e agregação dos dados extraídos para prepará-los para o consumo ou análise. Esta etapa pode envolver manipulação de dados, tratamento de valores ausentes, conversão de tipos e aplicação de regras de negócio.
*   **Carga (Load):** Inserção dos dados transformados em um sistema de destino, como um banco de dados relacional, um *data warehouse*, um *data lake*, ou outros sistemas de armazenamento.

## Funcionalidades

*   **Modularidade:** Código organizado em módulos para facilitar a manutenção e a reutilização.
*   **Flexibilidade:** Projetado para ser adaptável a diferentes fontes de dados e destinos.
*   **Reusabilidade:** Funções e classes genéricas para operações ETL comuns.
*   **Tratamento de Erros:** Mecanismos para lidar com exceções e registrar falhas durante o processo ETL.

## Tecnologias Utilizadas

*   **Python:** Linguagem principal para o desenvolvimento do pipeline ETL.
*   **Bibliotecas Python Comuns (a serem implementadas/utilizadas):**
    *   `pandas`: Para manipulação e análise de dados.
    *   `sqlalchemy` / `psycopg2` / `mysql-connector-python`: Para conexão e interação com bancos de dados.
    *   `requests`: Para extração de dados de APIs.
    *   `os`, `json`, `csv`: Para manipulação de arquivos.
    *   `logging`: Para registro de eventos e erros.

## Estrutura do Projeto

A estrutura esperada para este diretório é a seguinte (pode variar de acordo com a implementação):

```
02_ETL_python/
├── src/
│   ├── extract.py         # Módulo para funções de extração de dados
│   ├── transform.py       # Módulo para funções de transformação de dados
│   ├── load.py            # Módulo para funções de carga de dados
│   └── utils.py           # Módulo para funções utilitárias (e.g., configuração, logging)
├── main.py                # Ponto de entrada principal do script ETL
├── config/
│   └── settings.ini       # Arquivo de configuração (e.g., credenciais de BD, caminhos de arquivo)
├── data/
│   ├── raw/               # Diretório para dados brutos/extraídos
│   └── processed/         # Diretório para dados transformados (opcional)
├── notebooks/             # (Opcional) Jupyter notebooks para exploração/teste
│   └── data_exploration.ipynb
├── tests/                 # (Opcional) Testes unitários para as funções ETL
│   └── test_etl.py
├── .gitignore             # Arquivo para ignorar arquivos não versionados
└── README.md              # Este arquivo
```

## Como Usar / Executar

Para configurar e executar este projeto, siga os passos abaixo:

1.  **Clone o repositório:**

    ```bash
    git clone https://github.com/GregoryPierroti/eEDB011ingestao_dados_grupo_d.git
    cd eEDB011ingestao_dados_grupo_d/02_ETL_python
    ```

2.  **Crie e ative um ambiente virtual (recomendado):**

    ```bash
    python -m venv venv
    # No Windows
    .\venv\Scripts\activate
    # No macOS/Linux
    source venv/bin/activate
    ```

3.  **Instale as dependências:**
    Crie um arquivo `requirements.txt` na raiz de `02_ETL_python` com as bibliotecas necessárias e, em seguida, instale-as:

    ```bash
    pip install -r requirements.txt
    ```

    Exemplo de `requirements.txt`:
    ```
    pandas
    sqlalchemy
    psycopg2-binary # ou outro driver de BD
    requests
    ```

4.  **Configure as fontes e destinos de dados:**
    Edite o arquivo `config/settings.ini` ou ajuste as variáveis diretamente nos scripts `src/` conforme necessário para apontar para suas fontes de dados e destinos (ex: credenciais de banco de dados, caminhos de arquivo).

5.  **Execute o pipeline ETL:**

    ```bash
    python main.py
    ```

    Acompanhe a saída do console e os logs (se configurados) para verificar o progresso e possíveis erros.

## Contribuição

Contribuições são bem-vindas! Se você deseja contribuir para este projeto, por favor:

1.  Faça um *fork* do repositório.
2.  Crie uma nova *branch* (`git checkout -b feature/sua-feature`).
3.  Faça suas alterações e *commit* (`git commit -m 'Adiciona nova feature'`).
4.  Envie suas alterações (`git push origin feature/sua-feature`).
5.  Abra um *Pull Request*.
