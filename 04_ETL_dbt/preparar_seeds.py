import os
from pathlib import Path
import pandas as pd
import csv
import re
import unicodedata

SEEDS_DIR = Path("/home/gregory/projetos/ingestao-dados/eEDB011ingestao_dados_grupo_d/04_ETL_dbt/dbt/project/seeds")
DADOS_DIR = Path(__file__).resolve().parent / "dados"

CONFIGS = {
    "Bancos": {"sep": "\t", "encoding": "utf-8"},
    "Empregados": {"sep": "|", "encoding": "utf-8"},
    "Reclamacoes": {"sep": ";", "encoding": "latin-1"},
}

def _slugify(name: str) -> str:
    # remove acentos
    nfkd = unicodedata.normalize("NFKD", name)
    no_accents = "".join(ch for ch in nfkd if not unicodedata.combining(ch))
    # lower, troca não-alfanum por underscore
    s = re.sub(r"[^0-9a-zA-Z]+", "_", no_accents).strip("_").lower()
    # collapse múltiplos underscores
    s = re.sub(r"_+", "_", s)
    # nomes não podem começar com número em alguns destinos
    if s and s[0].isdigit():
        s = f"c_{s}"
    return s or "col"

def _validar_csv(caminho: Path) -> None:
    with caminho.open("r", newline="", encoding="utf-8") as f:
        r = csv.reader(f)
        try:
            header = next(r)
        except StopIteration:
            print(f"[VALIDAÇÃO] Arquivo vazio: {caminho}")
            return
        n = len(header)
        for i, row in enumerate(r, start=2):
            if len(row) != n:
                print(f"[VALIDAÇÃO] {caminho.name}: linha {i} tem {len(row)} colunas (esperado {n}). Início: {row[:5]}")

def processar_categoria(categoria: str, config: dict) -> None:
    origem = DADOS_DIR / categoria
    if not origem.exists():
        print(f"Diretório de origem não encontrado: {origem}")
        return

    for arquivo in origem.iterdir():
        if not arquivo.is_file():
            continue
        if arquivo.stat().st_size == 0:
            print(f"Arquivo vazio ignorado: {arquivo}")
            continue

        try:
            df = pd.read_csv(arquivo, sep=config["sep"], encoding=config["encoding"])
        except pd.errors.EmptyDataError:
            print(f"Sem dados para processar: {arquivo}")
            continue
        except Exception as e:
            print(f"Falha lendo {arquivo}: {e}")
            continue

        # normaliza header -> snake_case ascii seguro para dbt/SQL
        df.columns = [_slugify(str(c)) for c in df.columns]

        # normaliza brancos -> None
        df = df.replace(r"^\s*$", None, regex=True).where(pd.notna(df), None)

        # saída SEM subpastas
        nome_saida = f"{arquivo.stem}.csv"
        caminho_saida = SEEDS_DIR / nome_saida

        try:
            df.to_csv(
                caminho_saida,
                index=False,
                lineterminator="\n",
                quoting=csv.QUOTE_ALL,
                encoding="utf-8"
            )
            # valida estrutura após escrever
            _validar_csv(caminho_saida)
            print(f"Seed gerada: {caminho_saida}")
        except Exception as e:
            print(f"Falha escrevendo {caminho_saida}: {e}")

def main() -> None:
    SEEDS_DIR.mkdir(parents=True, exist_ok=True)

    # (opcional) limpar seeds antigos consolidados
    for antigo in ["bancos.csv", "empregados.csv", "reclamacoes.csv"]:
        caminho_antigo = SEEDS_DIR / antigo
        if caminho_antigo.exists():
            try:
                caminho_antigo.unlink()
                print(f"Removido seed consolidado antigo: {caminho_antigo}")
            except Exception as e:
                print(f"Falha removendo {caminho_antigo}: {e}")

    for categoria, config in CONFIGS.items():
        processar_categoria(categoria, config)

if __name__ == "__main__":
    main()
