import os
from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
DADOS_DIR = BASE_DIR / "dados"
SEEDS_DIR = BASE_DIR / "dbt" / "project" / "seeds"

CONFIGS = {
    "Bancos": {"sep": "\t", "encoding": "utf-8"},
    "Empregados": {"sep": "|", "encoding": "utf-8"},
    "Reclamacoes": {"sep": ";", "encoding": "latin-1"},
}

def processar_categoria(categoria: str, config: dict) -> None:
    """Itera sobre os arquivos de uma categoria, gera e salva seeds."""
    origem = DADOS_DIR / categoria
    destino_categoria = SEEDS_DIR / categoria
    destino_categoria.mkdir(parents=True, exist_ok=True)

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
        df = df.replace(r"^\s*$", None, regex=True).where(pd.notna(df), None)
        nome_saida = f"{arquivo.stem}.csv"
        caminho_saida = destino_categoria / nome_saida
        df.to_csv(caminho_saida, index=False)
        print(f"Seed gerada: {caminho_saida}")

def main() -> None:
    SEEDS_DIR.mkdir(parents=True, exist_ok=True)
    for antigo in ["bancos.csv", "empregados.csv", "reclamacoes.csv"]:
        caminho_antigo = SEEDS_DIR / antigo
        if caminho_antigo.exists():
            caminho_antigo.unlink()
            print(f"Removido seed consolidado antigo: {caminho_antigo}")
    for categoria, config in CONFIGS.items():
        processar_categoria(categoria, config)

if __name__ == "__main__":
    main()
