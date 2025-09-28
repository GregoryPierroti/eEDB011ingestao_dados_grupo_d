import csv
import json
import os
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
topic = os.getenv("TOPIC", "reclamacoes")
rate = float(os.getenv("MESSAGES_PER_SEC", "2"))
interval = 1.0 / rate if rate > 0 else 0
seeds_dir = Path(os.getenv("SEEDS_DIR", "/data/seeds"))


def clean_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    if value.lower() == "n.a.":
        return None
    return value


def parse_int(value: Optional[str]) -> Optional[int]:
    value = clean_text(value)
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def parse_float(value: Optional[str]) -> Optional[float]:
    value = clean_text(value)
    if value is None:
        return None
    value = value.replace(".", "").replace(",", ".")
    try:
        return float(value)
    except ValueError:
        return None


def parse_event_ts(ano: Optional[int], trimestre: Optional[str]) -> str:
    try:
        year = int(ano) if ano is not None else datetime.now().year
    except (TypeError, ValueError):
        year = datetime.now().year

    trimestre = (trimestre or "").strip().lower()
    quarter_map = {
        "1": 1,
        "1º": 1,
        "2": 4,
        "2º": 4,
        "3": 7,
        "3º": 7,
        "4": 10,
        "4º": 10,
    }
    month = quarter_map.get(trimestre, 1)
    dt = datetime(year, month, 1, tzinfo=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def load_reclamacoes() -> List[Dict[str, Any]]:
    if not seeds_dir.exists():
        print(f"[producer] diretório de seeds '{seeds_dir}' não encontrado.", file=sys.stderr)
        sys.exit(1)

    files = sorted(seeds_dir.glob("202*_tri_*.csv"))
    if not files:
        print(f"[producer] nenhum arquivo de reclamações encontrado em {seeds_dir}", file=sys.stderr)
        sys.exit(1)

    registros: List[Dict[str, Any]] = []
    for path in files:
        with path.open(encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                ano = parse_int(row.get("ano"))
                trimestre = clean_text(row.get("trimestre"))
                registro: Dict[str, Any] = {
                    "ano": ano,
                    "trimestre": trimestre,
                    "categoria": clean_text(row.get("categoria")),
                    "tipo": clean_text(row.get("tipo")),
                    "cnpj_if": clean_text(row.get("cnpj_if")),
                    "instituicao_financeira": clean_text(row.get("instituicao_financeira")),
                    "indice": parse_float(row.get("indice")),
                    "quantidade_de_reclamacoes_reguladas_procedentes": parse_int(row.get("quantidade_de_reclamacoes_reguladas_procedentes")),
                    "quantidade_de_reclamacoes_reguladas_outras": parse_int(row.get("quantidade_de_reclamacoes_reguladas_outras")),
                    "quantidade_de_reclamacoes_nao_reguladas": parse_int(row.get("quantidade_de_reclamacoes_nao_reguladas")),
                    "quantidade_total_de_reclamacoes": parse_int(row.get("quantidade_total_de_reclamacoes")),
                    "quantidade_total_de_clientes_ccs_e_scr": parse_int(row.get("quantidade_total_de_clientes_ccs_e_scr")),
                    "quantidade_de_clientes_ccs": parse_int(row.get("quantidade_de_clientes_ccs")),
                    "quantidade_de_clientes_scr": parse_int(row.get("quantidade_de_clientes_scr")),
                    "fonte_tabela": path.stem,
                }
                registro["event_ts"] = parse_event_ts(ano, trimestre)
                registros.append(registro)

    if not registros:
        print("[producer] nenhum registro de reclamações foi carregado.", file=sys.stderr)
        sys.exit(1)

    random.shuffle(registros)
    return registros


reclamacoes = load_reclamacoes()

# retry para conectar no broker
producer = None
backoff = 1
for attempt in range(1, 11):
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            acks="all",
            linger_ms=5,
            security_protocol="PLAINTEXT",
        )
        break
    except NoBrokersAvailable as e:
        print(f"[producer] broker indisponível ({e}). tentativa {attempt}/10; aguardando {backoff}s...", file=sys.stderr)
        time.sleep(backoff)
        backoff = min(backoff * 2, 15)

if producer is None:
    print("[producer] não foi possível conectar ao broker após várias tentativas. saindo.", file=sys.stderr)
    sys.exit(1)

print(f"[producer] conectado em {bootstrap}; publicando em '{topic}' @ {rate} msg/s com {len(reclamacoes)} registros base")

idx = 0
total = len(reclamacoes)
while True:
    base = reclamacoes[idx % total]
    idx += 1
    msg = dict(base)
    msg["event_ts"] = parse_event_ts(msg.get("ano"), msg.get("trimestre"))
    producer.send(topic, msg)
    print(json.dumps(msg, ensure_ascii=False))
    if total > 1 and idx % total == 0:
        random.shuffle(reclamacoes)
    time.sleep(interval)
