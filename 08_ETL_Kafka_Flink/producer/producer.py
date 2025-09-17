import json, os, time, random, string, sys
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
topic = os.getenv("TOPIC", "reclamacoes")
rate = float(os.getenv("MESSAGES_PER_SEC", "2"))
interval = 1.0 / rate if rate > 0 else 0

clientes = ["C1","C2","C3"]
produtos = ["P1","P2","P3","P4"]
descrs = ["produto com defeito","entrega atrasada","troca solicitada","atendimento ruim"]
rid = lambda n=8: ''.join(random.choices(string.ascii_letters+string.digits, k=n))

# retry para conectar no broker
producer = None
backoff = 1
for attempt in range(1, 11):
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
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

print(f"[producer] conectado em {bootstrap}; publicando em '{topic}' @ {rate} msg/s")
while True:
    msg = {
        "id": rid(),
        "cliente_id": random.choice(clientes),
        "produto_id": random.choice(produtos),
        "descricao": random.choice(descrs),
        "event_ts": int(time.time() * 1000),
    }
    producer.send(topic, msg)
    # 👇 imprime cada linha gerada
    print(json.dumps(msg, ensure_ascii=False))
    time.sleep(interval)
