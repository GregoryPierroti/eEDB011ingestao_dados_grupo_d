CREATE SCHEMA IF NOT EXISTS dim;

CREATE TABLE IF NOT EXISTS dim.clientes (
  cliente_id TEXT PRIMARY KEY,
  segmento   TEXT,
  uf         TEXT
);

CREATE TABLE IF NOT EXISTS dim.produtos (
  produto_id TEXT PRIMARY KEY,
  categoria  TEXT,
  linha      TEXT
);

INSERT INTO dim.clientes (cliente_id, segmento, uf) VALUES
  ('C1','S1','SP'),
  ('C2','S2','RJ'),
  ('C3','S1','MG')
ON CONFLICT DO NOTHING;

INSERT INTO dim.produtos (produto_id, categoria, linha) VALUES
  ('P1','Eletronicos','Linha A'),
  ('P2','Eletrodomesticos','Linha B'),
  ('P3','Moveis','Linha C'),
  ('P4','Moveis','Linha D')
ON CONFLICT DO NOTHING;
