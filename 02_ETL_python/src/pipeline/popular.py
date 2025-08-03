import os
import logging
import boto3
from dotenv import load_dotenv

# Configura logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class S3Uploader:
    def __init__(self):
        load_dotenv()

        self.local_base_dir = "/app/Dados"
        self.s3_bucket = os.getenv("S3_BUCKET")
        self.s3_base_prefix = "Dados"
        self.categorias = ["Bancos", "Reclamacoes", "Empregados"]

        if not self.s3_bucket:
            raise ValueError("❌ Variável de ambiente S3_BUCKET não está definida.")

        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_REGION"),
            endpoint_url=os.getenv("S3_ENDPOINT_URL", None),
        )

    def enviar_arquivos(self):
        total = 0
        for categoria in self.categorias:
            local_categoria_path = os.path.join(self.local_base_dir, categoria)

            if not os.path.isdir(local_categoria_path):
                logging.warning(f"Pasta não encontrada: {local_categoria_path}")
                continue

            for root, _, files in os.walk(local_categoria_path):
                for nome_arquivo in files:
                    caminho_local = os.path.join(root, nome_arquivo)
                    rel_path = os.path.relpath(caminho_local, self.local_base_dir)
                    s3_key = os.path.join(self.s3_base_prefix, rel_path).replace("\\", "/")

                    try:
                        self.s3_client.upload_file(Filename=caminho_local, Bucket=self.s3_bucket, Key=s3_key)
                        logging.info(f"✅ Enviado: {s3_key}")
                        total += 1
                    except Exception as e:
                        logging.error(f"❌ Falha ao enviar {s3_key}: {e}")

        logging.info(f"\n📦 Upload concluído. {total} arquivos enviados.")
