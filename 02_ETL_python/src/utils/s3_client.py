import os
import boto3
from io import BytesIO
import pandas as pd
from botocore.exceptions import ClientError
from datetime import datetime
import logging


class S3Client:
    def __init__(self):
        self.bucket = os.getenv("S3_BUCKET")
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_REGION"),
            endpoint_url=os.getenv("S3_ENDPOINT_URL", None),
        )

    def list_files(self, prefix):
        """
        List all files under a given prefix (folder) in the bucket.
        """
        try:
            response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
            return [
                obj["Key"]
                for obj in response.get("Contents", [])
                if not obj["Key"].endswith("/") and not obj["Key"].endswith(".keep")
            ]

        except ClientError as e:
            print(f"❌ Error listing files in {prefix}: {e}")
            return []

    def read_file(self, key):
        # try:
        response = self.s3.get_object(Bucket=self.bucket, Key=key)
        file_stream = BytesIO(response["Body"].read())

        if key.endswith(".tsv"):
            sep = "\t"
            encoding = "utf-8"
        elif "Empregados/" in key:
            sep = "|"
            encoding = "utf-8"
        else:
            sep = ";"
            encoding = "latin1"

        df = pd.read_csv(file_stream, sep=sep, encoding=encoding, dtype=str)
        # Padroniza colunas obrigatórias em arquivos de Empregados
        colunas_obrigatorias_empregados = ["Nome", "CNPJ", "Segmento"]
        if "Empregados/" in key:
            for col in colunas_obrigatorias_empregados:
                if col not in df.columns:
                    df[col] = None

        # print(df.head(10).to_string(index=False))  # Exibe as primeiras 5 linhas do DataFrame

        # Remove colunas 'Unnamed' que não interessam
        unnamed_cols = [col for col in df.columns if col.startswith("Unnamed")]
        if unnamed_cols:
            df.drop(columns=unnamed_cols, inplace=True)
        return df

        # except Exception as e:
        #    logging.error(f"❌ Error reading file {key}: {e}")
        #    raise ValueError(
        #        f"Não conseguiu ler arquivo {key} com separadores e encodings padrão."
        #    )

    def move_file(self, source_key, destination_prefix, add_timestamp=True):
        """
        Moves a file by copying and deleting original.
        Optionally adds timestamp before file extension in destination filename.

        Args:
            source_key (str): original file key
            destination_prefix (str): prefix/folder for destination (must end with /)
            add_timestamp (bool): if True, appends timestamp to filename
        """
        try:
            filename = source_key.split("/")[-1]
            if add_timestamp:
                name, ext = os.path.splitext(filename)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"{name}_{timestamp}{ext}"
            destination_key = destination_prefix + filename

            self.s3.copy_object(
                Bucket=self.bucket,
                CopySource={"Bucket": self.bucket, "Key": source_key},
                Key=destination_key,
            )
            self.s3.delete_object(Bucket=self.bucket, Key=source_key)
            print(f"📦 File moved from {source_key} to {destination_key}")
        except ClientError as e:
            print(f"❌ Error moving file {source_key}: {e}")

    def ensure_base_structure(self):
        """
        Ensures the base folder structure exists in the bucket by creating
        empty .keep files inside them.
        """
        folders = ["Dados/Bancos/", "Dados/Reclamacoes/", "Dados/Empregados/"]

        for folder in folders:
            key = f"{folder}.keep"
            try:
                self.s3.put_object(Bucket=self.bucket, Key=key, Body=b"")
                print(f"📁 Structure ensured: {folder}")
            except ClientError as e:
                print(f"❌ Failed to ensure structure for {folder}: {e}")

    def get_object_metadata(self, key):
        """
        Retorna os metadados de um objeto no S3 (ex: tamanho do arquivo).
        """
        return self.s3.head_object(Bucket=self.bucket, Key=key)
