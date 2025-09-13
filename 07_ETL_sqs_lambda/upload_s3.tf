# Upload data files to RAW bucket for initial processing
# These files will be automatically processed through the ETL pipeline

resource "aws_s3_object" "reclamacoes_2021_tri_01" {
  bucket = module.s3.raw_bucket_name  # Fixed: was bucket_name
  key    = "raw/Reclamacoes/2021_tri_01.csv"
  source = "./Dados/Reclamacoes/2021_tri_01.csv"
  etag   = filemd5("./Dados/Reclamacoes/2021_tri_01.csv")

  tags = var.common_tags
}

resource "aws_s3_object" "reclamacoes_2021_tri_02" {
  bucket = module.s3.raw_bucket_name
  key    = "raw/Reclamacoes/2021_tri_02.csv"
  source = "./Dados/Reclamacoes/2021_tri_02.csv"
  etag   = filemd5("./Dados/Reclamacoes/2021_tri_02.csv")

  tags = var.common_tags
}

resource "aws_s3_object" "reclamacoes_2021_tri_03" {
  bucket = module.s3.raw_bucket_name
  key    = "raw/Reclamacoes/2021_tri_03.csv"
  source = "./Dados/Reclamacoes/2021_tri_03.csv"
  etag   = filemd5("./Dados/Reclamacoes/2021_tri_03.csv")

  tags = var.common_tags
}

resource "aws_s3_object" "reclamacoes_2021_tri_04" {
  bucket = module.s3.raw_bucket_name
  key    = "raw/Reclamacoes/2021_tri_04.csv"
  source = "./Dados/Reclamacoes/2021_tri_04.csv"
  etag   = filemd5("./Dados/Reclamacoes/2021_tri_04.csv")

  tags = var.common_tags
}

resource "aws_s3_object" "reclamacoes_2022_tri_01" {
  bucket = module.s3.raw_bucket_name
  key    = "raw/Reclamacoes/2022_tri_01.csv"
  source = "./Dados/Reclamacoes/2022_tri_01.csv"
  etag   = filemd5("./Dados/Reclamacoes/2022_tri_01.csv")

  tags = var.common_tags
}

resource "aws_s3_object" "reclamacoes_2022_tri_03" {
  bucket = module.s3.raw_bucket_name
  key    = "raw/Reclamacoes/2022_tri_03.csv"
  source = "./Dados/Reclamacoes/2022_tri_03.csv"
  etag   = filemd5("./Dados/Reclamacoes/2022_tri_03.csv")

  tags = var.common_tags
}

resource "aws_s3_object" "reclamacoes_2022_tri_04" {
  bucket = module.s3.raw_bucket_name
  key    = "raw/Reclamacoes/2022_tri_04.csv"
  source = "./Dados/Reclamacoes/2022_tri_04.csv"
  etag   = filemd5("./Dados/Reclamacoes/2022_tri_04.csv")

  tags = var.common_tags
}

resource "aws_s3_object" "bancos_enquadramento" {
  bucket = module.s3.raw_bucket_name
  key    = "raw/Bancos/EnquadramentoInicia_v2.tsv"
  source = "./Dados/Bancos/EnquadramentoInicia_v2.tsv"
  etag   = filemd5("./Dados/Bancos/EnquadramentoInicia_v2.tsv")

  tags = var.common_tags
}

resource "aws_s3_object" "empregados_glassdoor" {
  bucket = module.s3.raw_bucket_name
  key    = "raw/Empregados/glassdoor_consolidado_join_match_v2.csv"
  source = "./Dados/Empregados/glassdoor_consolidado_join_match_v2.csv"
  etag   = filemd5("./Dados/Empregados/glassdoor_consolidado_join_match_v2.csv")

  tags = var.common_tags
}

resource "aws_s3_object" "empregados_glassdoor_less" {
  bucket = module.s3.raw_bucket_name
  key    = "raw/Empregados/glassdoor_consolidado_join_match_less_v2.csv"
  source = "./Dados/Empregados/glassdoor_consolidado_join_match_less_v2.csv"
  etag   = filemd5("./Dados/Empregados/glassdoor_consolidado_join_match_less_v2.csv")

  tags = var.common_tags
}

# Optional: Create a data manifest for tracking uploaded files
resource "aws_s3_object" "data_manifest" {
  bucket = module.s3.raw_bucket_name
  key    = "manifest/data_manifest.json"
  content = jsonencode({
    upload_timestamp = timestamp()
    files = [
      "raw/Reclamacoes/2021_tri_01.csv",
      "raw/Reclamacoes/2021_tri_02.csv", 
      "raw/Reclamacoes/2021_tri_03.csv",
      "raw/Reclamacoes/2021_tri_04.csv",
      "raw/Reclamacoes/2022_tri_01.csv",
      "raw/Reclamacoes/2022_tri_03.csv",
      "raw/Reclamacoes/2022_tri_04.csv",
      "raw/Bancos/EnquadramentoInicia_v2.tsv",
      "raw/Empregados/glassdoor_consolidado_join_match_v2.csv",
      "raw/Empregados/glassdoor_consolidado_join_match_less_v2.csv"
    ]
    description = "Initial data load for ETL pipeline"
  })

  tags = merge(var.common_tags, {
    Type = "Manifest"
  })
}