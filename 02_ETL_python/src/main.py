from pipeline.ingestao_raw import PipelineIngestaoRaw
from pipeline.transformacoes_trusted import TransformacoesTrusted
from pipeline.agregacoes_delivery import AgregacoesDelivery
from pipeline.popular import S3Uploader
def main():
    send = S3Uploader()
    send.enviar_arquivos()

    raw = PipelineIngestaoRaw()
    trusted = TransformacoesTrusted()
    delivery = AgregacoesDelivery()

    raw.executar()
    trusted.executar()
    delivery.executar()
if __name__ == "__main__":
    main()
