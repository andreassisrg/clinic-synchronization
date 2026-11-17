import sys
import grpc
import ambulatorio_pb2
import ambulatorio_pb2_grpc
import mytimer


def main():
    endereco_ambulatorio = sys.argv[1]
    porta_ambulatorio = int(sys.argv[2])
    mytimer.tstamp_msg(f'FE({endereco_ambulatorio},{porta_ambulatorio})')

    channel_ambulatorio = grpc.insecure_channel(f'{endereco_ambulatorio}:{porta_ambulatorio}')
    stub_ambulatorio = ambulatorio_pb2_grpc.AmbulatorioStub(channel_ambulatorio)

    termino_response = stub_ambulatorio.Termino(ambulatorio_pb2.TerminoRequest())
    valor_retorno = termino_response.num_liberados
    channel_ambulatorio.close()

    mytimer.tstamp_msg(f'FE -> {valor_retorno}')


main()
