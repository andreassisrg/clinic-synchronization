import sys
import grpc
import ambulatorio_pb2
import ambulatorio_pb2_grpc
import consultorio_pb2
import consultorio_pb2_grpc
import mytimer


def main():
    id_paciente = float(sys.argv[1])
    problema_paciente = sys.argv[2]
    endereco_ambulatorio = sys.argv[3]
    porta_ambulatorio = int(sys.argv[4])

    mytimer.tstamp_msg(f'PI({id_paciente:4.1f},{problema_paciente})')

    channel_ambulatorio = grpc.insecure_channel(f'{endereco_ambulatorio}:{porta_ambulatorio}')
    stub_ambulatorio = ambulatorio_pb2_grpc.AmbulatorioStub(channel_ambulatorio)

    try:
        procura_por_medico_response = stub_ambulatorio.ProcuraPorMedico(
            ambulatorio_pb2.ProcuraPorMedicoRequest(tipo_ident=id_paciente)
        )
        endereco_medico = procura_por_medico_response.tipo_ender
        porto_medico = procura_por_medico_response.tipo_porto
    except grpc.RpcError:
        # clinic is closed: same as no doctor available
        endereco_medico = ''
        porto_medico = 0
    channel_ambulatorio.close()

    mytimer.tstamp_msg(f'PPMR({endereco_medico}:{porto_medico})')

    # port 0 means a full waiting room or the end of the shift: leave
    if porto_medico == 0:
        return

    # the address comes from the clinic's peer(), e.g. %5B::1%5D, and gRPC accepts it as is
    channel_medico = grpc.insecure_channel(f'{endereco_medico}:{porto_medico}')
    stub_medico = consultorio_pb2_grpc.ConsultorioStub(channel_medico)

    consulta_response = stub_medico.Consulta(consultorio_pb2.ConsultaRequest(
        tipo_ident=id_paciente,
        descricao_problema=problema_paciente,
    ))
    id_medico = consulta_response.tipo_ident
    channel_medico.close()

    mytimer.tstamp_msg(f'PT -> {id_medico:4.1f}')


main()
