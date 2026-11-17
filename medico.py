from concurrent import futures
import sys
import threading
import grpc
import ambulatorio_pb2
import ambulatorio_pb2_grpc
import consultorio_pb2
import consultorio_pb2_grpc
import mytimer


class ConsultorioServicer(consultorio_pb2_grpc.ConsultorioServicer):
    def __init__(self, id_medico):
        super().__init__()
        self.id_medico = id_medico

        # dados que a thread da consulta passa pra main depois que o paciente entra
        self.id_paciente_atual = 0
        self.problema_paciente_atual = ''

        # um semaforo pra cada sinal entre a thread da consulta e a main
        self.porta_aberta = threading.Semaphore(0)       # main -> consulta: medico pronto pra atender
        self.paciente_entrou = threading.Semaphore(0)    # consulta -> main: dados do paciente ja salvos
        self.consulta_terminou = threading.Semaphore(0)  # main -> consulta: acabou o sleep, pode ir embora


    def Consulta(self, request, context):
        id_paciente = request.tipo_ident
        problema_paciente = request.descricao_problema
        mytimer.tstamp_msg(f'CI({id_paciente:4.1f},{problema_paciente})')

        self.porta_aberta.acquire()
        self.id_paciente_atual = id_paciente
        self.problema_paciente_atual = problema_paciente
        self.paciente_entrou.release()

        self.consulta_terminou.acquire()
        mytimer.tstamp_msg(f'CR({id_paciente:4.1f},{self.id_medico:4.1f})')
        return consultorio_pb2.ConsultaResponse(tipo_ident=self.id_medico)


def serve():
    porta_consultorio = int(sys.argv[1])
    id_medico = float(sys.argv[2])
    tempo_consulta = float(sys.argv[3])
    endereco_ambulatorio = sys.argv[4]
    porta_ambulatorio = int(sys.argv[5])

    # sobe o consultorio antes de registrar, senao um paciente pode chegar antes do servidor existir
    consultorio = ConsultorioServicer(id_medico)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    consultorio_pb2_grpc.add_ConsultorioServicer_to_server(consultorio, server)
    server.add_insecure_port(f'[::]:{porta_consultorio}')
    server.start()

    channel = grpc.insecure_channel(f'{endereco_ambulatorio}:{porta_ambulatorio}')
    stub_ambulatorio = ambulatorio_pb2_grpc.AmbulatorioStub(channel)

    try:
        registra_medico_response = stub_ambulatorio.RegistraMedico(ambulatorio_pb2.RegistraMedicoRequest(
            tipo_ident=id_medico,
            tipo_porto=porta_consultorio,
        ))
        status_registro = registra_medico_response.status
    except grpc.RpcError:
        # ambulatorio fora do ar, conta como registro que falhou
        status_registro = -1

    mytimer.tstamp_msg(f'MI({id_medico:4.1f}) x {tempo_consulta:3.1f} @ {porta_consultorio} = {status_registro}')

    while status_registro == 0:
        mytimer.tstamp_msg('MCPI')
        try:
            chama_paciente_response = stub_ambulatorio.ChamaPaciente(ambulatorio_pb2.ChamaPacienteRequest(
                tipo_ident=id_medico,
            ))
            id_paciente = chama_paciente_response.tipo_ident
        except grpc.RpcError:
            # ambulatorio ja fechou enquanto o medico atendia, entao eh fim de expediente tambem
            id_paciente = 0
        mytimer.tstamp_msg(f'MCPR -> {id_paciente:4.1f}')

        if id_paciente < 1:
            mytimer.tstamp_msg('MT')
            break

        consultorio.porta_aberta.release()
        consultorio.paciente_entrou.acquire()
        mytimer.tstamp_msg(f'MAI({consultorio.id_paciente_atual:4.1f},{consultorio.problema_paciente_atual})')

        mytimer.mysleep(tempo_consulta)

        consultorio.consulta_terminou.release()
        mytimer.tstamp_msg('MAF')

    channel.close()
    server.stop(grace=None).wait()


serve()
