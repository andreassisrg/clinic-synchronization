from concurrent import futures
import sys
import threading
import grpc
import ambulatorio_pb2
import ambulatorio_pb2_grpc
import mytimer


class AmbulatorioServicer(ambulatorio_pb2_grpc.AmbulatorioServicer):
    def __init__(self, num_cadeiras, num_medicos, stop_event):
        super().__init__()
        self.num_cadeiras = num_cadeiras
        self.num_medicos = num_medicos
        self.stop_event = stop_event

        self.fila_pacientes = []
        self.fila_medicos = []
        self.medicos_registrados = dict()

        # um lock so pra filas, diretorio e contador, assim nao tem como dar deadlock entre locks
        self.lock = threading.Lock()
        self.chamadas_ativas = 0
        self.sem_chamadas_ativas = threading.Condition(self.lock)
        self.encerrado = False


    def ProcuraPorMedico(self, request, context):
        id_paciente = request.tipo_ident
        mytimer.tstamp_msg(f'APMI({id_paciente:4.1f})')

        with self.lock:
            self.chamadas_ativas += 1

            if self.encerrado:
                id_medico = 0
            elif len(self.fila_medicos) > 0:
                # tem medico esperando, completa o prontuario dele e acorda ele
                prontuario = self.fila_medicos.pop(0)
                prontuario['id_paciente'] = id_paciente
                prontuario['cond'].notify()
                id_medico = prontuario['id_medico']
            elif len(self.fila_pacientes) >= self.num_cadeiras:
                # sala cheia, paciente vai embora com medico 0
                id_medico = 0
            else:
                prontuario = novo_prontuario(id_paciente, None, self.lock)
                self.fila_pacientes.append(prontuario)
                mytimer.tstamp_msg(f'APME({id_paciente:4.1f})')
                # quem tira da fila eh quem completa o prontuario, aqui so espera o campo ser preenchido
                while prontuario['id_medico'] is None:
                    prontuario['cond'].wait()
                id_medico = prontuario['id_medico']

            # medico 0 nao ta no diretorio entao volta endereco vazio e porta 0
            endereco_medico, porta_medico = self.medicos_registrados.get(id_medico, ('', 0))
            mytimer.tstamp_msg(f'APMR({id_paciente:4.1f}) -> M({id_medico:4.1f}) @ {endereco_medico}:{porta_medico}')
            self.sai_chamada()

        return ambulatorio_pb2.ProcuraPorMedicoResponse(
            tipo_ender=endereco_medico,
            tipo_porto=porta_medico,
        )


    def RegistraMedico(self, request, context):
        id_medico = request.tipo_ident
        porta_medico = request.tipo_porto
        endereco_medico, _ = split_peer(context.peer())

        with self.lock:
            self.chamadas_ativas += 1

            if len(self.medicos_registrados) >= self.num_medicos:
                return_status = -1
            else:
                return_status = 0
                self.medicos_registrados[id_medico] = (endereco_medico, porta_medico)

            mytimer.tstamp_msg(f'ARM({id_medico:4.1f}) @ {endereco_medico}:{porta_medico} = {return_status}')
            self.sai_chamada()

        return ambulatorio_pb2.RegistraMedicoResponse(status=return_status)


    def ChamaPaciente(self, request, context):
        id_medico = request.tipo_ident
        mytimer.tstamp_msg(f'ACPI({id_medico:4.1f})')

        with self.lock:
            self.chamadas_ativas += 1

            if self.encerrado:
                id_paciente = 0
            elif len(self.fila_pacientes) > 0:
                prontuario = self.fila_pacientes.pop(0)
                prontuario['id_medico'] = id_medico
                prontuario['cond'].notify()
                id_paciente = prontuario['id_paciente']
            else:
                prontuario = novo_prontuario(None, id_medico, self.lock)
                self.fila_medicos.append(prontuario)
                mytimer.tstamp_msg(f'ACPE({id_medico:4.1f})')
                while prontuario['id_paciente'] is None:
                    prontuario['cond'].wait()
                id_paciente = prontuario['id_paciente']

        # agua do medico fica fora do lock pra nao travar o resto do ambulatorio
        # e no fim do expediente nao tem agua
        if id_paciente != 0:
            mytimer.mysleep(0.1)

        with self.lock:
            mytimer.tstamp_msg(f'ACPR({id_medico:4.1f}) -> P({id_paciente:4.1f})')
            self.sai_chamada()

        return ambulatorio_pb2.ChamaPacienteResponse(tipo_ident=id_paciente)


    def Termino(self, request, context):
        mytimer.tstamp_msg('ATEI')

        with self.lock:
            self.encerrado = True

            num_medicos_liberados = len(self.fila_medicos)
            for prontuario in self.fila_medicos:
                prontuario['id_paciente'] = 0
                prontuario['cond'].notify()
            self.fila_medicos.clear()

            num_pacientes_liberados = len(self.fila_pacientes)
            for prontuario in self.fila_pacientes:
                prontuario['id_medico'] = 0
                prontuario['cond'].notify()
            self.fila_pacientes.clear()

            # so segue quando todo mundo que tava dentro de uma chamada ja retornou
            while self.chamadas_ativas > 0:
                self.sem_chamadas_ativas.wait()

        # nunca tem medico e paciente esperando ao mesmo tempo, entao no maximo um dos dois eh != 0
        if num_medicos_liberados > 0:
            num_liberados = num_medicos_liberados
        else:
            num_liberados = -num_pacientes_liberados

        mytimer.tstamp_msg(f'ATER -> {num_liberados}')
        self.stop_event.set()
        return ambulatorio_pb2.TerminoResponse(num_liberados=num_liberados)


    def sai_chamada(self):
        # tem que chamar com o lock ja pego
        self.chamadas_ativas -= 1
        if self.chamadas_ativas == 0:
            self.sem_chamadas_ativas.notify_all()


def novo_prontuario(id_paciente, id_medico, lock):
    # cada prontuario tem sua condicao mas todas usam o lock das filas
    return {
        'id_paciente': id_paciente,
        'id_medico': id_medico,
        'cond': threading.Condition(lock),
    }


def split_peer(addr_str: str):
    # Remove gRPC's transport prefix, if present.
    if addr_str.startswith(("ipv4:", "ipv6:")):
        addr_str = addr_str.split(":", 1)[1]

    address, separator, port = addr_str.rpartition(":")
    # Remove brackets around an IPv6 address.
    if address.startswith("[") and address.endswith("]"):
        address = address[1:-1]

    return address, int(port)


def serve():
    num_cadeiras = int(sys.argv[1])
    num_medicos = int(sys.argv[2])
    porto_servidor = int(sys.argv[3])

    mytimer.tstamp_msg(f'AI({num_cadeiras},{num_medicos})@{porto_servidor}')

    stop_event = threading.Event()
    # cada paciente/medico esperando segura uma thread, entao sobra folga pro Termino e pros que chegam
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=num_cadeiras + num_medicos + 10))
    ambulatorio_pb2_grpc.add_AmbulatorioServicer_to_server(AmbulatorioServicer(
        num_cadeiras=num_cadeiras,
        num_medicos=num_medicos,
        stop_event=stop_event,
    ), server)
    server.add_insecure_port(f'[::]:{porto_servidor}')
    server.start()

    # Termino so seta o evento, quem desliga eh a main
    # grace de 1s pra resposta do Termino chegar no fim_de_expediente
    stop_event.wait()
    server.stop(grace=1).wait()


serve()
