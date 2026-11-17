# fspd-tp

Trabalho prático de Fundamentos de Sistemas Paralelos e Distribuídos (DCC/UFMG).

É um mini pronto atendimento feito com gRPC em Python. Os médicos e os pacientes se encontram no ambulatório, que faz o pareamento entre eles, e depois a consulta acontece direto no consultório do médico. O foco do trabalho é juntar RPC com sincronização de threads dentro dos servidores, usando só lock, variável de condição e semáforo.

## Como funciona

São quatro programas:

- `ambulatorio.py`: servidor central. Guarda a fila de pacientes sentados na ante-sala e a fila de médicos livres, e junta um com o outro na ordem de chegada
- `medico.py`: sobe o servidor do consultório, se registra no ambulatório e fica num loop chamando paciente e atendendo
- `paciente.py`: procura um médico no ambulatório e, se achar, vai no consultório fazer a consulta
- `fim_de_expediente.py`: só chama o término no ambulatório, que libera quem tava esperando e desliga o servidor

```
                 ProcuraPorMedico                  RegistraMedico
   paciente  ----------------------> ambulatorio <---------------- medico
      |                                            ChamaPaciente    ^
      |                                                             |
      +------------------------- Consulta --------------------------+
                          (servidor do consultorio)
```

As interfaces ficam em `ambulatorio.proto` e `consultorio.proto`.

## Requisitos

Basta usar o venv:

```bash
source fspd-tp-venv/bin/activate
```

## Rodando

Primeiro gera os stubs a partir dos `.proto`:

```bash
make stubs
```

Os stubs (`*_pb2.py`, `*_pb2_grpc.py`, `*_pb2.pyi`) não ficam no repositório, é só rodar `make stubs`.

Pra apagar os arquivos gerados e deixar só o que é da entrega:

```bash
make clean
```

Argumentos de cada programa:

| programa | argumentos |
| --- | --- |
| `ambulatorio.py` | `cadeiras medicos porta` |
| `medico.py` | `porta_consultorio id_medico tempo_consulta endereco_ambulatorio porta_ambulatorio` |
| `paciente.py` | `id_paciente problema endereco_ambulatorio porta_ambulatorio` |
| `fim_de_expediente.py` | `endereco_ambulatorio porta_ambulatorio` |

Exemplo com o médico chegando primeiro (é o caso 1 do enunciado):

```bash
python3 ambulatorio.py 2 2 9999 > amb.out 2>&1 &
python3 medico.py 9991 99.1 2 localhost 9999 > med1.out 2>&1 &
sleep 1
python3 paciente.py 6.1 enxaqueca localhost 9999 > pac1.out 2>&1 &
sleep 5
python3 fim_de_expediente.py localhost 9999 > fim.out 2>&1
```

Saída do ambulatório nesse caso:

```
  0.0: mytime started at Tue Nov 17 09:28:10 2026
  0.0: AI(2,2)@9999
  0.0: ARM(99.1) @ %5B::1%5D:9991 = 0
  0.0: ACPI(99.1)
  0.0: ACPE(99.1)
  1.0: APMI( 6.1)
  1.0: APMR( 6.1) -> M(99.1) @ %5B::1%5D:9991
  1.1: ACPR(99.1) -> P( 6.1)
  3.1: ACPI(99.1)
  3.1: ACPE(99.1)
  6.1: ATEI
  6.1: ACPR(99.1) -> P( 0.0)
  6.1: ATER -> 1
```

O `%5B::1%5D` é o `::1` (localhost em IPv6) do jeito que o `context.peer()` do gRPC devolve. O paciente usa esse endereço direto pra conectar no consultório e funciona normal.

Toda saída passa pelo `tstamp_msg` do `mytimer.py`, que é o módulo de tempo fornecido pelo professor. Nenhum outro print é feito, porque a correção é automática e compara stdout e stderr.
