# Clinic Synchronization

A small urgent-care clinic built with gRPC in Python. Doctors and patients meet at the clinic, which pairs them up, and the appointment then happens directly in the doctor's office. The project combines RPC with thread synchronization inside the servers, using only locks, condition variables and semaphores.

## How it works

There are four programs:

- `clinic.py`: the central server. It keeps the queue of patients seated in the waiting room and the queue of free doctors, and pairs them in order of arrival.
- `doctor.py`: starts the office server, registers with the clinic, and loops calling patients and seeing them.
- `patient.py`: asks the clinic for a doctor and, if one is found, goes to the doctor's office for the appointment.
- `end_of_shift.py`: calls the clinic's shutdown, which releases everyone still waiting and stops the server.

```
                    FindDoctor                   RegisterDoctor
    patient  ----------------------> clinic <------------------- doctor
      |                                          CallPatient        ^
      |                                                             |
      +----------------------- Consultation ------------------------+
                          (doctor's office server)
```

The interfaces are defined in `clinic.proto` and `office.proto`.

## Requirements

```bash
pip install grpcio grpcio-tools
```

## Running

First generate the stubs from the `.proto` files:

```bash
make stubs
```

The stubs (`*_pb2.py`, `*_pb2_grpc.py`, `*_pb2.pyi`) are not tracked in the repository. To delete them:

```bash
make clean
```

Arguments for each program:

| program | arguments |
| --- | --- |
| `clinic.py` | `chairs doctors port` |
| `doctor.py` | `office_port doctor_id appointment_time clinic_address clinic_port` |
| `patient.py` | `patient_id problem clinic_address clinic_port` |
| `end_of_shift.py` | `clinic_address clinic_port` |

Example with the doctor arriving first:

```bash
python3 clinic.py 2 2 9999 > clinic.out 2>&1 &
python3 doctor.py 9991 99.1 2 localhost 9999 > doctor1.out 2>&1 &
sleep 1
python3 patient.py 6.1 migraine localhost 9999 > patient1.out 2>&1 &
sleep 5
python3 end_of_shift.py localhost 9999 > end.out 2>&1
```

Clinic output for this run:

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

`%5B::1%5D` is `::1` (IPv6 localhost) in the form returned by gRPC's `context.peer()`. The patient uses this address as is to connect to the doctor's office.

All output goes through `tstamp_msg` in `mytimer.py`, which prefixes each line with the elapsed time in seconds. Nothing else is printed, so every program's log has a fixed format.
