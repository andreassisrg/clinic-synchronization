from concurrent import futures
import sys
import threading
import grpc
import clinic_pb2
import clinic_pb2_grpc
import office_pb2
import office_pb2_grpc
import mytimer


class OfficeServicer(office_pb2_grpc.OfficeServicer):
    def __init__(self, doctor_id):
        super().__init__()
        self.doctor_id = doctor_id

        # data the appointment thread hands to the main thread once the patient comes in
        self.current_patient_id = 0
        self.current_patient_problem = ''

        # one semaphore per signal between the appointment thread and the main thread
        self.door_open = threading.Semaphore(0)       # main -> appointment: doctor is ready
        self.patient_entered = threading.Semaphore(0)    # appointment -> main: patient data is stored
        self.consultation_done = threading.Semaphore(0)  # main -> appointment: sleep is over, patient may leave


    def Consultation(self, request, context):
        patient_id = request.id
        patient_problem = request.problem_description
        mytimer.tstamp_msg(f'CI({patient_id:4.1f},{patient_problem})')

        self.door_open.acquire()
        self.current_patient_id = patient_id
        self.current_patient_problem = patient_problem
        self.patient_entered.release()

        self.consultation_done.acquire()
        mytimer.tstamp_msg(f'CR({patient_id:4.1f},{self.doctor_id:4.1f})')
        return office_pb2.ConsultationResponse(id=self.doctor_id)


def serve():
    office_port = int(sys.argv[1])
    doctor_id = float(sys.argv[2])
    consultation_time = float(sys.argv[3])
    clinic_address = sys.argv[4]
    clinic_port = int(sys.argv[5])

    # start the office server before registering, otherwise a patient could arrive before it exists
    office = OfficeServicer(doctor_id)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    office_pb2_grpc.add_OfficeServicer_to_server(office, server)
    server.add_insecure_port(f'[::]:{office_port}')
    server.start()

    channel = grpc.insecure_channel(f'{clinic_address}:{clinic_port}')
    clinic_stub = clinic_pb2_grpc.ClinicStub(channel)

    try:
        register_doctor_response = clinic_stub.RegisterDoctor(clinic_pb2.RegisterDoctorRequest(
            id=doctor_id,
            port=office_port,
        ))
        registration_status = register_doctor_response.status
    except grpc.RpcError:
        # clinic is down: count it as a failed registration
        registration_status = -1

    mytimer.tstamp_msg(f'MI({doctor_id:4.1f}) x {consultation_time:3.1f} @ {office_port} = {registration_status}')

    while registration_status == 0:
        mytimer.tstamp_msg('MCPI')
        try:
            call_patient_response = clinic_stub.CallPatient(clinic_pb2.CallPatientRequest(
                id=doctor_id,
            ))
            patient_id = call_patient_response.id
        except grpc.RpcError:
            # the clinic closed while the doctor was seeing a patient, so the shift is over too
            patient_id = 0
        mytimer.tstamp_msg(f'MCPR -> {patient_id:4.1f}')

        if patient_id < 1:
            mytimer.tstamp_msg('MT')
            break

        office.door_open.release()
        office.patient_entered.acquire()
        mytimer.tstamp_msg(f'MAI({office.current_patient_id:4.1f},{office.current_patient_problem})')

        mytimer.mysleep(consultation_time)

        office.consultation_done.release()
        mytimer.tstamp_msg('MAF')

    channel.close()
    server.stop(grace=None).wait()


serve()
