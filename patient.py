import sys
import grpc
import clinic_pb2
import clinic_pb2_grpc
import office_pb2
import office_pb2_grpc
import mytimer


def main():
    patient_id = float(sys.argv[1])
    patient_problem = sys.argv[2]
    clinic_address = sys.argv[3]
    clinic_port = int(sys.argv[4])

    mytimer.tstamp_msg(f'PI({patient_id:4.1f},{patient_problem})')

    clinic_channel = grpc.insecure_channel(f'{clinic_address}:{clinic_port}')
    clinic_stub = clinic_pb2_grpc.ClinicStub(clinic_channel)

    try:
        find_doctor_response = clinic_stub.FindDoctor(
            clinic_pb2.FindDoctorRequest(id=patient_id)
        )
        doctor_address = find_doctor_response.address
        doctor_port = find_doctor_response.port
    except grpc.RpcError:
        # clinic is closed: same as no doctor available
        doctor_address = ''
        doctor_port = 0
    clinic_channel.close()

    mytimer.tstamp_msg(f'PPMR({doctor_address}:{doctor_port})')

    # port 0 means a full waiting room or the end of the shift: leave
    if doctor_port == 0:
        return

    # the address comes from the clinic's peer(), e.g. %5B::1%5D, and gRPC accepts it as is
    office_channel = grpc.insecure_channel(f'{doctor_address}:{doctor_port}')
    office_stub = office_pb2_grpc.OfficeStub(office_channel)

    consultation_response = office_stub.Consultation(office_pb2.ConsultationRequest(
        id=patient_id,
        problem_description=patient_problem,
    ))
    doctor_id = consultation_response.id
    office_channel.close()

    mytimer.tstamp_msg(f'PT -> {doctor_id:4.1f}')


main()
