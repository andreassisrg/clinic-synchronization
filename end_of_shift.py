import sys
import grpc
import clinic_pb2
import clinic_pb2_grpc
import mytimer


def main():
    clinic_address = sys.argv[1]
    clinic_port = int(sys.argv[2])
    mytimer.tstamp_msg(f'FE({clinic_address},{clinic_port})')

    clinic_channel = grpc.insecure_channel(f'{clinic_address}:{clinic_port}')
    clinic_stub = clinic_pb2_grpc.ClinicStub(clinic_channel)

    terminate_response = clinic_stub.Terminate(clinic_pb2.TerminateRequest())
    return_value = terminate_response.num_released
    clinic_channel.close()

    mytimer.tstamp_msg(f'FE -> {return_value}')


main()
