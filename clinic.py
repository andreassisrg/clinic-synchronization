from concurrent import futures
import sys
import threading
import grpc
import clinic_pb2
import clinic_pb2_grpc
import mytimer


class ClinicServicer(clinic_pb2_grpc.ClinicServicer):
    def __init__(self, num_chairs, num_doctors, stop_event):
        super().__init__()
        self.num_chairs = num_chairs
        self.num_doctors = num_doctors
        self.stop_event = stop_event

        self.patient_queue = []
        self.doctor_queue = []
        self.registered_doctors = dict()

        # a single lock guards the queues, the directory and the counter, so locks can never deadlock each other
        self.lock = threading.Lock()
        self.active_calls = 0
        self.no_active_calls = threading.Condition(self.lock)
        self.closed = False


    def FindDoctor(self, request, context):
        patient_id = request.id
        mytimer.tstamp_msg(f'APMI({patient_id:4.1f})')

        with self.lock:
            self.active_calls += 1

            if self.closed:
                doctor_id = 0
            elif len(self.doctor_queue) > 0:
                # a doctor is waiting: fill in their record and wake them up
                record = self.doctor_queue.pop(0)
                record['patient_id'] = patient_id
                record['cond'].notify()
                doctor_id = record['doctor_id']
            elif len(self.patient_queue) >= self.num_chairs:
                # waiting room is full: the patient leaves with doctor 0
                doctor_id = 0
            else:
                record = new_record(patient_id, None, self.lock)
                self.patient_queue.append(record)
                mytimer.tstamp_msg(f'APME({patient_id:4.1f})')
                # whoever dequeues the record fills it in; here we only wait for the field to be set
                while record['doctor_id'] is None:
                    record['cond'].wait()
                doctor_id = record['doctor_id']

            # doctor 0 is not in the directory, so this returns an empty address and port 0
            doctor_address, doctor_port = self.registered_doctors.get(doctor_id, ('', 0))
            mytimer.tstamp_msg(f'APMR({patient_id:4.1f}) -> M({doctor_id:4.1f}) @ {doctor_address}:{doctor_port}')
            self.leave_call()

        return clinic_pb2.FindDoctorResponse(
            address=doctor_address,
            port=doctor_port,
        )


    def RegisterDoctor(self, request, context):
        doctor_id = request.id
        doctor_port = request.port
        doctor_address, _ = split_peer(context.peer())

        with self.lock:
            self.active_calls += 1

            if len(self.registered_doctors) >= self.num_doctors:
                return_status = -1
            else:
                return_status = 0
                self.registered_doctors[doctor_id] = (doctor_address, doctor_port)

            mytimer.tstamp_msg(f'ARM({doctor_id:4.1f}) @ {doctor_address}:{doctor_port} = {return_status}')
            self.leave_call()

        return clinic_pb2.RegisterDoctorResponse(status=return_status)


    def CallPatient(self, request, context):
        doctor_id = request.id
        mytimer.tstamp_msg(f'ACPI({doctor_id:4.1f})')

        with self.lock:
            self.active_calls += 1

            if self.closed:
                patient_id = 0
            elif len(self.patient_queue) > 0:
                record = self.patient_queue.pop(0)
                record['doctor_id'] = doctor_id
                record['cond'].notify()
                patient_id = record['patient_id']
            else:
                record = new_record(None, doctor_id, self.lock)
                self.doctor_queue.append(record)
                mytimer.tstamp_msg(f'ACPE({doctor_id:4.1f})')
                while record['patient_id'] is None:
                    record['cond'].wait()
                patient_id = record['patient_id']

        # the doctor's water break happens outside the lock so the rest of the clinic is not blocked;
        # there is no water break at the end of the shift
        if patient_id != 0:
            mytimer.mysleep(0.1)

        with self.lock:
            mytimer.tstamp_msg(f'ACPR({doctor_id:4.1f}) -> P({patient_id:4.1f})')
            self.leave_call()

        return clinic_pb2.CallPatientResponse(id=patient_id)


    def Terminate(self, request, context):
        mytimer.tstamp_msg('ATEI')

        with self.lock:
            self.closed = True

            num_doctors_released = len(self.doctor_queue)
            for record in self.doctor_queue:
                record['patient_id'] = 0
                record['cond'].notify()
            self.doctor_queue.clear()

            num_patients_released = len(self.patient_queue)
            for record in self.patient_queue:
                record['doctor_id'] = 0
                record['cond'].notify()
            self.patient_queue.clear()

            # only continue once every call in progress has returned
            while self.active_calls > 0:
                self.no_active_calls.wait()

        # doctors and patients never wait at the same time, so at most one of the two is != 0
        if num_doctors_released > 0:
            num_released = num_doctors_released
        else:
            num_released = -num_patients_released

        mytimer.tstamp_msg(f'ATER -> {num_released}')
        self.stop_event.set()
        return clinic_pb2.TerminateResponse(num_released=num_released)


    def leave_call(self):
        # must be called with the lock held
        self.active_calls -= 1
        if self.active_calls == 0:
            self.no_active_calls.notify_all()


def new_record(patient_id, doctor_id, lock):
    # each record has its own condition, but they all share the queues' lock
    return {
        'patient_id': patient_id,
        'doctor_id': doctor_id,
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
    num_chairs = int(sys.argv[1])
    num_doctors = int(sys.argv[2])
    server_port = int(sys.argv[3])

    mytimer.tstamp_msg(f'AI({num_chairs},{num_doctors})@{server_port}')

    stop_event = threading.Event()
    # each waiting patient/doctor holds a thread, so leave headroom for Terminate and new arrivals
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=num_chairs + num_doctors + 10))
    clinic_pb2_grpc.add_ClinicServicer_to_server(ClinicServicer(
        num_chairs=num_chairs,
        num_doctors=num_doctors,
        stop_event=stop_event,
    ), server)
    server.add_insecure_port(f'[::]:{server_port}')
    server.start()

    # Terminate only sets the event; the main thread shuts the server down,
    # with a 1 s grace period so Terminate's response reaches end_of_shift
    stop_event.wait()
    server.stop(grace=1).wait()


serve()
