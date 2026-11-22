PYTHON = python3

all: stubs

stubs:
	$(PYTHON) -m grpc_tools.protoc -I. --python_out=. --pyi_out=. --grpc_python_out=. clinic.proto office.proto

clean:
	rm -f *_pb2.py *_pb2.pyi *_pb2_grpc.py
	rm -rf __pycache__

.PHONY: all stubs clean
