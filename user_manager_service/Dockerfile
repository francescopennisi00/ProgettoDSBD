FROM python:3

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY notifier_um_pb2.py .
COPY notifier_um_pb2.pyi .
COPY notifier_um_pb2_grpc.py .
COPY WMS_um_pb2.py .
COPY WMS_um_pb2.pyi .
COPY WMS_um_pb2_grpc.py .
COPY user_manager/user_manager_main.py .

CMD [ "python", "user_manager_main.py" ]