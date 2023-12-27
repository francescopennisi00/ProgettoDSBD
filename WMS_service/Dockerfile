FROM python:3

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY WMS_um_pb2.py .
COPY WMS_um_pb2.pyi .
COPY WMS_um_pb2_grpc.py .
COPY WMS/WMS_main.py .

CMD [ "python", "WMS_main.py" ]