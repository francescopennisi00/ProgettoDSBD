FROM python:3

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY notifier_um_pb2.py .
COPY notifier_um_pb2.pyi .
COPY notifier_um_pb2_grpc.py .
COPY notifier/user_manager_main.py .

ENV HOSTNAME="um_mysqlDB"
ENV PORT=3308
ENV USER="root"
ENV PASSWORD="toor"
ENV DATABASE="um"

CMD [ "python", "user_manager_main.py" ]