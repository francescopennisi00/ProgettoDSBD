FROM python:3

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY notifier_um_pb2.py .
COPY notifier_um_pb2.pyi .
COPY notifier_um_pb2_grpc.py .
COPY notifier/notifier_main.py .

ENV APP_PASSWORD="ifph uxrh kjaf ylkt"
ENV HOSTNAME="notifier_mysqlDB"
ENV PORT=3306
ENV USER="root"
ENV PASSWORD="toor"
ENV DATABASE="notifier"
ENV EMAIL="noreplydsbd@gmail.com"

CMD [ "python", "notifier_main.py" ]