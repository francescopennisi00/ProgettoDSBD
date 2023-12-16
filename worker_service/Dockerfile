FROM python:3

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY worker/worker_main.py .

CMD [ "python", "worker_main.py" ]