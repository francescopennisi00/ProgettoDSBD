FROM python:3

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY SLA_manager/SLA_manager_main.py .
CMD [ "python", "SLA_manager_main.py" ]