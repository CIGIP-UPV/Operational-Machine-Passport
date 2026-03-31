FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY opcua_demo ./opcua_demo
COPY config ./config
COPY opcua-server.py .

EXPOSE 4840

CMD ["python", "-m", "opcua_demo.app"]
