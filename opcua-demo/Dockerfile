FROM python:3.11-slim

# Crear directorio de trabajo
WORKDIR /app

# Instalar dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el script
COPY opcua-server.py .

# Puerto OPC-UA
EXPOSE 4840

# Ejecutar el servidor
CMD ["python", "opcua-server.py"]
